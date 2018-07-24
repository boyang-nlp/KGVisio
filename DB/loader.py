import unicodedata
from py2neo import authenticate, Graph
from tqdm import tqdm, tqdm_notebook
from bs4 import BeautifulSoup
import urllib
import requests


def collect_entity(rel_file, attr_file, title_file):
    entity_set = set()
    with open(rel_file, encoding='utf-8') as f:
        for line in f:
            line = unicodedata.normalize('NFKC', line)
            h, r, t = line[:-1].split("\t")
            entity_set.add(h)
            entity_set.add(t)
    with open(attr_file, encoding='utf-8') as f:
        for line in f:
            line = unicodedata.normalize('NFKC', line)
            h, a, av = line[:-1].split("\t")
            entity_set.add(h)
    with open(title_file, encoding='utf-8') as f:
        for line in f:
            line = unicodedata.normalize('NFKC', line)
            h, t = line[:-1].split("\t")
            entity_set.add(h)
    return entity_set


def bind_attr(entity_set, attr_file, title_file):
    entities = {}
    for entity in entity_set:
        entities[entity] = {}
    with open(attr_file, encoding='utf-8') as f:
        for line in f:
            line = unicodedata.normalize('NFKC', line)
            h, a, av = line[:-1].split("\t")
            if not a in entities[h]:
                entities[h][a] = []
            entities[h][a].append(av)
    for entity in entities:
        for a in entities[entity]:
            if type(entities[entity][a]) == list:
                entities[entity][a] = '、'.join(entities[entity][a])
    with open(title_file, encoding='utf-8') as f:
        for line in f:
            line = unicodedata.normalize('NFKC', line)
            h, t = line[:-1].split("\t")
            entities[h]['title'] = t
    for entity_uid in tqdm_notebook(entities):
        entities[entity_uid]['uid'] = entity_uid
        partitions = entity_uid.find('/')
        if 'text' in entity_uid[:partitions]:
            entities[entity_uid]['name'] = entity_uid[partitions+1:]
            entities[entity_uid]['from'] = 'text'
        else:
            if partitions == -1:
                entities[entity_uid]['name'] = entity_uid
            else:
                entities[entity_uid]['name'] = entity_uid[:partitions]
            entities[entity_uid]['from'] = 'hyper_link'
        if not 'title' in entities[entity_uid]:
            entities[entity_uid]['title'] = ''
            # Entity without 'title'
            """
            r = requests.get('http://baike.baidu.com/item/'+entity_uid)
            soup = BeautifulSoup(r.content, "lxml")
            title = soup.title.string
            if '（' in title and '）' in title:
                start = title.find('（')
                end = title.find('）')
                entities[entity_uid]['title'] = title[start + 1:end]
            """
    return [entities[entity_uid] for entity_uid in entities]


def make_CREATE(entity):
    attr_str = []
    for attr in entity:
        attr_value = entity[attr].replace('\\', '\\\\').replace('"', '\\"')
        attr_str.append('`%s`:"%s"' % (attr, attr_value))
    return 'CREATE (n{%s})' % ','.join(attr_str)


def write_entities(entities):
    authenticate("localhost:7474", "neo4j", "admin")
    graph = Graph("http://localhost:7474/db/data/")
    print('Write entities to db...\n')
    for entity in tqdm_notebook(entities):
        graph.data(make_CREATE(entity))


def connect_entities(rel_file):
    authenticate("localhost:7474", "neo4j", "admin")
    graph = Graph("http://localhost:7474/db/data/")
    with open(rel_file, encoding='utf-8') as f:
        lines = [line for line in f]
        for line in tqdm_notebook(lines):
            line = unicodedata.normalize('NFKC', line)
            line = line.replace('\\', '\\\\').replace('"', '\\"')
            h, r, t = line[:-1].split("\t")
            if not graph.data('MATCH (h{`uid`:"%s"})-[r:%s]->(t{`uid`:"%s"}) RETURN r' % (h, r, t)):
                graph.data('MATCH (h{`uid`:"%s"}),(t{`uid`:"%s"}) CREATE (h)-[r:%s]->(t)' % (h, t, r))


