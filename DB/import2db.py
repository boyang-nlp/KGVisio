from json import dumps, loads
from bs4 import BeautifulSoup
from py2neo import Graph
from tqdm import tqdm

def normalize_url(url):
    # DONE
    after_item = url.find('item/')+5
    remain_url = url[after_item:].split('/')
    name = remain_url[0]
    if len(remain_url) > 1:
        i = 0
        while i < len(remain_url[1]) and remain_url[1][i].isdigit():
            i += 1
        distinct = remain_url[1][:i]
    else:
        distinct = ''
    if distinct:
        return '/'.join(['http:/', 'baike.baidu.com', 'item', name, distinct])
    else:
        return '/'.join(['http:/', 'baike.baidu.com', 'item', name])


def parse_infovalue(value):
    value = '<html><body>' + value.replace('"', '\'').replace('\\', '\\\\') + '</html></body>'
    soup = BeautifulSoup(value, 'lxml')
    html_tags = soup.find_all()[2:]
    for x in html_tags:
        if not ('href' in x.attrs and '/item' in x['href']):
            x.extract()
    html_tags = soup.find_all()[2:]
    if html_tags:
        return True, html_tags
    else:
        return False, soup.text.split('\n')


def parse_infobox(infobox):
    attrs = []
    rels = []
    for info in infobox:
        info_name = info['info_name']
        info_value = info['info_value']
        if not info_name:
            continue
        info_name = '`'+info_name+'`'  # add backticks to avoid clash of identifiers
        result = parse_infovalue(info_value)
        if result[0]:
            for ele in result[1]:
                if not info_name:
                    continue
                rels.append((info_name, ele))
        else:
            if not info_name:
                continue
            attrs.append((info_name, result[1]))
    return attrs, rels


def parse_tag(tags):
    return list(map(lambda tag: '`'+tag+'`', tags))


def parse_entity(crawled_json):
    entity = {}
    entity['name'] = crawled_json['baike_title']
    entity['url'] = normalize_url(crawled_json['baike_url'])
    entity['tag'] = parse_tag(crawled_json['baike_tags'])
    entity['poly'] = crawled_json['poly']
    entity['attr'], entity['rels'] = parse_infobox(crawled_json['baike_info'])
    return entity


def parse_json_file(json_file):
    with open(json_file, encoding='utf-8') as f:
        nodes = [parse_entity(loads(line)) for line in f]
    return nodes


def make_CREATE(node):
    title = ''
    for tag in node['tag']:
        if tag == '``':
            continue
        title += ':' + tag
    attr_str = []
    for attr in node['attr']:
        value = '['+','.join(map(lambda val: '"' + val + '"', attr[1]))+']'
        attr_str.append("%s:%s" % (attr[0], value))
    attr_str.append('url:"%s"' % node['url'])
    attr_str.append('name:"%s"' % node['name'])
    attr_str.append('poly:"%s"' % node['poly'])
    attr_str = '{'+','.join(attr_str)+'}'
    return 'CREATE (n%s%s)' % (title, attr_str)


def make_MERGE(node):
    title = ''
    for tag in node['tag']:
        if tag == '``':
            continue
        title += ':' + tag
    attr_str = []
    for attr in node['attr']:
        value = '['+','.join(map(lambda val: '"' + val + '"', attr[1]))+']'
        attr_str.append("%s:%s" % (attr[0], value))
    attr_str.append('url:"%s"' % node['url'])
    attr_str.append('name:"%s"' % node['name'])
    attr_str = '{'+','.join(attr_str)+'}'
    return 'MERGE (n%s%s)' % (title, attr_str)


def import2neo(nodes):
    graph = Graph(password="admin")
    # Greate Node
    print('Write nodes to Neo4j...\n')
    for node in tqdm(nodes):
        if not graph.data('MATCH (n{url:"%s"}) RETURN n' % node['url']):
            graph.data(make_CREATE(node))


def connect_nodes(nodes):
    graph = Graph(password='admin')
    print('Connect nodes to Neo4j...\n')
    for node in tqdm(nodes):
        rels = node['rels']
        for rel, tail_entity_link in rels:
            link = 'http://baike.baidu.com' + tail_entity_link.attrs['href']
            tail_entity = graph.data('MATCH (n{url:"%s"})RETURN n' % link)
            if tail_entity:
                if not graph.data('MATCH (h{url:"%s"})-[r:%s]->(t{url:"%s"}) RETURN r' % (node['url'], rel, link)):
                    graph.data('MATCH (h{url:"%s"}),(t{url:"%s"}) CREATE (h)-[r:%s]->(t)' % (node['url'], link, rel))

if __name__ == '__main__':
    import2neo(parse_json_file('term.json'))
    connect_nodes(parse_json_file('term.json'))
