from flask import Flask, request, render_template, jsonify
from py2neo import authenticate, Graph
import random

app = Flask(__name__, static_url_path='/static')
graph = None
request_content = None


def get_polys(entity_name):
    cypher = 'MATCH (n{`name`:"%s"}) RETURN n' % entity_name
    results = graph.data(cypher)
    polys = []
    for i, result in enumerate(results):
        entity = result['n']
        if i >= 1 and entity['poly'] == "":
            continue
        polys.append({'poly_name': entity['title'], 'uid': entity['uid']})
    return polys


def chunking(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/query/extend/page/", methods=['GET', 'POST'])
def request_page():
    page = request.get_json(force=True)['page']
    return jsonify({'data': request_content[page]})


@app.route("/query/extend/entity_card/", methods=['GET', 'POST'])
def entity_card():
    query = request.get_json(force=True)
    entity_name = query["entity"]
    cypher = 'MATCH (n{`name`:"%s"}) RETURN n' % entity_name
    results = graph.data(cypher)
    polys = []
    i = 0
    for result in results:
        entity = result['n']
        if entity['from'] == 'text':
            continue

        if i >= 1 and entity['title'] == "":
            continue
        title = entity['title']
        if entity['from'] == 'hyper_link':
            title = '<strong>'+entity['title']+'</strong>'
        polys.append({'poly_name': title, 'uid': entity['uid'], 'key': 'entity'})
        i += 1
    return jsonify({'query': query, 'polys': polys})


@app.route("/query/extend/entity_card/confirm/", methods=['GET', 'POST'])
def entity_card_confirm():
    query = request.get_json(force=True)
    entity_uid = query['entity']
    cypher = 'MATCH (n{`uid`:"%s"}) RETURN n' % entity_uid
    response_data = {}
    node = graph.data(cypher)[0]['n']
    # Relations
    rels = graph.match(node)
    for rel in rels:
        end_node = rel.end_node()
        rel_type = rel.type()
        if rel_type not in response_data.keys():
            response_data[rel_type] = []
        response_data[rel_type].append(end_node['name'])
    for key, value in response_data.items():
        response_data[key] = '、'.join(value)
    # Properties
    pros = dict(node)
    for key, value in pros.items():
        if type(value) == list:
            value = ",".join(value)
        response_data[key] = value
    return jsonify({'pages': 0, 'data': response_data})


@app.route("/query/extend/rel_eg/", methods=['GET', 'POST'])
def rel_eg():
    query = request.get_json(force=True)
    rel_name = query['rel_name']
    cypher = 'MATCH p = (h)-[r:`%s`]->(t) RETURN p' % rel_name
    results = graph.data(cypher)
    response_data = [{'h': p['p'].start_node()['name'], 't': p['p'].end_node()['name']} for p in results]
    global request_content
    request_content = list(chunking(response_data, 10))
    if request_content:
        data = request_content[0]
    else:
        data = []
    return jsonify({'pages': len(request_content), 'data': data})


@app.route("/query/extend/entity_retrieval/", methods=['GET', 'POST'])
def entity_retrieval():
    query = request.get_json(force=True)
    entity_name = query["entity"]
    cypher = 'MATCH (n{`name`:"%s"}) RETURN n' % entity_name
    results = graph.data(cypher)
    polys = []
    i = 1
    for result in results:
        entity = result['n']
        if entity['title'] == "":
            title = entity_name+'('+str(i)+')'
            i += 1
        else:
            title = entity['title']
        if entity['from'] == 'hyper_link':
            title = '<strong>'+title+'</strong>'
        polys.append({'poly_name': title, 'uid': entity['uid'], 'key': 'entity'})
    return jsonify({'query': query, 'polys': polys})


@app.route("/query/extend/entity_retrieval/confirm/", methods=['GET', 'POST'])
def entity_retrieval_confirm():
    query = request.get_json(force=True)
    entity_uid = query['entity']
    rel = query['rel']
    mode = query['mode']
    if mode == 'tail':
        cypher = 'MATCH (h{`uid`:"%s"})-[r:`%s`]->(e) RETURN e' % (entity_uid, rel)
    else:
        cypher = 'MATCH (e)-[r:`%s`]->(t{`uid`:"%s"}) RETURN e' % (rel, entity_uid)
    results = graph.data(cypher)
    response_data = [{mode: entity['e']['name']} for entity in results]
    global request_content
    request_content = list(chunking(response_data, 10))
    if request_content:
        data = request_content[0]
    else:
        data = []
    return jsonify({'pages': len(request_content), 'data': data})


@app.route("/query/extend/connect/", methods=['GET', 'POST'])
def connect():
    query = request.get_json(force=True)
    head = query['head']
    tail = query['tail']
    cypher = 'MATCH (n{`name`:"%s"}) RETURN n'
    head_results = graph.data(cypher % head)
    tail_results = graph.data(cypher % tail)
    polys = {'head': [], 'tail': []}
    i = 1
    for result in head_results:
        entity = result['n']
        if entity['title'] == "":
            title = head+'('+str(i)+')'
            i += 1
        else:
            title = entity['title']
        if entity['from'] == 'hyper_link':
            title = '<strong>' + title + '</strong>'
        polys['head'].append({'poly_name': title, 'uid': entity['uid'], 'key': 'head'})

    i = 1
    for result in tail_results:
        entity = result['n']
        if entity['title'] == "":
            title = tail+'('+str(i)+')'
            i += 1
        else:
            title = entity['title']
        if entity['from'] == 'hyper_link':
            title = '<strong>'+title+'</strong>'
        polys['tail'].append({'poly_name': title, 'uid': entity['uid'], 'key': 'tail'})
    return jsonify({'query': query, 'polys': polys})


@app.route("/query/extend/connect/confirm/", methods=['GET', 'POST'])
def connect_confirm():
    query = request.get_json(force=True)
    head = query['head']
    tail = query['tail']
    path_len = query['len']
    cypher = 'MATCH p = (h{`uid`:"%s"})-[r*1..%s]-(t{`uid`:"%s"}) RETURN p' % (head, path_len, tail)
    results = graph.data(cypher)
    response_data = []
    minus = '<i class="fa fa-minus" aria-hidden="true"></i>'
    arrow_rht = '<i class="fa fa-arrow-right" aria-hidden="true"></i>'
    arrow_lft = '<i class="fa fa-arrow-left" aria-hidden="true"></i>'
    badge_p_start = '<h5><span class="badge badge-pill badge-primary">'
    badge_p_end = '</span></h5>'
    badge_s_end = '</span>'
    badge_s_start = '<span class="badge badge-success">'
    for result in results:
        path = ""
        start_node = result['p'].start_node()
        rels = result['p'].relationships()
        path += badge_p_start + start_node['name'] + badge_p_end
        pre = start_node
        for rel in rels:
            if rel.start_node() == pre:
                path += minus + badge_s_start + rel.type() + badge_s_end + arrow_rht + \
                        badge_p_start + rel.end_node()['name'] + badge_p_end
                pre = rel.end_node()
            else:
                path += arrow_lft + badge_s_start + rel.type() + badge_s_end + minus + \
                        badge_p_start + rel.start_node()['name'] + badge_p_end
                pre = rel.start_node()
        response_data.append(path)
    global request_content
    request_content = list(chunking(response_data, 10))
    if request_content:
        data = request_content[0]
    else:
        data = []
    return jsonify({'pages': len(request_content), 'data': data})


@app.route("/query/entity/", methods=['GET', 'POST'])
def entity_query():
    query = request.get_json(force=True)
    entity = query['entity']
    cypher = 'MATCH (n{`name`:"%s"}) RETURN n' % entity
    results = graph.data(cypher)
    polys = []
    i = 1
    for result in results:
        entity = result['n']
        if entity['title'] == "":
            title = entity['name']+'('+str(i)+')'
            i += 1
        else:
            title = entity['title']
        if entity['from'] == 'hyper_link':
            title = '<strong>'+title+'</strong>'
        polys.append({'poly_name': title, 'uid': entity['uid'], 'key': 'entity'})
    return jsonify({'query': query, 'polys': polys})


@app.route("/query/entity/confirm/", methods=['GET', 'POST'])
def entity_query_confirm():
    query = request.get_json(force=True)
    entity = query['entity']
    cypher = 'MATCH p = (h{`uid`:"%s"})-[r*1..2]-(t) RETURN p' % entity
    results = graph.data(cypher)
    nodes = []
    edges = []
    nodes_add = set()
    for i, result in enumerate(results):
        path = result['p']
        entities = path.nodes()
        relationships = path.relationships()
        for node in entities:
            if node['uid'] in nodes_add:
                continue
            nodes_add.add(node['uid'])
            data = {}
            # Properties
            pros = dict(node)
            for key, value in pros.items():
                if key in ['name', 'title', 'uid', 'from']:
                    continue
                if type(value) == list:
                    value = ",".join(value)
                data[key] = value

            if node['uid'] == entity:
                size = random.randrange(20, 30)
                color = "#05e1dc"
            else:
                size = random.randrange(5, 10)
                color = random.choice(['#49e187', '#83e161', '#bde1cd'])
            if node['from'] == 'text':
                color = '#FF4438'
            nodes.append({
                'id': node['uid'],
                'label': node['name'],
                'x': random.random(),
                'y': random.random(),
                'size': size,
                'color': color,
                'data': data
            })

        for j, r in enumerate(relationships):
            edges.append({'id': str(i) + str(j),
                          'label': r.type(),
                          'source': r.start_node()['uid'],
                          'target': r.end_node()['uid'],
                          'type': 'arrow',
                          'size': 1,
                          'color': "#ccc"})
    return jsonify({'pages': 0, 'data': {'nodes': nodes, 'edges': edges, 'prop': []}})


if __name__ == "__main__":
    authenticate("localhost:7474", "neo4j", "admin")
    graph = Graph("http://localhost:7474/db/data/")
    app.run()
