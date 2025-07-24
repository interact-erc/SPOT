from neo4j import GraphDatabase
from neo4j.exceptions import SessionError
from graph import Node, Relation, Graph

CYPHER_DATATYPES = {
    "INTEGER": "toInteger",
    "FLOAT": "toFloat",
    "BOOLEAN": "toBoolean",
    "DATE": "toDate",
    "TIME": "toTime",
    "DATETIME": "toDateTime",
    "LOCALDATETIME": "toLocalDateTime",
    "DURATION": "toDuration",
    "UUID": "toUUID",
    "POINT": "toPoint",
}

CYPHER_MODIFIER_NUMBER_TYPES = {
    "INTEGER": "",
    "FLOAT": "",
    "DURATION": "",
}

CYPHER_MODIFIER_DATE_TYPES = {
    "DATE": "",
    "TIME": "",
    "DATETIME": "",
    "LOCALDATETIME": "",
}


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None):
        with self.driver.session() as session:
            try:
                with session.begin_transaction(timeout=10) as tx:
                    result = tx.run(query, parameters)
                    return [record for record in result]
            except SessionError as e:
                return []


def get_node(nodes, id):
    for n in nodes:
        if n.id == id:
            return n


def build_cypher_graph(graph, entity_mapping):
    gr = Graph()
    gr.entity_mapping = entity_mapping
    gr.return_attribute = graph["return_attribute"]
    gr.return_property = graph["return_property"]
    gr.nodes = []
    for node in graph["nodes"]:
        n = Node(node["id"])
        n.is_answer_node = node["is_answer_node"]
        n.grounded_entity = node["grounded_entity"]
        if n.grounded_entity == "None":
            n.grounded_entity = None
        n.attribute = node["attribute"]
        n.class_type = node["class_type"]
        n.modifier_edge = node["modifier_edge"]
        n.modifier = node["modifier"]
        n.datatype = node["datatype"]
        gr.nodes.append(n)
    gr.answer_node = gr.nodes[0]
    for relation in graph["relations"]:
        in_node = get_node(gr.nodes, relation["in_node_id"])
        out_node = get_node(gr.nodes, relation["out_node_id"])
        rel = Relation(
            in_node,
            out_node,
        )
        rel.rel_type = relation["rel_type"]
        gr.relations.append(rel)
        in_node.out_relations.append(rel)
        out_node.in_relations.append(rel)
    return gr
