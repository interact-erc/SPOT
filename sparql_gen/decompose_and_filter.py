import argparse
import pandas as pd
from ast import literal_eval
from graph import Node, Relation, Graph
from query_writer import SPARQLQueryWriter
from nl_provider import JsonSchemaSPARQLNLProvider
from generate_batch import get_graph_string
from grounder import SPARQLGraphGrounder
import random
import csv
from tqdm import tqdm
import itertools
import copy


def compose_sample_to_save(graph, nl_provider, id):
    sparql_writer = SPARQLQueryWriter(graph)
    sparql_query = sparql_writer.write_query()
    sparql_writer.set_nl_provider(nl_provider)
    sparql_proto_nl = sparql_writer.write_proto_nl()

    entity_mapping = graph.entity_mapping

    graph_string = get_graph_string(graph)

    sample = {
        "original_sample_id": id,
        "proto_nl": sparql_proto_nl,
        "query": sparql_query,
        "entity_mapping": entity_mapping,
        "graph": graph_string,
    }
    return sample


def get_node(nodes, id):
    for n in nodes:
        if n.id == id:
            return n


def build_graph(graph, entity_mapping):
    gr = Graph()
    gr.entity_mapping = entity_mapping
    gr.nodes = []
    for node in graph["nodes"]:
        n = Node(node["id"])
        n.is_answer_node = node["is_answer_node"]
        n.grounded_entity = node["grounded_entity"]
        n.attribute = node["attribute"]
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


def trim_graph(graph, ent_map):
    node = graph["nodes"][-1]
    graph["nodes"] = graph["nodes"][:-1]
    graph["relations"] = [
        i
        for i in graph["relations"]
        if i["in_node_id"] != node["id"] and i["out_node_id"] != node["id"]
    ]
    new_ent = {}
    for n in graph["nodes"]:
        gr_e = n["grounded_entity"]
        if gr_e:
            if gr_e in ent_map:
                new_ent[gr_e] = ent_map[gr_e]
            else:
                assert n["attribute"].startswith("type.")
    return graph, new_ent


def get_all_inside_nodes_comb_sparql(gr, node_id):
    combs = []
    for rel in gr.nodes[node_id].in_relations + gr.nodes[node_id].out_relations:
        if rel.in_node.id > node_id:
            combs += get_all_inside_nodes_comb_sparql(gr, rel.in_node.id)
        elif rel.out_node.id > node_id:
            combs += get_all_inside_nodes_comb_sparql(gr, rel.out_node.id)

    all_combinations = []

    for r in range(1, len(combs) + 1):
        for combo in itertools.combinations(combs, r):
            all_combinations.append([item for sublist in combo for item in sublist])
    if len(all_combinations) == 0:
        all_combinations = [[node_id]]
    all_combinations = [[node_id] + sublist for sublist in all_combinations]
    all_combinations = [list(set(i)) for i in all_combinations]
    all_combinations = [
        list(tup) for tup in set(tuple(lst) for lst in all_combinations)
    ]

    all_combinations.append([node_id])
    return all_combinations


def remove_nodes_from_graph_sparql(graph, nodes):
    graph["nodes"] = [i for i in graph["nodes"] if i["id"] in nodes]
    graph["relations"] = [
        i
        for i in graph["relations"]
        if i["in_node_id"] in nodes and i["out_node_id"] in nodes
    ]
    return graph


def remap_nodes_numbers_sparql(graph):
    node_map = {}
    for i, j in enumerate(graph["nodes"]):
        node_map[j["id"]] = i

    for node in graph["nodes"]:
        node["id"] = node_map[node["id"]]
    for rel in graph["relations"]:
        rel["in_node_id"] = node_map[rel["in_node_id"]]
        rel["out_node_id"] = node_map[rel["out_node_id"]]
    return graph


def get_new_ent_map_sparql(graph, ent_map):
    new_ent = {}
    for n in graph["nodes"]:
        gr_e = n["grounded_entity"]
        if gr_e:
            if gr_e in ent_map:
                new_ent[gr_e] = ent_map[gr_e]
            else:
                assert n["attribute"].startswith("type.")
    return new_ent


def get_decomposed_graphs_sparql(gr, graph, ent_map):
    combs = get_all_inside_nodes_comb_sparql(gr, 0)
    combs.remove([0])

    trimmed_graphs = []
    new_ent_maps = []
    for comb in combs:
        new_graph = copy.deepcopy(graph)
        new_graph = remove_nodes_from_graph_sparql(new_graph, comb)
        new_graph = remap_nodes_numbers_sparql(new_graph)
        new_ent_map = get_new_ent_map_sparql(new_graph, ent_map)
        trimmed_graphs.append(new_graph)
        new_ent_maps.append(new_ent_map)

    return trimmed_graphs, new_ent_maps


def update_graph_info_node(graph_info, node_id, grounded_entity, datatype=None):
    for n in graph_info["nodes"]:
        if n["id"] == node_id:
            n["grounded_entity"] = grounded_entity
            n["datatype"] = datatype


def ground_new_graph(graph, graph_info, grounder):
    nodes_to_ground = []
    for node in graph.nodes:
        if not node.is_answer_node:
            if not node.modifier:
                if len(node.in_relations + node.out_relations) == 1:
                    if not node.grounded_entity:
                        nodes_to_ground.append(node)

    if len(nodes_to_ground) == 0:
        return graph, graph_info, True

    sparql_writer = SPARQLQueryWriter(graph)
    query = sparql_writer.write_query()

    query = query.split("\n")
    query[0] = "SELECT DISTINCT"
    for i in nodes_to_ground:
        query[0] += f", ?x{i.id}"
    query[0] = query[0].replace("DISTINCT,", "DISTINCT")
    query = "\n".join(query)
    try:
        res = grounder._execute_query(query)
    except:
        return graph, graph_info, False
    if len(res) == 0:
        return graph, graph_info, False
    res = random.choice(res)

    is_grounded = True
    for node in nodes_to_ground:
        entity = res[f"x{node.id}"]

        if not node.attribute.startswith("type"):
            entity_label = grounder._query_label(entity)
            if entity_label is None:
                is_grounded = False
            else:
                graph.entity_mapping[entity_label] = entity
                node.grounded_entity = entity_label
                update_graph_info_node(
                    graph_info,
                    node.id,
                    entity_label,
                )

        else:
            try:
                datatype = grounder._query_datatype(node.in_relations[0].rel_type)
                node.grounded_entity = entity
                node.datatype = datatype

                update_graph_info_node(graph_info, node.id, entity, datatype=datatype)
            except:
                is_grounded = False

    return graph, graph_info, is_grounded


def sparql_decomposition(args):
    df = pd.read_csv(args.input_file)
    graphs = df["graph"].tolist()
    entity_mappings = df["entity_mapping"].tolist()

    grounder = SPARQLGraphGrounder(args.json_schema)
    sparql_provider = JsonSchemaSPARQLNLProvider(args.json_schema)

    samples = []

    for graph_id, (graph, ent_map) in enumerate(
        tqdm(zip(graphs, entity_mappings), total=len(graphs))
    ):
        graph = literal_eval(graph)
        ent_map = literal_eval(ent_map)
        gr = build_graph(graph, ent_map)

        dec_graphs, dec_ent_maps = get_decomposed_graphs_sparql(gr, graph, ent_map)

        for d_graph, d_ent_map in zip(dec_graphs, dec_ent_maps):
            gr = build_graph(d_graph, d_ent_map)
            gr, d_graph, is_grounded = ground_new_graph(gr, d_graph, grounder)
            if is_grounded:
                sample = compose_sample_to_save(gr, sparql_provider, graph_id)
                samples.append(sample)

    return samples


def has_repeated_relations(graph):
    rels = set()
    for relation in graph["relations"]:
        rel_type = relation["rel_type"]
        if rel_type in rels:
            return True
        rels.add(rel_type)
    return False


def has_middle_modifier(graph):
    nid = None
    for i in graph["nodes"]:
        if i["modifier"]:
            nid = i["id"]
    if not nid:
        return False
    if nid == 0:
        return False
    count = 0
    for rel in graph["relations"]:
        if rel["in_node_id"] == nid:
            count += 1
        if rel["out_node_id"] == nid:
            count += 1
    if count > 1:
        return True
    else:
        return False


def has_repeated_entities(sample):
    query = sample["query"]
    for i in sample["entity_mapping"]:
        count = query.count(i[0])
        if count > 1:
            return True
    return False


def graph_passes_filter(sample):
    if has_middle_modifier(sample["graph"]):
        return False

    if has_repeated_relations(sample["graph"]):
        return False

    if has_repeated_entities(sample):
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file", type=str, help="input generations file", required=True
    )
    parser.add_argument(
        "--json-schema", type=str, help="kg json schema path", required=True
    )
    parser.add_argument("--output-file", type=str, help="output file", required=True)
    args = parser.parse_args()

    samples = sparql_decomposition(args)

    samples = [i for i in samples if graph_passes_filter(i)]

    if len(samples) > 0:
        with open(args.output_file, mode="w", newline="") as file:
            fieldnames = samples[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(samples)

    print(f"Total produced samples: {len(samples)}")
