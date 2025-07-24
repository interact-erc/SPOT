import argparse
import csv
import pandas as pd
from ast import literal_eval
from query_writer import CypherQueryWriter
from nl_provider import JsonSchemaCypherNLProvider
from generate_batch import get_graph_string
from grounder import CypherGraphGrounder
from utils import build_cypher_graph
from collections import Counter
from tqdm import tqdm
import itertools
import copy


def is_edge_node(node):
    if node.is_answer_node:
        return False
    if (len(node.in_relations) + len(node.out_relations)) == 1:
        return True
    return False


def graph_has_3_same_relations(graph):
    rels = [i.rel_type for i in graph.relations]
    counts = Counter(rels)
    repeated = [item for item, count in counts.items() if count >= 3]
    if len(repeated) > 0:
        return True
    return False


def graph_has_2_edge_nodes_unbounded(graph):
    count = 0
    for node in graph.nodes:
        if is_edge_node(node):
            if node.grounded_entity is None:
                count += 1
    return count > 1


def graph_has_repeated_entity_value(graph):
    ents = set()
    for node in graph.nodes:
        if node.grounded_entity is not None:
            if node.grounded_entity in ents:
                return True
            else:
                ents.add(node.grounded_entity)
    return False


def graph_has_unbounded_edge_same_answer_node_class(graph):
    if len(graph.nodes) < 3:
        return False
    answer_node_class_type = graph.nodes[0].class_type
    for node in graph.nodes:
        if is_edge_node(node):
            if node.grounded_entity is None:
                if node.class_type == answer_node_class_type:
                    return True
    return False


def get_rel_other_node(rel, node_id):
    if rel.in_node.id == node_id:
        return rel.out_node
    return rel.in_node


def recursive_inspect_node_parallel_rels(node, starting_rel_node):
    new_rels = []
    all_rels = node.in_relations + node.out_relations
    for rel in all_rels:
        if rel.in_node.id != starting_rel_node and rel.out_node.id != starting_rel_node:
            new_rels.append(rel)
    if len(new_rels) == 0:
        return False

    rel_by_types = {}
    for rel in new_rels:
        if rel.rel_type in rel_by_types:
            rel_by_types[rel.rel_type] += [rel]
        else:
            rel_by_types[rel.rel_type] = [rel]

    for key in rel_by_types.keys():
        same_rels = rel_by_types[key]
        if len(same_rels) > 1:
            has_bounded = False
            has_unbounded = False
            for r in same_rels:
                dest_node = get_rel_other_node(r, node.id)
                if dest_node.grounded_entity:
                    has_unbounded = True
                else:
                    has_bounded = True
            if has_bounded and has_unbounded:
                return True

    for rel in new_rels:
        dest_node = get_rel_other_node(rel, node.id)
        check = recursive_inspect_node_parallel_rels(dest_node, node.id)
        if check:
            return True
    return False


def graph_has_parallel_same_one_unbounded_rels(graph):
    answer_node = graph.nodes[0]
    return recursive_inspect_node_parallel_rels(answer_node, -1)


def graph_passes_filter(graph):

    if graph_has_3_same_relations(graph):
        return False

    if graph_has_2_edge_nodes_unbounded(graph):
        return False

    if graph_has_repeated_entity_value(graph):
        return False

    if graph_has_unbounded_edge_same_answer_node_class(graph):
        return False

    if graph_has_parallel_same_one_unbounded_rels(graph):
        return False

    return True


def compose_cypher_sample_to_save(graph, nl_provider, id):
    cypher_writer = CypherQueryWriter(graph)
    cypher_query = cypher_writer.write_query()
    cypher_writer.set_nl_provider(nl_provider)
    cypher_proto_nl = cypher_writer.write_proto_nl()

    entity_mapping = graph.entity_mapping

    graph_string = get_graph_string(graph)

    sample = {
        # "original_sample_id": id,
        "proto_nl": cypher_proto_nl,
        "query": cypher_query,
        "entity_mapping": entity_mapping,
        "graph": graph_string,
        "graph_object": graph,
    }
    return sample


def get_all_inside_nodes_comb(gr, node_id):
    combs = []
    for rel in gr.nodes[node_id].in_relations + gr.nodes[node_id].out_relations:
        if (
            rel.in_node.id > node_id
        ):  # Then it's a relation deeper in the tree, we take the combs
            combs += get_all_inside_nodes_comb(gr, rel.in_node.id)
        elif rel.out_node.id > node_id:
            combs += get_all_inside_nodes_comb(gr, rel.out_node.id)

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


def remove_nodes_from_graph(graph, nodes):
    graph["nodes"] = [i for i in graph["nodes"] if i["id"] in nodes]
    graph["relations"] = [
        i
        for i in graph["relations"]
        if i["in_node_id"] in nodes and i["out_node_id"] in nodes
    ]
    return graph


def remap_nodes_numbers(graph):
    node_map = {}
    for i, j in enumerate(graph["nodes"]):
        node_map[j["id"]] = i

    for node in graph["nodes"]:
        node["id"] = node_map[node["id"]]
    for rel in graph["relations"]:
        rel["in_node_id"] = node_map[rel["in_node_id"]]
        rel["out_node_id"] = node_map[rel["out_node_id"]]
    return graph


def get_decomposed_graphs(gr, graph):
    combs = get_all_inside_nodes_comb(gr, 0)
    combs.remove([0])

    trimmed_graphs = []
    for comb in combs:
        new_graph = copy.deepcopy(graph)
        new_graph = remove_nodes_from_graph(new_graph, comb)
        new_graph = remap_nodes_numbers(new_graph)
        trimmed_graphs.append(new_graph)

    return trimmed_graphs


def extract_cypher_values(records):
    extracted = []

    for record in records:
        for key in record.keys():
            extracted.append(record[key])
    return extracted


def cypher_decomposition(args):
    df = pd.read_csv(args.input_file)
    graphs = df["graph"].tolist()
    entity_mappings = df["entity_mapping"].tolist()

    grounder = CypherGraphGrounder(args.json_schema)
    cypher_provider = JsonSchemaCypherNLProvider(args.json_schema)

    samples_queries = set()
    samples = []

    for graph_id, (graph, ent_map) in enumerate(
        tqdm(zip(graphs, entity_mappings), total=len(graphs))
    ):
        graph = literal_eval(graph)
        ent_map = literal_eval(ent_map)
        gr = build_cypher_graph(graph, ent_map)

        dec_graphs = get_decomposed_graphs(gr, graph)

        dec_res_map = {}

        for d_graph in dec_graphs:
            gr = build_cypher_graph(d_graph, ent_map)
            cypher_writer = CypherQueryWriter(gr)
            query = cypher_writer.write_query()
            try:
                res = grounder._execute_query(query)
                res = extract_cypher_values(res)
                res = [j.element_id if hasattr(j, "element_id") else j for j in res]
                res = tuple(sorted(list((set(res)))))
                if res in dec_res_map:
                    dec_res_map[res].append(gr)
                else:
                    dec_res_map[res] = [gr]
            except:
                continue

        for k in dec_res_map.keys():
            bucket = dec_res_map[k]
            for sample_gr in bucket:
                sample = compose_cypher_sample_to_save(
                    sample_gr, cypher_provider, graph_id
                )
                if sample["query"] not in samples_queries:
                    samples.append(sample)
                    samples_queries.add(sample["query"])

    return samples


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

    samples = cypher_decomposition(args)

    samples = [i for i in samples if graph_passes_filter(i["graph_object"])]
    for sample in samples:
        sample.pop("graph_object", None)

    if len(samples) > 0:
        with open(args.output_file, mode="w", newline="") as file:
            fieldnames = samples[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(samples)

    print(f"Total produced samples: {len(samples)}")
