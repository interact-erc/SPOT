import argparse
from pattern_generator import PatternGenerator
from grounder import CypherGraphGrounder
from query_writer import CypherQueryWriter
from nl_provider import JsonSchemaCypherNLProvider
import csv
from tqdm import tqdm
import random
import copy
import json


def get_graph_string(graph):
    nodes = []
    for node in graph.nodes:
        nodes.append(
            {
                "id": node.id,
                "is_answer_node": node.is_answer_node,
                "grounded_entity": f"{node.grounded_entity}",
                "attribute": node.attribute,
                "class_type": node.class_type,
                "modifier_edge": node.modifier_edge,
                "modifier": node.modifier,
                "datatype": node.datatype,
            }
        )
    relations = []
    for rel in graph.relations:
        relations.append(
            {
                "in_node_id": rel.in_node.id,
                "out_node_id": rel.out_node.id,
                "rel_type": rel.rel_type,
            }
        )

    return {
        "nodes": nodes,
        "relations": relations,
        "return_attribute": graph.return_attribute,
        "return_property": graph.return_property,
    }


def entity_graph_anonymization(graph):
    for node in graph.nodes:
        node.grounded_entity = None
    graph.ground_anonymously()
    return graph


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json-schema", type=str, help="kg json schema path", required=True
    )
    parser.add_argument("--output-file", type=str, help="output file", required=True)
    parser.add_argument(
        "--cast-cypher-modifiers",
        action="store_true",
        help="Cast cypher float and ints in the MR",
    )
    parser.add_argument(
        "--num-patterns",
        type=int,
        help="number of patterns to try",
        default=10000,
        required=False,
    )
    parser.add_argument(
        "--grounding-per-pattern",
        type=int,
        default=1,
        help="number of different grounding per pattern to try to get",
    )
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=7,
        help="Max number of nodes to include in the patterns",
    )
    parser.add_argument(
        "--exact-nodes",
        type=int,
        help="Exact number of nodes to include in the patterns",
    )
    parser.add_argument(
        "--max-grounder-iterations",
        type=int,
        default=20,
        help="Max number of iterations of the grounder",
    )
    parser.add_argument(
        "--max-pattern-retries",
        type=int,
        default=5,
        help="If the grounding of a pattern fails, how many times to retry",
    )
    parser.add_argument(
        "--saving-interval",
        type=int,
        default=5,
        help="Every how many iterations to save the generations",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="random seed",
    )
    parser.add_argument(
        "--resume-from",
        type=int,
        default=0,
        help="Resume generation from pattern id instead of from first one",
    )
    parser.add_argument(
        "--remove-modifiers",
        nargs="+",
        help="List of modifiers to remove that will not be instantiated.",
    )
    parser.add_argument(
        "--diverse-sampling", action="store_true", help="Sample diverse relations"
    )
    parser.add_argument(
        "--diverse-parallel-relations",
        action="store_true",
        help="Sample diverse relations for parallel edges",
    )
    parser.add_argument(
        "--save-diverse-sampling",
        type=str,
        help="Filepath to save the classes and relation sampled for diverse sampling",
    )
    parser.add_argument(
        "--resume-diverse-sampling",
        type=str,
        help="Filepath to load the classes and relation sampled for diverse sampling",
    )
    args = parser.parse_args()

    if args.seed:
        random.seed(args.seed)

    num_patterns = args.num_patterns
    pattern_generator = PatternGenerator()
    pattern_generator.max_nodes = args.max_nodes
    if args.remove_modifiers:
        pattern_generator.remove_modifiers = args.remove_modifiers

    graphs = pattern_generator.generate_patterns(num_patterns, unique=True)

    if args.exact_nodes:
        graphs = [i for i in graphs if len(i.nodes) == args.exact_nodes]

    if args.resume_from:
        graphs = graphs[args.resume_from :]

    grounder = CypherGraphGrounder(args.json_schema)
    cypher_provider = JsonSchemaCypherNLProvider(args.json_schema)

    if args.diverse_sampling:
        grounder.diverse_sampling = True
    if args.resume_diverse_sampling:
        with open(args.resume_diverse_sampling, "r") as json_file:
            div_dict = json.load(json_file)
        for i in div_dict["classes"]:
            grounder.sampled_classes.add(i)
        for i in div_dict["relations"]:
            grounder.sampled_rel.add(i)

    if args.diverse_parallel_relations:
        grounder.diverse_parallel_relations = True

    grounder.max_iterations = args.max_grounder_iterations

    samples = []

    for ittr, graph_orig in tqdm(enumerate(graphs), total=len(graphs)):
        external_retries = 0
        for _ in range(args.grounding_per_pattern):
            graph = copy.deepcopy(graph_orig)
            is_grounded = False
            while not is_grounded and external_retries < args.max_pattern_retries:
                external_retries += 1
                try:
                    grounded_graphs, is_grounded = grounder.ground_graph(graph)
                except:
                    is_grounded = False

            if is_grounded:
                for grounded_graph in grounded_graphs:
                    try:
                        cypher_writer = CypherQueryWriter(grounded_graph)
                        cypher_writer.cast_modifiers = args.cast_cypher_modifiers
                        cypher_query = cypher_writer.write_query()
                        cypher_writer.set_nl_provider(cypher_provider)
                        cypher_proto_nl = cypher_writer.write_proto_nl()

                        entity_mapping = grounded_graph.entity_mapping

                        graph_string = get_graph_string(grounded_graph)

                        grounded_graph = entity_graph_anonymization(grounded_graph)
                        cypher_writer = CypherQueryWriter(grounded_graph)
                        cypher_writer.cast_modifiers = args.cast_cypher_modifiers
                        cypher_query_ent_anon = cypher_writer.write_query()
                        cypher_writer.set_nl_provider(cypher_provider)
                        cypher_proto_nl_ent_anon = cypher_writer.write_proto_nl()

                        graph.ground_anonymously()
                        cypher_writer = CypherQueryWriter(graph)
                        anon_cypher_query = cypher_writer.write_query()
                        cypher_writer.set_nl_provider(cypher_provider)
                        anon_cypher_proto_nl = cypher_writer.write_proto_nl()

                        sample = {
                            "anon_proto_nl": anon_cypher_proto_nl,
                            "anon_query": anon_cypher_query,
                            "proto_nl": cypher_proto_nl,
                            "query": cypher_query,
                            "entity_mapping": entity_mapping,
                            "graph": graph_string,
                            "ent_anon_proto_nl": cypher_proto_nl_ent_anon,
                            "ent_anon_query": cypher_query_ent_anon,
                        }
                        samples.append(sample)

                    except:
                        continue

        if ittr % args.saving_interval == 0 or ittr == args.num_patterns - 1:
            if len(samples) > 0:
                with open(args.output_file, mode="w", newline="") as file:
                    fieldnames = samples[0].keys()
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(samples)

                if args.save_diverse_sampling:
                    div_dict = {
                        "classes": list(grounder.sampled_classes),
                        "relations": list(grounder.sampled_rel),
                    }
                    with open(args.save_diverse_sampling, "w") as json_file:
                        json.dump(div_dict, json_file, indent=4)

    print(f"Total produced samples: {len(samples)}")
