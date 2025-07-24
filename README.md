# SPOT

This repository contains the code for the paper [SPOT: Zero-Shot Semantic Parsing Over Property Graphs](https://aclanthology.org/2025.findings-acl.524/) accepted at Findings of ACL 2025. 
The project enables automatic creation of data pairs for semantic parsing, starting from a knowledge graph using either Cypher or SPARQL queries.


If you use or find our resource useful please cite:
```
@inproceedings{cazzaro-etal-2025-spot,
    title = "{SPOT}: Zero-Shot Semantic Parsing Over Property Graphs",
    author = "Cazzaro, Francesco  and
      Kleindienst, Justin  and
      Gomez, Sofia M{\'a}rquez  and
      Quattoni, Ariadna",
    editor = "Che, Wanxiang  and
      Nabende, Joyce  and
      Shutova, Ekaterina  and
      Pilehvar, Mohammad Taher",
    booktitle = "Findings of the Association for Computational Linguistics: ACL 2025",
    month = jul,
    year = "2025",
    address = "Vienna, Austria",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2025.findings-acl.524/",
    pages = "10057--10073",
    ISBN = "979-8-89176-256-5",
    abstract = "Knowledge Graphs (KGs) have gained popularity as a means of storing structured data, with property graphs, in particular, gaining traction in recent years. Consequently, the task of semantic parsing remains crucial in enabling access to the information in these graphs via natural language queries. However, annotated data is scarce, requires significant effort to create, and is not easily transferable between different graphs. To address these challenges we introduce SPOT, a method to generate training data for semantic parsing over Property Graphs without human annotations. We generate tree patterns, match them to the KG to obtain a query program, and use a finite-state transducer to produce a proto-natural language realization of the query. Finally, we paraphrase the proto-NL with an LLM to generate samples for training a semantic parser. We demonstrate the effectiveness of SPOT on two property graph benchmarks utilizing the Cypher query language. In addition, we show that our approach can also be applied effectively to RDF graphs."
}
```

Before beginning, we reccomend creating a new environment:
```shell
conda create -n spot python=3.9
conda activate spot
pip install -r requirements.txt
```

## 1) Generating Cypher data

To run Cypher queries over a knowledge graph we use neo4j. As a prerequisite you must set it up, load a graph (e.g. [pole](https://github.com/neo4j-graph-examples/pole)) and have it running. For more details, refer to the official [documentation](https://neo4j.com/docs/getting-started/).
Besides, you must also have a graph schema file containing the graph specifications. You can find an example [here](https://github.com/interact-erc/SPOT/blob/main/kg_schemas/schema_pole.json) for [Zograscope](https://github.com/interact-erc/ZOGRASCOPE).

Before beginning go to cypher_gen/grounder.py and at lines 32-34 edit uri, username and password for neo4j.

### Sample generation
The first step of the process consists in generating the Cypher queries and the associated proto-NLs via the following command:
```shell
python cypher_gen/generate_batch.py --json-schema kg_schemas/schema_pole.json --output-file path/to/generation_output.csv --grounding-per-pattern 2 --max-pattern-retries 5 --saving-interval 5 --max-nodes 3 --max-grounder-iterations 5 --seed 1 --remove-modifiers sum avg
```
Besides setting the correct paths for the --json-schema and the --output-file some relevant parameters to set are:
- --max-nodes: maximum number of nodes in a cypher query (minimum is 2)
- --grounding-per-pattern: how many cypher queries to generate per pattern (maximum, conditional on whether the pattern is grounded)
- --max-pattern-retries: how many times to retry grounding a pattern if the grounding failed.
- --max-grounder-iterations: how many internal iterations for the grounder to attempt a grounding.
- --seed: seed number

After we have created the samples we perform the decomposition process:
```shell
python cypher_gen/decompose_and_filter.py --json-schema kg_schemas/schema_pole.json --input-file path/to/generation_output.csv --output-file path/to/decomposition_output.csv
```

### NL generation
Once we have obtained the samples we must derive the natural language realization. For this process we are going to use an LLM, therefore this part should be run on a GPU with cuda. The following command will obtain a file with three predicted NLs for each sample:
```shell
python paraphrasing/main.py --dataset path/to/decomposition_output.csv --output-file path/to/llm_output.csv  --load-column proto_nl --model Qwen/Qwen3-14B --prompt-path paraphrasing/prompts/cypher_prompt.txt --max-new-tokens 250 --truncate-token [END] --batch-size 24
```

### Dataset creation
Finally we combine the decomposition file and the predicted NLs file to obtain our final dataset of sample pairs:
```shell
python paraphrasing/merge_cypher_paraph.py --input-file path/to/decomposition_output.csv --paraph-file path/to/llm_output.csv --output-file path/to/final_dataset.csv
```

## 2) Generating SPARQL data

To run SPARQL queries over a knowledge graph we use virtuoso to set up a local server. For further details on setting it up refer to the official [repo](https://github.com/openlink/virtuoso-opensource).
Besides, you must also have a graph schema file containing the graph specifications. You can find an example [here](https://github.com/interact-erc/SPOT/blob/main/kg_schemas/schema_freebase.json) for [Freebase](https://github.com/dki-lab/Freebase-Setup).

### Sample generation
The first step of the process consists in generating the SPARQL queries and the associated proto-NLs via the following command:
```shell
python sparql_gen/generate_batch.py --json-schema kg_schemas/schema_freebase.json --output-file path/to/generation_output.csv --grounding-per-pattern 2 --max-nodes 3 --max-grounder-iterations 10 --max-pattern-retries 10 --seed 1 --diverse-sampling --diverse-parallel-relations
```
Besides setting the correct paths for the --json-schema and the --output-file some relevant parameters to set are:
- --max-nodes: maximum number of nodes in a sparql query (minimum is 2)
- --grounding-per-pattern: how many sparql queries to generate per pattern (maximum, conditional on whether the pattern is grounded)
- --max-pattern-retries: how many times to retry grounding a pattern if the grounding failed.
- --max-grounder-iterations: how many internal iterations for the grounder to attempt a grounding.
- --seed: seed number

We also reccomend running with the --diverse-sampling and --diverse-parallel-relations parameters.

After we have created the samples we perform the decomposition process:
```shell
python sparql_gen/decompose_and_filter.py --json-schema kg_schemas/schema_freebase.json --input-file path/to/generation_output.csv --output-file path/to/decomposition_output.csv
```
### NL generation
Once we have obtained the samples we must derive the natural language realization. For this process we are going to use an LLM, therefore this part should be run on a GPU with cuda. The process involve two prediction steps and can be carried out via the following commands setting the correct file paths:
```shell
python paraphrasing/sparql_protonl_tonl_process1.py --input-file path/to/decomposition_output.csv --output-file path/to/intermediate_sptonl_1.tsv

python paraphrasing/main.py --dataset path/to/intermediate_sptonl_1.tsv  --load-column X --model meta-llama/Llama-3.1-8B --output-file path/to/nl_predsv1.csv --prompt-path paraphrasing/prompts/proto_nl_to_nl_prompt_part1.txt --max-new-tokens 120 --truncate-token [END] --batch-size 32

python paraphrasing/sparql_protonl_tonl_process2.py --input-file path/to/nl_predsv1.csv --v1-file path/to/intermediate_sptonl_1.tsv --output-file path/to/intermediate_sptonl_2.tsv

python paraphrasing/main.py --dataset path/to/intermediate_sptonl_2.tsv  --load-column X --model meta-llama/Llama-3.1-8B --output-file path/to/nl_predsv2.csv --prompt-path paraphrasing/prompts/proto_nl_to_nl_prompt_part2.txt --max-new-tokens 120 --truncate-token [END] --batch-size 32
```

### Dataset creation
Finally we combine the decomposition file and the predicted NLs file to obtain our final dataset of sample pairs:
```shell
python paraphrasing/merge_sparql_paraph.py --input-file path/to/decomposition_output.csv --paraph-file path/to/nl_predsv2.csv --output-file path/to/final_dataset.csv
```
