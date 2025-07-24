import argparse
import ast
import pandas as pd

MAX_NL_LENGTH = 1024


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file", type=str, help="csv file with the generations", required=True
    )
    parser.add_argument(
        "--paraph-file", type=str, help="file with the paraphrasis", required=True
    )
    parser.add_argument(
        "--output-file", type=str, help="output-file-path", required=True
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input_file)
    mrs = df["query"].tolist()
    entities = df["entity_mapping"].tolist()

    df = pd.read_csv(args.paraph_file)
    nls = df["beam_0"].tolist()

    samples = []

    for n, m, e in zip(nls, mrs, entities):

        e = ast.literal_eval(e)

        ent = []
        for i in e.keys():
            ent.append((e[i], i))

        add_ent = ""
        for i in ent:
            add_ent += f"{i[0]} = {i[1]}\n"
        question = add_ent + n.strip()

        if len(n) < MAX_NL_LENGTH:
            sample = {
                "nl": n.strip(),
                "nl_linked": question,
                "query": m,
                "entities": ent,
            }
            samples.append(sample)

    print(f"Total samples: {len(samples)}")

    data_df = pd.DataFrame.from_dict(samples)
    data_df.to_csv(
        args.output_file,
        index=False,
        sep="\t" if args.output_file.endswith(".tsv") else ",",
    )
