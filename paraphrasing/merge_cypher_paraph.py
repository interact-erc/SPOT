import argparse
import re
import pandas as pd

MAX_NL_LENGTH = 1024


def extract_entities(mr):
    pattern = r'([A-Za-z0-9_]+)\s*WHERE\s*[A-Za-z0-9_]+\.[A-Za-z0-9_]+\s*=\s*"([^"]+)"'
    matches = re.findall(pattern, mr)
    dic = {}
    for i in matches:
        dic[i[1]] = f"[{i[0]}]"
    return dic


def extract_paraphs(paraphs):
    paraphs = paraphs.strip()
    if not paraphs.startswith("1)"):
        paraphs = "1) " + paraphs

    paraphs = paraphs.split("\n")

    ps = []

    for i in paraphs:
        if i.startswith(("1)", "2)", "3)")):
            p = i[2:]
            p = p.strip()
            ps.append(p)
    return ps


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

    df = pd.read_csv(args.paraph_file)
    nls = df["beam_0"].tolist()

    samples = []

    for n, m in zip(nls, mrs):
        e = extract_entities(m)

        ent = []
        for i in e.keys():
            ent.append((e[i], i))

        paraph = extract_paraphs(n)

        add_ent = ""
        for i in ent:
            add_ent += f"{i[0]} = {i[1]}\n"

        for prp in paraph:
            question = add_ent + prp.strip()

            if len(n) < MAX_NL_LENGTH:
                sample = {
                    "nl": prp.strip(),
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
