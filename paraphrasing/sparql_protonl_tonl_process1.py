import pandas as pd
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        type=str,
        help="input-file-path",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="output-file-path",
    )
    args = parser.parse_args()

    df = pd.read_csv(
        args.input_file, sep="\t" if args.input_file.endswith(".tsv") else ","
    )
    nls = df["proto_nl"].tolist()

    count = 0
    samples = []

    for i in nls:
        i = i.split("\nAND")
        i = [i.strip() for i in i]
        for j in i:
            samples.append({"id": count, "X": j})
        count += 1

    data_df = pd.DataFrame.from_dict(samples)
    data_df.to_csv(
        args.output_file,
        index=False,
        sep="\t" if args.output_file.endswith(".tsv") else ",",
    )
