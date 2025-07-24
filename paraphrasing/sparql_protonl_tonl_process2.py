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
        "--v1-file",
        type=str,
        help="v1 input-file-path",
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
    preds = df["beam_0"].tolist()

    df = pd.read_csv(args.v1_file, sep="\t" if args.v1_file.endswith(".tsv") else ",")
    ids = df["id"].tolist()

    assert len(ids) == len(preds)

    samples = [[] for _ in range(ids[-1] + 1)]

    for i, j in zip(ids, preds):
        samples[i].append(j.strip())

    processed_samples = []
    for s in samples:
        a = "\n- ".join(s)
        a = "- " + a
        processed_samples.append({"X": a})

    data_df = pd.DataFrame.from_dict(processed_samples)
    data_df.to_csv(
        args.output_file,
        index=False,
        sep="\t" if args.output_file.endswith(".tsv") else ",",
    )
