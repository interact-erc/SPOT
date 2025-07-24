from models import HFDecoderModel
import argparse
import pandas as pd
import torch

PROMPT_QUERY_CODE = "[[QUERY]]"


def compose_prompt(query, prompt):
    prompt = prompt.replace(PROMPT_QUERY_CODE, query)
    return prompt


def load_prompt(prompt_path):
    with open(prompt_path, "r") as file:
        prompt = file.read()
    if prompt[-1] == "\n":  # One \n gets added at the end of the file automatically
        prompt = prompt[:-1]
    return prompt


def preprocess_data(data, args):
    prompt = load_prompt(args.prompt_path)
    data = [compose_prompt(i, prompt) for i in data]

    return data


def truncate(x, stop_token, include_stop=True):
    idx = x.find(stop_token)
    if idx != -1:
        if include_stop:
            return x[: idx + len(stop_token)]
        else:
            return x[:idx]
    return x


def postprocess_preds(preds, stop_token, include_stop=True):
    preds = [[truncate(j, stop_token, include_stop) for j in i] for i in preds]
    return preds


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Debug mode use gpt2")
    parser.add_argument(
        "--model", type=str, help="model", default="gpt2", required=False
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=128,
        help="max new generated tokens",
        required=False,
    )
    parser.add_argument("--stop-token", type=str, help="stop token", required=False)
    parser.add_argument(
        "--num-beams", type=int, default=1, help="num beams", required=False
    )
    parser.add_argument(
        "--num-return-sequences",
        type=int,
        default=1,
        help="num return sequences",
        required=False,
    )
    parser.add_argument(
        "--do-sample", action="store_true", help="Do sample at inference"
    )
    parser.add_argument(
        "--partial-savefile", type=str, help="partial savefile path", required=False
    )
    parser.add_argument(
        "--prompt-path", type=str, help="prompt file path", required=True
    )
    parser.add_argument("--dataset", type=str, help="csv file", required=True)
    parser.add_argument(
        "--load-column", type=str, help="csv column to process", required=True
    )
    parser.add_argument(
        "--output-file", type=str, help="output-file-path", required=False
    )
    parser.add_argument(
        "--truncate-token", type=str, help="Truncate the prediction at this token"
    )
    parser.add_argument("--batch-size", type=int, default=1, help="batch size")
    parser.add_argument("--seed", type=int, help="seed", required=False)
    args = parser.parse_args()

    df = pd.read_csv(args.dataset, sep="\t" if args.dataset.endswith(".tsv") else ",")
    data = df[args.load_column].tolist()

    if args.debug:
        data = data[:2]

    if args.seed:
        torch.manual_seed(args.seed)
        torch.cuda.manual_seed_all(args.seed)

    data = preprocess_data(data, args)

    model = HFDecoderModel(args, args.model)

    output = model.inference(
        data,
        batch_size=args.batch_size,
        num_beams=args.num_beams,
        num_return_sequences=args.num_return_sequences,
        max_new_tokens=args.max_new_tokens,
        partial_save_file=args.partial_savefile,
        stop_token=args.stop_token,
        do_sample=args.do_sample,
    )

    if args.truncate_token:
        output = postprocess_preds(output, args.truncate_token, include_stop=False)

    if args.output_file:
        model._save_partial_file_preds(
            output, args.num_return_sequences, args.output_file
        )
