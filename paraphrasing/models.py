import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    DefaultDataCollator,
)
import pandas as pd


class HFDecoderModel:
    def __init__(self, args, model_name, load_path=None, dtype=torch.float16):
        self.model = None
        self.tokenizer = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.args = args
        self.model_name = model_name
        self.dtype = dtype
        self.bf16 = False  # TODO TBD
        if args.debug:
            self.model_name = "gpt2"
            self.dtype = torch.float32
            self.fp16 = False
        self.max_length = 768

        self.load_model(load_path=load_path)

    def preprocess_data(self, data):
        return data

    def load_model(self, load_path=None):
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name, torch_dtype=self.dtype
        )
        self.tokenizer.padding_side = "left"

        if load_path is not None:
            self.model.load_state_dict(torch.load(load_path))

        self.model.to(self.device)

    def _save_partial_file_preds(self, preds, num_ret_sequences, filepath):
        new_df_data = {}
        for i in range(num_ret_sequences):
            new_df_data["beam_" + str(i)] = [j[i] for j in preds]
        new_df = pd.DataFrame(new_df_data)
        new_df.to_csv(filepath, index=False)

    def inference(
        self,
        samples,
        batch_size=1,
        num_beams=1,
        num_return_sequences=1,
        stop_token=None,
        max_new_tokens=768,
        partial_save_file=None,
        do_sample=False,
    ):
        self.model.eval()
        if stop_token:
            stop_token = self.tokenizer.encode(stop_token)[0]
            terminators = [self.tokenizer.eos_token_id, stop_token]
        else:
            terminators = [self.tokenizer.eos_token_id]

        samples = self.preprocess_data(samples)
        dataset = self.HFDataset(samples, self.tokenizer)
        data_collator = self.HFDataCollator(
            self.tokenizer, self.max_length, self.device, test=True
        )
        dataloader = DataLoader(
            dataset, batch_size=batch_size, shuffle=False, collate_fn=data_collator
        )
        preds = []
        with torch.no_grad():
            for i in dataloader:
                i = i.to(self.device)
                generated_ids = self.model.generate(
                    **i,
                    max_new_tokens=max_new_tokens,
                    do_sample=do_sample,
                    num_beam_groups=num_beams,
                    num_return_sequences=num_return_sequences,
                    pad_token_id=self.tokenizer.pad_token_id,
                    eos_token_id=terminators,
                )

                for j in range(len(i["input_ids"])):
                    outputs = []
                    prompt = i["input_ids"][j]
                    for k in range(num_return_sequences):
                        output_ids = generated_ids[j * num_return_sequences + k][
                            len(prompt) :
                        ]
                        output = self.tokenizer.decode(
                            output_ids, skip_special_tokens=True
                        )
                        outputs.append(output)
                    preds.append(outputs)

                if len(preds) % 100 == 0:
                    if partial_save_file:
                        self._save_partial_file_preds(
                            preds, num_return_sequences, partial_save_file
                        )

        return preds

    class HFDataset(Dataset):
        def __init__(self, data, tokenizer):
            self.x = data
            self.tokenizer = tokenizer

        def __len__(self):
            return len(self.x)

        def __getitem__(self, i):
            return self.x[i]

    class HFDataCollator(DefaultDataCollator):
        def __init__(self, tokenizer, max_length, device, test=False):
            self.tokenizer = tokenizer
            self.max_length = max_length
            self.device = device
            self.test = test

        def __call__(self, examples):
            if not self.test:
                examples = [i + self.tokenizer.eos_token for i in examples]
            batch = self.tokenizer(
                examples, return_tensors="pt", padding=True, add_special_tokens=False
            )
            if not self.test:
                batch["labels"] = batch["input_ids"]
            if "token_type_ids" in batch.keys():
                del batch["token_type_ids"]
            return batch
