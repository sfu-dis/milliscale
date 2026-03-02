import csv
import re
import os
from dataclasses import dataclass, field
import shlex
import subprocess
from typing import Any, Callable, Iterable, List
from itertools import product


def expr_avg(l):
    return sum(map(float, l)) / len(l)

def expr_sum(l):
    return sum(map(float, l))

def expr_max(l):
    return max(map(float, l))

def expr_min(l):
    return min(map(float, l))

def expr_first(l):
    return l[0]

def expr_last(l):
    return l[-1]

@dataclass
class Rule:
    name: str
    regex: str
    process: Callable[[List[str]], str]

@dataclass
class Command:
    name: str = ""
    value: List = field(default_factory=list)
    pattern: str = ""
    suffix: str = None
    symbols: str = None

    def __post_init__(self):
        assert self.name is not None and self.name != "", "Error, each command should have a unique name"
        if self.suffix is None:
            self.suffix = self.pattern
        if self.symbols:
            assert len(self.value) == len(self.symbols), "Error, len(values) != len(symbols)"

@dataclass
class CombineCommand:
    cmd: str
    suffix: str
    params: dict[str, Any]


def expand_commands(commands: List[Command]) -> Iterable[CombineCommand]:
    """
    Compute Cartesian product of given command list, return cmd and suffix
    if commands is an empty list, then it still yield once (empty string)
    
    Args:
       commands: List[Command]

    Returns:
        Iterable[CombineCommand]: return cmd (separate by space), suffix, and value of cmd(in dict)
    """
    value_lists = [cmd.value for cmd in commands]
    symbol_lists = [cmd.symbols if cmd.symbols else cmd.value for cmd in commands]
    for combo, symbols in zip(product(*value_lists), product(*symbol_lists)):
        parts = []
        suffixes = []
        params = {}
        for (cmd, val, symbol) in zip(commands, combo, symbols):
            params[cmd.name] = symbol
            if isinstance(val, (list, tuple)):
                parts.append(cmd.pattern.format(*val))
                suffixes.append(cmd.suffix.format(*symbol))
            else:
                parts.append(cmd.pattern.format(val))
                suffixes.append(cmd.suffix.format(symbol))
        yield CombineCommand(cmd=" ".join(parts), suffix="".join(suffixes), params=params)


def __read_file__(filename: str):
    with open(filename, "r") as f:
        text = f.read()
    return text


def __merge_result__(result: dict, **kwargs):
    for k, v in kwargs.items():
        if k in result:
            print(f"[Warning] Overwriting key '{k}': {result[k]} -> {v}")
        result[k] = v


def __run_cmd__(log_dir: str, filename: str, cmd: str):
    os.makedirs(log_dir, exist_ok=True)
    filename = os.path.join(log_dir, filename)
    if os.path.exists(filename):
        print(f"[SKIP] {filename} already exists.")
        return
    print(f"[RUN] {filename}")
    with open(filename, "w") as f:
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
            

def from_files(fields: List[Rule], filenames: List[str], **kwargs) -> dict[str, Any]:
    # { field_name: [values...] }
    values_map = {field.name: [] for field in fields}

    for fname in filenames:
        if not os.path.exists(fname):
            print("[Warning] skip:", fname)
            continue

        text = __read_file__(fname)

        for field in fields:
            match = re.search(field.regex, text)
            if match:
                values_map[field.name].append(float(match.group(1)))
            else:
                print(f"[Warning] {fname} not match with {field.regex}")

    result = {}
    for field in fields:
        vals = values_map[field.name]
        if vals:
            result[field.name] = field.process(vals)
        else:
            result[field.name] = None

    __merge_result__(result, **kwargs)
    return result


def execute(basic_cmd:str, commands:List[Command], filename: str, log_dir='./', run=True):
    for ccommand in expand_commands(commands):
        custom_cmd, suffix = ccommand.cmd, ccommand.suffix
        cmd = " ".join([basic_cmd, custom_cmd])
        if run:
            __run_cmd__(log_dir=log_dir, filename=filename+suffix, cmd=shlex.split(cmd))
        else:
            print(cmd + " >> " + os.path.join(log_dir, filename + suffix))


def parse(commands:List[Command], fields: List[Rule], filename:str, out_csv:str, log_dir='./'):
    # assume group by the last cmd
    groups = [
        ([os.path.join(log_dir, f"{filename}{c1.suffix}{c2.suffix}") 
        for c2 in expand_commands(commands[-1:])], c1.params)
        for c1 in expand_commands(commands[:-1])
    ]
    results = [from_files(fields, group, **param) for group, param in groups]
    dict_to_csv(results, out_csv)


def dict_to_csv(data: list[dict], csv_file: str):
    """
    Check that all dictionaries in a list have the same keys and export them to a CSV file.

    Args:
        data (List[Dict]): List of dictionaries to export.
        csv_file (str): Path to the output CSV file.

    Raises:
        ValueError: If the list is empty or if dictionaries have inconsistent keys.
    """
    if not data or len(data) == 0:
        raise ValueError("The data list is empty.")
    
    # Use the keys of the first dictionary as reference
    keys = set(data[0].keys())

    # Check that all dictionaries have the same keys
    for i, d in enumerate(data):
        if set(d.keys()) != keys:
            raise ValueError(f"Dictionary at index {i} has inconsistent keys: {d.keys()}")

    def format_row(row):
        return {
            k: (f"{v:.4f}" if isinstance(v, float) else v)
            for k, v in row.items()
        }

    # Export to CSV
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(format_row(row) for row in data)
