import os
import re
from statistics import mean

def extract_metrics(text):
    metrics = {}
    buff_fill_match = re.search(r"avg_logbuf_fill_time :\s+([0-9.eE+-]+)\s+ms", text)
    latency_match = re.search(r"avg_latency:\s+([0-9.eE+-]+)\s+ms", text)
    throughput_match = re.search(r"agg_throughput:\s+([0-9.eE+-]+)\s+ops/sec", text)
    metrics["buffer_fill"] = float(buff_fill_match.group(1)) if buff_fill_match else None
    metrics["avg_latency"] = float(latency_match.group(1)) if latency_match else None
    metrics["throughput"] = float(throughput_match.group(1)) if throughput_match else None
    for key in ["min", "p50", "p90", "p95", "p99", "p999", "p9999", "max"]:
        match = re.search(rf"{key}_latency:\s+([0-9.eE+-]+)\s+ms", text)
        metrics[key] = float(match.group(1)) if match else None
    return metrics

def aggregate_metrics(filename, runs=(1, 2, 3)):
    collected = []

    for run in runs:
        fname = f"{filename}_run-{run}.txt"
        if not os.path.exists(fname):
            print(f"Warning: {fname} not found, skipping.")
            continue
        
        with open(fname, "r") as f:
            text = f.read()
        collected.append(extract_metrics(text))

    if not collected:
        return {}

    keys = collected[0].keys()
    avg_metrics = {}

    for k in keys:
        values = [m[k] for m in collected if m[k] is not None]
        avg_metrics[k] = mean(values) if values else None

    return avg_metrics

'''
if __name__ == "__main__":
    
    import sys
    if len(sys.argv) < 2:
        print("Usage: python agg_script.py <filename_prefix>")
        exit(1)

    filename_prefix = sys.argv[1]
    
    for buffer_size in [2048, 1024, 512, 256, 128, 64]:
        for combine_log in [1, 2, 4]:
            filename = f"ycsb_buffer_size/ycsb_buffer-{buffer_size}_combine-{combine_log}"
    result = aggregate_metrics(filename_prefix)
    for k, v in result.items():
        print(f"{k}: {v}")
'''
# ---- CSV OUTPUT ----

import csv

output_file = "buffer_results.csv"

with open(output_file, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    header = ["buffer_size", "combine_log", "throughput", "avg_latency",
              "min", "p50", "p90", "p95", "p99", "p999", "p9999", "max", "buffer_fill"]
    writer.writerow(header)

    for buffer_size in [2048, 1024, 512, 256, 128, 64]:
        for combine_log in [1, 2, 4]:
            filename = f"ycsb_buffer_size/ycsb_buffer-{buffer_size}_combine-{combine_log}"
            metrics = aggregate_metrics(filename)
            if metrics is None:
                continue

            row = [
                buffer_size,
                combine_log,
            ]

            # Format all metric values to 2 decimal places (None stays None)
            for key in header[2:]:
                val = metrics.get(key)
                row.append(f"{val:.2f}" if isinstance(val, (int, float)) else "")

            writer.writerow(row)

print(f"Done. Results saved to {output_file}")
