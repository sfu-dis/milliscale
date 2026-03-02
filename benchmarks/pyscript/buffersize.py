from custom_command import *
import os
import subprocess

common_cmd = [
    "../../build/benchmarks/ycsb/ycsb_SI_sequential",
    "-ycsb_workload=U",
    "-node_memory_gb=40",
    "-null_log_device=0",
    "-pcommit=1",
    "-ycsb_hot_table_size=30000000",
    "-ycsb_cold_table_size=0",
    "-ycsb_ops_per_tx=10",
    "-ycsb_cold_ops_per_tx=10",
    "-ycsb_ops_per_hot_tx=10",
    "-ycsb_hot_tx_percent=1",
    "-ycsb_update_per_tx=10",
    "-seconds=10",
    "-log_direct_io=false",
    "-threads=16",
    "-log_compress=false"
]

def run_cmd(log_dir, filename, num_runs, cmd):
    os.makedirs(log_dir, exist_ok=True)
    for run in range(1, num_runs + 1):
        output_file = os.path.join(log_dir, f"{filename}_run-{run}.txt")
        if os.path.exists(output_file):
            print(f"[SKIP] {output_file} already exists.")
            continue
        print(f"[RUN] {filename}, run={run}")
        with open(output_file, "w") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)

if __name__ == '__main__':
    output = "ycsb_buffer_size"
    varients = [Flusher_1]
    for v in varients:
        name = class_name(v)
        for buffer_size in [2048, 1024, 512, 256, 128, 64]:
            for combine_log in [1, 2, 4]:
                filename = f"ycsb_buffer-{buffer_size}_combine-{combine_log}"
                command = common_cmd + v.command + [f"-log_buffer_kb={buffer_size}", f"-n_combine_log={combine_log}"]
                run_cmd(output, filename, 3, command)

    os.system("shutdown /s /t 0")
