import expr
from custom_command import *

common_cmd = " ".join([
    "../../build/benchmarks/ycsb/ycsb_SI_sequential",
    "-node_memory_gb=40",
    "-null_log_device=0",
    "-pcommit=1",
    "-tls_alloc=1",
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
    "-log_compress=false",
    "-dependency_aware=1",
    "-ycsb_log_update_delta=true"
])

flusher = [Flusher_1, Flusher_3copy]
varients = [IO2, GP3, GeneralBucket] + flusher
varients = [Flusher_1]

COMMANDS = [
    expr.Command(
        name="storage",
        value=[" ".join(v.command) for v in varients],
        pattern="{}",
        suffix="{}",
        symbols=[class_name(v) for v in varients]
    ),
    expr.Command(
        name="workload",
        value=["A", "B"],
        pattern="-ycsb_workload={}",
        suffix="-workload_{}"
    ),
    expr.Command(
        name="buffer_size",
        value=[4096, 2048, 1024, 512, 256, 128],
        pattern="-log_buffer_kb={}",
        suffix="-buffer_{}"
    ),
    expr.Command(
        name="combine_logs",
        value=[1, 2, 4],
        pattern="-n_combine_log={}",
        suffix="-combine_{}"
    ),
    expr.Command(
        name="run",
        value=[1, 2, 3],
        pattern="",
        suffix="-run_{}.txt"
    )
]

if __name__ =='__main__':
    filename = "ycsb_delta_buffer_aware_"
    out_dir = "./ycsb_delta_buffer"
    expr.execute(common_cmd, COMMANDS, filename,  out_dir, run=True)
    expr.parse(COMMANDS, FIELDS, filename=filename, log_dir=out_dir, out_csv="ycsb_delta_buffer_size.csv")

