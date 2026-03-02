import expr
from custom_command import *

common_cmd = " ".join([
    "../../build/benchmarks/tpcc/tpcc_SI_sequential",
    "-node_memory_gb=40",
    "-null_log_device=0",
    "-pcommit=1",
    "-tls_alloc=1",
    "-tpcc_wh_spread=1",
    "-tpcc_scale_factor=100",
    "-tpcc_new_order_fast_id_gen=1",
    "-tpcc_new_order_remote_item_pct=1",
    "-tpcc_payment_remote_wh_pct=15",
    "-tpcc_log_update_delta=1",
    "-seconds=10",
    "-log_compress=0",
    "-log_direct_io=false",
    "-threads=16",
    "-optimize_dequeue=1",
    '-dependency_aware=0'
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
    filename = "tpcc_delta_buffer_"
    out_dir = "./tpcc_delta_buffer"
    expr.execute(common_cmd, COMMANDS, "tpcc_delta_buffer_",  "./tpcc_delta_buffer", run=True)
    expr.parse(COMMANDS, FIELDS, filename=filename, log_dir=out_dir, out_csv="tpcc_delta_buffer_size.csv")



