import os
import expr
from custom_command import *

common_cmd = " ".join([
    "../../build/benchmarks/tpcc/tpcc_SI_sequential",
    "-node_memory_gb=40",
    "-null_log_device=0",
    "-pcommit=1",
    "-tls_alloc=1",
    "-tpcc_wh_spread=0",
    "-tpcc_scale_factor=64",
    "-tpcc_new_order_fast_id_gen=1",
    "-tpcc_new_order_remote_item_pct=1",
    "-tpcc_payment_remote_wh_pct=15",
    "-tpcc_log_update_delta=1",
    "-seconds=10",
    "-log_compress=0",
    "-dependency_aware=1",
    "-log_direct_io=false",
])

flusher = [Flusher_1, Flusher_2copy]
varients = [IO2, GP3, GeneralBucket] + flusher
# varients = flusher

COMMANDS = [
    expr.Command(
        name="storage",
        value=[" ".join(v.command) for v in varients],
        pattern="{}",
        suffix="{}",
        symbols=[class_name(v) for v in varients]
    ),
    expr.Command(
        name="threads",
        value=[1, 2, 4, 8, 16],
        pattern="-threads={}",
        suffix="-threads_{}"
    ),
    expr.Command(
        name="buffer_size",
        value=[(1024, 2), (512, 1)],
        pattern="-log_buffer_kb={} -n_combine_log={}",
        suffix="-buffer_{}-combine_{}"
    ),
    expr.Command(
        name="dequeue",
        value=[1, 2],
        pattern="-optimize_dequeue={}",
        suffix="-dequeue_{}",
    ),
    expr.Command(
        name="run",
        value=[1, 2, 3],
        pattern="",
        suffix="-run_{}.txt"
    )
]

if __name__ =='__main__':
    filename = "tpcc_delta_dequeue_aware_"
    out_dir = "./tpcc_delta_dequeue"
    expr.execute(common_cmd, COMMANDS, filename=filename, log_dir=out_dir, run=True)
    expr.parse(COMMANDS, FIELDS, filename=filename, log_dir=out_dir, out_csv="tpcc_delta_dequeue_aware.csv")
