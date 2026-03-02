Our async

```
taskset -c 1-16 \
./benchmarks/ycsb/ycsb_SI_sequential   -node_memory_gb=40   -null_log_device=0   -pcommit=1   -ycsb_workload=U   -ycsb_hot_table_size=30000000   -ycsb_cold_table_size=0   -ycsb_ops_per_tx=10   -ycsb_cold_ops_per_tx=10   -ycsb_ops_per_hot_tx=10   -ycsb_hot_tx_percent=1   -ycsb_update_per_tx=10   -threads=16  -seconds=10 -log_buffer_kb=2048   -log_direct_io=false -flusher_thread=0 -enable_s3 -default_flusher=false -log_compress=false
```
<hr>
async aws sdk

```
taskset -c 1-16 \
./benchmarks/ycsb/ycsb_SI_sequential   -node_memory_gb=40   -null_log_device=0   -pcommit=1   -ycsb_workload=U   -ycsb_hot_table_size=30000000   -ycsb_cold_table_size=0   -ycsb_ops_per_tx=10   -ycsb_cold_ops_per_tx=10   -ycsb_ops_per_hot_tx=10   -ycsb_hot_tx_percent=1   -ycsb_update_per_tx=10   -threads=16  -seconds=10 -log_buffer_kb=2048   -log_direct_io=false -flusher_thread=0 -enable_s3 -default_flusher=true -log_compress=false -loaders=1
```
<hr>
flusher with sync sdk

```
taskset -c 1-16 \
./benchmarks/ycsb/ycsb_SI_sequential   -node_memory_gb=40   -null_log_device=0   -pcommit=1   -ycsb_workload=U   -ycsb_hot_table_size=30000000   -ycsb_cold_table_size=0   -ycsb_ops_per_tx=10   -ycsb_cold_ops_per_tx=10   -ycsb_ops_per_hot_tx=10   -ycsb_hot_tx_percent=1   -ycsb_update_per_tx=10   -threads=16  -seconds=10 -log_buffer_kb=2048   -log_direct_
io=false -flusher_thread=1 -enable_s3 -default_flusher=false -log_compress=false
```
