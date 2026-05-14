## Milliscale

Milliscale is a high-performance database system optimised for object storage and main memory. 
It offers low-latency, high throughput and low cost OLTP by leveraging low-latency object storage (AWS S3 Express One Zone) and in-memory and multicore optimisations.

See more details in our technical report here: https://arxiv.org/pdf/2603.02108

## Environment configurations
Step 1: Software dependencies
* cmake
* python3
* gcc-10
* libnuma
* libgflags
* libgoogle-glog
* liburing
* AWS SDK

Example for Ubuntu
```
$ sudo apt-get install cmake gcc-10 g++-10 libnuma-dev libgflags-dev libgoogle-glog-dev liburing-dev
```

Install AWS SDK: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

Step 2: Make sure you have enough huge pages

Milliscale uses `mmap` with `MAP_HUGETLB` (available after Linux 2.6.32) to allocate huge pages. Almost all memory allocations come from the space carved out here. Assuming the default huge page size is 2MB, the command below will allocate 2x MB of memory:
```
$ sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```

Step 3: Set mlock limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your username):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
Step 4: Re-login to apply the changes

## Build
We do not allow building in the source directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug|Release|RelWithDebInfo]
$ make
```
## Run
Required options:
* -log_data_dir=[path] (path to the directory where the log files will be stored)
* -node_memory_gb=[#] (the amount of memory in GB per NUMA socket)
* -seconds=[#]
* -threads=[#]

YCSB
```
./benchmarks/ycsb/ycsb_SI_sequential \
-node_memory_gb=40 \
-null_log_device=0 \
-pcommit=1 \
-tls_alloc=1 \
-log_compress=0 \
-ycsb_workload=F \
-ycsb_hot_table_size=30000000 \
-ycsb_cold_table_size=0 \
-ycsb_ops_per_tx=10 \
-ycsb_cold_ops_per_tx=10 \
-ycsb_ops_per_hot_tx=10 \
-ycsb_hot_tx_percent=1 \
-ycsb_update_per_tx=10 \
-ycsb_log_update_delta=1 \
-threads=16 \
-seconds=10 \
-log_buffer_kb=1024 \
-log_direct_io=0 \
-n_combine_log=1 \
-enable_s3 \
-flusher_thread=1 \
-s3_bucket_names=[bucket_1, bucket_2] \
-optimize_dequeue=1 \
-dependency_aware=0
```

TPC-C
```
./benchmarks/tpcc/tpcc_SI_sequential \
-null_log_device=0 \
-pcommit=1 \
-node_memory_gb=40 \
-tpcc_scale_factor=100 \
-seconds=10 \
-threads=16 \
-log_buffer_kb=2048 \
-tpcc_wh_spread=1 \
-log_compress=0 \
-tls_alloc=1 \
-tpcc_new_order_remote_item_pct=1 \
-tpcc_payment_remote_wh_pct=15 \
-tpcc_new_order_fast_id_gen=1 \
-enable_s3 \
-flusher_thread=1 \
-s3_bucket_names=[bucket_1, bucket_2] \
-tpcc_log_update_delta=1 \
-dependency_aware=0 \
-n_combine_log=2 \
-optimize_dequeue=0
```

## Logging optimization parameters
* `-n_combine_log`: Specify the number of workers per log, n_combine_log = 2 means each log is shared by 2 workers
* `-optimize_dequeue`: 0: Dequeue transactions based on their CSN. 1: Dequeue transactions based on their maximum dependency CSN. 2: Dequeue transactions based on their start timestamp.
* `-dependency_aware`: Dequeue a transaction if it is the head of the queue and all of its **direct** predcessors are local. Also try to place the transaction's log records to the same log as its dependencies'.
* `-enable_s3=1 -flusher_thread=1 -s3_bucket_names=[bucket_1, bucket_2]`: Using S3 Express One Zone for log storage.
