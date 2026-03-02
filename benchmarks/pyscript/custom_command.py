import expr

class GeneralBucket:
    command = ["-enable_s3", "-is_general_bucket", "-s3_bucket_names=[sfu-general-purpose]"]

# class Onezone:
#     command = ["-enable_s3"]

# class Onezone_3copy:
#     command = [
#        "-enable_s3",
#        "-s3_bucket_names=[sfu-hello-world--use1-az6--x-s3,dis-3--use1-az5--x-s3,dis-2--use1-az4--x-s3]"
#     ]

# class Onezone_2copy:
#     command = [
#        "-enable_s3",
#        "-s3_bucket_names=[sfu-hello-world--use1-az6--x-s3,dis-3--use1-az5--x-s3]"
#     ]

# class SSD:
#     command = ["-log_data_dir=/mnt/localssd1/mosaic-logs"]

class IO2:
    command = ["-log_data_dir=/mnt/io2"]

class GP3:
    command = ["-log_data_dir=/mnt/gp3"]

class Flusher_1:
    command = ["-enable_s3", "-flusher_thread=1"]

class Flusher_2copy:
    command = [
        "-enable_s3", "-flusher_thread=1",
        "-s3_bucket_names=[sfu-hello-world--use1-az6--x-s3,dis-3--use1-az5--x-s3]"
    ]

class Flusher_3copy:
    command = [
       "-enable_s3", "-flusher_thread=1",
       "-s3_bucket_names=[sfu-hello-world--use1-az6--x-s3,dis-3--use1-az5--x-s3,dis-2--use1-az4--x-s3]"
    ]

def class_name(clazz):
    return clazz.__name__


FIELDS = [
    expr.Rule(
        name="throughput",
        regex=r"agg_throughput:\s+([0-9.eE+-]+)\s+ops/sec",
        process=expr.expr_avg
    ),
    expr.Rule(
        name="buffer_fill_time",
        regex=r"avg_logbuf_fill_time:\s+([0-9.eE+-]+)\s+ms",
        process=expr.expr_avg
    ),
    expr.Rule(
        name="flush_count",
        regex=r"Flush count:\s+([0-9.eE+-]+)\s+",
        process=expr.expr_avg
    ),
    *[expr.Rule(
        name=f"{key}_latency",
        regex=rf"{key}_latency:\s+([0-9.eE+-]+)\s+ms",
        process=expr.expr_avg
    ) for key in ["avg", "min", "p50", "p90", "p95", "p99", "p999", "p9999", "max"]]
]

