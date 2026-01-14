import psutil
import os

process = psutil.Process(os.getpid())

def get_used_mb():
    return process.memory_info().rss / 1024 / 1024
print(f"Used memory: {get_used_mb():.2f} MB")

def get_memory_limit_mb():
    # with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
    #     limit_bytes = int(f.read())
    # return limit_bytes / 1024 / 1024
    pass
#print(f"Memory limit: {get_memory_limit_mb():.2f} MB")

MAX_USAGE_RATIO = 0.80  # stop at 80% to avoid OOM

def memory_guard():
    used = get_used_mb()
    limit = get_memory_limit_mb()

    usage_ratio = used / limit

    print(f"Memory used: {used:.2f} MB / {limit:.2f} MB ({usage_ratio:.1%})")

    if usage_ratio > MAX_USAGE_RATIO:
        raise RuntimeError(
            f"Memory usage exceeded {MAX_USAGE_RATIO:.0%}. "
            f"Stopping job before OOM."
        )

import pandas as pd

df = pd.DataFrame.from_dict({'col1': range(1000000), 'col2': range(1000000)})
print(f"Used memory: {get_used_mb():.2f} MB")