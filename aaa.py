import boto3
import awswrangler as wr
import math

# -----------------------------
# Config
# -----------------------------

bucket = "MEU BUCKET"
table_prefix = "KEY/tbl_clientes_struct/"
partition = "anomesdia=20250907/"

prefix = f'{table_prefix}{partition}'


max_group_size = 256 * 1024 * 1024  # 256 MB
s3_path = f"s3://{bucket}/{prefix}"

# -----------------------------
# Step 1: List parquet files under prefix
# -----------------------------
s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

files = []
total_size = 0
for page in pages:
    if "Contents" in page:
        for obj in page["Contents"]:
            #if obj["Key"].endswith(".parquet"):
            size = obj["Size"]
            files.append({"key": obj["Key"], "size": size})
            total_size += size

print(f"Found {len(files)} parquet files")
print(f"Total size: {total_size/1024/1024:.2f} MB")

# -----------------------------
# Step 2: Decide number of groups
# -----------------------------
num_groups = max(1, math.ceil(total_size / max_group_size))
target_size = total_size / num_groups

print(f"Splitting into {num_groups} groups of ~{target_size/1024/1024:.2f} MB each")

# -----------------------------
# Step 3: Distribute files across groups
# -----------------------------
groups = [[] for _ in range(num_groups)]
group_sizes = [0] * num_groups

# Sort files largest → smallest (helps balancing)
files_sorted = sorted(files, key=lambda x: x["size"], reverse=True)

for f in files_sorted:
    # Find group with smallest current size
    idx = group_sizes.index(min(group_sizes))
    groups[idx].append(f)
    group_sizes[idx] += f["size"]

# Show results
for i, g in enumerate(groups):
    total = sum(f["size"] for f in g)
    print(f"Group {i}: {len(g)} files, {total/1024/1024:.2f} MB")

# -----------------------------
# Step 4: Process each group with awswrangler
# -----------------------------
dtype = {
    "id_cliente": "string",
    "dados": "map<string,string>",
    "anomesdia":"string"
}

for i, group in enumerate(groups):
    paths = [f"s3://{bucket}/{f['key']}" for f in group]

    df = wr.s3.read_parquet(path=paths)
    print(f"\nProcessing Group {i}: {len(paths)} files, {len(df)} rows")

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="append",
        dtype=dtype
    )
    print(f"Group {i} written back to {s3_path}")


# -----------------------------
# Step 5: Delete old files
# -----------------------------
for old_files in groups:
    files_to_delete = [{"Key": f["key"]} for f in old_files]
    response = s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": files_to_delete}
    )
    deleted = response.get("Deleted", [])
    print(f"Deleted {len(deleted)} objects in batch {i//1000 + 1}")


# for i in range(0, len(old_files), 1000):
#     batch = old_files[i:i+1000]
#     response = s3.delete_objects(
#         Bucket=bucket,
#         Delete={"Objects": batch}
#     )
#     deleted = response.get("Deleted", [])
#     print(f"Deleted {len(deleted)} objects in batch {i//1000 + 1}")