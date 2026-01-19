import boto3
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
import gc

def compact_parquet_files(source_paths, destination_path, memory_threshold_gb=4.0):
    """
    Reads multiple small Parquet files and merges them into a single destination file.
    buffers data in memory up to `memory_threshold_gb` to create large Row Groups.
    """
    fs = s3fs.S3FileSystem()
    
    # 1. Get Schema from first file
    if not source_paths:
        return
    
    try:
        schema = pq.read_schema(source_paths[0], filesystem=fs)
    except Exception as e:
        print(f"Error reading schema: {e}")
        return

    # buffer_list holds the PyArrow tables in RAM before writing
    buffer_list = []
    current_buffer_size_bytes = 0
    
    # Define threshold in bytes (e.g. 4GB)
    # 1 DPU has 16GB. 4GB-6GB is a very safe "Chunk" size.
    threshold_bytes = memory_threshold_gb * 1024**3 

    print(f"Target Destination: {destination_path}")
    print(f"Memory Buffer Threshold: {memory_threshold_gb} GB")

    # 2. Open the Single Output File
    with fs.open(destination_path, 'wb') as f_out:
        with pq.ParquetWriter(f_out, schema=schema, compression='snappy') as writer:
            
            # 3. Iterate through all source files
            for file_path in source_paths:
                
                # Read file into memory (PyArrow is more efficient than Pandas)
                # We read the whole small file because it's small (10MB)
                try:
                    with fs.open(file_path, 'rb') as f_in:
                        table = pq.read_table(f_in)
                except Exception as e:
                    print(f"Skipping corrupt file {file_path}: {e}")
                    continue

                # Add to buffer
                buffer_list.append(table)
                current_buffer_size_bytes += table.nbytes # Exact RAM usage of uncompressed data
                
                # 4. Check if Buffer is Full
                if current_buffer_size_bytes >= threshold_bytes:
                    print(f"Buffer full ({current_buffer_size_bytes / 1024**3:.2f} GB). Flushing...")
                    
                    # Merge all small tables in buffer into one big table
                    merged_table = pa.concat_tables(buffer_list)
                    
                    # Write as ONE BIG Row Group to the output file
                    writer.write_table(merged_table)
                    
                    # 5. CLEAR MEMORY IMMEDIATELY
                    del merged_table
                    del buffer_list[:] # Clear list in place
                    buffer_list = []
                    current_buffer_size_bytes = 0
                    gc.collect() # Force cleanup
            
            # 6. Flush remaining data after loop finishes
            if buffer_list:
                print(f"Flushing final buffer ({current_buffer_size_bytes / 1024**3:.2f} GB)...")
                merged_table = pa.concat_tables(buffer_list)
                writer.write_table(merged_table)
                del merged_table
                gc.collect()

    print("Compaction complete. 1 file created.")

# --- Usage ---
input_files = [
    "s3://bucket/small-1.parquet",
    "s3://bucket/small-2.parquet",
    # ... imagine 50 files here ...
]
output_file = "s3://bucket/compacted-data.parquet"

compact_parquet_files(input_files, output_file, memory_threshold_gb=4.0)