import sys
import zstandard
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
import json
from pyspark.sql import SparkSession

def process_all_zst_files(directory_path, query_syntax):
    if zstandard is None:
        raise ImportError("pip install zstandard")
    
    result = []
    
    for zstd_path in file_path:
        archive = Path(zstd_path).expanduser()
        # out_path = Path(outpath).expanduser().resolve()
        # need .resolve() in case intermediate relative dir doesn't exist
    
        dctx = zstandard.ZstdDecompressor()
    
         # Check if the input is a URL
        if zstd_path.startswith(('http://', 'https://')):
            # Stream the file from the URL
            response = requests.get(zstd_path, stream=True)
            reader = dctx.stream_reader(response.raw)
        else:
            # If the input is a file path
            f = open(zstd_path, 'rb')
            reader = dctx.stream_reader(f)
    
        text_stream = io.TextIOWrapper(reader, encoding='utf-8')
        line_counter = 0
        start_time = time.time()
        str_text_stream = text_stream.read()
        # print(str_text_stream)
        data = json.loads(str_text_stream)
        # Monitor read files
        line_counter += 1
        if line_counter % 100_000 == 0:
            elapsed_time = time.time() - start_time
            print(f'Processed {line_counter:,d} lines in {elapsed_time:,.2f} seconds ({line_counter / elapsed_time:,.2f} lines/sec)')
    
        # Don't forget to close the file if it's not a URL
        if not zstd_path.startswith(('http://', 'https://')):
            f.close()
    
        # Convert the dictionary to a JSON string
        json_string = json.dumps(data)
        # Parallelize the list containing the JSON string
        json_rdd = spark.sparkContext.parallelize([json_string])
        # Load the JSON string into a PySpark DataFrame
        df = spark.read.json(json_rdd)
        # # Print the schema and data types
        # df.printSchema()
        # df.show(10)
    
        # Query the df
        df_list = df.select('*').where(query_syntax).collect()
        for row in df_list:
            result.append(row)
    
    if df.isEmpty() == False:
        # Get the schema of the existing DataFrame
        schema = df.schema
        # Create a final DataFrame with the same schema
        final_df = spark.createDataFrame(result, schema)
        final_df.show()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <query_syntax>")
        sys.exit(1)

    # Extract input parameters
    directory_path = sys.argv[1]
    query_syntax = sys.argv[2]

    process_all_zst_files(directory_path, query_syntax)

# Command example: python script.py </path/to/zst_files> <"age > 25 and city == 'Example City'">
