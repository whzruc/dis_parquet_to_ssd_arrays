import os
import subprocess
import time
from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa

# Define column names and file partition sizes for each table
schema = {
    'region': ['r_regionkey', 'r_name', 'r_comment'],
    'nation': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'],
    'part': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'],
    'supplier': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'],
    'partsupp': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
    'customer': ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
    'orders': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
    'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
}

split_size = {
    'customer': 319150,
    'lineitem': 600040,
    'nation': 100,
    'orders': 638300,
    'part': 769240,
    'partsupp': 360370,
    'region': 10,
    'supplier': 333340
}

BLOCK_SIZE = 10


def process_files(prefix):
    """
    Process files in the current directory.

    This function iterates over all '.tbl' files in the current directory.
    For each '.tbl' file, it reads the content, removes the trailing '|' character from each line,
    splits the content into blocks based on predefined partition sizes,
    writes the blocks to temporary '.tbl' files, converts them to Parquet format,
    and distributes the Parquet files to specific directories corresponding to the provided prefix list.
    Finally, it constructs SQL-like view statements for each table and returns them.

    Args:
        prefix (list): A list of paths where the Parquet files will be distributed.

    Returns:
        output_strings (list): A list of SQL-like view statements for each processed table.
    """
    output_strings = []
    output_string = ""
    start = 0
    for filename in os.listdir('.'):
        print(f"Processing {filename}")

        if filename.endswith('.tbl'):
            lines_buffer = []
            i = 0
            with open(filename, 'r') as infile:
                table_name = filename[:-4]
                column_names = schema[table_name]
                start = time.time()
                output_string = "CREATE VIEW " + table_name + " AS SELECT * FROM parquet_scan(["
                print("Processing table:", table_name, end=' ')
                for line in infile:
                    # Remove the trailing '|' and add a newline character '\n'.
                    # This part of the logic can also be encapsulated into a function to make the code clearer.
                    processed_line = line.rstrip('|\n') + '\n'
                    lines_buffer.append(processed_line)
                    if len(lines_buffer) == split_size[table_name]:
                        # When reaching the block size, start processing.
                        print("start")
                        table_path = f'partition/{table_name}/'
                        os.makedirs(table_path, exist_ok=True)
                        tbl_name = table_path + table_name + '_' + str(i) + '.tbl'

                        target_path = prefix[i % len(prefix)] + f'{table_name}/{table_name}-{i}.parquet'
                        dir_path = os.path.dirname(target_path)
                        os.makedirs(dir_path, exist_ok=True)
                        # parquet_name=table_path+table_name+'_'+str(i)+'.parquet'
                        with open(tbl_name, 'w') as outfile:
                            for buffer_line in lines_buffer:
                                outfile.write(buffer_line)

                        lines_buffer = []
                        # 
                        table = csv.read_csv(
                            tbl_name,
                            read_options=csv.ReadOptions(column_names=column_names),
                            parse_options=csv.ParseOptions(delimiter='|')
                        )

                        pq.write_table(
                            table,
                            where=target_path,
                            compression='none'
                        )
                        target_path_output = prefix[i % len(prefix)] + f'{table_name}/*.parquet'
                        if i == len(prefix) - 1:
                            output_string += f'\"{target_path_output}\"])'
                        elif i < len(prefix) - 1:
                            output_string += f'\"{target_path_output}\",'
                        else:
                            pass
                        i += 1

                # Process the remaining lines at the end of the file if they are less than BLOCK_SIZE.
                if lines_buffer:
                    table_path = f'partition/{table_name}/'
                    os.makedirs(table_path, exist_ok=True)
                    tbl_name = table_path + table_name + '_' + str(i) + '.tbl'
                    # parquet_name=table_path+table_name+'_'+str(i)+'.parquet'
                    target_path = prefix[i % len(prefix)] + f'{table_name}/{table_name}-{i}.parquet'
                    dir_path = os.path.dirname(target_path)
                    os.makedirs(dir_path, exist_ok=True)
                    with open(tbl_name, 'w') as outfile:
                        for buffer_line in lines_buffer:
                            outfile.write(buffer_line)
                    table = csv.read_csv(
                        tbl_name,
                        read_options=csv.ReadOptions(column_names=column_names),
                        parse_options=csv.ParseOptions(delimiter='|')
                    )
                    column_encodings = {
                        "int_column": "RLE",  # Integer columns use DELTA_BINARY_PACKED encoding
                        "string_column": "PL",  # String columns use PLAIN encoding
                        "byte_array_column": "RLE"  # Byte array columns use DELTA_LENGTH_BYTE_ARRAY encoding
                    }
                    pq.write_table(
                        table,
                        where=target_path,
                        compression='none'
                    )
                    target_path_output = prefix[i % len(prefix)] + f'{table_name}/*.parquet'
                    if i < len(prefix) - 1:
                        output_string += f'\"{target_name_output}\"])'
                    else:
                        pass

        output_strings.append(output_string)
        print(f"- Total time: {time.time() - start:.2f} seconds")
    return output_strings


if __name__ == "__main__":
    # Example of passing the prefix parameter. In actual use, it can be replaced with a real list of paths as needed.
    prefix = []
    schema_name = "tpch-1000"  # Replace with the actual schema name
    folder_name = "parquet_ssd"  # Replace with the actual folder name
    for num in range(1, 25):
        formatted_num = str(num).zfill(2)
        path = f"/data/9a3-{formatted_num}/{schema_name}/{folder_name}/"
        prefix.append(path)
    # prefix = ["/9a3-01/tpch-1/parquet-ssd/", "/9a3-02/tpch-1/parquet-ssd/"]
    output_strings = process_files(prefix)
    for output_string in output_strings:
        print(output_string + ";")
    # print(output_strings)
