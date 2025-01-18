import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db= params.db
    url = params.url
    table_name = params.table_name

    # download the file
    file_name= 'output.parquet'

    curl_command = f"curl -o {file_name} {url}"
    exit_code = os.system(curl_command)
    if exit_code != 0:
        raise RuntimeError(f"Failed to download file from {url} using curl")
    print(f"File downloaded successfully as {file_name}")
    
    engine= create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(file_name)

    print("Creating table headers...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Table headers created...")
    pq_iter = pq.ParquetFile(file_name)

    pq_iter.metadata.num_rows

    chunk_size = 100000

    start_row = 0
    total_rows = pq_iter.metadata.num_rows
    print(f"{total_rows} detected \n Inserting rows into database")
    while start_row < total_rows:
        t_start = time()

        end_row = min(start_row + chunk_size, total_rows)
        chunk = pq_iter.read().to_pandas()[start_row:end_row]
        
        chunk.to_sql(table_name, engine, if_exists='append', index=True)

        t_end = time()

        print(f"Inserted rows {start_row} to {end_row} out of {total_rows}... Time taken: %.3f seconds" % (t_end - t_start))
        
        start_row = end_row



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        prog='ingest_data',
                        description='Injest parquet data to postgres database'
                        )


    parser.add_argument('--user', help='username for postgres database')
    parser.add_argument('--password', help='password for postgres database')
    parser.add_argument('--host', help='hostname for postgres database')
    parser.add_argument('--port', help='port for postgres database')
    parser.add_argument('--db', help='database name for  postgres database')
    parser.add_argument('--table_name', help='name of the table to write results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)







