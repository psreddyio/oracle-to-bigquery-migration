import pandas as pd
import oracledb
import concurrent.futures
import time

# from utils.gcp import TableOperations, read_json_from_gcs


class OracleOperations:
    def __init__(
        self,
        oracledb_username,
        oracledb_password,
        oracledb_hostname,
        oracledb_sid,
        oracledb_port,
        arraysize,
    ):
        dsn = oracledb.makedsn(oracledb_hostname, oracledb_port, oracledb_sid)
        self.conn = oracledb.connect(
            user=oracledb_username, password=oracledb_password, dsn=dsn
        )
        self.cursor = self.conn.cursor()
        self.cursor.arraysize = arraysize
        # self.table_operations = TableOperations()

    def fetch_data(
        self,
        gcp_project_id,
        gcp_dataset,
        gcp_landing_table,
        bucket_name,
        schema_file_path,
        where_condition_column,
        oracledb_table,
        end_condition,
        retries=10000,
    ):
        if retries < 0:
            raise ValueError("Maximum number of retries exceeded")

        latest_date_loaded = get_latest_date_from_bq(
            gcp_project_id, gcp_dataset, gcp_landing_table, where_condition_column
        )

        query = construct_oracle_sql_query(
            oracledb_table,
            where_condition_column,
            latest_date_loaded,
            end_condition,
        )
        self.cursor.execute(query)
        rows = []
        while True:
            try:
                batch = self.cursor.fetchmany()
                if not batch:
                    break
                rows.extend(batch)

                df = pd.DataFrame(
                    batch, columns=[i[0] for i in self.cursor.description]
                )
                df = df.astype(str)

                table_schema = read_json_from_gcs(bucket_name, schema_file_path)

                df.to_gbq(
                    f"{gcp_project_id}.{gcp_dataset}.{gcp_landing_table}",
                    if_exists="append",
                    table_schema=table_schema,
                )

                # self.cursor().execute("SELECT 1 FROM DUAL")

            except Exception as e:
                time.sleep(60)
                # self.conn.close()
                print(f"An error occurred: {e}")
                print("Reconnecting...")
                # If an error occurs, rerun the function with one less retry
                return self.fetch_data(
                    gcp_project_id,
                    gcp_dataset,
                    gcp_landing_table,
                    bucket_name,
                    schema_file_path,
                    where_condition_column,
                    oracledb_table,
                    end_condition,
                    retries=retries - 1,
                )

        # return pd.DataFrame(rows, columns=[i[0] for i in self.cursor.description])

    def fetch_and_write(
        self,
        query,
        gcp_project_id,
        gcp_dataset,
        gcp_landing_table,
        bucket_name,
        schema_file_path,
    ):
        table_schema = read_json_from_gcs(bucket_name, schema_file_path)
        df = self.fetch_data(query)
        print("data fetched from oracle")
        df = df.astype(str)
        print("data converted to string")
        df.to_gbq(
            f"{gcp_project_id}.{gcp_dataset}.{gcp_landing_table}",
            if_exists="append",
            table_schema=table_schema,
        )


import json
from google.cloud import storage
from google.api_core.exceptions import NotFound


def read_json_from_gcs(bucket_name, schema_file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(schema_file_path)
        json_data = blob.download_as_string()
        return json.loads(json_data)
    except NotFound:
        print(f"The file {schema_file_path} was not found in the bucket {bucket_name}.")
        return None
    except json.JSONDecodeError:
        print(f"The file {schema_file_path} could not be decoded as JSON.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def construct_oracle_sql_query(
    oracledb_table,
    where_condition_column,
    latest_date_loaded,
    end_condition,
):
    sql = f"select * from {oracledb_table}"
    if where_condition_column:
        if latest_date_loaded:
            sql += f" where TO_DATE({where_condition_column}) >= '{latest_date_loaded}'"
        if end_condition:
            sql += f" and TO_DATE({where_condition_column}) < '{end_condition}'"
    print(f"SQL query: {sql}")
    return sql


from google.cloud.exceptions import NotFound
from google.cloud import bigquery


def get_latest_date_from_bq(
    gcp_project_id, gcp_dataset, gcp_landing_table, where_condition_column
):
    try:
        client = bigquery.Client()
        bq_query = f"SELECT FORMAT_TIMESTAMP('%d-%b-%Y',TIMESTAMP(MAX({where_condition_column}))) FROM `{gcp_project_id}.{gcp_dataset}.{gcp_landing_table}`"
        bq_query_job = client.query(bq_query)
        results = bq_query_job.result()
        for row in results:
            latest_date = row[0]
            print(f"Latest date loaded: {latest_date}")
        return latest_date
    except NotFound:
        print("Table not found, returning default date")
        return "01-Jan-0001"
    except Exception as e:
        print(f"An error occurred: {e}")
