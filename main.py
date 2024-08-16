"""
This script connects to an Oracle database and a BigQuery dataset to perform data ingestion.
It loads data from the Oracle database table into a BigQuery table.
The script also performs some data transformations and creates a final BigQuery table
"""

# Import required libraries
import os
import argparse

from utils.gcp import TableOperations
from utils.oracle import (
    OracleOperations,
    construct_oracle_sql_query,
    get_latest_date_from_bq,
)


def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--oracledb_username", help="Oracle DB Username")
    # parser.add_argument("--oracledb_password", help="Oracle DB Password")
    # parser.add_argument("--oracledb_hostname", help="Oracle DB Hostname")
    # parser.add_argument("--oracledb_port", help="Oracle DB Port")
    # parser.add_argument("--oracledb_sid", help="Oracle DB SID")
    parser.add_argument("--oracledb_table", help="Oracle DB Table")
    parser.add_argument("--gcp_project_id", help="GCP Project ID")
    parser.add_argument("--gcp_dataset", help="GCP Dataset ID")
    parser.add_argument("--gcp_landing_table", help="GCP Landing Table ID")
    parser.add_argument("--gcp_final_table", help="GCP Final Table ID")
    parser.add_argument("--chunksize", help="Chunksize", type=int)
    parser.add_argument("--where_condition_column", help="Date Column")
    parser.add_argument("--start_condition", help="Start Date")
    parser.add_argument("--end_condition", help="End Date")
    parser.add_argument("--bucket_name", help="Bucket Name")
    parser.add_argument("--schema_file_path", help="Schema File Path")
    parser.add_argument("--sql_file_path", help="SQL File Path")

    args = parser.parse_args()

    # oracledb_username = args.oracledb_username
    # oracledb_password = args.oracledb_password
    # oracledb_hostname = args.oracledb_hostname
    # oracledb_port = args.oracledb_port
    # oracledb_sid = args.oracledb_sid
    oracledb_table = args.oracledb_table
    gcp_project_id = args.gcp_project_id
    gcp_dataset = args.gcp_dataset
    gcp_landing_table = args.gcp_landing_table
    gcp_final_table = args.gcp_final_table
    chunksize = args.chunksize
    where_condition_column = args.where_condition_column
    start_condition = args.start_condition
    end_condition = args.end_condition
    bucket_name = args.bucket_name
    schema_file_path = args.schema_file_path
    sql_file_path = args.sql_file_path

    # Pass Oracle DB credentials as environment variables
    oracledb_username = os.environ.get("oracledb_username")
    oracledb_password = os.environ.get("oracledb_password")
    oracledb_hostname = os.environ.get("oracledb_hostname")
    oracledb_port = os.environ.get("oracledb_port")
    oracledb_sid = os.environ.get("oracledb_sid")

    print(
        f"""
          debugging information:
            {oracledb_username=},
            {oracledb_password=},
            {oracledb_hostname=},
            {oracledb_port=},
            {oracledb_sid=},
            {oracledb_table=},
            {gcp_project_id=},
            {gcp_dataset=},
            {gcp_landing_table=},
            {gcp_final_table=},
            {chunksize=},
            {where_condition_column=},
            {start_condition=},
            {end_condition=},
            {bucket_name=},
            {schema_file_path=}
            {sql_file_path=}
          """
    )

    table_operations = TableOperations(
        gcp_project_id,
        gcp_dataset,
        gcp_landing_table,
        bucket_name,
    )

    # Delete the table if it exists
    # table_operations.delete_landing_table()

    # Create the table from schema file
    # table_operations.create_landing_table(schema_file_path)

    oracle_operations = OracleOperations(
        oracledb_username,
        oracledb_password,
        oracledb_hostname,
        oracledb_sid,
        oracledb_port,
        chunksize,
    )

    oracle_operations.fetch_data(
        gcp_project_id,
        gcp_dataset,
        gcp_landing_table,
        bucket_name,
        schema_file_path,
        where_condition_column,
        oracledb_table,
        end_condition,
    )

    # Loading the final table
    table_operations.bq_landing_to_final(gcp_final_table, sql_file_path)


if __name__ == "__main__":
    main()
