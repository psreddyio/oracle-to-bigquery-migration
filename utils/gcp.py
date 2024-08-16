import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import storage


class TableOperations:
    def __init__(
        self,
        gcp_project_id,
        gcp_dataset,
        gcp_landing_table,
        bucket_name,
    ):
        self.PROJECT_ID = gcp_project_id
        self.DATASET_ID = gcp_dataset
        self.TABLE_ID = gcp_landing_table
        self.bucket_name = bucket_name
        self.bigquery_client = bigquery.Client()
        self.table_ref = self.bigquery_client.dataset(gcp_dataset).table(
            gcp_landing_table
        )

    def generate_schema(self):
        schema = []
        try:
            with open("schema.json") as f:
                bigqueryColumns = json.load(f)
                for col in bigqueryColumns:
                    schema.append(
                        bigquery.SchemaField(col["name"], col["type"], col["mode"])
                    )
        except FileNotFoundError:
            print("The file schema.json was not found.")
        except json.JSONDecodeError:
            print("There was an error decoding the JSON.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return schema

    def create_landing_table(self, schema_file_path):
        try:
            # schema = self.generate_schema()
            schema = read_json_from_gcs(self.bucket_name, schema_file_path)
            create_table = bigquery.Table(self.table_ref, schema=schema)
            self.bigquery_client.create_table(create_table)
            print(f"{self.DATASET_ID}.{self.TABLE_ID} is created")
        except NotFound as e:
            print(f"Dataset or table not found: {e}")
        except BadRequest as e:
            print(f"Bad request error (possibly invalid schema): {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def delete_landing_table(self):
        try:
            print(f"Checking if {self.TABLE_ID} table exists in {self.DATASET_ID}")
            table = self.bigquery_client.get_table(self.table_ref)
            self.bigquery_client.delete_table(table)
            print(f"{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID} is deleted")
        except NotFound:
            print(f"{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID} doesn't exist")
        except BadRequest as e:
            print(f"Error deleting {self.DATASET_ID}.{self.TABLE_ID}: {str(e)}")

    def bq_landing_to_final(self, gcp_final_table, sql_file_path):
        self.TABLE_ID_FINAL = gcp_final_table
        sql = read_sql_query_from_gcs(self.bucket_name, sql_file_path)
        job_config = bigquery.QueryJobConfig()
        table_ref = self.bigquery_client.dataset(self.DATASET_ID).table(
            self.TABLE_ID_FINAL
        )
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.destination = table_ref

        try:
            query_job = self.bigquery_client.query(
                sql,
                # Location must match everywhere
                location="US",
                job_config=job_config,
            )

            query_job.result()
            print(
                f"Data load completed for {self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID_FINAL}"
            )
        except BadRequest as e:
            print(f"Bad request error (possibly invalid SQL): {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


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


from google.cloud import storage


def read_sql_query_from_gcs(bucket_name, sql_file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(sql_file_path)
        sql_query = blob.download_as_text()
        return sql_query
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
