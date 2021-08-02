import json
import re
import logging
from google.cloud import bigquery
from google.cloud import error_reporting
import google.cloud.logging

BQ = bigquery.Client()
ERR_CLIENT = error_reporting.Client()
LOG_CLIENT = google.cloud.logging.Client()
LOG_CLIENT.get_default_handler()
LOG_CLIENT.setup_logging()

with open("./config.json") as config_file:
    config_data = json.load(config_file)
    BQ_DATASET = config_data["dataSetName"]


def stream_csv_files_from_cs_to_bq(data, context):

    bucket_name = data["bucket"]
    file_name = data["name"]
    bq_table = get_bq_table_name(file_name)
    dataset_ref = BQ.dataset(BQ_DATASET)

    job_config = bigquery.LoadJobConfig(max_bad_records=20,
                                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                        source_format=bigquery.SourceFormat.CSV,
                                        skip_leading_rows=1)

    job_config.schema = get_bq_schema(bq_table)

    uri = "gs://"+bucket_name+"/"+file_name

    try:
        load_job = BQ.load_table_from_uri(uri,
                                          dataset_ref.table(bq_table),
                                          job_config=job_config)

        logging.info("Starting job with id: {}".format(load_job.job_id))

        logging.info("file {}".format(data["name"]))

        load_job.result()
        logging.info("job finished")

        destination_table = BQ.get_table(dataset_ref.table(bq_table))
        logging.info("Total no. of rows: {}".format(destination_table.num_rows))

    except Exception as exception:
        ERR_CLIENT.report_exception()
        raise exception


def get_bq_table_name(file_name):

    split_file_name = file_name.split("-")
    word_list_for_table = []
    for value in split_file_name:
        if re.match("[^0-9]", value):
            word_list_for_table.append(value.lower())

    return "_".join(word_list_for_table)


def get_bq_schema(bq_table):

    with open("./schema.json") as schema_file:
        schema_data = json.load(schema_file)
        schema_config = []
        for column in schema_data[bq_table]:
            schema_config.append(bigquery.SchemaField(column["name"], column["type"], mode=column["mode"]))

    return schema_config





