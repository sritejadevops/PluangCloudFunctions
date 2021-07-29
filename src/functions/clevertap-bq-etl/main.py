import json
import re
from google.cloud import bigquery
from google.cloud import  error_reporting


BQ=bigquery.Client()
ERR_CLIENT=error_reporting.Client()

with open("./config.json") as config:
    data=json.load(config)
    BQ_DATASET=data["dataSetName"]


def streamCsvFilesFromCsToBq(data,context):

    bucket_name=data["bucket"]
    file_name=data["name"]
    dataset_ref=BQ.dataset(BQ_DATASET)

    job_config=bigquery.LoadJobConfig(autodetect=True,
                                      max_bad_records=20,
                                      write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                      source_format=bigquery.SourceFormat.CSV,
                                      skip_leading_rows=1)

    uri="gs://"+bucket_name+"/"+file_name
    bqTable=getBqTableName(file_name)

    try:
        load_job=BQ.load_table_from_uri(uri,
                                        dataset_ref.table(bqTable),
                                        job_config=job_config)

        print("Starting job with id: {}".format(load_job.job_id))
        print("file {}".format(data["name"]))

        load_job.result()
        print("job finished")

        destination_table=BQ.get_table(dataset_ref.table(bqTable))
        print("Total no. of rows: {}".format(destination_table.num_rows))

    except Exception as exception:
        print(exception)
        ERR_CLIENT.report_exception()


def getBqTableName(file_name):

    split_file_name = file_name.split("-")
    word_list_for_table = []
    for value in split_file_name:
        if re.match("[^0-9]", value):
            word_list_for_table.append(value.lower())

    return "_".join(word_list_for_table)





