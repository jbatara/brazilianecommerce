""" data_transformation.py is a Dataflow pipeline which reads a file and writes 
its contents to a BigQuery table.
This example reads a json schema of the intended output into BigQuery, 
and transforms the date data to match the format BigQuery expects.
"""

from __future__ import absolute_import
import argparse
import csv
import logging
import os
import pandas as pd


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from google.cloud import pubsub

class DataTransformation:
    """A helper class which contains the logic to translate the file into a JSON
  format to be sent to pubsub."""

    def __init__(self):
        self.dir_path = 'gs://tura-project-001/data/raw'

    def parse_send_method(self):
        """This method reads a csv and loads it into dataframes to be processed into a comprehensive JSON
        """
        orders_df = pd.read_csv(self.dir_path+'olist_orders_dataset.csv')
        orderItems_df = pd.read_csv(self.dir_path+'olist_order_items_dataset.csv')
        sellers_df = pd.read_csv(self.dir_path+'olist_sellers_dataset.csv')
        customers_df = pd.read_csv(self.dir_path+'olist_customers_dataset.csv')
        orderitem_seller_joined_df = pd.merge(left=orderItems_df, right=sellers_df, how='left', left_on='seller_id', right_on='seller_id')
        uniqueOrderKeys = orders_df.order_id.unique()

        publisher = pubsub.PublisherClient()

        topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id='tura-project-001',
            topic='OrderJSON',
        )
        # myfile = open('myjson.json', 'w')
        for orderID in uniqueOrderKeys:
            o_df = orders_df[orders_df['order_id'] == orderID]
            oi_JSON = orderitem_seller_joined_df[orderitem_seller_joined_df['order_id'] == orderID].to_json(
                orient='records')
            c_id = o_df.iloc[0]['customer_id']
            c_JSON = customers_df[customers_df['customer_id']
                                  == c_id].to_json(orient='records')
            jsonobj = '{"order_id": "' + str(orderID) + '", "date": "' + str(
                o_df.iloc[0]['order_purchase_timestamp']) + '", "customer": ' + c_JSON + ', "orderItem": ' + oi_JSON + '} \n'
            publisher.publish(topic_name, jsonobj.encode('utf-8'))


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   Specifically
    # we have the input file to load and the output table to write to.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # It is useful for developing on a small set of data
        default='gs://tura-project-001/data/raw/olist_orders_dataset.csv')
    # parser.add_argument(
    #     '--topic_name', dest='topic_name', required=False,
    #     help='Input file to read.  This can be a local file or '
    #          'a file in a Google Storage Bucket.',
    #     # This example file contains a total of only 10 lines.
    #     # It is useful for developing on a small set of data
    #     default='OrderJSON')
    # parser.add_argument(
    #     '--project_name', dest='project_name', required=False,
    #     help='Input file to read.  This can be a local file or '
    #          'a file in a Google Storage Bucket.',
    #     # This example file contains a total of only 10 lines.
    #     # It is useful for developing on a small set of data
    #     default='tura-project-001')
    # This defaults to the temp dataset in your BigQuery project.  You'll have
    # to create the temp dataset yourself using bq mk temp

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    # DataTransformation is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataTransformation()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should
    # store temp files, and what the project id is.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file.  This is the source of the pipeline.  All further
     # processing starts with lines read from the file.  We use the input
     # argument from the command line.  We also skip the first line which is a
     # header row.
     | 'Read and Send' >> beam.ParDo(data_ingestion.parse_send_method()))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
