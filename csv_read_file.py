#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re
import pandas as pd

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import pubsub


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def __init__(self):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super(WordExtractingDoFn, self).__init__()
    beam.DoFn.__init__(self)
    self.words_counter = Metrics.counter(self.__class__, 'words')
    self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
    self.word_lengths_dist = Metrics.distribution(
        self.__class__, 'word_len_dist')
    self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    text_line = element.strip()
    if not text_line:
      self.empty_line_counter.inc(1)
    words = re.findall(r'[\w\']+', text_line, re.UNICODE)
    for w in words:
      self.words_counter.inc()
      self.word_lengths_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
    return words

def datatopubsub(dir_path):
    orders_df = pd.read_csv(dir_path+'olist_orders_dataset.csv')
    orderItems_df = pd.read_csv(dir_path+'olist_order_items_dataset.csv')
    sellers_df = pd.read_csv(dir_path+'olist_sellers_dataset.csv')
    customers_df = pd.read_csv(dir_path+'olist_customers_dataset.csv')
    orderitem_seller_joined_df = pd.merge(
        left=orderItems_df, right=sellers_df, how='left', left_on='seller_id', right_on='seller_id')
    uniqueOrderKeys = orders_df.order_id.unique()

    publisher = pubsub.PublisherClient()

    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id='tura-project-001',
        topic='OrderJson',
    )
    output = ""
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
        output+="/n"+jsonobj
    return output

def run(argv=None, save_main_session=True):
  bucket="tura-project-001"
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default=f'gs://{bucket}/data/raw/lorem_ipsum.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.',
      default=f"gs://{bucket}/tmp")
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  p = beam.Pipeline(options=pipeline_options)

  (p 
      | 'personal function' >> datatopubsub('gs://tura-project-001/data/raw/')
      | 'write' >> WriteToText(known_args.output))

  result = p.run()
  result.wait_until_finish()

  


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
