from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='./data/dates.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='./outputs/pardo',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:

      class DateExtractor(beam.DoFn):
          def process(self, data_item):
              return (str(data_item).split(','))[0]

      (p
      | 'ReadMyFile 01' >> ReadFromText('./data/dates.csv')
      | 'Splitter using beam.ParDo 01' >> beam.ParDo(DateExtractor())
      | 'Output' >> WriteToText(known_args.output + "_pardo"))

  with beam.Pipeline(options=pipeline_options) as p:

      (p
       | 'ReadMyFile 02' >> ReadFromText('./data/dates.csv')
       | 'Splitter using beam.Map 02' >> beam.Map(lambda record: (record.split(','))[0])
       | 'Output' >> WriteToText(known_args.output + "_map")
       )

  with beam.Pipeline(options=pipeline_options) as p:

      class DateExtractorCorrected(beam.DoFn):
          def process(self, data_item):
              return [(str(data_item).split(','))[0]]

      (p
      | 'ReadMyFile 01' >> ReadFromText('./data/dates.csv')
      | 'Splitter using beam.ParDo 01' >> beam.ParDo(DateExtractorCorrected())
      | 'Output' >> WriteToText(known_args.output + "_pardo_corrected"))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
