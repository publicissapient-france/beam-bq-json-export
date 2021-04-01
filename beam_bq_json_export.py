import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Big Query table from which to read')

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output GCS bucket to write results to.')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        def dict_without_keys(d, forbidden_keys):
            return {x: d[x] for x in d if x not in forbidden_keys}


        def set_keys(row):
            """
            ex:
            >>> row = {"start_station_id": 351,
                       "end_station_id": 340,
                       "start_date": "2021-01-11",
                       "file_name"="2021-01-11/351_000"}

            >>> set_keys(row)
            ("2021-01-11/351_000", '{"start_station_id": 351, "end_station_id": 340, "start_date": "2021-01-11"}')
            """
            return (row["file_name"], str(dict_without_keys(row, ["file_name"])))


        class WindowedWritesFn(beam.DoFn):
            """
            Write one file per window/key.
            Thank you Guillem Xercavins: https://stackoverflow.com/questions/56234318/write-to-one-file-per-window-in-dataflow-using-python
            """

            def __init__(self, outdir):
                self.outdir = outdir

            def process(self, element):
                (file_name, rows) = element
                rows = list(rows)

                with FileSystems.create(self.outdir + "/{}.json".format(file_name), mime_type="text/plain") as writer:
                    file_content = "[" + ",\n".join(rows) + "]"
                    writer.write(file_content.encode("utf-8"))


        (p
         | 'ReadTable' >> beam.io.gcp.bigquery.ReadFromBigQuery(table=known_args.input)
         | "SetKeys" >> beam.Map(lambda s: set_keys(s))
         | "Grouping keys" >> beam.GroupByKey()
         | 'Windowed Writes' >> beam.ParDo(WindowedWritesFn(known_args.output)))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()