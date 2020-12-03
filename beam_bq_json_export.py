import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def dict_without_keys(d, forbidden_keys):
        return {x: d[x] for x in d if x not in forbidden_keys}


def set_keys(element):
        return (element["file_name"], str(dict_without_keys(element, ["file_name"])))


class WindowedWritesFn(beam.DoFn):
    """
    Write one file per window/key.
    Thank you Guillem Xercavins: https://stackoverflow.com/questions/56234318/write-to-one-file-per-window-in-dataflow-using-python
    """
    def __init__(self, outdir):
        self.outdir = outdir
        
    def process(self, element):
        (file_name, elements) = element
        elements = list(elements)

        writer = beam.io.filesystems.FileSystems.create(self.outdir + "/{}.json".format(file_name), mime_type="text/plain")
        if len(elements) > 1:
            first_line = "[" + str(elements[0]) + ",\n"
            writer.write(first_line.encode("utf-8"))
            
            for row in elements[1:-1]:
                line = str(row) + ",\n"
                writer.write(line.encode("utf-8"))

            last_line = str(elements[-1]) + "]"
            writer.write(last_line.encode("utf-8"))
        elif len(elements)==1:
            first_line = "[" + str(elements[0]) + "]"
            writer.write(first_line.encode("utf-8"))
        
        writer.close()


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
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'ReadTable' >> beam.io.gcp.bigquery.ReadFromBigQuery(table=known_args.input)
     | "SetKeys" >> beam.Map(lambda s: set_keys(s))
     | "Grouping keys" >> beam.GroupByKey()
     | 'Windowed Writes' >> beam.ParDo(WindowedWritesFn(known_args.output)))
    
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()