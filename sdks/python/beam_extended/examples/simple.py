"""
A simple example of how to use the MongoDB reader and writer.

If you like, you can test it out with these commands (requires Docker and
virtualenv for python2):

    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install google-cloud-dataflow pymongo
    $ # The following line is optional if mongod is running already
    $ sudo service mongod stop
    $ docker run -p 27017:27017 --name dataflowtest --rm mongo:3.2
    $ docker exec -it dataflowtest mongo
    > use mydb
    > db.mycollection.insert({ _id: ObjectId() })
    > exit
    $ python -m simple_example --view india_dnn
    $ # The following line is optional if mongod was shut down previously
    $ sudo service mongod start

"""

from __future__ import absolute_import

import logging

import apache_beam as beam

from io.mongodbio import ReadFromMongo, WriteToMongo


def transform_doc(document):
    print(document)
    return {'_id': str(document['_id'])}


def run(argv=None):
    """Main entry point; defines and runs the aggregation pipeline."""

    connection_string = 'mongodb://localhost:27017'
    # Can also fetch a connection string from a Google Cloud Storage file.
    # This might be preferable to avoid pickling the mongodb connection string.
    # E.g.
    # connection_string = 'gs://my-bucket/mongo_connection_string.txt'
    # where "mongo_connection_string.txt" contains a single line with the connection string.

    # with beam.Pipeline(runner='DirectRunner', options=PipelineOptions()) as pipeline:
    options = PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'read' >> ReadFromMongo(connection_string, 'mydb', 'mycollection', query={}, projection=['_id'])
         | 'transform' >> beam.Map(transform_doc)
        #  | 'save' >> WriteToMongo(connection_string, 'mydb', 'mycollection'))
         | 'save' >> beam.io.WriteToText(connection_string, 'mydb', 'mycollection'))


if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.INFO)
    
    run()
