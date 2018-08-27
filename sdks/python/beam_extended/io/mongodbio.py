"""MongoDB Apache Beam IO utilities.

Tested with google-cloud-dataflow package version 2.0.0

"""

__all__ = ['ReadFromMongo', 'WriteToMongo']

import datetime
import logging
import re
import json

from pymongo import MongoClient

from apache_beam.transforms import PTransform, ParDo, DoFn, Create
from apache_beam.io import iobase, range_trackers

logger = logging.getLogger(__name__)

iso_match = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')

def clean_query(query):
    new_query = {}
    for key, val in query.iteritems():
        if isinstance(val, basestring):
            val = str(val) # Because unicode and 2.7 :-(

        # If the string is an ISO date, turn it into a real datetime object so pymongo can understand it.
        if isinstance(val, basestring) and iso_match.match(val):
            val = datetime.datetime.strptime(val[0:19], '%Y-%m-%dT%H:%M:%S')
        elif isinstance(val, dict):
            val = clean_query(val)
        new_query[str(key)] = val
    return new_query

class _MongoSource(iobase.BoundedSource):
    """A :class:`~apache_beam.io.iobase.BoundedSource` for reading from MongoDB."""

    def __init__(self, url, db, coll, query={}, projection=None):
        """Initializes :class:`_MongoSource`"""
        self._url = url
        self._db = db
        self._coll = coll
        self._query = query
        self._projection = projection
        self._client = None

        # Prepare query
        logger.info('Raw query: {}'.format(query))
        self._query = clean_query(self._query)
        logger.info('Cleaned query: {}'.format(self._query))

    @property
    def client(self):
        """Returns a PyMongo client. The client is not pickable so it cannot
        be part of the main object.

        """
        if self._client:
            logger.info('Reusing existing PyMongo client')
            return self._client

        # Prepare client, assumes a full connection string.
        logger.info('Preparing new PyMongo client')
        self._client = MongoClient(self._url)
        return self._client

    def estimate_size(self):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self.client[self._db][self._coll].count(self._query)

    def get_range_tracker(self, start_position, stop_position):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        coll = self.client[self._db][self._coll]
        for doc in coll.find(self._query, projection=self._projection):
            yield doc

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`

        This function will currently not be called, because the range tracker
        is unsplittable

        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)


class ReadFromMongo(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from MongoDB.

    """
    def __init__(self, url, db, coll, query=None, projection=None):
        """Initializes :class:`ReadFromMongo`

        Uses source :class:`_MongoSource`

        """
        super(ReadFromMongo, self).__init__()
        self._url = url
        self._db = db
        self._coll = coll
        self._query = query
        self._projection = projection
        self._source = _MongoSource(
            self._url,
            self._db,
            self._coll,
            query=self._query,
            projection=self._projection)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        logger.info('Starting MongoDB read from {}.{} with query {}'
                    .format(self._db, self._coll, self._query))
        return pcoll | iobase.Read(self._source)

    def display_data(self):
        return {'source_dd': self._source}

class _MongoSink(iobase.Sink):
    """A :class:`~apache_beam.io.iobase.Sink`."""

    def __init__(self, url, db, coll):
        self._url = url
        self._db = db
        self._coll = coll
        self._client = None

    @property
    def client(self):
        if self._client:
            return self._client

        self._client = MongoClient(self._url)
        return self._client

    def initialize_write(self):
        # Ensure the client is available before any write operation
        # self._client = MongoClient(self._url)
        pass

    def open_writer(self, init_result, uid):
        return _WriteToMongo(self.client[self._db][self._coll], uid)

    def finalize_write(self, init_result, writer_results):
        pass

class _WriteToMongo(iobase.Writer):
    """A :class:`~apache_beam.io.iobase.Writer` for writing to MongoDB."""

    def __init__(self, coll, uid):
        self._coll = coll
        self._uid = uid

    def write(self, document):
        document = json.loads(document)
        self._coll.insert_one(document)

    def close(self):
        pass
        #print '>>>>>>>>>>>>>>>>>>>> close'

class WriteToMongo(PTransform):
    """A :class:`~apache_beam.transforms.PTransform` wrapper for _MongoSink."""

    def __init__(self, url, db, coll):
        super(WriteToMongo, self).__init__()

        self._url = url
        self._db = db
        self._coll = coll
        self._sink = _MongoSink(
            self._url,
            self._db,
            self._coll
        )

    def expand(self, pcoll):
        return pcoll | iobase.Write(self._sink)