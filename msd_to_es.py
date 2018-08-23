"""
This script will read the MSD summary file, extract
metadata and upload it to an Elasticsearch server.

The Million Song Dataset summary only contains textual
metadata, and not the audio features.
"""

import argparse
import elasticsearch
from elasticsearch.exceptions import ConnectionError
import json
import sys

import hdf5_getters

from log import logger
from track_generator import TrackGeneratorFromDirectory, TrackGeneratorFromSummary

# Location of MSD files. Can be rewritten by command-line arguments.
# Expect a file structure like {}/[A-Z]/[A-Z]/[A-Z]/*.h5
# Will only consider files starting with TR and with h5 extension
MSD_SUMMARY_FILE = "."

# ES configuration.
# Can be rewritten by command-line arguments
ESHOST = "localhost"
ESPORT = 9200
ESINDEX = "msd"
ESTYPE = "msd_track"
es_bulk_size = 1000

# Fields to extract from MSD and add to ES and
# getters to these fields
msd_fields = ['artist_name', 'artist_id', 'artist_location', 'artist_mbid', 'duration',
              'release', 'tempo', 'title', 'year', 'key', 'mode']
getters = filter(lambda x: x.split('get_')[-1] in msd_fields, hdf5_getters.__dict__.keys())


class Eshelper:
    def __init__(self, es_host, es_port, es_index, es_type):
        self.es_full_host = "{}:{}".format(es_host, es_port)
        self.es = elasticsearch.Elasticsearch(self.es_full_host)
        self.es_index = es_index
        self.es_port = es_port
        self.es_type = es_type
        pass

    def ingest_to_es(self, es_bulk_docs):
        bulk_data = ""
        for i, (doc_id, doc_data) in enumerate(es_bulk_docs.iteritems()):
            head = json.dumps({"index": {'_id': doc_id}})
            jdoc = json.dumps(doc_data)
            bulk_data += head + '\n' + jdoc + '\n'

        if bulk_data != "":
            self.es.bulk(index=self.es_index, doc_type=self.es_type, body=bulk_data)
            self.es.indices.flush(self.es_index)

    def check_host_reachable(self):
        try:
            alive = self.es.ping()
            if not alive:
                raise ConnectionError()
        except ConnectionError:
            logger.error("Elasticsearch server not reachable at {}. Abort.".format(self.es_full_host))
            sys.exit(1)

    def check_index_safe(self, force):
        # Check ES index does not exist or force is set
        index_exists = self.es.indices.exists(self.es_index)
        if index_exists and not force:
            logger.error(
                "An index named {} already exists on host. Use option --force (-f) to bypass this test. Abort."
                .format(self.es_index))
            sys.exit(1)


class Ingestor:
    def __init__(self, es_helper, track_generator):
        self.es_helper = es_helper
        self.track_generator = track_generator

    def ingest(self):
        # Browse MSD summary file
        es_bulk_docs = {}
        msd_id = ""
        sng_idx = 0

        for h5_fd, sng_idx in self.track_generator.get_track():
            msd_doc = {}
            msd_id = hdf5_getters.get_track_id(h5_fd, sng_idx)

            for getter in getters:

                field_name = getter.split("get_")[-1]
                msd_field_name = "msd_" + field_name  # prefixed for ES storage
                try:
                    msd_field_value = hdf5_getters.__getattribute__(getter)(h5_fd, sng_idx)

                    # Type conversions
                    msd_field_value = Ingestor.convert_type(msd_field_value)

                    msd_doc[msd_field_name] = msd_field_value
                except AttributeError, e:
                    logger.debug("ERROR. AttributeError. {}".format(e))
                    pass

            es_bulk_docs[msd_id] = msd_doc

            # Ingest bulk if size is enough
            if len(es_bulk_docs) == es_bulk_size:
                logger.debug("{} files read. Bulk ingest.".format(sng_idx + 1))
                logger.debug("Last MSD id read: {}".format(msd_id))
                self.es_helper.ingest_to_es(es_bulk_docs)
                es_bulk_docs = {}

        if len(es_bulk_docs) > 0:
            logger.debug("{} files read. Bulk ingest.".format(sng_idx + 1))
            logger.debug("Last MSD id read: {}".format(msd_id))
            self.es_helper.ingest_to_es(es_bulk_docs)

        self.track_generator.close()

    @staticmethod
    def convert_type(data):
        """
        Will convert type found in hdf5 file to a python
        type neatly convertible to json. i.e. int32 to int
        :param data: The data to convert
        :return: The converted data
        """
        if data.__class__.__name__ == 'ndarray':
            if len(data.shape) > 1:
                raise ValueError("Ndarrays of more than one dimension are not supported")
            data = data.tolist()

        if data.__class__.__name__ == 'int32':
            data = int(data)

        if data.__class__.__name__ == 'float64':
            data = float(data)

        return data


if __name__ == '__main__':
    # Parse arguments first
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--eshost", help="Host of elasticsearch.", default=ESHOST)
    parser.add_argument("-p", "--esport", help="Port of elasticsearch host.", default=ESPORT)
    parser.add_argument("-i", "--esindex", help="Name of index to store to.", default=ESINDEX)
    parser.add_argument("-t", "--estype", help="Type of index to store to.", default=ESTYPE)
    parser.add_argument("-m", "--msdsummaryfile", help="MSD summary file.")
    parser.add_argument("-d", "--msddirectory", help="MSD directory structure.")
    parser.add_argument("-f", "--force", help="Force writing in existing ES index.", default=False, action="store_true")
    args = parser.parse_args()

    # Setup elasticsearch
    eshelper = Eshelper(args.eshost, args.esport, args.esindex, args.estype)
    force_index = bool(args.force)
    eshelper.check_host_reachable()
    eshelper.check_index_safe(force_index)

    # Setup track generator
    if args.msdsummaryfile:
        logger.info("Load summary file {}".format(args.msdsummaryfile))
        track_gen = TrackGeneratorFromSummary()
        track_gen.load(args.msdsummaryfile)
    elif args.msddirectory:
        logger.info("Use directory {}".format(args.msddirectory))
        track_gen = TrackGeneratorFromDirectory()
        track_gen.load(args.msddirectory)
    else:
        logger.error("-m or -d must be given as a parameter")
        sys.exit(1)
    track_gen.check()

    # Setup ingestor
    ingestor = Ingestor(eshelper, track_gen)

    ingestor.ingest()
