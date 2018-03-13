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
import logging
import os
import sys

import hdf5_getters

# Logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)  # deactivate ES basic logs
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

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
            logging.error("Elasticsearch server not reachable at {}. Abort.".format(self.es_full_host))
            sys.exit(1)

    def check_index_safe(self, force):
        # Check ES index does not exist or force is set
        index_exists = self.es.indices.exists(self.es_index)
        if index_exists and not force:
            logging.error(
                "An index named {} already exists on host. Use option --force (-f) to bypass this test. Abort."
                .format(self.es_index))
            sys.exit(1)


class TrackGenerator:
    """
    This class will generate the next MSD track to be ingested
    in the ES. The returned hdf5 table object will contain the
    metadata to ingest.

    The files can either come from the summary file (a huge hdf5
    file containing 1 million song) or a file structure (one
    million of a small h5 files)
    """
    def __init__(self):
        self.num_songs = 0
        self.msd_summary_file = None
        self.h5_fd = None

    def load_from_summary(self, msd_summary_file):
        self.msd_summary_file = msd_summary_file
        self.check_msd_summary_file_exists_or_abort()

        self.h5_fd = hdf5_getters.open_h5_file_read(self.msd_summary_file)
        self.num_songs = int(hdf5_getters.get_num_songs(self.h5_fd))
        logging.debug("Found {} songs in summary file".format(self.num_songs))

    def check_msd_summary_file_exists_or_abort(self):
        file_exists = os.path.exists(self.msd_summary_file)
        if not file_exists:
            logging.error("Could not find MSD summary file located at '{}'. Abort.".format(self.msd_summary_file))
            sys.exit(1)

    def get_track(self):
        for sng_idx in range(0, self.num_songs):
            yield self.h5_fd, sng_idx

    def close(self):
        self.h5_fd.close()


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
                    logging.debug("ERROR. AttributeError. {}".format(e))
                    pass

            es_bulk_docs[msd_id] = msd_doc

            # Ingest bulk if size is enough
            if len(es_bulk_docs) == es_bulk_size:
                logging.debug("{} files read. Bulk ingest.".format(sng_idx + 1))
                logging.debug("Last MSD id read: {}".format(msd_id))
                self.es_helper.ingest_to_es(es_bulk_docs)
                es_bulk_docs = {}

        if len(es_bulk_docs) > 0:
            logging.debug("{} files read. Bulk ingest.".format(sng_idx + 1))
            logging.debug("Last MSD id read: {}".format(msd_id))
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
    parser.add_argument("-m", "--msdsummaryfile", help="MSD summary file.", default=MSD_SUMMARY_FILE)
    parser.add_argument("-f", "--force", help="Force writing in existing ES index.", default=False, action="store_true")
    args = parser.parse_args()

    # Setup elasticsearch
    eshelper = Eshelper(args.eshost, args.esport, args.esindex, args.estype)
    force_index = bool(args.force)
    eshelper.check_host_reachable()
    eshelper.check_index_safe(force_index)

    # Setup track generator
    track_gen = TrackGenerator()
    track_gen.load_from_summary(args.msdsummaryfile)
    track_gen.check_msd_summary_file_exists_or_abort()

    # Setup ingestor
    ingestor = Ingestor(eshelper, track_gen)

    ingestor.ingest()
