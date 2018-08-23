import os
import sys

import hdf5_getters

from log import logger


class TrackGeneratorFromSummary:
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

    def load(self, msd_summary_file):
        self.msd_summary_file = msd_summary_file
        self.check()

        self.h5_fd = hdf5_getters.open_h5_file_read(self.msd_summary_file)
        self.num_songs = int(hdf5_getters.get_num_songs(self.h5_fd))
        logger.debug("Found {} songs in summary file".format(self.num_songs))

    def check(self):
        file_exists = os.path.exists(self.msd_summary_file)
        if not file_exists:
            logger.error("Could not find MSD summary file located at '{}'. Abort.".format(self.msd_summary_file))
            sys.exit(1)

    def get_track(self):
        for sng_idx in range(0, self.num_songs):
            yield self.h5_fd, sng_idx

    def close(self):
        self.h5_fd.close()


class TrackGeneratorFromDirectory:
    """
    This class will generate the next MSD track to be ingested
    in the ES. The returned hdf5 table object will contain the
    metadata to ingest.

    The files come from the MSD directory structure, like:
      /A/A/A/TRAAAPK128E0786D96.h5
    """
    def __init__(self):
        self.num_songs = 0
        self.msd_directory = None
        self.h5_fd = None

    def load(self, msd_directory):
        self.msd_directory = msd_directory
        self.check()

    def check(self):
        file_exists = os.path.exists(self.msd_directory)
        file_is_dir = os.path.isdir(self.msd_directory)
        if not file_exists or not file_is_dir:
            logger.error("Could not locate MSD directory at '{}'. Abort.".format(self.msd_directory))
            sys.exit(1)

    def get_track(self):
        sng_idx = 0
        for dirpath, dnames, fnames in os.walk(self.msd_directory):
            for f in fnames:
                if f.endswith(".h5"):
                    path = os.path.join(dirpath, f)
                    hdf5_fd = hdf5_getters.open_h5_file_read(path)
                    yield hdf5_fd, sng_idx
                    hdf5_fd.close()

    def close(self):
        self.h5_fd.close()


if __name__ == "__main__":
    path = "/data/nfs/analysis/audio_database/million_song_dataset/features/MillionSongSubset/"
    track_gen = TrackGeneratorFromDirectory()
    track_gen.load(path)
    track_one = track_gen.get_track().next()

    pass