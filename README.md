# ElasticMSD

This project enables you to convert the MSD summary file into an Elasticsearch index.

## Why?

[Elasticsearch](https://www.elastic.co/products/elasticsearch) is a distributed, RESTful search and analytics engine that allows powerful text searches. Although MSD is an audio-featured focused dataset, it also contains metadata that one wants to make research with. 

## Installation

You need the Python elasticsearch and tables packages. I suggest you to work in a Python virtual environment, it's a good practice.

Set up your virtualenv:
```commandline
pip install virtualenv
virtualenv ~/.env/elasticmsd
source ~/.env/elasticmsd/bin/activate
```

Install dependencies:
```commandline
git clone https://github.com/deezer/elasticmsd
cd elasticmsd
pip install -r requirements.txt
```

Install hdf5_getters.py from from [tbertinmahieux/MsongDB repository](https://github.com/tbertinmahieux/MSongsDB/blob/master/PythonSrc/hdf5_getters.py). You must then run a pt2to3 on this file (program shipped with tables package) even if you stay in Python2. hdf5_getters uses an old tables convention:
```commandline
wget https://raw.githubusercontent.com/tbertinmahieux/MSongsDB/master/PythonSrc/hdf5_getters.py -O hdf5_getters_2.py
pt2to3 hdf5_getters_2.py > hdf5_getters.py
rm hdf5_getters_2.py
```

Download MSD summary file:
```commandline
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/msd_summary_file.h5 -O msd_summary_file.h5
```

If you need so, you can install a local instance of an Elasticsearch server via docker:
```commandline
docker run --rm -p 9200:9200 -p 9300:9300 -d --name=local_elasticsearch elasticsearch:2.3
```

## Usage

This command will browse the [MSD summary file](https://labrosa.ee.columbia.edu/millionsong/pages/what-are-song-aggregate-summary-files) (a big h5 file) to an Elasticsearch index

```commandline
python msd_to_es.py \
        -H localhost \
        -p 9200 \ 
        -i research_msd \ 
        -f \ 
        -m msd_summary_file.h5
```

Output logs will look like:
```text
2018-03-13 11:01:13,702 Found 1000000 songs in summary file
2018-03-13 11:01:17,037 1000 files read. Bulk ingest.
2018-03-13 11:01:17,037 Last MSD id read: TRMMENV12903CDDA6A
2018-03-13 11:01:22,221 2000 files read. Bulk ingest.
2018-03-13 11:01:22,221 Last MSD id read: TRMWQUX12903CD7496
```

### Parameters
```commandline
python msd_to_es.py -h
usage: msd_to_es.py [-h] [-H ESHOST] [-p ESPORT] [-i ESINDEX] [-t ESTYPE]
                    [-m MSDSUMMARYFILE] [-f]

optional arguments:
  -h, --help            show this help message and exit
  -H ESHOST, --eshost ESHOST
                        Host of elasticsearch.
  -p ESPORT, --esport ESPORT
                        Port of elasticsearch host.
  -i ESINDEX, --esindex ESINDEX
                        Name of index to store to.
  -t ESTYPE, --estype ESTYPE
                        Type of index to store to.
  -m MSDSUMMARYFILE, --msdsummaryfile MSDSUMMARYFILE
                        MSD summary file.
  -f, --force           Force writing in existing ES index.
```

### Document in ES

The Document in Elasticsearch will look like this:
```json
{
    "msd_tempo" : 120.299,
    "msd_artist_name" : "Darrell Scott",
    "msd_artist_mbid" : "98063361-cdd8-4a9e-b95c-1f29bff780d6",
    "msd_title" : "Shattered Cross",
    "msd_artist_id" : "ARZKPUC1187B99052C",
    "msd_year" : 2006,
    "msd_duration" : 325.53751,
    "msd_mode" : 1,
    "msd_artist_location" : "London, KY",
    "msd_release" : "Transatlantic Sessions - Series 3: Volume One",
    "msd_key" : 9
}
```

