import os
import logging
import gzip
import csv

from bq_etl import executeTemplates

if __name__ == '__main__':

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    PATH = 'sql'
    PROJECT = os.environ['PROJECT']
    DATASET = os.environ['DATASET']
    BUCKET = os.environ['BUCKET']
    PARAMS = {'threshold':3}

    tables = executeTemplates(PATH, PROJECT, DATASET, BUCKET, PARAMS)

    # Perform a BQ extract
    tables['main_color_trees'].extract()

    # Download and read the extracted data
    tables['main_color_trees'].download_extract(PATH)

    for fname in os.listdir(PATH):
        if fname.endswith("csv.gz"):
            with gzip.open(os.path.join(PATH, fname), mode='rt') as f:
                for line in csv.reader(f):
                    print(line)
