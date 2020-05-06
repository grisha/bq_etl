'''
Copyright 2020 Gregory Trubetskoy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import sys
import os
import copy
import base64
import hashlib
import re
import logging
from collections import deque
from datetime import datetime, timedelta

from google.cloud.bigquery.table import _table_arg_to_table_ref
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

log = logging.getLogger(__name__)

class ShortLivedTable(object):

    def __init__(self,
                 project,
                 dataset,
                 name_prefix,
                 sql,
                 bucket=None,
                 gcs_prefix="_bq_etl"):

        self.bq_client = bigquery.Client(project=project)
        self.gcs_client = storage.Client(project=project)
        self.project = project
        self.dataset = dataset
        self.name_prefix = name_prefix
        self.sql = sql
        self.bucket = bucket
        self.gcs_prefix = gcs_prefix
        self.table_ref = _table_arg_to_table_ref(name_prefix, default_project=project)

        # Make sure dataset exists
        if not self.dataset_exists():
            raise NotFound(f"Dataset '{self.table_ref.dataset_id}' does not exist, please create it manually.")

        # Generate and set the table name
        h = self._short_hash(sql+self.name_prefix)
        self.table_ref._table_id = f"{self.table_ref._table_id}_{h}"

    def _short_hash(self, s, sz=6):
        if type(s) is str:
            s = s.encode('utf8')
        return str(base64.b32encode(hashlib.sha1(s).digest()), 'utf8').rstrip('=').lower()[:sz]

    def _full_name(self):
        return f"{self.project}.{self.dataset}.{self.table_ref._table_id}"

    @property
    def full_name(self):
        return self._full_name()

    def dataset_exists(self):
        try:
            return self.bq_client.get_dataset(self.table_ref.dataset_id) is not None
        except NotFound as e:
            return False

    def table_exists(self):
        try:
            return self.bq_client.get_table(self.table_ref) is not None
        except NotFound as e:
            return False

    # https://grisha.org/blog/2016/11/14/table-names-from-sql/
    def _tables_in_sql(self):

        # remove the /* */ comments
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", self.sql)

        # remove whole line -- and # comments
        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

        # remove trailing -- and # comments
        q = " ".join([re.split("--|#", line)[0] for line in lines])

        # split on blanks, parens and semicolons
        tokens = re.split(r"[\s)(;]+", q)

        # scan the tokens. if we see a FROM or JOIN, we set the get_next
        # flag, and grab the next one (unless it's SELECT).

        result = set()
        get_next = False
        for tok in tokens:
            if get_next:
                if tok.lower() not in ["", "select"]:
                    result.add(tok)
                get_next = False
            get_next = tok.lower() in ["from", "join"]

        return result

    def qualify_table(self, table):
        parts = table.strip("`").split(".")
        if len(parts) not in (2,3):
            # invalid table, but that's okay in some cases
            return None

        project = self.bq_client.project
        if len(parts) == 2:
            dataset, table = parts
        else: # == 3
            project, dataset, table = parts

        return f"{project}.{dataset}.{table}"

    def parents(self):
        tables = [self.qualify_table(t) for t in self._tables_in_sql()]
        return [t for t in tables if t is not None]

    def set_expires(self, seconds=60*60*24*14):
        t = self.bq_client.get_table(self.table_ref)
        t.expires = datetime.now() + timedelta(seconds=seconds)
        self.bq_client.update_table(t, ['expires'])
        log.info(f"Table '{self.table_ref.table_id}' expiration set to {t.expires}")

    def execute(self, force=False):
        if force or not self.table_exists():
            log.info(f"Creating/overwriting table `{self.table_ref.table_id}` (force: {force}).")
            self._execute()
            self.set_expires()
            return True
        else:
            log.info(f"Table {self.table_ref.table_id} already exists, skipping execution.")
            return False

    def _execute(self):

        job_config = bigquery.QueryJobConfig(**{
            "destination":self.table_ref,
            "create_disposition":"CREATE_IF_NEEDED",
            "write_disposition":"WRITE_TRUNCATE",
        })

        job = self.bq_client.query(self.sql,
                                   job_config=job_config)

        log.info("Waiting for BigQuery job to finish...")
        job.result()
        log.info("BigQuery job finished.")


    def extract_exists(self):
        if not self.bucket:
            raise Exception("GCS bucket not configured. Pass a bucket argument to constructor.")
        bucket = self.gcs_client.bucket(self.bucket)
        if not bucket.exists():
            raise Exception(f"GCS bucket '{self.bucket}' does not exist, please create it manually.")

        prefix = os.path.join(self.gcs_prefix, self.table_ref.table_id)
        for blob in self.gcs_client.list_blobs(self.bucket, prefix=prefix):
            return True # any match here is all we need

        return False

    def extract(self, force=False):
        if force or not self.extract_exists():
            log.info(f"Extracting table `{self.table_ref.table_id}` data to GCS (force: {force}).")
            self._extract()
            return True
        else:
            log.info(f"Table {self.table_ref.table_id} extract already exists, skipping extracting.")
            return False

    def _extract(self):

        job_config = bigquery.ExtractJobConfig(**{
            "compression":"GZIP",
            "destinationFormat":"CSV",
        })

        name = os.path.join(self.gcs_prefix, self.table_ref.table_id)
        job = self.bq_client.extract_table(self.table_ref, f'gs://{self.bucket}/{name}*.csv.gz', job_config=job_config)

        log.info("Waiting for BigQuery Table Extract job to finish...")
        result = job.result()
        # result.destination_uri_file_counts
        log.info("BigQuery job finished.")


    def download_extract(self, dest_dir):
        if not self.bucket:
            raise Exception("GCS bucket not configured. Pass a bucket argument to constructor.")
        bucket = self.gcs_client.bucket(self.bucket)
        if not bucket.exists():
            raise Exception(f"GCS bucket '{self.bucket}' does not exist, please create it manually.")

        prefix = os.path.join(self.gcs_prefix, self.table_ref.table_id)
        for blob in self.gcs_client.list_blobs(self.bucket, prefix=prefix):
            _, fname = os.path.split(blob.name)
            path = os.path.join(dest_dir, fname)
            if os.path.exists(path):
                log.info(f"File '{path}' exists, skipping download.")
                continue
            log.info(f"Downloading '{blob.name}' to '{path}'...")
            blob.download_to_filename(path, raw_download=True)
            log.info(f"Download of '{blob.name}' to '{path}' complete.")

        return False


def executeTemplates(path, project, dataset, bucket=None, params={}):

    graph = {}
    by_full_name = {}
    by_name = {}

    todo = []
    for fname in os.listdir(path):
        if fname.endswith(".sql") and not fname.startswith('.'):
            name = os.path.splitext(fname)[0]
            fpath = os.path.join(path,fname)
            log.info(f"Reading template '{fpath}'...")
            with open(fpath) as f:
                tmpl = f.read()
                todo.append((name, tmpl))


    log.info("Resolving template parameters...")
    todo = deque(todo) # so we can popleft()
    errcnt, max_errcnt, last_err = 0, len(todo), ''
    while len(todo) > 0:
        name, tmpl = todo.popleft()
        try:
            params.update(by_name) # add tables to params
            sql = tmpl.format(**params)
            t = ShortLivedTable(project, dataset, f"{dataset}.{name}", sql, bucket=bucket)
            by_name[name] = t
            by_full_name[t.full_name] = t
            graph[t.full_name] = t.parents()
        except KeyError as e:
            log.debug(f"... unable to resolve {e} in {name}, will try again later.")
            todo.append((name, tmpl)) # put it back in the queue
            errcnt += 1
            last_err = f"{e} in '{name}'"
        if errcnt >= max_errcnt:
            log.info(f"Unable to resolve {last_err}.")
            raise Exception(f"Infinite cycle detected: either there is a circular reference or unresolvable variables.")
    log.info("All template parameters resolved.")

    # select only the tables we know
    for table in graph:
        our_parents = [parent for parent in graph[table] if parent in graph]
        graph[table] = our_parents

    # execute the table that can be executed
    i, count = 0, 0
    while len(graph) > 0:
        i += 1
        log.debug(f"Execution pass {i}...")
        for name in list(graph.keys()):
            if not graph[name]: # no parents, can execute
                table = by_full_name[name]
                if table.execute():
                    count += 1
                    if count > len(params):
                        raise Exception(f"Execution count ({count}) is exceeding the table count ({len(params)}).")
                # remove this table from parents
                for k in graph:
                    if name in graph[k]:
                        graph[k].remove(name)
                # remove the table itself
                del graph[name]

    return by_name
