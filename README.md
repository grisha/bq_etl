# A Simple Google BigQuery ETL

Tired of messy execution pipelines? Drop a few .sql files into a
directory, e.g. foo.sql, bar.sql and call executeTemplates() with some
parameters. It will resolve dependencies between these SQL statements,
execute them in correct order sending the output to a table with a
unique name which includes the hash of the SQL. If the table already
exists, it will not be executed again. Tables will eventually expire
so you do not need to worry about deleting them.

For more details see [example](https://github.com/grisha/bq_etl/tree/master/example) directory.

The only dependencies of this package are google-cloud-bigquery and
google-cloud-storage.

It is purposely made bare-bones. If you want a feature such as a
specific export format other than .csv.gz, etc. - pull requests are
more than welcome.

# Installation

```
  pip install bq_etl
```
# Description

A module to execute BigQuery ETL jobs comprising of multiple
interdependent SQL statements with no configuration other than the SQL
itself. It can infer dependencies, execute the SQL in correct order,
and optionally extract and download the table data.

Similarly to how compilers compile programs, if any of the steps
(execution, extract or download) are already completed, they are not
repeated again. This module does not need any external tools or
configuration to keep track of its state - it is entirely based on
what is in BigQuery, GCS or your local filesystem.

The key hack is that the actual BigQuery output table names are
postfixed with short hashes of the SQL statements thereby making them
unique and specific to the tables. Changing the SQL will result in a
different table name.

# License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
