# A Simple Google BigQuery ETL

Drop a few sql files files into a directory, e.g. foo.sql, bar.sql and
call executeTemplates() with some parameters. Bq_etl will resolve
depndencies between these SQL statements, execute them in correct
order sending the output to a table with a unique name which includes
the hash of the SQL. If the table already exists, it will not be
executed.

# Installation

```
  pip install .
```

# License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
