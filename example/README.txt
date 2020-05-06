
This simple example selects most popular colors of New York trees
based on a configurable threshold into a table called "main_colors",
then selects all trees of that color into another table called
"main_color_trees".

You will need credentials, a project, a dataset and a GCS bucket. Run
the example like this:

GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json \
PROJECT=YOUR_PROJECT_NAME \
DATASET=YOUR_DATASET_NAME \
BUCKET=YOUR_GCS_BUCKET_NAME \
python etl.py

First time you run it you should see something like:

2020-05-06 13:05:55 INFO     Reading template 'sql/main_colors.sql'...
2020-05-06 13:05:55 INFO     Reading template 'sql/main_color_trees.sql'...
2020-05-06 13:05:55 INFO     Resolving template parameters...
2020-05-06 13:05:56 INFO     All template parameters resolved.
2020-05-06 13:05:56 INFO     Creating/overwriting table `main_colors_xe33ey` (force: False).
2020-05-06 13:05:57 INFO     Waiting for BigQuery job to finish...
2020-05-06 13:05:58 INFO     BigQuery job finished.
2020-05-06 13:05:59 INFO     Table 'main_colors_xe33ey' expiration set to 2020-05-20 13:05:59.204000+00:00
2020-05-06 13:05:59 INFO     Creating/overwriting table `main_color_trees_7lsxyd` (force: False).
2020-05-06 13:06:00 INFO     Waiting for BigQuery job to finish...
2020-05-06 13:06:01 INFO     BigQuery job finished.
2020-05-06 13:06:02 INFO     Table 'main_color_trees_7lsxyd' expiration set to 2020-05-20 13:06:01.930000+00:00
2020-05-06 13:06:02 INFO     Table main_color_trees_7lsxyd extract already exists, skipping extracting.
2020-05-06 13:06:02 INFO     Downloading '_slt_extracts/main_color_trees_7lsxyd000000000000.csv.gz' to 'sql/main_color_trees_7lsxyd000000000000.csv.gz'...
2020-05-06 13:06:02 INFO     Download of '_slt_extracts/main_color_trees_7lsxyd000000000000.csv.gz' to 'sql/main_color_trees_7lsxyd000000000000.csv.gz' complete.
['species_common_name', 'fall_color']
['American Elm', 'Yellow']
['American Hophornbeam', 'Yellow']
['American Linden', 'Yellow']
['Amur Maackia', 'Yellow']
['Callery Pear', 'Maroon']

Run it again, and notice how steps that are already done are not performed again:

2020-05-06 13:07:34 INFO     Reading template 'sql/main_colors.sql'...
2020-05-06 13:07:34 INFO     Reading template 'sql/main_color_trees.sql'...
2020-05-06 13:07:34 INFO     Resolving template parameters...
2020-05-06 13:07:35 INFO     All template parameters resolved.
2020-05-06 13:07:35 INFO     Table main_colors_xe33ey already exists, skipping execution.
2020-05-06 13:07:35 INFO     Table main_color_trees_7lsxyd already exists, skipping execution.
2020-05-06 13:07:36 INFO     Table main_color_trees_7lsxyd extract already exists, skipping extracting.
2020-05-06 13:07:36 INFO     File 'sql/main_color_trees_7lsxyd000000000000.csv.gz' exists, skipping download.
['species_common_name', 'fall_color']
['American Elm', 'Yellow']
['American Hophornbeam', 'Yellow']
['American Linden', 'Yellow']
['Amur Maackia', 'Yellow']
['Callery Pear', 'Maroon']

That's it!
