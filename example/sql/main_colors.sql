SELECT fall_color, COUNT(1) AS cnt
FROM `bigquery-public-data.new_york_trees.tree_species`
GROUP BY 1
HAVING cnt > {threshold}
