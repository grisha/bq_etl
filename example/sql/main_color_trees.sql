SELECT species_common_name, t.fall_color
  FROM `bigquery-public-data.new_york_trees.tree_species` t
  JOIN `{main_colors.full_name}` c
    ON t.fall_color = c.fall_color
ORDER BY 1
LIMIT 5
