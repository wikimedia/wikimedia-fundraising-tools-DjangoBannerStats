# NOTE: as of 2023-02-07 this has not been reviewed, and may not properly
# create the view that exists on the production instance.

DROP TABLE `donatewiki_counts`;

CREATE VIEW `donatewiki_counts` AS (
  SELECT utm_source, utm_campaign, link_id, COUNT(*) AS count
  FROM donatewiki_unique
  GROUP BY utm_source, utm_campaign, link_id
);
