DROP TABLE `donatewiki_counts`;

CREATE VIEW `donatewiki_counts` AS (
  SELECT utm_source, utm_campaign, link_id, COUNT(*) AS count
  FROM donatewiki_unique
  GROUP BY utm_source, utm_campaign, link_id
);
