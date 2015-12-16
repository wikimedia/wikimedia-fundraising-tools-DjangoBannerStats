ALTER TABLE donatewiki_unique
DROP KEY utm_source;

ALTER TABLE donatewiki_unique
ADD UNIQUE KEY utm_source (utm_source, contact_id);
