CREATE TABLE IF NOT EXISTS `donatewiki_unique` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  contact_id      VARCHAR(32)   CHARACTER SET utf8 DEFAULT '' NOT NULL,
  link_id         VARCHAR(128)  CHARACTER SET utf8 DEFAULT '' NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (utm_source, utm_campaign, contact_id)
);

CREATE TABLE IF NOT EXISTS `donatewiki_counts` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  link_id         VARCHAR(128)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  count           MEDIUMINT(11) UNSIGNED DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY (utm_source, utm_campaign, link_id)
);

