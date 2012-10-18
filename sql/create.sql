CREATE TABLE IF NOT EXISTS `squidlog` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  filename        VARCHAR(128)  NOT NULL,
  impressiontype  VARCHAR(128)  NOT NULL,
  timestamp       TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY (filename)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `squidhost` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  hostname  VARCHAR(128)  NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (hostname)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `squidrecord` (
  id        INT(11)       UNSIGNED AUTO_INCREMENT,
  squid_id  SMALLINT(3)   UNSIGNED,
  sequence  INT(11)       UNSIGNED,
  timestamp TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY (squid_id, sequence)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `project` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  project   VARCHAR(128)  NOT NULL,

  PRIMARY KEY(id),
  UNIQUE KEY(project)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `language` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  language  VARCHAR(128)  DEFAULT '' NOT NULL,
  iso_code  VARCHAR(24)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (iso_code)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `country` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  country   VARCHAR(128)  DEFAULT '' NOT NULL,
  iso_code  VARCHAR(8) NOT NULL,

  PRIMARY KEY(id),
  UNIQUE KEY (iso_code)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `bannerimpression_raw` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  squid_id        SMALLINT(11)  UNSIGNED DEFAULT NULL,
  squid_sequence  INT(11)       UNSIGNED DEFAULT NULL,
  banner          VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  campaign        VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  sample_rate     SMALLINT(4)   UNSIGNED DEFAULT 1,
  processed       TINYINT(1)    DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY (squid_id, squid_sequence, timestamp)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `bannerimpressions` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  banner          VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  campaign        VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  count           MEDIUMINT(11) DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY(timestamp, banner, campaign, project_id, language_id, country_id),
  INDEX (timestamp),
  INDEX (banner),
  INDEX (campaign),
  INDEX (project_id),
  INDEX (language_id),
  INDEX (country_id)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `landingpageimpression_raw` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  squid_id        SMALLINT(11)  UNSIGNED DEFAULT NULL,
  squid_sequence  INT(11)       UNSIGNED DEFAULT NULL,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_medium      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_key         VARCHAR(128)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  landingpage     VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  processed       TINYINT(1)    DEFAULT 0,

  PRIMARY KEY (id),
  INDEX (timestamp),
  INDEX (utm_source),
  INDEX (utm_campaign),
  INDEX (utm_medium),
  INDEX (utm_key),
  INDEX (landingpage),
  INDEX (project_id),
  INDEX (language_id),
  INDEX (country_id),
  INDEX (processed),
  UNIQUE (squid_id, squid_sequence, timestamp)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `landingpageimpressions` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_medium      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  landingpage     VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  count           MEDIUMINT(11) UNSIGNED DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY (timestamp, utm_source, utm_campaign, utm_medium,
          landingpage, project_id, language_id, country_id) -- WE SHOULD OPTIMIZE THIS KEY, WHAT ORDER IS BEST?
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

