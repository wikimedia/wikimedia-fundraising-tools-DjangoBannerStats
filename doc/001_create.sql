DROP TABLE IF EXISTS bannerimpression_raw;
CREATE TABLE bannerimpression_raw (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL DEFAULT current_timestamp(),
  squid_id int(11) unsigned DEFAULT NULL,
  squid_sequence int(11) unsigned DEFAULT NULL,
  banner varchar(255) NOT NULL DEFAULT '',
  campaign varchar(255) NOT NULL DEFAULT '',
  project_id smallint(3) unsigned DEFAULT NULL,
  language_id smallint(3) unsigned DEFAULT NULL,
  country_id smallint(3) unsigned DEFAULT NULL,
  sample_rate smallint(4) unsigned DEFAULT 1,
  processed tinyint(1) DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY squid_id (squid_id,squid_sequence,timestamp),
  KEY processed (processed),
  KEY sample_rate (sample_rate),
  KEY timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS bannerimpressions;
CREATE TABLE bannerimpressions (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL DEFAULT current_timestamp(),
  banner varchar(255) NOT NULL DEFAULT '',
  campaign varchar(255) NOT NULL DEFAULT '',
  project_id smallint(3) unsigned DEFAULT NULL,
  language_id smallint(3) unsigned DEFAULT NULL,
  country_id smallint(3) unsigned DEFAULT NULL,
  count mediumint(11) DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY timestamp (timestamp,banner,campaign,project_id,language_id,country_id),
  KEY timestamp_2 (timestamp),
  KEY banner (banner),
  KEY campaign (campaign),
  KEY project_id (project_id),
  KEY language_id (language_id),
  KEY country_id (country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS country;
CREATE TABLE country (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  country varchar(128) NOT NULL DEFAULT '',
  iso_code varchar(8) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY iso_code (iso_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS donatewiki_unique;
CREATE TABLE donatewiki_unique (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL DEFAULT current_timestamp(),
  utm_source varchar(255) NOT NULL DEFAULT '',
  utm_campaign varchar(255) NOT NULL DEFAULT '',
  contact_id varbinary(255) DEFAULT NULL,
  link_id varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY utm_source (utm_source,contact_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS landingpageimpression_raw;
CREATE TABLE landingpageimpression_raw (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL DEFAULT current_timestamp(),
  squid_id smallint(11) unsigned DEFAULT NULL,
  squid_sequence int(11) unsigned DEFAULT NULL,
  utm_source varchar(255) NOT NULL DEFAULT '',
  utm_campaign varchar(255) NOT NULL DEFAULT '',
  utm_medium varchar(255) NOT NULL DEFAULT '',
  utm_key varchar(128) NOT NULL DEFAULT '',
  landingpage varchar(255) NOT NULL DEFAULT '',
  project_id smallint(3) unsigned DEFAULT NULL,
  language_id smallint(3) unsigned DEFAULT NULL,
  country_id smallint(3) unsigned DEFAULT NULL,
  processed tinyint(1) DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY squid_id (squid_id,squid_sequence,timestamp),
  KEY timestamp (timestamp),
  KEY utm_source (utm_source),
  KEY utm_campaign (utm_campaign),
  KEY utm_medium (utm_medium),
  KEY utm_key (utm_key),
  KEY landingpage (landingpage),
  KEY project_id (project_id),
  KEY language_id (language_id),
  KEY country_id (country_id),
  KEY processed (processed)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS landingpageimpressions;
CREATE TABLE landingpageimpressions (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  utm_source varchar(255) NOT NULL DEFAULT '',
  utm_campaign varchar(255) NOT NULL DEFAULT '',
  utm_medium varchar(255) NOT NULL DEFAULT '',
  landingpage varchar(255) NOT NULL DEFAULT '',
  project_id smallint(3) unsigned DEFAULT NULL,
  language_id smallint(3) unsigned DEFAULT NULL,
  country_id smallint(3) unsigned DEFAULT NULL,
  count mediumint(11) unsigned DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY timestamp (timestamp,utm_source,utm_campaign,utm_medium,landingpage,project_id,language_id,country_id) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS language;
CREATE TABLE language (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  language varchar(128) NOT NULL DEFAULT '',
  iso_code varchar(24) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY iso_code (iso_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS optout;
CREATE TABLE optout (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  contribution_id int(11) DEFAULT NULL,
  contact_id int(11) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  trxn_id varchar(255) DEFAULT NULL,
  hash varchar(255) DEFAULT NULL,
  hash_conf varchar(255) DEFAULT NULL,
  verified tinyint(1) DEFAULT 0,
  PRIMARY KEY (id),
  KEY contact_id (contact_id),
  KEY contribution_id (contribution_id),
  KEY email (email),
  KEY trxn_id (trxn_id),
  KEY hash (hash),
  KEY hash_conf (hash_conf),
  KEY verified (verified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS project;
CREATE TABLE project (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  project varchar(128) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY project (project)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS squidhost;
CREATE TABLE squidhost (
  id smallint(3) unsigned NOT NULL AUTO_INCREMENT,
  hostname varchar(128) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY hostname (hostname)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS squidlog;
CREATE TABLE squidlog (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  filename varchar(128) NOT NULL,
  impressiontype varchar(128) NOT NULL,
  timestamp timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (id),
  UNIQUE KEY filename (filename)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
