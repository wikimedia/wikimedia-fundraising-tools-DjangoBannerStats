DROP TABLE IF EXISTS recenthits;
CREATE TABLE recenthits (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  timestamp timestamp NOT NULL,
  address varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  KEY ts (timestamp),
  KEY ts_address (timestamp, address)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;