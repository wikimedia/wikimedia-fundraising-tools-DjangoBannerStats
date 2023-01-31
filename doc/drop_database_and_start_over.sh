#!/bin/bash
DB=dev_pgehres
mysql -e "DROP DATABASE IF EXISTS ${DB}"
mysql -e "CREATE DATABASE ${DB}"
mysql ${DB} < 001_create.sql
mysql ${DB} < 002_populate_countries.sql
mysql ${DB} < 003_populate_languages.sql
