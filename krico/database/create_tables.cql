CREATE KEYSPACE IF NOT exists krico
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

DROP TABLE IF exists krico.host_aggregate;
DROP TABLE IF exists krico.image;
DROP TABLE IF exists krico.classifier_instance;
DROP TABLE IF exists krico.classifier_network;
DROP TABLE IF exists krico.predictor_instance;
DROP TABLE IF exists krico.predictor_network;
DROP TYPE IF exists krico.host;
DROP TYPE IF exists krico.flavor;

CREATE TYPE krico.host (
  disk map<text,text>,
  ram map<text,text>,
  name text,
  configuration_id text,
  cpu map<text,text>
);

CREATE TYPE krico.flavor (
  vcpus int,
  disk int,
  ram int,
  name text
);

CREATE TABLE krico.host_aggregate (
  configuration_id text PRIMARY KEY,
  name text,
  cpu map<text,text>,
  ram map<text,text>,
  disk map<text,text>
);

CREATE TABLE krico.image (
  image text PRIMARY KEY,
  category text
);

CREATE TABLE krico.classifier_instance (
  id uuid,
  category text,
  name text,
  configuration_id text,
  parameters map<text,double>,
  host_aggregate FROZEN<host>,
  image text,
  host text,
  instance_id text,
  load_measured map<text,double>,
  stop_time timestamp,
  flavor FROZEN<flavor>,
  start_time timestamp,
  PRIMARY KEY ((configuration_id, category), id)
);

CREATE TABLE krico.classifier_statistic (
  id uuid PRIMARY KEY,
  category text,
  name text,
  metrics map<text,double>,
  configuration_id text
);

CREATE TABLE krico.classifier_network (
  configuration_id text,
  network blob,
  PRIMARY KEY(configuration_id)
);

CREATE TABLE krico.predictor_instance (
  id uuid PRIMARY KEY,
  instance_id text,
  category text,
  image text,
  requirements map<text,double>,
  parameters map<text,double>
);

CREATE TABLE krico.predictor_network (
  id uuid PRIMARY KEY,
  configuration_id text,
  image text,
  category text,
  network blob
);
