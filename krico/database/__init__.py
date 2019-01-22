"""Database module."""
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.management import create_keyspace_simple
from cassandra.cqlengine.management import drop_keyspace
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType

import datetime

import json

import uuid

import krico.core
import krico.core.exception
import krico.core.logger

_logger = krico.core.logger.get(__name__)
_configuration = krico.core.configuration['database']


class Host(UserType):
    disk = columns.Map(columns.Text(), columns.Text())
    ram = columns.Map(columns.Text(), columns.Text())
    name = columns.Text()
    configuration_id = columns.Text()
    cpu = columns.Map(columns.Text(), columns.Text())


class Flavor(UserType):
    vcpus = columns.Integer()
    disk = columns.Integer()
    ram = columns.Integer()
    name = columns.Text()


class HostAggregate(Model):
    configuration_id = columns.Text(primary_key=True)
    name = columns.Text()
    cpu = columns.Map(columns.Text(), columns.Text())
    ram = columns.Map(columns.Text(), columns.Text())
    disk = columns.Map(columns.Text(), columns.Text())


class Image(Model):
    image = columns.Text(primary_key=True)
    category = columns.Text()


class ClassifierInstance(Model):
    id = columns.UUID(primary_key=True)
    category = columns.Text(partition_key=True)
    name = columns.Text()
    configuration_id = columns.Text(partition_key=True)
    parameters = columns.Map(columns.Text(), columns.Double())
    host_aggregate = columns.UserDefinedType(Host)
    image = columns.Text()
    host = columns.Text()
    instance_id = columns.Text()
    load_measured = columns.Map(columns.Text(), columns.Double())
    stop_time = columns.DateTime()
    flavor = columns.UserDefinedType(Flavor)
    start_time = columns.DateTime()


class ClassifierNetwork(Model):
    configuration_id = columns.Text(primary_key=True)
    network = columns.Blob()


class PredictorInstance(Model):
    id = columns.UUID(primary_key=True)
    instance_id = columns.Text()
    category = columns.Text()
    image = columns.Text()
    parameters = columns.Map(columns.Text(), columns.Double())
    requirements = columns.Map(columns.Text(), columns.Double())


class PredictorNetwork(Model):
    id = columns.UUID(primary_key=True)
    configuration_id = columns.Text()
    image = columns.Text()
    category = columns.Text()
    network = columns.Blob()


class MonitorSample(Model):
    id = columns.UUID(primary_key=True)
    instance_id = columns.Text()
    configuration_id = columns.Text()
    metrics = columns.Map(columns.Text(), columns.Double())


def connect():
    try:
        connection.setup([_configuration['host']], _configuration['keyspace'])
        _logger.info('Connected to Cassandra database!')
    except CQLEngineException:
        raise krico.core.exception.DatabaseConnectionError(
            'Cannot connect to Cassandra database at {}:{}.'.format(
                _configuration['host'], _configuration['keyspace']))


def delete_database():
    if not connection.session:
        connect()
    drop_keyspace(_configuration['keyspace'])


def fill(classifier_data_path, predictor_data_path):
    connect()
    drop_keyspace(_configuration['keyspace'])
    create_keyspace_simple(_configuration['keyspace'],
                           _configuration['replication_factor'])
    sync_table(Image)
    sync_table(HostAggregate)
    sync_table(ClassifierInstance)
    sync_table(ClassifierNetwork)
    sync_table(PredictorInstance)
    sync_table(PredictorNetwork)

    with open(classifier_data_path) as f:
        data = json.load(f)

    for row in data:
        ClassifierInstance.create(
            id=uuid.UUID(row['id']),
            category=row['category'],
            name=row['name'],
            configuration_id=row['host_aggregate']['configuration_id'],
            parameters=row['parameters'],
            host_aggregate=krico.database.Host(
                disk=row['host_aggregate']['disk'],
                ram=row['host_aggregate']['ram'],
                name=row['host_aggregate']['name'],
                configuration_id=row['host_aggregate']['configuration_id'],
                cpu=row['host_aggregate']['cpu']
            ),
            flavor=krico.database.Flavor(
                vcpus=row['flavor']['vcpus'],
                disk=row['flavor']['disk'],
                ram=row['flavor']['ram'],
                name=row['flavor']['name']
            ),
            image=row['image'],
            host=row['host'],
            instance_id=row['instance_id'],
            load_measured=row['load_measured'],
            stop_time=datetime.datetime.strptime(row['start_time'],
                                                 "%Y-%m-%dT%H:%M:%S.%fZ"),
            start_time=datetime.datetime.strptime(row['start_time'],
                                                  "%Y-%m-%dT%H:%M:%S.%fZ")
        )

        HostAggregate.create(
            disk=row['host_aggregate']['disk'],
            ram=row['host_aggregate']['ram'],
            name=row['host_aggregate']['name'],
            configuration_id=row['host_aggregate']['configuration_id'],
            cpu=row['host_aggregate']['cpu']
        )

    with open(predictor_data_path) as f:
        data = json.load(f)

    for row in data:
        PredictorInstance.create(
            id=uuid.UUID(row['id']),
            instance_id=row['instance_id'],
            image=row['image'],
            category=row['category'],
            requirements=row['requirements'],
            parameters=row['parameters']
        )

        Image.create(
            image=row['image'],
            category=row['category']
        )


if __name__ == '__main__':
    fill('./models/classifier-instances.json',
         './models/predictor-instances.json')
