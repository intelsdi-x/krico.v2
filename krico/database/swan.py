"""Integration with Swan database module."""
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


import krico.analysis.dataprovider
import krico.database
import krico.core
import krico.core.exception


import uuid

METRIC_BATCH_SIZE = len(krico.analysis.dataprovider.METRICS)

METRIC_NAMES_MAP = {
    'cputime': 'cpu:time',
    'memory/rss': 'ram:used',
    'cache-references': 'cpu:cache:references',
    'cache-misses': 'cpu:cache:misses',
    'wrbytes': 'disk:bandwidth:read',
    'rdbytes': 'disk:bandwidth:write',
    'wrreq': 'disk:operations:read',
    'rdreq': 'disk:operations:write',
    'txbytes': 'network:bandwidth:send',
    'rxbytes': 'network:bandwidth:receive',
    'txpackets': 'network:packets:send',
    'rxpackets': 'network:packets:receive'
}


class Metrics(Model):
    ns = columns.Text(partition_key=True)
    ver = columns.Integer(partition_key=True)
    host = columns.Text(partition_key=True)
    time = columns.DateTime(primary_key=True)
    boolval = columns.Boolean()
    doubleval = columns.Double()
    strval = columns.Text()
    tags = columns.Map(columns.Text(), columns.Text())
    valtype = columns.Text()


class Tags(Model):
    key = columns.Text(partition_key=True)
    val = columns.Text(partition_key=True)
    time = columns.DateTime(primary_key=True)
    ns = columns.Text(primary_key=True)
    ver = columns.Integer(primary_key=True)
    host = columns.Text(primary_key=True)
    boolval = columns.Boolean()
    doubleval = columns.Double()
    strval = columns.Text()
    tags = columns.Map(columns.Text(), columns.Text())
    valtype = columns.Text()


class Metadata(Model):
    experiment_id = columns.Text(primary_key=True)
    timeuuid = columns.TimeUUID(primary_key=True)
    kind = columns.Text()
    metadata = columns.Map(columns.Text(), columns.Text())
    time = columns.DateTime()


def fill(experiment_id):
    # Connect with Cassandra DB
    krico.database.connect()

    # Create query to get metrics from SWAN experiment
    metrics = Metrics.objects().filter(
        tags__contains=experiment_id
    ).allow_filtering()

    # Check if metrics are available
    if metrics.count() <= 0:
        raise krico.core.exception.NotEnoughResourcesError(
            "No metrics found for experiment: {0}".format(experiment_id)
        )

    # Count batch number
    batch_number = int(metrics.count()) / METRIC_BATCH_SIZE

    # Check if there are available all batchs with metrics
    if batch_number * METRIC_BATCH_SIZE != int(metrics.count()):
        raise krico.core.exception.NotEnoughResourcesError(
            "Not all batches with metrics are available!")

    # Save host aggregate
    metric = metrics.first()

    krico.database.HostAggregate(
        configuration_id=metric.tags[
            'host_aggregate_configuration_id'],
        name=metric.tags['host_aggregate_name'],
        disk={
            'iops':
                metric.tags['host_aggregate_disk_iops'],
            'size':
                metric.tags['host_aggregate_disk_size']
        },
        ram={
            'bandwidth':
                metric.tags['host_aggregate_ram_bandwidth'],
            'size':
                metric.tags['host_aggregate_ram_size']
        },
        cpu={
            'performance':
                metric.tags['host_aggregate_cpu_performance'],
            'threads':
                metric.tags['host_aggregate_cpu_threads']
        }
    ).save()

    # Save metrics
    classifier_instances = []
    predictor_instances = []
    load_measured = {}
    iter_counter = 0

    for metric in metrics:
        # Group metrics
        if metric.ns not in load_measured:
            load_measured[metric.ns] = list()
        load_measured[metric.ns].append(metric.doubleval)

        if iter_counter % METRIC_BATCH_SIZE == 0:
            classifier_instances.append(krico.database.ClassifierInstance(
                id=uuid.uuid4(),
                category=metric.tags['category'],
                name=metric.tags['name'],
                configuration_id=metric.tags[
                    'host_aggregate_configuration_id'],
                parameters=_get_parameters(metric),
                host_aggregate=krico.database.Host(
                    name=metric.tags['host_aggregate_name'],
                    configuration_id=metric.tags[
                        'host_aggregate_configuration_id'],
                    disk={
                        'iops':
                            metric.tags['host_aggregate_disk_iops'],
                        'size':
                            metric.tags['host_aggregate_disk_size']
                    },
                    ram={
                        'bandwidth':
                            metric.tags['host_aggregate_ram_bandwidth'],
                        'size':
                            metric.tags['host_aggregate_ram_size']
                    },
                    cpu={
                        'performance':
                            metric.tags['host_aggregate_cpu_performance'],
                        'threads':
                            metric.tags['host_aggregate_cpu_threads']
                    }
                ),
                flavor=krico.database.Flavor(
                    vcpus=metric.tags['flavor_vcpus'],
                    disk=metric.tags['flavor_disk'],
                    ram=metric.tags['flavor_ram'],
                    name=metric.tags['flavor_name']
                ),
                image=metric.tags['image'],
                host=metric.host,
                instance_id=metric.tags['instance_id'],
            ))

            predictor_instances.append(krico.database.PredictorInstance(
                id=uuid.uuid4(),
                image=metric.tags['image'],
                instance_id=metric.tags['name'],
                category=metric.tags['category'],
                parameters=_get_parameters(metric)
            ))

        iter_counter += 1

    # Change SNAP metrics keys to KRICO
    _change_keys(load_measured)

    # Fill
    for i in range(0, batch_number):
        for key in load_measured.keys():
            classifier_instances[i].load_measured[key] = load_measured[key][i]
        classifier_instances[i].save()
        predictor_instances[i].requirements = _get_requirements(
            classifier_instances[i].load_measured)
        predictor_instances[i].save()


def _get_parameters(metric):
    parameters = {}

    for parameter \
            in krico.analysis.dataprovider.PARAMETERS[metric.tags['category']]:
        parameters[parameter] = metric.tags[parameter]

    return parameters


def _get_requirements(metrics):
    return {
        'cpu_threads': metrics['cpu:time'],
        'ram_size': metrics['ram:used'],
        'disk_iops':
            metrics['disk:operations:read']
            + metrics['disk:operations:write'],
        'network_bandwidth':
            metrics['network:bandwidth:send']
            + metrics['network:bandwidth:receive']
    }


def _change_keys(load_measured):

    for metric_name in METRIC_NAMES_MAP.keys():
        for key in load_measured.keys():
            if metric_name in key:
                load_measured[METRIC_NAMES_MAP[metric_name]] \
                    = load_measured[key]
                del load_measured[key]
