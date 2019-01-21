"""Data providing component."""
import collections

import krico.core
import krico.core.exception
import krico.core.logger
import krico.database

import numpy

_logger = krico.core.logger.get(__name__)
_configuration = krico.core.configuration

# Names of workloads categories.
CATEGORIES = sorted([
    category['name']
    for category in _configuration['workloads']['categories']
    ])

# Parameter names of workloads.
PARAMETERS = {
    category['name']: category['parameters']
    for category in _configuration['workloads']['categories']
    }

# Names of Virtual Machine usage metrics needed in prediction process.
REQUIREMENTS = sorted(_configuration['predictor']['requirements'])

# Names of Virtual Machine usage metrics needed in classification process.
METRICS = sorted(_configuration['classifier']['metrics'])


def get_host_aggregates(configuration_id=None):
    """Return host aggregates from database.

    When configuration id is provides, returns host aggregates for specific
    configuration. If not, return all host aggregates.

    Keyword arguments:
    ------------------
    configuration_id : string
        Name of host aggregate.

    Returns:
    --------
    host_aggregates: list of HostAggregate objects
    """
    if configuration_id:
        list(krico.database.HostAggregate.objects.filter(
            configuration_id=configuration_id).allow_filtering().all())

    return krico.database.HostAggregate.objects.all()


def get_images_names(category):
    """Return names of images in specific category.

    Images was running on cluster.

    Keyword arguments:
    ------------------
    category: string
        Name of category.

    Returns:
    --------
    images: list()
        List with image names.

    """
    images = []
    for row in krico.database.Image.objects.filter(
            category=category).allow_filtering():
        images.append(row.image)

    return images


def get_classifier_learning_set(category, configuration_id):
    """Prepare classifier learning data for specific host aggregate.

    Keyword arguments:
    ------------------
    category: string
        Name of category.
    configuration_id : string
        Name of host aggregate.

    Returns:
    --------
    x, y

    """
    x = []
    y = []

    instance_query = krico.database.ClassifierInstance.objects.filter(
        configuration_id=configuration_id,
        category=category
    ).allow_filtering()

    for instance in instance_query:
        requirements = collections.OrderedDict(
            sorted(instance.load_measured.items()))
        x.append(requirements.values())
        y.append(CATEGORIES.index(instance.category))

    x = numpy.array(x)
    y = numpy.array(y)

    return x, y


def get_predictor_learning_set(category, image=None):
    """Prepare predictor learning data for category (and image).

    Keyword arguments:
    ------------------
    category : string
        Name of category.

    image : string
        Name of image.

    Returns:
    --------
    x, y

    """
    x = []
    y = []

    if category:

        if image:
            instances_query = krico.database.PredictorInstance.objects.filter(
                category=category,
                image=image
            ).allow_filtering()
        else:
            instances_query = krico.database.PredictorInstance.objects.filter(
                category=category
            ).allow_filtering()

    else:
        raise krico.core.exception.Error(
            "Category is required for this function!")

    for instance in instances_query:
        parameters = collections.OrderedDict(
            sorted(instance.parameters.items()))
        requirements = collections.OrderedDict(
            sorted(instance.requirements.items()))

        x.append(parameters.values())
        y.append(requirements.values())

    x = numpy.array(x)
    y = numpy.array(y)

    return x, y
