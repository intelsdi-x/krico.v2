"""Workload classification component."""

import collections

import pickle

import uuid

import keras

import krico.analysis.converter
import krico.analysis.dataprovider
import krico.analysis.keras_utils
import krico.core
import krico.core.exception
import krico.core.logger
import krico.database

import numpy


_logger = krico.core.logger.get(__name__)
_configuration = krico.core.configuration['classifier']


class _Classifier(object):
    def __init__(self, configuration_id):
        self.configuration_id = configuration_id
        self.x_maxima = []
        krico.analysis.keras_utils.set_backend(_configuration['keras_backend'])
        self._create_model()
        self._compile_model()

    def train(self, data):
        # Save maxima needed for prediction input
        self.x_maxima = numpy.amax(data[0], axis=0)

        # Data normalization
        x = keras.utils.normalize(data[0])

        # Convert y for categorical_crossentropy loss function
        y = keras.utils.to_categorical(
            y=data[1],
            num_classes=len(krico.analysis.dataprovider.CATEGORIES))

        epochs = len(data[0])

        self.model.fit(
            x=x,
            y=y,
            batch_size=_configuration['batch_size'],
            epochs=epochs,
            validation_split=_configuration['validation_split']
        )

    def predict(self, sample):
        sample = collections.OrderedDict(sorted(sample.items()))

        # Prediction operates on matrix (ndim=2)
        sample = numpy.array(sample.values(), ndmin=2)

        # Normalize input
        sample = numpy.true_divide(sample, self.x_maxima)

        # Return best prediction
        return numpy.argmax(self.model.predict(sample))

    def _create_model(self):
        input_size = len(krico.analysis.dataprovider.METRICS)
        output_size = len(krico.analysis.dataprovider.CATEGORIES)

        self.model = keras.Sequential([
            keras.layers.Dense(
                input_size,
                input_shape=(input_size,),
                activation='softmax'
            ),
            keras.layers.Dense(18, activation='sigmoid'),
            keras.layers.Dense(output_size, activation='softmax')
        ])

    def _compile_model(self, learning_rate=0.01, momentum=0.9, decay=1e-4):
        optimizer = keras.optimizers.SGD(learning_rate, momentum, decay)
        self.model.compile(
            loss='categorical_crossentropy',
            optimizer=optimizer,
            metrics=['accuracy']
        )
        _logger.info('Model compiling...')


def classify(instance_id):
    """Predict requirements for specific workload.

    Keyword arguments:
    ------------------
    instance_id : string
        A name of instance.

    Returns:
    --------
    category: string
        A predicted category for workload.

    Example:
    -------
    >> classify('my_instance')

    bigdata
    """
    instance = krico.database.ClassifierInstance.objects.filter(
        instance_id=instance_id).allow_filtering().first()

    monitor_samples = krico.database.MonitorSample.objects.filter(
        instance_id=instance_id.host_aggregate.configuration_id).all()

    classifier = _load_classifier(instance.host_aggregate.configuration_id)

    mean_load = krico.analysis.converter.prepare_mean_sample(
        monitor_samples, krico.analysis.dataprovider.METRICS)

    prediction = classifier.predict(mean_load)

    _logger.info('Category predicted for {} is: {}({})'.format(
        instance.instance_id,
        krico.analysis.dataprovider.CATEGORIES[prediction],
        prediction
    ))

    return krico.analysis.dataprovider.CATEGORIES[prediction]


def refresh():
    """Refresh all classifier networks."""
    _logger.info('Refreshing classifiers.')

    for network in krico.database.ClassifierNetwork.all():
        network.delete()

    for host_aggregate in krico.analysis.dataprovider.get_host_aggregates():
        _logger.info('Refreshing classifier for "{}" host aggregate'.format(
            host_aggregate.configuration_id
        ))

        for category in krico.analysis.dataprovider.CATEGORIES:
            if _enough_samples(category, host_aggregate.configuration_id):
                _create_and_save_classifier(category,
                                            host_aggregate.configuration_id)
            else:
                _logger.warning(
                    'Not enough samples for "{}" host aggregate '
                    'and "{}" category'.format(
                        host_aggregate.configuration_id, category))


def _create_and_save_classifier(category, configuration_id):

    learning_set = krico.analysis.dataprovider.get_classifier_learning_set(
        category,
        configuration_id)

    classifier = krico.database.ClassifierNetwork.objects.filter(
        configuration_id=configuration_id
    ).allow_filtering().first()

    if not classifier:
        classifier = _Classifier(configuration_id)
    else:
        classifier = pickle.load(classifier.network)

    classifier.train(learning_set)

    krico.database.ClassifierNetwork.create(
        configuration_id=configuration_id,
        network=pickle.dumps(classifier)
    )


def _load_classifier(configuration_id):
    classifier = krico.database.ClassifierNetwork.objects.filter(
        configuration_id=configuration_id
    ).allow_filtering().first()

    if not classifier:
        classifier = krico.database.ClassifierNetwork.objects.filter(
            configuration_id=_configuration['default_configuration_id']
        ).allow_filtering().first()

    return pickle.loads(classifier.network)


def _enough_samples(category, configuration_id):

    sample_count = krico.database.ClassifierInstance.objects.filter(
        configuration_id=configuration_id,
        category=category
    ).allow_filtering().count()

    if sample_count < _configuration['minimal_samples']:
        return False

    return True
