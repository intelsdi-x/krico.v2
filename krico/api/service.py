"""Communication module."""

import signal

import sys

import time

from concurrent import futures

from threading import Thread

import grpc

import krico.analysis.classifier
import krico.analysis.predictor
import krico.core
import krico.core.exception
import krico.core.logger
import krico.database
import krico.database.swan

from krico.api.proto import api_pb2 as api_messages
from krico.api.proto import api_pb2_grpc as api_service

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_configuration = krico.core.configuration['api']
_logger = krico.core.logger.get('krico.api')


class Api(api_service.ApiServicer):
    def Classify(self, request, context):
        classified_as = krico.analysis.classifier.classify(request.instance_id)
        return api_messages.ClassifyResponse(classified_as=classified_as)

    def Predict(self, request, context):
        requirements = krico.analysis.predictor.predict(
            category=request.category,
            image=request.image,
            parameters=request.parameters,
            configuration_id=request.configuration_id,
            allocation_mode=request.allocation
        )
        return api_messages.PredictResponse(requirements=requirements)

    def RefreshClassifier(self, request, context):
        krico.analysis.classifier.refresh()
        return api_messages.RefreshClassifierResponse()

    def RefreshPredictor(self, request, context):
        krico.analysis.predictor.refresh()
        return api_messages.RefreshPredictorResponse()

    # TODO: Implementation
    def RefreshInstances(self, request, context):
        return api_messages.RefreshInstancesResponse()

    def WorkloadsCategories(self, request, context):
        return api_messages.WorkloadsCategoriesResponse(
            workloads_categories=_configuration['workloads']['categories'])

    def LoadSwanExperiment(self, request, context):
        krico.database.swan.fill(request.experiment_id)
        return api_messages.LoadSwanExperimentResponse()


class ApiWorker(Thread):
    def __init__(self):
        super(ApiWorker, self).__init__()
        _logger.info('Initializing ApiWorker')
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        api_service.add_ApiServicer_to_server(Api(), self.server)
        self.server.add_insecure_port('{0}:{1}'.format(
            _configuration['host'], _configuration['port']))
        self.daemon = True
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signal, frame):
        self.server.stop(0)
        _logger.info('ApiWorker is stopped')
        sys.exit(0)

    def run(self):
        try:
            self.server.start()
            _logger.info('Listening on {0}:{1}'.format(
                _configuration['host'], _configuration['port']))
            signal.pause()

        except Exception as e:
            _logger.error(e)
            self.server.stop(0)
            _logger.info('ApiWorker is stopped')


if __name__ == '__main__':
    try:
        krico.database.connect()
    except krico.core.exception.Error:
        sys.exit()

    api = ApiWorker()
    api.start()

    while True:
        time.sleep(_ONE_DAY_IN_SECONDS)
