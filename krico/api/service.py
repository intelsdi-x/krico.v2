from concurrent import futures
import multiprocessing
import time
import grpc

from krico.api.proto import api_pb2 as api_messages, api_pb2_grpc as api_service

import krico.core.configuration
import krico.core.logger

import krico.core.database
import krico.analysis.classifier
import krico.analysis.predictor.refresh
import krico.api.helper

_configuration = krico.core.configuration.root

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_LOGGER_NAME = 'gRPC API'


class Api(api_service.ApiServicer):
    def Classify(self, request, context):
        classified_as = krico.api.helper.classify(request.instance_id)
        return api_messages.ClassifyResponse(classified_as=classified_as)

    def Predict(self, request, context):
        requirements = krico.api.helper.predict(request)
        return api_messages.PredictResponse(requirements=requirements)

    def RefreshClassifier(self, request, context):
        krico.api.helper.refresh_classifier()
        return api_messages.RefreshClassifierResponse()

    def RefreshPredictor(self, request, context):
        krico.api.helper.refresh_predictor()
        return api_messages.RefreshPredictorResponse()

    def RefreshInstances(self, request, context):
        krico.api.helper.refresh_instances()
        return api_messages.RefreshInstancesResponse()

    def WorkloadsCategories(self, request, context):
        workloads_categories = krico.api.helper.get_workloads_categories()
        return api_messages.WorkloadsCategoriesResponse(workloads_categories=workloads_categories)


class ApiWorker(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self._logger = krico.core.logger.get(_LOGGER_NAME)
        self._logger.info('Initializing ApiWorker')
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        api_service.add_ApiServicer_to_server(Api(), self.server)
        self.server.add_insecure_port(_configuration.service.api.host + ':' + str(_configuration.service.api.port))

    def run(self):
        try:
            self.server.start()
            self._logger.info('Listening')
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)

        except Exception as e:
            self._logger.error('Exception during run.')
            self._logger.error(e)
            self.server.stop(0)
        except:
            self._logger.error('Unknown exception.')

    def terminate(self):
        self._logger.info('ApiWorker is stopped')
        self.server.stop(0)


if __name__ == '__main__':
    worker = ApiWorker()
    worker.start()

