import grpc

from krico.api.proto import api_pb2 as api_messages, api_pb2_grpc as api_service

import krico.core.configuration
import krico.core.logger

_logger = krico.core.logger.get(__name__)
_configuration = krico.core.configuration.root


class ApiClient(object):
    def __init__(self):
        channel = grpc.insecure_channel(_configuration.service.api.host+':'+str(_configuration.service.api.port))
        self.stub = api_service.ApiStub(channel)

    def classify(self, instance_id):
        classified_as = self.stub.Classify(api_messages.ClassifyRequest(instance_id=instance_id))
        return classified_as

    def predict(self, category, image, parameters, availability_zone, allocation):
        requirements = self.stub.Predict(
            api_messages.PredictRequest(category=category, image=image, parameters=parameters,
                                        availability_zone=availability_zone, allocation=allocation
                                        ))
        return requirements

    def refresh_classifier(self):
        self.stub.RefreshClassifier(api_messages.RefreshClassifierRequest())

    def refresh_predictor(self):
        self.stub.RefreshPredictor(api_messages.RefreshPredictorRequest())

    def refresh_instances(self):
        self.stub.RefreshInstances(api_messages.RefreshInstancesRequest())

    def workloads_categories(self):
        workloads_categories = self.stub.WorkloadsCategories(api_messages.WorkloadsCategoriesRequest())
        return workloads_categories


if __name__ == '__main__':
    client = ApiClient()

    workloads = client.workloads_categories()

    print(workloads)
