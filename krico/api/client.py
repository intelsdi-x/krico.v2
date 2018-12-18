"""KRICO API client."""

import grpc

from krico.api.proto import api_pb2 as api_messages
from krico.api.proto import api_pb2_grpc as api_service


class ApiClient(object):
    """Class providing communication between KRICO and client."""

    def __init__(self, ip_address):
        channel = grpc.insecure_channel(ip_address)
        self.stub = api_service.ApiStub(channel)

    def classify(self, instance_id):
        classified_as = self.stub.Classify(api_messages.ClassifyRequest(
            instance_id=instance_id))
        return classified_as

    def predict(self, category, image, parameters,
                availability_zone, allocation):
        requirements = self.stub.Predict(
            api_messages.PredictRequest(
                category=category,
                image=image,
                parameters=parameters,
                availability_zone=availability_zone,
                allocation=allocation
            ))
        return requirements

    def refresh_classifier(self):
        self.stub.RefreshClassifier(api_messages.RefreshClassifierRequest())

    def refresh_predictor(self):
        self.stub.RefreshPredictor(api_messages.RefreshPredictorRequest())

    def refresh_instances(self):
        self.stub.RefreshInstances(api_messages.RefreshInstancesRequest())

    def workloads_categories(self):
        workloads_categories = self.stub.WorkloadsCategories(
            api_messages.WorkloadsCategoriesRequest())
        return workloads_categories

    def load_swan_experiment(self, experiment_id):
        self.stub.LoadSwanExperiment(api_messages.LoadSwanExperimentRequest(
            experiment_id=experiment_id
        ))
