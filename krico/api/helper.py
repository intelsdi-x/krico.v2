import krico.core.configuration
import krico.analysis.categories

import krico.api.proto.api_pb2 as api_messages

_configuration = krico.core.configuration.root


def get_workloads_categories():
    workloads_categories = _configuration.dictionary()['analysis']['workloads']['categories']
    return workloads_categories


# TODO: Implementation
def classify(instance_id):
    predicted_category = 'unknown'
    return predicted_category


# TODO: Implementation
def predict(request):
    requirements = list()
    requirements.append(api_messages.
                        PredictRequirements(cpu_threads=1.0, disk_iops=1.0, network_bandwidth=1.0, ram_size=1.0))
    return requirements


# TODO: Implementation
def refresh_classifier():
    return NotImplementedError


# TODO: Implementation
def refresh_predictor():
    return NotImplementedError


# TODO: Implementation
def refresh_instances():
    return NotImplementedError
