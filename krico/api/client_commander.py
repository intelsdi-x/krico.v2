import sys

import krico.core.commander
import krico.core.configuration
import krico.core.logger
import krico.api.client

_configuration = krico.core.configuration.root
_logger = krico.core.logger.get(__name__)


class _Commander(krico.core.commander.CommandDispatcher):
    @classmethod
    def test_classify(cls):
        print krico.api.client.ApiClient().classify(instance_id='phase-3-science-hpcc-00023')

    @classmethod
    def test_predict(cls):
        print krico.api.client.ApiClient().predict(
            image='bigdata-hadoop-wordcount',
            category='bigdata',
            parameters={
                'data': 32,
                'processors': 24,
                'memory': 32,
                'disk': 50
            },
            availability_zone='some_zone',
            allocation='some_allocation'
        )


def main():
    _Commander.dispatch_commandline()


if __name__ == '__main__':
    sys.exit(main())
