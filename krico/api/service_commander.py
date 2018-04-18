import os
import signal
import sys
import time
import datetime

import krico.core.configuration
import krico.core.database
import krico.core.exception
import krico.core.executable.daemon
import krico.core.executable.interrupt
import krico.core.logger
import krico.core.timestamp

import krico.analysis.classifier
import krico.analysis.predictor.refresh

import krico.api.service

_configuration = krico.core.configuration.root
_logger = krico.core.logger.get(__name__)


class ServiceDispatcher(krico.core.executable.daemon.DaemonDispatcher):
    _name = 'krico-service'

    @classmethod
    def _execute(cls):
        _logger.info('Starting the KRICO service daemon...')

        worker = krico.api.service.ApiWorker()

        tasks = [
            _PeriodicTask(
                'refresh-models',
                _configuration.service.refresh.models,
                cls.refresh_models
            )
        ]
        
        _logger.info('Starting the KRICO worker...')
        try:
            worker.start()
            _logger.info('KRICO worker started.')

            while True:
                _logger.info('Waiting for task refresh...')
                time.sleep(5)
                
                _logger.info('Refreshing tasks...')
                for task in tasks:
                    try:
                        task.refresh()
                    except:
                        _logger.error('Cannot refresh task {}!'.format(task.name))
                _logger.info('All tasks refreshed.')

        except krico.core.executable.interrupt.TerminateInterrupt:
            _logger.info('Termination requested.')
            raise

        except Exception as ex:
            _logger.error('Fatal error in service daemon: {}'.format(ex))
            raise

        finally:
            os.kill(worker.ident, signal.SIGINT)
            _logger.info('Termination signal sent.')

            worker.join(5)
            worker.terminate()

            _logger.info('KRICO service terminated.')

    @classmethod
    def refresh_models(cls):
        cls.refresh_classifier()
        cls.refresh_predictor()

    @classmethod
    def refresh_classifier(cls):
        krico.analysis.classifier.refresh()

    @classmethod
    def refresh_predictor(cls):
        krico.analysis.predictor.refresh.run()

    @classmethod
    def setup(cls):
        cls.refresh_models()


class _PeriodicTask(object):
    def __init__(self, name, interval, functor):
        object.__init__(self)

        self.__name = name
        self.__functor = functor
        self.__interval = datetime.timedelta(seconds=interval)
        self.__deadline = krico.core.timestamp.now() + self.__interval

    def refresh(self):
        seconds_left = (self.__deadline - krico.core.timestamp.now()).total_seconds()

        if seconds_left <= 0:
            _logger.debug('[Periodic task {}] Deadline reached, executing task...'.format(self.__name))
            self.__functor()

            while self.__deadline <= krico.core.timestamp.now():
                self.__deadline += self.__interval

        else:
            _logger.debug('[Periodic task {}] {} seconds remaining until deadline...'.format(self.__name, seconds_left))
    
    @property
    def name(self):
        return self.__name


def main():
    try:
        krico.core.database.connect()
    except krico.core.exception.Error:
        sys.exit(1)
        
    ServiceDispatcher.dispatch_commandline()


if __name__ == '__main__':
    sys.exit(main())
