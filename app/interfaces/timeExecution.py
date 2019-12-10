import sys
import logging
import datetime
from datetime import timedelta

class timeExecution(object):
    def __init__(self):
        self.start = datetime.datetime.now()
        self.end = datetime.datetime.now()
        self.logger =  logging.getLogger('timeExecution')
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-2s [%(filename)s:%(lineno)d] %(message)s'
                            , level=logging.INFO)

    def getStart(self):
        return self.start

    def getEnd(self):
        return self.end

    def getTime(self) -> None:
        """
        Method [ getTime ] Returns time execution.
        """
        try:
            self.end = datetime.datetime.now()
            difference = self.end - self.start
            self.logger.info('Time Execution : %s ' % difference)
        except Exception as e:
            self.logger.error('%s' % e)
            exit()
