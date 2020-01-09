# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution


if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('data-pipeline-base')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    QUERY = Query()
    DB_WRITE = Database(conf=CONFIG.db)
    DB_WRITE.execute_command(QUERY.delete_data(PARAMS))
    DATA = DB_WRITE.select_to_dict(QUERY.seller_return_over_current(PARAMS))
    DB_WRITE.insert_data(DATA)
    DB_WRITE.close_connection()
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
