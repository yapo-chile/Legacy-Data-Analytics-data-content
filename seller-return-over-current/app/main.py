# pylint: disable=no-member
# utf-8
import sys
import logging
import pandas as pd
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution

def source_data(params: ReadParams,
                config: getConf):
    query = Query()
    DB_SOURCE = Database(conf=config.db)
    data_current = DB_SOURCE.select_to_dict(query \
                                           .seller_return_over_current(params))
    data_past = DB_SOURCE.select_to_dict(query \
                                        .seller_return_over_past(params))
    DB_SOURCE.close_connection()
    return data_past, data_current



def destiny_data(params: ReadParams,
                 config: getConf,
                 data_past: pd.DataFrame,
                 data_curent: pd.DataFrame) -> None:
    query = Query()
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_past(params))
    DB_WRITE.execute_command(query.delete_current(params))
    DB_WRITE.insert_current(data_curent)
    DB_WRITE.insert_past(data_past)
    DB_WRITE.close_connection()


def end_pipeline(time: TimeExecution, logger: logging) -> None:
    time.get_time()
    logger.info('Process ended successfully.')




if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('data-pipeline-base')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_PAST, DATA_CURRENT = source_data(PARAMS, CONFIG)
    destiny_data(PARAMS, CONFIG, DATA_PAST, DATA_CURRENT)
    end_pipeline(TIME, LOGGER)
