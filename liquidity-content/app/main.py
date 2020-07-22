# utf-8
import sys
import logging
from infraestructure.conf import getConf
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution
from usecases.liquidity import Liquidity


if __name__ == '__main__':
    # Basic init config
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('liquidity-content')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    TIME.get_time()
    PARAMS = ReadParams(sys.argv)
    Liquidity(CONFIG, PARAMS).generate_for_time_frame()
    LOGGER.info('Process ended successfully.')
