# utf-8
import sys
import logging
from infraestructure.conf import getConf
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution
from usecases.RetentionSellerPacks import RetentionSellerPacks
from usecases.RetentionSellerPacksDetail import RetentionSellerPacksDetail
from usecases.SendEmailSellersPackLeak import SendEmailSellersPackLeak


if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('retention-seller-pack')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    TIME.get_time()
    # Calling main process

    RetentionSellerPacks(CONFIG, PARAMS).generate()
    RetentionSellerPacksDetail(CONFIG, PARAMS).generate()
    SendEmailSellersPackLeak(CONFIG, PARAMS).generate()

    # End process
    LOGGER.info('Process ended successfully.')
