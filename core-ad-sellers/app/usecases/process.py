# pylint: disable=no-member
# pylint: disable=W0201
# utf-8
from utils.read_params import ReadParams
from usecases.ad_sellers_to_stg import AdSellersToStg
#from usecases.ad_sellers_to_ods import AdSellersToOds


class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        # First step: Getting ad sellers from blocket db data
        # to stg schema into dwh
        self.ad_sellers_to_stg = AdSellersToStg(self.config,
                                                self.params,
                                                self.logger).generate()

        # Second step: Getting ad sellers from DWH stg schema to DWH ods schema
        #self.ad_sellers_to_ods = AdSellersToOds(self.config,
        #                                        self.params,
        #                                        self.logger).generate()
