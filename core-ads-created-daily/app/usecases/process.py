# pylint: disable=no-member
# utf-8
from usecases.ads_to_stg import AdsToStg
from usecases.ads_to_ods import AdsToOds

class Process(AdsToStg, AdsToOds):
    def __init__(self,
                 config,
                 params,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.data_blocket_ads_created_daily = self.config.db
        self.save_to_stg_ad()
        #self.save_to_stg_ad_approved()
        #self.save_to_stg_ad_deleted()
        #self.save_to_ods_ad()
