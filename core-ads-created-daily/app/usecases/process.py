# pylint: disable=no-member
# utf-8
from utils.read_params import ReadParams
from usecases.ads_to_stg import AdsToStg
from usecases.ads_to_ods import AdsToOds

class Process(AdsToStg, AdsToOds):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        #self.save_to_stg_ad_approved()
        #self.save_to_stg_ad_deleted()
        #self.save_to_ods_ad()
        self.ads_to_stg = AdsToStg(self.config,
                                   self.params,
                                   self.logger).generate()
