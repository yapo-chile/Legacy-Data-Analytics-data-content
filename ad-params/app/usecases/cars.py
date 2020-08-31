# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query import AdParamsCarsQuery
from utils.read_params import ReadParams


class AdCarParams(AdParamsCarsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def blocket_data_cars(self):
        return self.__blocket_data_cars

    @blocket_data_cars.setter
    def blocket_data_cars(self, config):
        db_source = Database(conf=config)
        blocket_data_cars = db_source.select_to_dict(self.blocket_ad_params())
        db_source.close_connection()
        self.__blocket_data_cars = blocket_data_cars

    @property
    def dwh_data_cars(self):
        return self.__dwh_data_cars

    @dwh_data_cars.setter
    def dwh_data_cars(self, config):
        db_source = Database(conf=config)
        dwh_data_cars = db_source.select_to_dict(self.dwh_ad_params())
        db_source.close_connection()
        self.__dwh_data_cars = dwh_data_cars

    def insert_to_table(self):
        dwh = Database(conf=self.config.db)        
        dwh.insert_copy(self.cleaned_cars, "ods", "ads_cars_params")

    def generate(self):
        self.blocket_data_cars = self.config.blocket
        self.dwh_data_cars = self.config.db
        # Merging blocket and dwh dataset to check if they already
        # exists on final table
        merged_data = self.blocket_data_cars.\
            merge(self.dwh_data_cars.drop_duplicates(), on=['ad_id_nk'], 
                   how='left', indicator=True)
        merged_data = merged_data[merged_data['_merge'] == 'left_only']
        if len(merged_data) >= 1:
            merged_data = merged_data[merged_data.columns[~merged_data.columns.str.endswith('_y')]]
            merged_data.columns = merged_data.columns.str.replace(r'_x$', '')
            del merged_data['_merge']
            self.cleaned_cars = merged_data
            for column in ["ad_id_nk", 
                        "car_year",
                        "car_type",
                        "brand",
                        "model",
                        "version",
                        "mileage",
                        "cubiccms",
                        "fuel",
                        "gearbox"]:
                self.cleaned_cars[column] = pd.to_numeric(self.cleaned_cars[column],
                                                          errors='coerce').convert_dtypes()
                

            self.logger.info("First records as evidence")
            self.logger.info(self.cleaned_cars.head())
            self.insert_to_table()
            self.logger.info("Ad car params succesfully saved")
            return True
        else:
            self.logger.info("No new data to insert")
            return False


