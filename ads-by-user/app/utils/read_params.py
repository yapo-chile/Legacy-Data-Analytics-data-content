import datetime
from datetime import datetime as date_str
from datetime import timedelta
import logging


class ReadParams:
    """
    Class that allow read params by sys.
    """
    def __init__(self, str_parse_params) -> None:
        self.str_parse_params = str_parse_params
        self.start_date = None
        self.end_date = None
        self.master = None
        self.logger = logging.getLogger('readParams')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.load_params()
        self.validate_params()

    def get_start_date(self) -> str:
        """
        Method that get start_date attribute
        """
        return self.start_date.strftime('%Y-%m-%d')

    def get_end_date(self) -> str:
        """
        Method that get end_date attribute
        """
        return self.end_date.strftime('%Y-%m-%d')

    def get_current_year(self) -> str:
        """
        Method that get current_year attribute
        """
        return str(self.start_date.year)

    def get_current_month(self) -> str:
        """
        Method that get current_month attribute
        """
        if self.start_date.month < 10:
            return '0' + str(self.start_date.month)
        return str(self.start_date.month)

    def get_current_day(self) -> str:
        """
        Method that get current_day attribute
        """
        if self.start_date.day < 10:
            return '0' + str(self.start_date.day)
        return str(self.start_date.day)

    def get_last_year(self) -> str:
        """
        Method that get last_year attribute
        """
        return str(int(self.start_date.year) - 1)

    def get_last_year_week(self, delta: int) -> str:
        """
        Method that get last_year_week attribute
        """
        return str((self.\
                    start_date + timedelta(days=delta)).strftime('%Y-%m-%d'))

    def get_inital_day(self, delta) -> datetime:
        tmp_date = datetime.datetime(self.start_date.year - 1, 1, 1)
        return str((tmp_date + timedelta(days=delta)).strftime('%Y-%m-%d'))


    def get_master(self) -> str:
        """
        Method that get master attribute
        """
        return self.master

    def set_start_date(self, start_date: datetime):
        """
        Method that set start_date attribute
        """
        self.start_date = start_date

    def set_end_date(self, end_date: datetime):
        """
        Method that set end_date attribute
        """
        self.end_date = end_date

    def load_params(self) -> None:
        """
        Method [ load_params ] is method that load params into each attribute.
        """
        self.logger.info('Python name : %s ', self.str_parse_params[0])
        for i in range(1, len(self.str_parse_params)):
            self.logger.info('[%s] : %s ', i, self.str_parse_params[i])
            param = self.str_parse_params[i].split("=")
            self.mapping_params(param[0], param[1])

    def mapping_params(self, key: str, value: str) -> None:
        """
        Method [ mapping_params ] is method that join attribute with key.
        Param  [ key ] is the key that be compare with
            params define for assign to attribute.
        Param  [ value ] is value that will be assign to attribute.
        """
        if key == '-start_date':
            self.start_date = date_str.strptime(value, '%Y-%m-%d').date()
        elif key == '-end_date':
            self.end_date = date_str.strptime(value, '%Y-%m-%d').date()
        elif key == '-master':
            self.master = value

    def validate_params(self) -> None:
        """
        Method [ validate_params ] is method validate
        that each attribute have assign a value.
        """
        self.logger.info('Validate params.')
        current_date = datetime.datetime.now()
        if self.start_date is None:
            temp_date = current_date + timedelta(days=-1)
            self.start_date = temp_date.date()
        if self.end_date is None:
            temp_date = current_date + timedelta(days=-1)
            self.end_date = temp_date.date()
        if self.master is None:
            self.master = 'local'

        self.logger.info('Date from : %s', self.start_date)
        self.logger.info('Date to   : %s', self.end_date)
        self.logger.info('Current year : %s', self.get_current_year())
        self.logger.info('Last year : %s', self.get_last_year())
        self.logger.info('Node : %s', self.master)
