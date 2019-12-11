import logging


class ConfigFile:
    def __init__(self, config_file):
        self.log = logging.getLogger('conf')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.config_file = config_file
        self.conf = {}
        self.load_conf()

    def get_val(self, key):
        """
        Method that allow get value from dict.
        """
        return self.conf.get(key)

    def load_conf(self):
        """
        Method that read configuration file and load data into dict.
        """
        self.log.info('Load configuration read file %s', self.config_file)
        with open(self.config_file, 'r') as read_file:
            for line in read_file.readlines():
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    self.conf[key] = value
                    self.log.info('%s : %s', key, value)
