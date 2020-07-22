import os

class AwsCredentials:
    """
    Class that set enviroment variables for AWS services in boto3
    """
    def __init__(self, conf) -> None:
        self.conf = conf
        self.set_enviroment_variables()

    def set_enviroment_variables(self) -> None:
        """
        Method that set enviroments variables to aws use
        """
        os.environ["AWS_ACCESS_KEY_ID"] = self.conf.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.conf.secret_access_key
