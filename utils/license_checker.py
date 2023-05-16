import datetime
import configparser


class LicenseChecker:
    def __init__(self, file):
        self._file = file

    def parse_license_file(self):
        parser = configparser.ConfigParser()
        parser.read(self._file)
        return parser

    def get_file_contents(self):
        with open(self._file, 'rb') as file:
            content = file.readlines()
        return content

    def check_if_license_is_expired(self, expiration_date):
        """
        Match with the expiration date from the license file with system date and check if the license has expired or
        not.
        :param expiration_date: datetime
        :return: boolean
        """
        if str(datetime.datetime.now().date()) > expiration_date:
            return True
        else:
            return False

    def check_if_execution_limit_is_over(self, execution_limit, execution_count):
        """
        Match limit of execution count with an incrementing count stored after each execution.
        :param execution_limit: int
        :param execution_count: int
        :return: boolean
        """

        if execution_count > int(execution_limit):
            return True
        else:
            return False

    def check_if_user_is_valid(self, hostname, system_hostname):
        """
        Match with the user in license file with the system user and verify user
        :param user: str
        :param syystem_user: str
        :return: boolean
        """
        if str(hostname).lower() == str(system_hostname).lower():
            return True
        else:
            return False
