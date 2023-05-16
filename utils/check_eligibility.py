import os
import socket
from utils.license_checker import LicenseChecker
from utils.encryption import Encryption
import configparser


class CheckEligibility:
    # Access the file to get contents
    current_dir = os.path.dirname(os.path.abspath(__file__))
    license_file_dir = current_dir[:-5] + "apps"
    license_file = license_file_dir + "\\app_license.ini"
    license_checker = LicenseChecker(license_file)
    contents = license_checker.get_file_contents()
    # Get the private key
    encryption = Encryption()
    key = encryption.get_key()
    # Create configparser object from the decrypted contents
    config = configparser.ConfigParser()
    section = encryption.decrypt(contents[0], key)
    config.add_section(section)
    options = {}
    for i in range(1, len(contents)):
        dec_content = encryption.decrypt(contents[i], key)
        option = str(dec_content).split("=")
        options[option[0]] = option[1]
    config[section] = options
    # Get the license information from the configparser object
    section = config.sections().pop()
    options = config.options(section)
    hostname = config[section][options[0]]
    expiration_date = config[section][options[1]]
    # Get the system hostname to compare with license
    system_hostname = socket.gethostname()

    is_eligible = True
    def __init__(self):
        self.check_if_eligible()

    def check_if_eligible(self):
        message = ''
        if self.license_checker.check_if_license_is_expired(self.expiration_date) is True:
            self.is_eligible = False
            message = "License Expired"
            self.notify(message)
        else:#TODO: check if license will expire in n days, show notification with remaining days if true
            pass

        if self.license_checker.check_if_user_is_valid(self.hostname, self.system_hostname) is True:
            pass
        else:
            self.is_eligible = False
            message = "Invalid hostname"
            self.notify(message)

        return self.is_eligible

    def notify(self, message=''):
        print(message)
