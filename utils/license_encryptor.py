import os
from utils.license_checker import LicenseChecker
from utils.encryption import Encryption

current_dir = os.path.dirname(os.path.abspath(__file__))
license_file_dir = current_dir[:-5] + "apps"
license_file = license_file_dir + "\\license.ini"
app_license_file = license_file_dir + "\\app_license.ini"

license_checker = LicenseChecker(license_file)
content = license_checker.parse_license_file()

encryption = Encryption()
encryption.generate_key()
key = encryption.get_key()

section = content.sections().pop()

options = {}

with open(app_license_file, 'wb') as configfile:
    configfile.write(encryption.encrypt(section, key))
    for option in content[section]:
        line =  option + "=" + content[section][option]
        configfile.write("\n".encode("UTF-8") + encryption.encrypt(line, key))

