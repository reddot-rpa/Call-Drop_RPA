import configparser
import os
from pathlib import Path

project_root_directory = Path(__file__).parent
config_loc = str(project_root_directory) + os.sep + 'config.ini'
if not os.path.exists(config_loc):
    project_root_directory = project_root_directory.parent
    config_loc = str(project_root_directory) + os.sep + 'config.ini'
    if not os.path.exists(config_loc):
        raise FileNotFoundError('config.ini file not found in project folder!')
print(f"Found file at location : {config_loc}")


class Config:
    __instance = None

    @staticmethod
    def get_instance(location=config_loc):
        """ Static access method. """
        if Config.__instance is None:
            Config.__instance = Config(location)
        return Config.__instance

    def __init__(self, config_file_location=config_loc):
        if Config.__instance is not None:
            raise Exception("This class is a singleton! Call get_instance() function use object")
        self.config_file = config_file_location

    @property
    def element(self):
        parser = configparser.ConfigParser()
        parser.read(self.config_file)
        return parser

    def element_to_array(self, element, delimitor=','):
        return element.split(delimitor)
