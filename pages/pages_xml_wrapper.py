import json
import time

import xmltodict
from selenium.webdriver.common.by import By

from pages.pages import Pages


class XMLWrapper:

    def __init__(self):
        self.webpage = Pages(browser="chrome")
        self.init_action()

    def init_action(self):
        self.action = {'open': lambda target, value: self.webpage.navigate(target),
                       'type': lambda target, value: self.webpage.input_text(self.get_locator(target),
                                                                             self.get_locator_type(target), value),
                       'click': lambda target, value: self.webpage.click(self.get_locator(target),
                                                                         self.get_locator_type(target)),
                       'clear': lambda target, value: self.webpage.cleartext(self.get_locator(target),
                                                                             self.get_locator_type(target)),
                       'submit': lambda target, value: self.webpage.submit(self.get_locator(target),
                                                                           self.get_locator_type(target)),
                       'sendKeys': lambda target, value: self.webpage.press_key(self.get_locator(target),
                                                                                self.get_locator_type(target),
                                                                                text=value),
                       'select': lambda target, value: self.webpage.select(self.get_locator(target),
                                                                           self.get_locator_type(target)),
                       'selectFrame': lambda target, value: self.webpage.switch_frame(self.get_locator(target)),
                       'doubleClick': lambda target, value: self.webpage.double_click(self.get_locator(target),
                                                                                      self.get_locator_type(target))
                       }

    extensions = ('id', 'name', "link", 'xpath', 'css', 'index')

    def get_locator(self, target):
        if any(target.startswith(ext) for ext in self.extensions):
            return target.split("=", 1)[1]
        else:
            return target

    def get_locator_type(self, target):
        if any(target.startswith(ext) for ext in self.extensions):
            return target.split("=", 1)[0]
        else:
            return 'xpath'

    def job(self, command, target, value):
        lambdafunc = self.action.get(command)
        lambdafunc(target, value)
        time.sleep(1)

    def automate_xml(self, location="", df=None, func=None, *args, **kwargs):
        if df is None:
            with open(location) as fd:
                doc = xmltodict.parse(fd.read())
            xml_in_json = json.loads(json.dumps(doc))
            todo_list = xml_in_json['TestCase']['selenese']
            for argument in todo_list:
                print(argument['command'], argument['target'], argument['value'])
                self.job(argument['command'], argument['target'], argument['value'])
        else:
            with open(location) as fd:
                template_xml = fd.read()
            for index, row in df.iterrows():
                template_dict = {"Something to ignore": "Ignore information TOFA"}
                for iterator in range(len(row)):
                    template_dict[df.columns[iterator]] = row[iterator]
                xml_string = template_xml.format(**template_dict)
                doc = xmltodict.parse(xml_string)
                xml_in_json = json.loads(json.dumps(doc))
                todo_list = xml_in_json['TestCase']['selenese']
                for argument in todo_list:
                    print(argument['command'], argument['target'], argument['value'])
                    self.job(argument['command'], argument['target'], argument['value'])
                # func(*args, **kwargs)
