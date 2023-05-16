from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException

from webdrivers.webhelper import WebHelper


class IllegalArgumentError(ValueError):
    pass


class BrowserWebElementMissingError(NoSuchElementException):
    pass


class ExpectedDataNotFoundException(ValueError):
    pass


class NoneResponseException(ValueError):
    pass


class ElementNotClickable(ElementClickInterceptedException):

    def __init__(self, element, element_type, message):
        self.element, self.element_type, self.message = element, element_type, message
        super.__init__(self.message)

    def __str__(self):
        return f'Unable to Click element : {self.element}  element type : {self.element_type} message : {self.message}'
