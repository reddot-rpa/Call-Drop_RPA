import time
import traceback

from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, \
    StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from utils.constants import LocatorType
from webdrivers.webhelper import WebHelper


def wait(func):
    def wrapper(instance, *args, **kwargs):
        try:
            func(instance, *args, **kwargs)
        except NoSuchElementException as exception:
            try:
                element_present = instance.get_element_presence(*args, **kwargs)
                WebDriverWait(instance.driver, instance.timeout).until(element_present)
                print(traceback.format_exc())
                func(instance, *args, **kwargs)
            except TimeoutException as timeoutexception:
                print("Element not found Details : {0} - {1} - {2}".format(func.__name__, args, kwargs))
                print(timeoutexception)
                print(traceback.format_exc())
                raise timeoutexception

    return wrapper


class Pages:
    def __init__(self, browser):
        webhelper = WebHelper()
        self.browser = browser
        self.driver = webhelper.open_browser(browser)
        self.driver.implicitly_wait(10)
        self.driver.maximize_window()
        self.timeout = 1

    locator_type = LocatorType()
    max_attempts = 5

    def navigate(self, url):
        self.driver.get(url=url)

    @wait
    def click(self, locator, locator_type='xpath'):
        print(locator, locator_type)

        try:
            self.get_element(locator, locator_type).click()
        except ElementClickInterceptedException as intercept_exception:
            print(traceback.format_exc())
            print(intercept_exception)
            time.sleep(5)
            self.click(locator, locator_type)
        except StaleElementReferenceException as stale_element:
            print('in exception')
            attempt = 1
            if attempt == self.max_attempts:
                raise
            attempt += 1
            # time.sleep()
            self.click(locator, locator_type)

    def get_element(self, locator, locator_type='xpath'):
        if locator_type == self.locator_type.id:
            element = self.driver.find_element_by_id(locator)
        elif locator_type == self.locator_type.name:
            element = self.driver.find_element_by_name(locator)
        elif locator_type == self.locator_type.link_text:
            element = self.driver.find_element_by_link_text(locator)
        elif locator_type == self.locator_type.xpath:
            element = self.driver.find_element_by_xpath(locator)
        elif locator_type == self.locator_type.css:
            element = self.driver.find_element_by_css_selector(locator)
        else:
            element = self.driver.find_element_by_xpath(locator_type + '=' + locator)
        return element

    def get_elements(self, locator, locator_type='xpath'):
        if locator_type == self.locator_type.id:
            element = self.driver.find_elements_by_id(locator)
        elif locator_type == self.locator_type.name:
            element = self.driver.find_elements_by_name(locator)
        elif locator_type == self.locator_type.link_text:
            element = self.driver.find_elements_by_link_text(locator)
        elif locator_type == self.locator_type.xpath:
            element = self.driver.find_elements_by_xpath(locator)
        elif locator_type == self.locator_type.css:
            element = self.driver.find_elements_by_css_selector(locator)
        else:
            element = self.driver.find_elements_by_xpath(locator_type + '=' + locator)
        return element

    @wait
    def double_click(self, locator, locator_type):
        element = self.get_element(locator, locator_type)
        actionchains = ActionChains(self.driver)
        actionchains.double_click(element).perform()

    @wait
    def input_text(self, locator, locator_type, text):
        # print(locator, locator_type, text)
        self.get_element(locator, locator_type).send_keys(text)

    @wait
    def press_key(self, locator, locator_type, text):
        if (text == "${KEY_ENTER}"):
            self.get_element(locator, locator_type).send_keys(Keys.ENTER)
        else:
            self.get_element(locator, locator_type).send_keys(text)

    def end(self):
        self.driver.close()
        self.driver.quit()

    def hover(self, locator):
        action = ActionChains(driver=self.driver)
        element = self.get_element(locator)
        action.move_to_element(element).perform()

    def refresh(self):
        self.driver.refresh()

    @wait
    def cleartext(self, locator, locator_type='xpath'):
        self.get_element(locator, locator_type).clear()

    @wait
    def select(self, locator, locator_type, value):
        element = self.get_element(locator, locator_type)
        select = Select(element)
        select.select_by_visible_text(value)
        # try:
        #     select.select_by_value(value)
        # except NoSuchElementException as exception:
        #     select.select_by_visible_text(value)

    def find_all_elements(self, locator, locator_type):
        if locator_type == 'xpath':
            return self.driver.find_elements_by_xpath(locator)
        else:
            return self.driver.find_elements_by_tag_name(locator)

    def new_tab(self, url):
        self.driver.execute_script("window.open('');")
        self.driver.switch_to.window(self.driver.window_handles[1])
        self.driver.get(url)

    def switch_to_main(self):
        self.driver.switch_to.window(self.driver.window_handles[0])

    def switch_frame(self, index):
        # print(f"Enter switch frame with index = {index}")
        index = int(index)
        self.driver.switch_to.frame(index)

    @wait
    def submit(self, locator, locator_type):
        self.get_element(locator, locator_type).submit()

    def get_element_presence(self, locator, locator_type):
        element = None
        if locator_type == self.locator_type.id:
            element = EC.presence_of_element_located((By.ID, locator))
        elif locator_type == self.locator_type.name:
            element = EC.presence_of_element_located((By.NAME, locator))
        elif locator_type == self.locator_type.link_text:
            element = EC.presence_of_element_located((By.LINK_TEXT, locator))
        elif locator_type == self.locator_type.xpath:
            element = EC.presence_of_element_located((By.XPATH, locator))
        elif locator_type == self.locator_type.css:
            element = EC.presence_of_element_located((By.CSS_SELECTOR, locator))
        elif locator_type == self.locator_type.xpath:
            element = EC.presence_of_element_located((By.XPATH, locator))
        else:
            element = EC.presence_of_element_located((By.XPATH, locator_type + '=' + locator))
        return element

    def look_for_element(self, *args, **kwargs):
        try:
            element_present = self.get_element_presence(*args, **kwargs)
            WebDriverWait(self.driver, self.timeout).until(element_present)
            return True
        except TimeoutException as timeoutexception:
            print(f"Element not found Details : {self.look_for_element.__name__} - {args} - {kwargs}")
            print(timeoutexception)
            print(traceback.format_exc())
            return False
