import json
import os
import sys
import zipfile
import autoit
import paramiko
import datetime as dt
from datetime import datetime
import time
import traceback
import pandas as pd
import psutil
import pytz
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import Select
from functools import wraps
from pages.pages_xml_wrapper import XMLWrapper

from apps.call_drop_database import CallDropDB
from utils.custom_exception import NoneResponseException, ExpectedDataNotFoundException, ElementNotClickable
from utils.logger import Logger
from selenium.webdriver.support.wait import WebDriverWait
from apps.app_utils import AppUtils
from apps.api_and_database import CRMAPI, ADB
from apps.database import DB
from apps.api_helper import RPAApi
from apps.remote_connect import RemoteServerConnection
from apps.remote_server import RemoteServerOperation
from utils.mail import Mail
from apps.config import ConfigParser

crm_api_object = RPAApi()
api = CRMAPI()
db = ADB()
db_obj = DB()
db_object = db_obj
conf = ConfigParser()

credentials = conf.get_credentials()
default_download_path = AppUtils.get_download_path()


def retry(delay=10, retries=4):
    def retry_decorator(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            opt_dict = {'retries': retries, 'delay': delay}
            while opt_dict['retries'] > 1:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    msg = "Exception: {}, Retrying in {} seconds...".format(e, delay)
                    print(msg)
                    time.sleep(opt_dict['delay'])
                    opt_dict['retries'] -= 1
            return f(*args, **kwargs)

        return f_retry

    return retry_decorator


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


class Helper:

    def __init__(self, webpage, driver):
        self.webpage = webpage
        self.driver = driver
        self.log = Logger.get_instance()
        self.log.log_warn(type(self.webpage))
        self.robi_rate_dict = dict()
        self.airtel_rate_dict = dict()
        self.all_files_list = list()

    def stop_execution(self):
        print('Stopping Execution')
        self.webpage.end()
        self.log_info(msg="Execution Stopped")
        sys.exit('Stopping Execution')

    def escape_unavailability(self, push_keys, data_type="count"):
        push_key = push_keys
        for i in range(1, 3):
            print("Initiating loop TryCatch = " + str(i))
            self.log_info(msg="Initiating loop TryCatch = " + str(i))
            push_key_action = push_key
            if data_type == "count":
                if int(push_key_action) > 0:
                    print("got beak for " + str(push_key_action))
                    break
                else:
                    push_key
            else:
                if str(push_key_action) > 5:
                    print("got beak for " + str(push_key_action))
                    break
                else:
                    push_key

        return push_key

    def primary_data_initiate_smart_script(self, pull_primary_data):

        element_check = self.check_table_record(output_data="data")
        self.log_info(msg="Getting CRM Open Complain List = " + str(element_check))
        pull_primary_data = self.pull_primary_data(element_check=element_check)
        self.log_info(msg="Push CRM Complain list to an list = " + str(element_check))
        self.click_first_complain()
        self.click_smart_screen_menu()
        self.choosing_smart_screen_item()
        return pull_primary_data

    def initiate_smart_script_data(self, pull_primary_data, index=0):
        smart_script_table_rows_total = self.check_table_record(output_data="count")
        self.log_info(msg="Getting CRM " + str(index) + " Index Complain Smart Script Data = " + str(
            smart_script_table_rows_total))
        if smart_script_table_rows_total > 1:
            smart_script_table_rows = self.check_table_record(output_data="data")
            self.log_info(msg="Index " + str(index) + " Smart Script Data = " + str(smart_script_table_rows))
            pull_primary_data = self.push_smart_script_data_to_data_grid(index_key=index,
                                                                         primary_data=pull_primary_data,
                                                                         smart_script_element_check=smart_script_table_rows)
            self.log_info(msg="Index " + str(index) + " Smart Script Data push into Main Data Grid = " + str(
                smart_script_table_rows))
        else:
            self.log_info(msg="No Smart Script Found for " + str(index) + " complain list.")

        return pull_primary_data

    def push_more_smart_script_data(self, pull_primary_data):
        self.log_info(
            msg="Checking Length main data grid for getting other smart script data = " + str(len(pull_primary_data)))
        if len(pull_primary_data) > 1:
            self.log_info(msg="Got max length to execute other smart script = " + str(len(pull_primary_data)))
            for i in range(1, len(pull_primary_data)):
                print("Loop for SMart Scrip " + str(i))
                self.clicking_crm_data_info_slide()
                time.sleep(3)
                pull_primary_data = self.initiate_smart_script_data(pull_primary_data=pull_primary_data, index=i)
        else:
            self.log_info(msg="Data Grid Data Length Not Match to Generate Other Smart Script = ")

        return pull_primary_data

    def log_info(self, msg):
        return self.log.log_info(msg)

    def log_critical(self, msg):
        return self.log.log_critical(msg)

    @retry(1, 5)
    def crm_table_rows(self, locator, read_col=''):
        table_rows = 0
        data_row = []
        for row in self.webpage.get_elements(locator=locator, locator_type="css"):
            if read_col != '':
                col = []
                for td in row.find_elements_by_css_selector(read_col):
                    col.append(td.text)
                data_row.append(col)
            else:
                table_rows = table_rows + 1
        if read_col != '':
            return data_row
        else:
            return table_rows

    def check_table_record(self, output_data="count"):
        record_table_available = 0
        try:
            time.sleep(3)
            if output_data == "count":
                element_check = self.crm_table_rows(locator="#s_2_l tbody tr")
                record_table_available = int(element_check) - 1
                self.log.log_info("Count Table Record Found = " + str(record_table_available))
            else:
                element_check = self.crm_table_rows(locator="#s_2_l tbody tr", read_col='td')
                record_table_available = element_check
                self.log.log_info("Table Data Record Found = " + str(record_table_available))
        except NoSuchElementException:
            self.log.log_critical("Table Not Exists ")
            #   print('Table Exists = ', 'Data Not Available')
        #   print('Table Record = ', record_table_available)
        return record_table_available

    def number_clean(self, string):
        self.log_info(msg=f'{str(string)}')
        print(f'{str(string)}')
        print(string)
        string = ''.join([c for c in string if c in "1234567890"])
        return string

    @retry(1, 5)
    def populate_smart_script_data(self, locator, read_col=''):
        table_rows = 0
        data_row = []
        for row in self.webpage.get_elements(locator=locator, locator_type="css"):
            if read_col != '':
                col = []
                cell_col = 1
                for td in row.find_elements_by_css_selector(read_col):
                    if cell_col == 1:
                        col.append(td.text)
                    else:
                        input_field_value = td.find_element_by_css_selector("input").get_attribute('value')
                        col.append(input_field_value)
                    cell_col = cell_col + 1
                data_row.append(col)
            else:
                table_rows = table_rows + 1
        if read_col != '':
            return data_row
        else:
            return table_rows

    def is_element_present(self, locator, locator_type="xpath"):
        print("Searching for relogin button")
        self.log_info(msg=f"{locator}")
        try:
            if locator_type == "xpath":
                self.driver.find_element_by_xpath(locator)
                print('Element search & found success fully xpath')
                return True
            elif locator_type == "css":
                self.driver.find_element_by_css_selector(locator)
                print('Element search & found success fully using css selector')
                return True
            elif locator_type == "name":
                self.driver.find_element_by_name(locator)
                print('Element search & found success fully using name selector')
                return True
            elif locator_type == "linktext":
                self.driver.find_element_by_link_text(link_text=locator)
                print('Element search & found success fully using link_text selector')
                return True
            else:
                self.driver.find_element_by_xpath(locator)
                print('Element search & found success fully xpath')
                return True
        except NoSuchElementException:
            print('Element search but not found')
            return False

    def add_commission_profile_window_set(self, commissionProfileForm_table_tr, otfData):
        row_no = 1
        otf_commission_successful = False
        for rowData in commissionProfileForm_table_tr:
            if row_no > 2:
                startRangeElement = rowData.find_element_by_css_selector("td:nth-child(2)")
                startRange = startRangeElement.find_element_by_css_selector("input").get_attribute("value")
                if len(startRange) == 0:
                    print("Found Normally Empty in Grid")
                    startRange = startRangeElement.find_element_by_css_selector("input")
                    endRangeElement = rowData.find_element_by_css_selector("td:nth-child(3)")
                    endRange = endRangeElement.find_element_by_css_selector("input")
                    otfAmountElement = rowData.find_element_by_css_selector("td:nth-child(5)")
                    otfAmount = otfAmountElement.find_element_by_css_selector("input")
                    startRange.clear()
                    endRange.clear()
                    otfAmount.clear()

                    startRange.send_keys(otfData.get('DENO'))
                    endRange.send_keys(otfData.get('DENO'))
                    slabTypeElement = rowData.find_element_by_css_selector("td:nth-child(4)")
                    slabType = slabTypeElement.find_element_by_css_selector("select")
                    slabType.click()
                    Select(slabType).select_by_visible_text("Amt")
                    otfAmount.send_keys(otfData.get('OTF_AMOUNT'))
                    otf_commission_successful = True
                else:
                    if row_no == len(commissionProfileForm_table_tr):
                        startRange = startRangeElement.find_element_by_css_selector("input")
                        endRangeElement = rowData.find_element_by_css_selector("td:nth-child(3)")
                        endRange = endRangeElement.find_element_by_css_selector("input")
                        otfAmountElement = rowData.find_element_by_css_selector("td:nth-child(5)")
                        otfAmount = otfAmountElement.find_element_by_css_selector("input")
                        startRange.clear()
                        endRange.clear()
                        otfAmount.clear()
                        startRange.send_keys(otfData.get('DENO'))
                        endRange.send_keys(otfData.get('DENO'))
                        slabTypeElement = rowData.find_element_by_css_selector("td:nth-child(4)")
                        slabType = slabTypeElement.find_element_by_css_selector("select")
                        slabType.click()
                        Select(slabType).select_by_visible_text("Amt")
                        otfAmount.send_keys(otfData.get('OTF_AMOUNT'))
                        print("Not Found Normally Empty in Grid")
                        otf_commission_successful = True

                #   startRange = rowData.find_element_by_xpath("/html/body/html:html/form[2]/table/tbody/tr/td/table[4]/tbody/tr[18]/td[2]/input").get_attribute("value")
                print('row_no = ', row_no, startRange)
            row_no = row_no + 1
        time.sleep(1)

        return otf_commission_successful

    def hadnle_popup_window_add_close(self):
        time.sleep(1)
        popup_window = self.driver.current_window_handle
        print('Popup = ', self.driver.current_window_handle)
        self.driver.find_element_by_name(
            name="addAdditional").click()
        time.sleep(4)
        #   print('After Popup = ', xml_wrapper_object.webpage.driver.current_window_handle)
        # if popup_window == self.driver.current_window_handle:
        #     self.driver.close()
        #   time.sleep(4)

    def handle_otf_window_frames(self, original_window):
        otf_commission_error = ""
        self.driver.switch_to.window(original_window)
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(0)
        self.driver.switch_to.frame(0)
        print(self.driver.current_window_handle)
        print(self.driver.current_url)
        print(self.driver.page_source)
        time.sleep(10)  # Mandatory to recheck source (Please Don't Delete This Sleep)
        #   print(helper.is_element_present(locator="save", locator_type="name"))
        otf_save_button = False
        if self.is_element_present(locator="save", locator_type="name"):
            self.driver.find_element_by_name(name="save").click()
            otf_save_button = True
        else:
            otf_commission_error = "Validation Error on Save"
        return otf_save_button, otf_commission_error

    @retry(1, 5)
    def check_dcrm_data_table(self, locator):
        table_rows = 0
        for row in self.webpage.get_elements(locator=locator, locator_type="css"):
            table_rows = table_rows + 1
            if table_rows > 0:
                print(f"table rows param check for few true = {table_rows}")
                self.log_info(msg=f"table rows param check for few true = {table_rows}")
                return table_rows

        return table_rows

    @retry(1, 5)
    def verify_dcrm_first_row(self):
        verify_first_row_td = False
        cell_col = 1
        for row in self.webpage.get_elements(locator="#srListDt tbody tr", locator_type="css"):
            for td in row.find_elements_by_css_selector("td"):
                if cell_col == 1:
                    if td.text == "No data available in table":
                        verify_first_row_td = True
                    else:
                        return False
                cell_col = cell_col + 1

        return verify_first_row_td

    def check_dcrm_complaint_list(self):
        record_table_available = 0
        time.sleep(2)
        if self.is_element_present(locator='//*[@id="srListDt"]'):
            try:
                time.sleep(3)
                element_check = self.check_dcrm_data_table(locator="#srListDt tbody tr")
                record_table_available = int(element_check)
                print("Count Table Record Found = " + str(record_table_available))
                self.log.log_info("Count Table Record Found = " + str(record_table_available))
                return record_table_available
            except NoSuchElementException:
                self.log.log_critical("Table Not Exists ")
                record_table_available = 0
        else:
            record_table_available = 0

            return record_table_available

    def smart_script_record(self, output_data="count"):
        record_table_available = 0
        time.sleep(2)
        if self.is_element_present(locator='//*[@id="SmartScriptViewDiv"]'):
            try:
                time.sleep(3)
                if output_data == "count":
                    element_check = self.populate_smart_script_data(locator="#SmartScriptViewDiv tr")
                    record_table_available = int(element_check)
                    self.log.log_info("Count Table Record Found = " + str(record_table_available))
                else:
                    element_check = self.populate_smart_script_data(locator="#SmartScriptViewDiv tr", read_col='td')
                    record_table_available = element_check
                    self.log.log_info("Table Data Record Found = " + str(record_table_available))
            except NoSuchElementException:
                self.log.log_critical("Table Not Exists ")
        else:
            if output_data == "count":
                record_table_available = 0
            else:
                record_table_available = []
        return record_table_available

    @wait
    def set_text(self, locator, locator_type="xpath", param_str=''):
        #   print("Clicking Locator = ", locator)
        #   return self.webpage.get_element(locator=locator, locator_type=locator_type).click()
        element = self.webpage.get_elements(locator=locator, locator_type=locator_type)
        element.sendKeys(param_str)

    @wait
    def click(self, locator, locator_type="xpath"):
        #   print("Clicking Locator = ", locator)
        #   return self.webpage.get_element(locator=locator, locator_type=locator_type).click()
        return self.webpage.click(locator=locator, locator_type=locator_type)

    @retry(1, 5)
    def click_first_complain(self):
        locator = '//*[@id="1SR_Number"]/a'
        self.log.log_info("Clicking First Complain Row")
        return self.click(locator=locator)

    def click_smart_screen_menu(self):
        locator = '#s_vctrl_div_tabScreen'
        locator_type = "css"
        self.log.log_info("Clicking First Complain Smart Script Menu")
        return self.click(locator=locator, locator_type=locator_type)

    def choosing_smart_screen_item(self):
        locator = '//a[text()="Smart Script"]'
        self.log.log_info("Clicking First Complain Smart Script Menu Item")
        return self.click(locator=locator)

    def pull_primary_data(self, element_check=None):
        if element_check is None:
            element_check = []
        pull_primary_data = []
        for row in element_check:
            if row[2] != "":
                row_col = [row[2], row[3], row[4]]
                pull_primary_data.append(row_col)
            else:
                self.log.log_critical("Failed to load primary data array from CRM ")
        #   print('pull_primary_data = ', pull_primary_data)
        self.log.log_info("Pull Primary Data = " + str(pull_primary_data))
        return pull_primary_data

    def push_smart_script_data_to_data_grid(self, index_key=0, primary_data=[], smart_script_element_check=[]):
        if len(smart_script_element_check) > 0:
            for td_row in smart_script_element_check:
                if td_row[1] != "":
                    primary_data[index_key].append(td_row[2])
            self.log.log_info(
                "Smart Script length " + str(len(smart_script_element_check)) + " And Push To Pull Data Array")
            return primary_data
        else:
            #   print("Smart Script Data ({index_key})", index_key)
            self.log.log_critical(
                "Smart Script length " + str(len(smart_script_element_check)) + " And Push To Pull Data Array")
            return primary_data

    def run_wrapper(self, wrapper, wrapper_df):
        rpa_environment = AppUtils.conf['rpa_environment']
        if str(rpa_environment) == "local":
            wrapper_xml_link = AppUtils.conf['xml_test_wrapper_path']
        else:
            wrapper_xml_link = AppUtils.conf['xml_wrapper_path']

        wrapper.automate_xml(location=wrapper_xml_link, df=wrapper_df)
        self.log_info(msg="Automate XML Run Complete")
        print('Checking Complaint Table background')
        compile_data = []

        return compile_data

    def upload_run_wrapper(self, wrapper, wrapper_df):
        rpa_environment = AppUtils.conf['rpa_environment']
        if str(rpa_environment) == "local":
            wrapper_xml_link = AppUtils.conf['upload_wrapper_path']
        else:
            wrapper_xml_link = AppUtils.conf['upload_wrapper_path']

        wrapper.automate_xml(location=wrapper_xml_link, df=wrapper_df)
        self.log_info(msg="Automate XML Run Complete")
        print('Checking Complaint Table background')
        compile_data = []

        return compile_data

    def read_csv_from_disk(self):
        self.log_info(msg="File Download And Moved = unwilling_combo_bundle_13_08_2020_06_25_23")
        df = pd.read_csv(r"..\process_files\running\unwilling_combo_bundle_13_08_2020_06_25_23.csv",
                         sep="\t",
                         header=None,
                         index_col=[0, 1, 2, 3],
                         encoding='utf-16le')
        print(df)
        compile_data = []
        for rows in df.iterrows():
            if rows[0][1] != "SR #":
                row = [rows[0][1], rows[0][2], rows[0][3]]
                compile_data.append(row)

        print('compile_data Length', len(compile_data))
        print('compile_data', compile_data)
        return compile_data

    def read_smart_script_and_validate_sr(self, grid_row, row_id, compile_total, wrapper):

        sr_id = grid_row[0]
        msisdn = grid_row[1]
        complain_ids = pd.DataFrame(
            columns=[
                "sr_id",
                "msisdn",
                "sr_name"
            ],
            data=[
                [
                    sr_id,
                    msisdn,
                    grid_row[2]
                ]
            ]
        )
        counter_log = str(row_id) + " Out of " + str(compile_total)

        smart_script_list = {
            'SR_ID': sr_id,
            'SR_SKIP': False,
            'MSISDN': msisdn,
            'USAGE': None,
            'DB_VALIDATION': False,
            'MSISDN_TYPE': None,
            'PACK_ID': None,
            'PRODUCT_ID': 0,
            'BONUS_PACK_ID': None,
            'LIST_TYPE': "white",
            'DATA_PACK_NAME': '',
            'BONUS_PACK_NAME': '',
            'DATA_PACK_RECHARGE_DATETIME': '',
            'DATA_PACK_PRICE': '',
            'RECHARGE_PACK_PRICE': 0,
            'BALANCE_ADJUST_PRICE': 0,
            'TOTAL_ADJUSTMENT': 0,
            'SMS_PACK_FOUND': False,
            'SMS_PACK_USAGE': 0,
            'SMS_PACK_TERMINATED': False,
            'SEC_PACK_FOUND': False,
            'SEC_PACK_USAGE': 0,
            'SEC_PACK_TERMINATED': False,
            'MIN_PACK_FOUND': False,
            'MIN_PACK_USAGE': 0,
            'MIN_PACK_TERMINATED': False,
            'HOUR_PACK_FOUND': False,
            'HOUR_PACK_USAGE': 0,
            'HOUR_PACK_TERMINATED': False,
            'TRADE_TIME': '',
            'REBATE_STATUS': False,
            'ERROR': '',
            'COUNTER_LOG': counter_log
        }

        grid_row.append(counter_log)
        print("Initiating SR#ID [" + str(grid_row) + "] Second Search XML")
        smart_script_list, error_msg = self.initial_validation(smart_script_list=smart_script_list)

        wrapper_find_smart_script = AppUtils.conf['xml_wrapper_smart_script_path']

        def smart_script_xml(recursion_limit=2):
            try:
                if recursion_limit:
                    wrapper.automate_xml(location=wrapper_find_smart_script, df=complain_ids)
                    time.sleep(3)
                else:
                    raise
            except ElementNotClickable as element_not_clickable:
                recursion_limit -= 1
                self.log_info(element_not_clickable.message)
                self.log_info('Trying to read from xml again')
                smart_script_xml(recursion_limit)

        recursion_limit = 2
        if len(error_msg) == 0:
            try:
                smart_script_xml()
            except ElementNotClickable as element_not_clickable:
                self.log_critical(element_not_clickable)
                self.log.log_error(element_not_clickable)
                smart_script_list.update(SR_SKIP=True)
                return smart_script_list

            total_row_in_smart_script = self.smart_script_record(output_data="count")
            if total_row_in_smart_script > 0:
                print("SR#ID [" + str(grid_row) + "] : Total Record = " + str(total_row_in_smart_script))
                smart_script = self.smart_script_record(output_data="data")
                for ss_row in smart_script:
                    if ss_row[0].strip() == "Bundle detail":
                        smart_script_list.update(DATA_PACK_NAME=ss_row[1].strip())
                    elif ss_row[0].strip() == "Date & time of Recharge (copy From dCRM) *":
                        smart_script_list.update(DATA_PACK_RECHARGE_DATETIME=ss_row[1].strip())
                    elif ss_row[0].strip() == "Bonus pack Name: (if any)":
                        smart_script_list.update(BONUS_PACK_NAME=ss_row[1].strip())
                    elif ss_row[0].strip() == "Price *":
                        smart_script_list.update(DATA_PACK_PRICE=self.number_clean(string=ss_row[1].strip()))

                    print("SR#ID [" + str(grid_row) + "] = Smart Script [ Field Name : " + str(
                        ss_row[0]) + " Value :" + str(ss_row[1]) + "]")
            else:
                print("SR#ID [" + str(grid_row) + "] : No Record Found")
                smart_script_list.update(ERROR="No Smart Script Found")
                smart_script_list.update(LIST_TYPE="Black")
            self.log_info(msg=str(smart_script_list))
        else:
            smart_script_list.update(LIST_TYPE="Black")
            smart_script_list.update(ERROR=error_msg)

        print(smart_script_list)
        return smart_script_list

    def initial_validation(self, smart_script_list):
        error_msg = ""
        msisdn_check, error_msg = self.validate_msisdn_type(sr_mobile=smart_script_list.get('MSISDN'))
        if len(error_msg) > 0:
            smart_script_list.update(ERROR=error_msg)
            smart_script_list.update(LIST_TYPE="Black")
            smart_script_list.update(SR_SKIP=True)
            # return smart_script_list, error_msg
        else:
            smart_script_list.update(MSISDN_TYPE=msisdn_check)

        print(smart_script_list)
        time.sleep(3)
        if len(error_msg) == 0:
            print('db_record_validate', smart_script_list.get('MSISDN'))
            sr_exists, error_msg = self.db_record_validate(msisdn=smart_script_list.get('MSISDN'),
                                                           msisdn_check=msisdn_check)
            print('Validation in DB Found =', sr_exists)
            if len(error_msg) > 0:
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="Black")
            else:
                smart_script_list.update(AVAILABLE_IN_DB=sr_exists)

        return smart_script_list, error_msg

    def get_product_terminate_rebate_detail(self, smart_script_list):
        self.log.log_info(msg=f"Initiating get_product_terminate_rebate_detail")
        error_msg = smart_script_list.get('ERROR')
        current_time = 20201010000000
        start_time = 20201010000000
        end_time = 20201010000000
        if smart_script_list.get('SR_SKIP') is False:
            if len(error_msg) > 0:
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="Black")
                # return smart_script_list
            else:
                current_time, start_time, end_time, error_msg = AppUtils.convert_smart_script_time(
                    date_time_str=smart_script_list.get('DATA_PACK_RECHARGE_DATETIME'))

            print(current_time, start_time, end_time, error_msg)

            #   exit(1)
            trade_time = '00000000000000'
            recharge_price = 0
            # get pack price & recharge time start
            #   exit(1)
            trade_time = current_time
            if len(error_msg) == 0 and smart_script_list.get('SR_SKIP') is False:
                recharge_price, trade_time, error_msg = self.get_price(msisdn=smart_script_list.get('MSISDN'),
                                                                       trade_time=current_time,
                                                                       start_time=start_time,
                                                                       end_time=end_time)
                print(recharge_price, trade_time, error_msg)
                #   exit(1)
                self.log_info(
                    msg=f"Getting price form recharge log {str(recharge_price)}, {trade_time}, {str(error_msg)}")

                print(recharge_price, error_msg)
                if len(error_msg) == 0 and float(recharge_price) > 0:
                    try:
                        smart_script_list.update(RECHARGE_PACK_PRICE=recharge_price)
                        convert_pack_price = float(float(recharge_price) / 10000)
                        smart_script_list.update(DATA_PACK_PRICE=convert_pack_price)
                        self.log_info(msg=f"Data Pack Price {str(convert_pack_price)}")
                    except Exception as e:
                        print(e)
                        self.log.log_error(e)
                        smart_script_list.update(SR_SKIP=True)

                else:
                    smart_script_list.update(ERROR=error_msg)
                    smart_script_list.update(LIST_TYPE="black")
            # get pack price & recharge time end
            print(smart_script_list)
            #   exit(1)
            product_id = 0
            if len(error_msg) > 0:
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="black")
                # return smart_script_list
            else:
                product_id, error_msg = self.check_cbs_product_detail(msisdn=smart_script_list.get('MSISDN'),
                                                                      recharge_time=trade_time,
                                                                      smart_script_data=smart_script_list)
                print(product_id, error_msg)
                #   exit(1)
                if len(error_msg) > 0:
                    smart_script_list.update(LIST_TYPE="black")
                    smart_script_list.update(ERROR=error_msg)
                    # return smart_script_list
                else:
                    if int(product_id) > 0:
                        smart_script_list.update(PRODUCT_ID=product_id)
                    else:
                        smart_script_list.update(LIST_TYPE="black")
                        smart_script_list.update(ERROR=error_msg)
                        # return smart_script_list
            print('product_id = ', product_id)
            #   exit(1)
            unused_bundle = False
            free_unit_data = {}
            if len(error_msg) > 0:
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="black")
                # return smart_script_list
            else:
                unused_bundle, free_unit_data, error_msg = self.check_free_unit_get_voice_sms(
                    msisdn=smart_script_list.get('MSISDN'),
                    trade_time=trade_time,
                    smart_script_data=smart_script_list)

            print(unused_bundle, free_unit_data)
            print(smart_script_list, error_msg)
            #   exit(1)
            if len(error_msg) > 0:
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="black")
                # return smart_script_list
            print(smart_script_list)
            #   exit(1)
            error_msg = smart_script_list.get('ERROR')
            if len(error_msg) == 0:
                if free_unit_data.get('sms_UsageAmount') > 0:
                    smart_script_list.update(ERROR="SMS pack already in use.")
                    smart_script_list.update(LIST_TYPE="black")
                    smart_script_list.update(SMS_PACK_FOUND=True)
                    smart_script_list.update(SMS_PACK_USAGE=free_unit_data.get('sms_UsageAmount'))
                    # return smart_script_list

                if free_unit_data.get('hour_UsageAmount') > 0:
                    smart_script_list.update(ERROR="Hours pack already in use.")
                    smart_script_list.update(LIST_TYPE="black")
                    smart_script_list.update(HOUR_PACK_FOUND=True)
                    smart_script_list.update(HOUR_PACK_USAGE=free_unit_data.get('hour_UsageAmount'))
                    # return smart_script_list

                if free_unit_data.get('minutes_UsageAmount') > 0:
                    smart_script_list.update(ERROR="Minutes pack already in use.")
                    smart_script_list.update(LIST_TYPE="black")
                    smart_script_list.update(MIN_PACK_FOUND=True)
                    smart_script_list.update(MIN_PACK_USAGE=free_unit_data.get('minutes_UsageAmount'))
                    # return smart_script_list

                if free_unit_data.get('sec_UsageAmount') > 0:
                    smart_script_list.update(ERROR="Second pack already in use.")
                    smart_script_list.update(LIST_TYPE="black")
                    smart_script_list.update(SEC_PACK_FOUND=True)
                    smart_script_list.update(SEC_PACK_USAGE=free_unit_data.get('sec_UsageAmount'))
                    # return smart_script_list

            error_msg = smart_script_list.get('ERROR')

            if free_unit_data.get('sms') is False and free_unit_data.get('sec') is False and free_unit_data.get(
                    'minutes') is False and free_unit_data.get('hour') is False:
                error_msg = "Unable to find user sms, second, minutes, hour pack"
                smart_script_list.update(ERROR=error_msg)
                smart_script_list.update(LIST_TYPE="black")
                #   return smart_script_list

            if len(error_msg) == 0:
                sum_free_unit = 0

                if free_unit_data.get('sms') is True and free_unit_data.get(
                        'sms_TotalInitialAmount') > 0 and free_unit_data.get('sms_UsageAmount') == 0:
                    smart_script_list.update(SMS_PACK_FOUND=True)
                    smart_script_list.update(SMS_PACK_USAGE=free_unit_data.get('sms_UsageAmount'))
                    service_pack_terminate, service_pack_rebate, service_error_msg = self.terminate_service_packs(
                        msisdn=smart_script_list.get('MSISDN'),
                        free_unit_type=free_unit_data.get('sms_FreeUnitType'),
                        free_unit_instance_id=free_unit_data.get('sms_FreeUnitInstanceID'),
                        adjustment_amount=free_unit_data.get('sms_TotalInitialAmount'))

                    print(
                        f"SMS Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    self.log_info(
                        msg=f"SMS Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    if not service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="black")
                        #   return smart_script_list
                    elif service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="off_white")
                        #   return smart_script_list
                    elif service_pack_terminate and service_pack_rebate:
                        smart_script_list.update(SMS_PACK_TERMINATED=True)

                if free_unit_data.get('sec') is True and free_unit_data.get('sec_TotalInitialAmount') > 0:
                    time.sleep(3)
                    smart_script_list.update(SEC_PACK_FOUND=True)
                    smart_script_list.update(SEC_PACK_USAGE=free_unit_data.get('sec_UsageAmount'))
                    service_pack_terminate, service_pack_rebate, service_error_msg = self.terminate_service_packs(
                        msisdn=smart_script_list.get('MSISDN'),
                        free_unit_type=free_unit_data.get('sec_FreeUnitType'),
                        free_unit_instance_id=free_unit_data.get('sec_FreeUnitInstanceID'),
                        adjustment_amount=free_unit_data.get('sec_TotalInitialAmount'))

                    print(
                        f"Second Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    self.log_info(
                        msg=f"Second Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    if not service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="black")
                        #   return smart_script_list
                    elif service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="off_white")
                        #   return smart_script_list
                    elif service_pack_terminate and service_pack_rebate:
                        smart_script_list.update(SEC_PACK_TERMINATED=True)

                if free_unit_data.get('minutes') is True and free_unit_data.get('minutes_TotalInitialAmount') > 0:
                    time.sleep(3)
                    smart_script_list.update(MIN_PACK_FOUND=True)
                    smart_script_list.update(MIN_PACK_USAGE=free_unit_data.get('minutes_UsageAmount'))
                    service_pack_terminate, service_pack_rebate, service_error_msg = self.terminate_service_packs(
                        msisdn=smart_script_list.get('MSISDN'),
                        free_unit_type=free_unit_data.get('minutes_FreeUnitType'),
                        free_unit_instance_id=free_unit_data.get('minutes_FreeUnitInstanceID'),
                        adjustment_amount=free_unit_data.get('minutes_TotalInitialAmount'))

                    print(
                        f"Minutes Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    self.log_info(
                        msg=f"Minutes Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    if not service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="black")
                        #   return smart_script_list
                    elif service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="off_white")
                        #   return smart_script_list
                    elif service_pack_terminate and service_pack_rebate:
                        smart_script_list.update(MIN_PACK_TERMINATED=True)

                if free_unit_data.get('hour') is True and free_unit_data.get('hour_TotalInitialAmount') > 0:
                    time.sleep(3)
                    smart_script_list.update(HOUR_PACK_FOUND=True)
                    smart_script_list.update(HOUR_PACK_USAGE=free_unit_data.get('hour_UsageAmount'))
                    service_pack_terminate, service_pack_rebate, service_error_msg = self.terminate_service_packs(
                        msisdn=smart_script_list.get('MSISDN'),
                        free_unit_type=free_unit_data.get('hour_FreeUnitType'),
                        free_unit_instance_id=free_unit_data.get('hour_FreeUnitInstanceID'),
                        adjustment_amount=free_unit_data.get('hour_TotalInitialAmount'))

                    print(
                        f"Hour Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    self.log_info(
                        msg=f"Hour Pack Service Terminate & Rebate status = {service_pack_terminate}, {service_pack_rebate}, {service_error_msg}")
                    if not service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="black")
                        #   return smart_script_list
                    elif service_pack_terminate and not service_pack_rebate:
                        smart_script_list.update(ERROR=service_error_msg)
                        smart_script_list.update(LIST_TYPE="off_white")
                        #   return smart_script_list
                    elif service_pack_terminate and service_pack_rebate:
                        smart_script_list.update(HOUR_PACK_TERMINATED=True)

            rebate_status = False
            if len(smart_script_list.get('ERROR')) == 0:
                rebate_status = self.rebate(msisdn=smart_script_list.get('MSISDN'),
                                            pack_price=smart_script_list.get('RECHARGE_PACK_PRICE'))
                print('rebate status = ', rebate_status)
                print('Error Msg =', error_msg)
                if rebate_status:
                    smart_script_list.update(REBATE_STATUS=True)
                else:
                    smart_script_list.update(LIST_TYPE="off_white")
                    smart_script_list.update(ERROR="Unable to rebate bundle price.")

        return smart_script_list

    def get_price(self, msisdn, trade_time, start_time, end_time):
        try:
            error_msg = ""
            c = 0
            data_pack_price = 0
            response = None
            prepaid = True
            while c < 3:
                response = crm_api_object.get_price(msisdn=msisdn, trade_time=trade_time, start_time=start_time,
                                                    end_time=end_time)
                if response is None:
                    c = c + 1
                else:
                    break

            if response is None:
                error_msg = 'Unable to Find Pack Price'

            pack_price, new_trade_time = self.get_price_amount(response=response, trade_time=trade_time)
            print(pack_price, new_trade_time)
            self.log_info(f"{pack_price}, {new_trade_time}")
            print(pack_price, new_trade_time)
            if pack_price == "pack_price_not_found":
                error_msg = "Unable to Find Pack Price"
                data_pack_price = 0
            elif pack_price is None:
                error_msg = "Unable to Find Pack Price"
                data_pack_price = 0
            elif pack_price == "not_easyload":
                error_msg = "Not easyload"
                data_pack_price = 0
            else:
                data_pack_price = pack_price
        except Exception as e:
            data_pack_price = 0
            new_trade_time = 000000000
            error_msg = "Unable to find recharge price due to API not responding"

        return data_pack_price, new_trade_time, error_msg

    def get_price_amount(self, response, trade_time):

        pack_price = None
        new_trade_time = None
        try:
            #   date = trade_time
            date = dt.datetime.strptime(trade_time, '%Y%m%d%H%M%S')
            if response.status_code == 200 and response.json()['ResultHeader'][
                'ResultDesc'] == "Operation successfully.":
                if 'RechargeInfo' in response.json()['QueryRechargeLogResult']:
                    data_list = response.json()['QueryRechargeLogResult']['RechargeInfo']
                    print('data_list', data_list)
                else:
                    pack_price = "pack_price_not_found"

                if isinstance(data_list, list):
                    print('one', data_list)
                    for row in data_list:
                        print(row, type(row), 188)
                        # print(row['TradeTime'],type(row['TradeTime']),189)
                        trade_time2 = str(row['TradeTime'])
                        date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                        diff = int(abs((date - date2).total_seconds()))
                        print(f"time diff: {diff}")
                        self.log_info(f"time diff: {diff}")

                        print('diff = ', diff)

                        if diff <= 30:
                            # print(row['AdditionalProperty'])
                            for value in row['AdditionalProperty']:
                                print(value)
                                self.log_info(f"{value}")
                                # print(value['Value'])
                                if value['Code'] == "OperatorID":
                                    if value['Value'] == "easyload":
                                        # print(value['Code'], value['Value'])
                                        pack_price = row['RechargeAmount']
                                        pack_price = str(pack_price)
                                        self.log_info(f"pack price: {pack_price}")
                                        pack_price = pack_price
                                        new_trade_time = row['TradeTime']


                                    else:
                                        pack_price = "not_easyload"
                else:
                    print('two')
                    # trade_time_new = str(data_list['TradeTime'])
                    trade_time2 = str(data_list['TradeTime'])
                    date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                    print(date2)

                    diff = int(abs((date - date2).total_seconds()))
                    print(f"time diff: {diff}, msisdn:")
                    # log.log_info(f"time diff: {diff}, msisdn: {msisdn}")
                    print(diff)
                    if diff <= 30:
                        print(data_list['AdditionalProperty'])
                        for value in data_list['AdditionalProperty']:
                            print(value)
                            self.log_info(f"{value['Value']}")
                            if value['Code'] == "OperatorID":
                                if value['Value'] == "easyload":
                                    pack_price = data_list['RechargeAmount']
                                    pack_price = str(pack_price)
                                    self.log_info(f"pack price: {pack_price}")
                                    pack_price = pack_price
                                    new_trade_time = data_list['TradeTime']
                                else:
                                    pack_price = "not_easyload"

                if pack_price is None or pack_price == "not_easyload":
                    if isinstance(data_list, list):
                        print('one', data_list)
                        for row in data_list:
                            print(row, type(row), 188)
                            # print(row['TradeTime'],type(row['TradeTime']),189)
                            trade_time2 = str(row['TradeTime'])
                            date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                            diff = int(abs((date - date2).total_seconds()))
                            print(f"time diff: {diff}")
                            self.log_info(f"time diff: {diff}")

                            print('diff = ', diff)

                            if diff <= 99:
                                # print(row['AdditionalProperty'])
                                for value in row['AdditionalProperty']:
                                    print(value)
                                    self.log_info(f"{value}")
                                    # print(value['Value'])
                                    if value['Code'] == "OperatorID":
                                        if value['Value'] == "easyload":
                                            # print(value['Code'], value['Value'])
                                            pack_price = row['RechargeAmount']
                                            pack_price = str(pack_price)
                                            self.log_info(f"pack price: {pack_price}")
                                            pack_price = pack_price
                                            new_trade_time = row['TradeTime']


                                        else:
                                            pack_price = "not_easyload"
                    else:
                        print('two')
                        # trade_time_new = str(data_list['TradeTime'])
                        trade_time2 = str(data_list['TradeTime'])
                        date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                        print(date2)

                        diff = int(abs((date - date2).total_seconds()))
                        print(f"time diff: {diff}, msisdn:")
                        # log.log_info(f"time diff: {diff}, msisdn: {msisdn}")
                        print(diff)
                        if diff <= 99:
                            print(data_list['AdditionalProperty'])
                            for value in data_list['AdditionalProperty']:
                                print(value)
                                self.log_info(f"{value['Value']}")
                                if value['Code'] == "OperatorID":
                                    if value['Value'] == "easyload":
                                        # print(value['Code'], value['Value'])
                                        pack_price = data_list['RechargeAmount']
                                        pack_price = str(pack_price)
                                        self.log_info(f"pack price: {pack_price}")
                                        pack_price = pack_price

                                        new_trade_time = data_list['TradeTime']
                                        # new_trade_time

                                    else:
                                        pack_price = "not_easyload"

            else:
                pack_price = "pack_price_not_found"
        except Exception as e:
            self.log_info(msg=f' Exception raised to get pack price API : {str(e)}')
            pack_price = "pack_price_not_found"

        return pack_price, new_trade_time

    def terminate_service_packs(self, msisdn, free_unit_type, free_unit_instance_id, adjustment_amount):
        service_pack_terminate = False
        service_pack_rebate = False

        rebate_status = self.rebate(msisdn=msisdn, pack_price=adjustment_amount)
        pack_terminated, error_msg = self.terminate_adjust_voice_sms(msisdn=msisdn,
                                                                     free_unit_type=free_unit_type,
                                                                     free_unit_instance_id=free_unit_instance_id,
                                                                     adjustment_amount=adjustment_amount)
        if pack_terminated:
            if rebate_status:
                service_pack_terminate = True
                service_pack_rebate = True
                error_msg = ""
                return service_pack_terminate, service_pack_rebate, error_msg
            else:
                service_pack_terminate = True
                error_msg = "Unable to rebate pack adjustment."
                return service_pack_terminate, service_pack_rebate, error_msg
        else:
            if rebate_status:
                self.rebate(msisdn=msisdn, pack_price=-adjustment_amount)
                service_pack_terminate = False
                error_msg = "Unable to terminate pack."
                return service_pack_terminate, service_pack_rebate, error_msg

    def read_smart_script_and_validate_srbk(self, grid_row, row_id, compile_total, wrapper):

        sr_id = grid_row[0]
        msisdn = grid_row[1]
        complain_ids = pd.DataFrame(
            columns=[
                "sr_id",
                "msisdn",
                "sr_name"
            ],
            data=[
                [
                    sr_id,
                    msisdn,
                    grid_row[2]
                ]
            ]
        )
        counter_log = str(row_id) + " Out of " + str(compile_total)
        # row_id = row_id + 1
        grid_row.append(counter_log)

        msisdn_check = ''
        sr_exists = ''
        trade_time = ''
        error_msg = ''
        smart_script_list = {
            'SR_ID': sr_id,
            'MSISDN': msisdn,
            'AVAILABLE_IN_DB': sr_exists,
            'DATA_PACK_NAME': '',
            'PRICE': '',
            'BALANCE_ADJUST_PRICE': 0,
            'TOTAL_ADJUSTMENT': 0,
            'RECHARGE': '',
            'PRODUCT_ID': '',
            'TRADE_TIME': trade_time,
            'MSISDN_TYPE': msisdn_check,
            'ERROR': error_msg,
            'COUNTER_LOG': counter_log
        }

        print('validate_msisdn_type', msisdn)
        product_id = 0
        unused_bundle = False
        msisdn_check, error_msg = self.validate_msisdn_type(sr_mobile=msisdn)
        if len(error_msg) == 0:
            smart_script_list['MSISDN_TYPE'] = msisdn_check
        else:
            smart_script_list['ERROR'] = error_msg

        time.sleep(3)
        print('db_record_validate', msisdn)
        sr_exists, error_msg = self.db_record_validate(msisdn=msisdn, msisdn_check=msisdn_check)
        print('Available in DB =', sr_exists)
        if len(error_msg) == 0:
            smart_script_list['AVAILABLE_IN_DB'] = sr_exists
        else:
            smart_script_list['ERROR'] = error_msg

        if len(error_msg) == 0:
            wrapper_find_smart_script = AppUtils.conf['xml_wrapper_smart_script_path']
            # wrapper_find_smart_script = new_config.element['XML']
            smart_script_list, error_msg = self.run_wrapper_get_smart_data(wrapper=wrapper, grid_row=grid_row,
                                                                           complain_ids=complain_ids,
                                                                           smart_script_list=smart_script_list)
        else:
            smart_script_list['ERROR'] = error_msg

        if len(error_msg) == 0:
            time.sleep(3)
            current_time, start_time, end_time, error_msg = AppUtils.convert_smart_script_time(
                date_time_str=smart_script_list['RECHARGE'])
        else:
            smart_script_list['ERROR'] = error_msg

        trade_time = '00000000000000'
        if len(error_msg) == 0:
            trade_time, error_msg = self.check_cbs_recharge_response(msisdn=msisdn, start_time=start_time,
                                                                     end_time=end_time)
            print('trade_time = ', trade_time)
            smart_script_list['TRADE_TIME'] = trade_time
        else:
            smart_script_list['ERROR'] = error_msg

        #   exit(1)
        product_id = 0
        if len(error_msg) == 0:
            product_id, error_msg = self.check_cbs_product_detail(msisdn=msisdn, recharge_time=trade_time,
                                                                  smart_script_data=smart_script_list)
            smart_script_list['PRODUCT_ID'] = product_id
        else:
            smart_script_list['ERROR'] = error_msg

        # print('error_msg = ', error_msg)
        print('product_id = ', product_id)
        # exit(1)
        unused_bundle = False
        free_unit_data = {}
        if len(error_msg) == 0:
            unused_bundle, free_unit_data, error_msg = self.check_free_unit_get_voice_sms(msisdn=msisdn,
                                                                                          trade_time=trade_time,
                                                                                          smart_script_data=smart_script_list)
        else:
            smart_script_list['ERROR'] = error_msg

        print(unused_bundle, free_unit_data)
        print(smart_script_list, error_msg)
        #   exit(1)

        if len(error_msg) == 0:
            sum_free_unit = 0
            if free_unit_data['sms'] is True and free_unit_data['sms_TotalInitialAmount'] > 0:
                sms_total_initial_amount = free_unit_data['sms_TotalInitialAmount'] / 10000
                sum_free_unit = sum_free_unit + sms_total_initial_amount
            if free_unit_data['sec'] is True and free_unit_data['sec_TotalInitialAmount'] > 0:
                sec_total_initial_amount = free_unit_data['sec_TotalInitialAmount'] / 10000
                sum_free_unit = sum_free_unit + sec_total_initial_amount
            if free_unit_data['minutes'] is True and free_unit_data['minutes_TotalInitialAmount'] > 0:
                minutes_total_initial_amount = free_unit_data['minutes_TotalInitialAmount'] / 10000
                sum_free_unit = sum_free_unit + minutes_total_initial_amount
            if free_unit_data['hour'] is True and free_unit_data['hour_TotalInitialAmount'] > 0:
                hour_total_initial_amount = free_unit_data['hour_TotalInitialAmount'] / 10000
                sum_free_unit = sum_free_unit + hour_total_initial_amount
            smart_script_list['BALANCE_ADJUST_PRICE'] = sum_free_unit
            total_sum_adjustment_raw = sum_free_unit + smart_script_list['PRICE']
            total_with_fixed_data = total_sum_adjustment_raw * 10000
            smart_script_list['TOTAL_ADJUSTMENT'] = total_with_fixed_data
            print(total_sum_adjustment_raw)
            print(total_with_fixed_data)

        # print(len(error_msg))
        # exit()
        rebate_status = False
        if len(error_msg) == 0:
            rebate_status = self.rebate(msisdn=msisdn, pack_price=smart_script_list['TOTAL_ADJUSTMENT'])
            print('rebate status = ', rebate_status)
            print('Error Msg =', error_msg)
            #   exit(1)
            if rebate_status:
                pack_terminated = False
                error_msg = ''
                if free_unit_data['sms'] and free_unit_data['sms_TotalInitialAmount'] > 0 and len(error_msg) == 0:
                    print('Checking SMS')
                    pack_terminated, error_msg = self.terminate_adjust_voice_sms(msisdn=msisdn,
                                                                                 free_unit_type=free_unit_data[
                                                                                     'sms_FreeUnitType'],
                                                                                 free_unit_instance_id=free_unit_data[
                                                                                     'sms_FreeUnitInstanceID'],
                                                                                 adjustment_amount=free_unit_data[
                                                                                     'sms_TotalInitialAmount'])

                if free_unit_data['sec'] and free_unit_data['sec_TotalInitialAmount'] > 0 and len(error_msg) == 0:
                    print('checking Sec')
                    pack_terminated, error_msg = self.terminate_adjust_voice_sms(msisdn=msisdn,
                                                                                 free_unit_type=free_unit_data[
                                                                                     'sec_FreeUnitType'],
                                                                                 free_unit_instance_id=free_unit_data[
                                                                                     'sec_FreeUnitInstanceID'],
                                                                                 adjustment_amount=free_unit_data[
                                                                                     'sec_TotalInitialAmount'])

                if free_unit_data['minutes'] and free_unit_data['minutes_TotalInitialAmount'] > 0 and len(
                        error_msg) == 0:
                    print('checking Minutes')
                    pack_terminated, error_msg = self.terminate_adjust_voice_sms(msisdn=msisdn,
                                                                                 free_unit_type=free_unit_data[
                                                                                     'minutes_FreeUnitType'],
                                                                                 free_unit_instance_id=free_unit_data[
                                                                                     'minutes_FreeUnitInstanceID'],
                                                                                 adjustment_amount=free_unit_data[
                                                                                     'minutes_TotalInitialAmount'])

                if free_unit_data['hour'] and free_unit_data['hour_TotalInitialAmount'] > 0 and len(error_msg) == 0:
                    print('checking Hour')
                    pack_terminated, error_msg = self.terminate_adjust_voice_sms(msisdn=msisdn,
                                                                                 free_unit_type=free_unit_data[
                                                                                     'hour_FreeUnitType'],
                                                                                 free_unit_instance_id=free_unit_data[
                                                                                     'hour_FreeUnitInstanceID'],
                                                                                 adjustment_amount=free_unit_data[
                                                                                     'hour_TotalInitialAmount'])
        print(smart_script_list)
        print('Before Extraction')
        # exit(1)
        print('error_msg = ', error_msg)
        if len(error_msg) > 0:
            print('FOund Error')
            #   exit(1)

        #   exit(1)
        final_status = "Failed"
        if len(error_msg) == 0:
            final_status = "Successful"
            self.smsapi(msisdn=msisdn,
                        msg="Dear Customer, We have suceessfully removed your bundle and recharged your amount.")
            return final_status, smart_script_list, error_msg
        else:
            self.smsapi(msisdn=msisdn,
                        msg=f"Dear Customer, We are unable to process your rebate due to {error_msg}.")
            return final_status, smart_script_list, error_msg

    def run_wrapper_get_smart_data(self, wrapper, grid_row, complain_ids, smart_script_list):
        print("Initiating SR#ID [" + grid_row[0] + "] Second Search XML")
        wrapper.automate_xml(location="../apps/xml/crm_erase_voucher_download_open_complain.xml", df=complain_ids)
        total_row_in_smart_script = self.check_table_record(output_data="count")
        if total_row_in_smart_script > 0:
            print("SR#ID [" + str(grid_row[0]) + "] : Total Record = " + str(total_row_in_smart_script))
            smart_script = self.check_table_record(output_data="data")
            print(smart_script)
            for ss_row in smart_script:
                if ss_row[1] != "":
                    grid_row.append(ss_row[2])
                    if ss_row[1] == "Data Pack Name":
                        smart_script_list['DATA_PACK_NAME'] = ss_row[2]
                    elif ss_row[1] == "Price":
                        smart_script_list['PRICE'] = int(ss_row[2])
                    elif ss_row[1] == "Date & Time of recharge (From CRM)":
                        smart_script_list['RECHARGE'] = ss_row[2]
                    print("SR#ID [" + str(grid_row[0]) + "] = Smart Script [ Field Name : " + str(
                        ss_row[1]) + " Value :" + str(ss_row[2]) + "]")
                    # in progress moved to other place.
        else:
            print("SR#ID [" + str(grid_row[0]) + "] : No Record Found")
            smart_script_list['ERROR'] = "Smart Script Not Found"

        return smart_script_list, smart_script_list['ERROR']

    def validate_msisdn_type(self, sr_mobile):
        msisdn_type = self.pre_post_check(msisdn=sr_mobile)
        msisdn_check = "Postpaid"
        error_msg = "MSISDN is Postpaid"
        if msisdn_type:
            msisdn_check = "Prepaid"
            error_msg = ""

        return msisdn_check, error_msg

    def update_dcrm_sr_status(self, msisdn, sr_id, status, error):
        print(f"Update request initiate for {msisdn}, {sr_id}, {status}, {error}")
        self.log_info(msg=f"Update request initiate for {msisdn}, {sr_id}, {status}, {error}")
        time.sleep(2)
        api_status = False
        if status == "Cancelled":
            api_status = crm_api_object.dcrm_complain_update(msisdn=msisdn, sr_id=sr_id, status=status, error=error)
            print(f'api_status for  = {status}', api_status)
            api_status_msg = "failed"
            if api_status:
                api_status_msg = "successful"
            log_msg = f"DCRM complaint status {status} update {api_status_msg} for SR ID {sr_id}, MSISDN {msisdn} | Action executed {status}"
            self.log_info(msg=log_msg)
            print(log_msg)
        else:
            api_status = crm_api_object.dcrm_complain_update(msisdn=msisdn, sr_id=sr_id, status="In Progress",
                                                             error=error)
            print('api_status for  = In Progress', api_status)
            api_status_msg = "failed"
            if api_status:
                api_status_msg = "successful"
            log_msg = f"DCRM complaint status {status} update {api_status_msg} for SR ID {sr_id}, MSISDN {msisdn} | Action executed In Progress"
            self.log_info(msg=log_msg)
            print(log_msg)
            time.sleep(2)
            api_status = crm_api_object.dcrm_complain_update(msisdn=msisdn, sr_id=sr_id, status="Completed",
                                                             error=error)
            print('api_status for  = Completed', api_status)
            api_status_msg = "failed"
            if api_status:
                api_status_msg = "successful"
            log_msg = f"DCRM complaint status {status} update {api_status_msg} for SR ID {sr_id}, MSISDN {msisdn} | Action executed Completed"
            self.log_info(msg=log_msg)
            print(log_msg)
            time.sleep(2)
            api_status = crm_api_object.dcrm_complain_update(msisdn=msisdn, sr_id=sr_id, status="Closed", error=error)
            print('api_status for  = Closed', api_status)
            api_status_msg = "failed"
            if api_status:
                api_status_msg = "successful"
                log_msg = f"DCRM complaint status {status} update {api_status_msg} for SR ID {sr_id}, MSISDN {msisdn} | Action executed Closed"
            self.log_info(msg=log_msg)
            print(log_msg)

        return api_status

    def update_sr_status(self, smart_script, api, status, message):
        print("Got Final Status ", status, message)
        self.log_info(msg="CBS Response For CRM = " + str(status) + " , " + str(message))
        if status == 'Successful':
            api.crm_complain_update_counter(smart_script['MSISDN'], smart_script['SR_ID'], current_status="Open",
                                            target_status="In Progress",
                                            error=message)
            time.sleep(2)
            api.crm_complain_update_counter(smart_script['MSISDN'], smart_script['SR_ID'],
                                            target_status="Completed",
                                            current_status="In Progress",
                                            error=message)
            time.sleep(2)
            api.crm_complain_update_counter(smart_script['MSISDN'], smart_script['SR_ID'],
                                            target_status="Close",
                                            current_status="Completed",
                                            error=message)
            print('Executing CRM SRID [Closed] : ', smart_script['MSISDN'], smart_script['SR_ID'], message)
        else:
            time.sleep(2)
            api.crm_complain_update_counter(smart_script['MSISDN'], smart_script['SR_ID'],
                                            target_status="Cancelled",
                                            current_status="Open",
                                            error=message)
            print('Executing CRM SRID [Completed] : ', smart_script['MSISDN'], smart_script['SR_ID'],
                  message)

        return status, message

    def insert_db_log(self, smart_script):
        self.log_info(msg="API Response for DB  = " + str(smart_script))
        ins_status = 0
        error_msg = ""

        try:
            insert_db_query = f"""INSERT INTO CRM_DV_UN_COMBO_VOICE_SMS (
                                            SR_NUMBER, 
                                            MSISDN, 
                                            PRICE, 
                                            DB_VALIDATION, 
                                            PACK, 
                                            PACK_ID, 
                                            SMS_PACK, 
                                            SMS_USAGE, 
                                            SEC_PACK, 
                                            SEC_USAGE, 
                                            MIN_PACK, 
                                            MIN_USAGE, 
                                            HOUR_PACK, 
                                            HOUR_USAGE, 
                                            ERROR,
                                            LIST_TYPE,
                                            CREATE_TIME, 
                                            COUNTER_LOG) 
                        VALUES (
                                '{smart_script.get('SR_ID')}',
                                '{smart_script.get('MSISDN')}',
                                '{smart_script.get('DATA_PACK_PRICE')}',
                                '{smart_script.get('AVAILABLE_IN_DB')}',
                                '{smart_script.get('DATA_PACK_NAME')}',
                                '{smart_script.get('PRODUCT_ID')}',
                                '{smart_script.get('SMS_PACK_FOUND')}',
                                '{smart_script.get('SMS_PACK_USAGE')}',
                                '{smart_script.get('SEC_PACK_FOUND')}',
                                '{smart_script.get('SEC_PACK_USAGE')}',
                                '{smart_script.get('MIN_PACK_FOUND')}',
                                '{smart_script.get('MIN_PACK_USAGE')}',
                                '{smart_script.get('HOUR_PACK_FOUND')}',
                                '{smart_script.get('HOUR_PACK_USAGE')}',
                                '{smart_script.get('ERROR')}',
                                '{smart_script.get('LIST_TYPE')}',
                                SYSDATE,
                                '{smart_script.get('COUNTER_LOG')}')"""

            print(insert_db_query)
            ins_status = db_object.execute_query(insert_db_query)
            print("DB Insert Completed")
            self.log_info(msg="DB Insert Completed")
        except Exception as e:
            print(e)
            error_msg = "Failed to insert DB log"
        return ins_status, error_msg

    def db_record_validate(self, msisdn, msisdn_check):
        sr_exists = 1
        error_msg = ''
        if msisdn_check == "Prepaid":
            sr_exists_db = self.check_sr_exists_db(sr_mobile=msisdn)
            if sr_exists_db == 0:
                sr_exists = 0
                error_msg = ''
            else:
                sr_exists = sr_exists_db
                error_msg = 'You are not elegible for rebate.'
        else:
            sr_exists = 1
            error_msg = 'Your are using Postpaid'

        return sr_exists, error_msg

    def check_sr_exists_db(self, sr_mobile):
        #   db_obj.open_connection()
        print("Checking DB record for existence in last 6 months record  = " + str(sr_mobile))
        self.log_info(msg="Checking DB record for existence in last 6 months record  = " + str(sr_mobile))
        insert_db_query = f"SELECT * FROM CRM_DV_UN_COMBO_VOICE_SMS WHERE MSISDN='{sr_mobile}' AND LIST_TYPE='white' AND  TO_DATE(CREATE_TIME,'yyyy/mm/dd') BETWEEN TO_DATE(ADD_MONTHS( CURRENT_DATE, -6 ),'yyyy/mm/dd') AND TO_DATE(CURRENT_DATE,'yyyy/mm/dd')"
        # insert_db_query = r"SELECT SR_ID FROM CRM_UNWILLING_COMBO_BUNDLE_PURCHASE WHERE MSISDN= '" + str(
        #     sr_mobile) + "'"
        print(insert_db_query)
        ins_status = db_obj.total_rows_count(insert_db_query)
        print("DB Record Found =" + str(ins_status))
        self.log_info(msg="DB Record Found =" + str(ins_status))
        return ins_status

    def pre_post_check(self, msisdn):
        str_msisdn = str(msisdn)
        msisdn_without_zero = str_msisdn[-10:]
        c = 0
        response = None
        prepaid = True
        while c < 3:
            response = api.prepaid_postpaid_check(msisdn_without_zero)
            if response is None:
                c = c + 1
            else:
                break

        if response is None:
            raise NoneResponseException
        else:
            try:
                prepost = response.json()['QueryCustomerInfoResult']['Subscriber']['SubscriberInfo']['Brand']
            except Exception as e:
                self.log.log_error(f"Exception: {e}")
                raise NoneResponseException

            if prepost == '301' or prepost == '302':
                #   raise ExpectedDataNotFoundException
                print('Not a Prepaid SIM')
                return False
            else:
                return True

    def terminate_pack(self, msisdn, plan_id):
        counter = 3
        adcs_terminate = False
        response = None
        result = None
        c = 0
        while c < 3:
            response = api.adcs_terminate_plan(msisdn, plan_id)
            if response is None:
                c = c + 1
            else:
                break

        if response is not None:
            if response.status_code == 200:
                result = response.json()['message']
                if result.upper() == 'OPERATION SUCCESSFUL' and response.json()['status'] == 0:
                    self.log.log_info(f"ADCS - Termination Success! DETAILS : MSISDN - {msisdn} PLAN_ID - {plan_id}")
                    adcs_terminate = True
                    return adcs_terminate

        # print(adcs_terminate)
        if not adcs_terminate:
            error = f"ADCS - Unable to Terminate, DETAILS : MSISDN - {msisdn}, PLAN_ID - {plan_id}, RESULT - {result}"
            self.log.log_critical(error)
            raise ExpectedDataNotFoundException

    def rebate(self, msisdn, pack_price):
        response = None
        result = None
        amount = float(str(pack_price))
        print(amount)
        rebate_flag = False
        cbs_msisdn = AppUtils.mob_num_to_10_digit(msisdn)
        c = 0
        while c < 3:
            response = crm_api_object.rebate_balance(msisdn=cbs_msisdn, amount=amount)
            if response is None:
                c = c + 1
            else:
                break

        if response is not None:
            if response.status_code == 200:
                result = response.json()['ResultHeader']['ResultDesc']
                print(response.text)
                print(result)
                if result == 'Operation successful.':
                    self.log.log_info(f"CBS - Rebate Success, DETAILS : MSISDN {msisdn} AMOUNT - {amount}")
                    print('Rebate Successful')
                    rebate_flag = True
                    return rebate_flag
        if not rebate_flag:
            error = f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount} RESULT - {result}"
            self.log.log_critical(error)

    def smsapi(self, msisdn, msg):
        self.log.log_info(f"sms api called: {msg}")
        msg = str(msg)
        api.smsapi(msisdn=msisdn, msg=msg)

    def find_pack_id(self, msisdn, pack, bonus_pack):
        c = 0
        response = None
        while c < 3:
            response = api.customer_pack_details(msisdn=msisdn)
            if response is None:
                c = c + 1
            else:
                break

        if response is None:
            raise ExpectedDataNotFoundException

        pack_details = {"pack_id": None, "plan_usage": None, "pack_found": False, "bonus_pack_id": None,
                        "bonus_pack_found": False}
        pack_found = False
        if response.status_code == 200:
            plan_list = response.json()['plan']
            self.log.log_debug(plan_list)
            print(len(plan_list))
            pack_found = False
            for plans in plan_list:
                plan_name = plans['planDefinition']['name']
                plan_name = plan_name.replace(' ', "").replace('\s', "")
                print(plan_name)
                self.log.log_debug(f"Comparing pack from list of pack from API {plan_name} to {pack} from SMART SCRIPT")
                if plan_name == pack:
                    print("Pack Found")
                    pack_found = True
                    pack_id = plans['id']
                    plan_usage = plans['usage']
                    pack_details.update(pack_id=pack_id)
                    pack_details.update(plan_usage=plan_usage)
                    pack_details.update(pack_found=pack_found)
                    if plan_usage == 0:
                        bonus_pack_found = False
                        bonus_pack_id = None
                        if bonus_pack is not None:
                            for bonus_plan in plan_list:
                                print(bonus_plan)
                                self.log.log_debug(
                                    f"Comparing Bonus pack from list of pack from API {bonus_plan['planDefinition']['name']} to {bonus_pack} from SMART SCRIPT")
                                api_bonus_plan = bonus_plan['planDefinition']['name']
                                api_bonus_plan = api_bonus_plan.replace(' ', "").replace('\s', "")
                                if api_bonus_plan == bonus_pack:
                                    bonus_pack_found = True
                                    bonus_pack_id = bonus_plan['id']
                                    pack_details.update(bonus_pack_id=bonus_pack_id)
                                    pack_details.update(bonus_pack_found=bonus_pack_found)
                                    break
                            break
                        else:
                            pack_details.update(error="bonus pack is None")
                            break
                    else:
                        pack_details.update(error="pack_has_been_used")
                        return pack_details
        else:
            pack_details.update(error="pack_not_found")

        if not pack_found:
            raise ExpectedDataNotFoundException

        print(pack_details)
        self.log.log_info(f"{pack_details}")
        return pack_details

    def check_cbs_recharge_response(self, msisdn, start_time, end_time):
        self.log.log_info(f"{msisdn}")
        api_response = None
        c = 0
        while c < 3:
            api_response = crm_api_object.check_cbs_recharge_log(msisdn=msisdn, start_time=start_time,
                                                                 end_time=end_time)
            if api_response is None:
                c = c + 1
            else:
                break
        operator_id = ''
        trade_time = ''
        error_msg = ''
        print('API Response Service Layer =', api_response.status_code)
        if api_response.status_code == 200:
            res = json.loads(api_response.text)
            if 'RechargeInfo' in res['QueryRechargeLogResult']:
                trade_time = res['QueryRechargeLogResult']['RechargeInfo']['TradeTime']
                additional_property = res['QueryRechargeLogResult']['RechargeInfo']['AdditionalProperty']
                for x in additional_property:
                    #   print(x['Code'])
                    if x['Code'] == "OperatorID":
                        operator_id = x['Value']
                #   print('QueryRechargeLogResult', operator_id)
            else:
                print('Res', res)
                error_msg = 'Invalid recharge detail.'

        else:
            print('Error = ', api_response.status_code)
            error_msg = 'Recharge log API not responding.'

        return trade_time, error_msg
        # if operator_id == "easyload":
        #     return trade_time, error_msg
        # elif operator_id != '' and operator_id != "easyload":
        #     return trade_time, "rebate is only eligible for easyload."
        # else:
        #     return trade_time, error_msg

    def check_cbs_product_detail(self, msisdn, recharge_time, smart_script_data):
        print(f'Init : check_cbs_product_detail {msisdn} / {recharge_time}')
        self.log.log_info(f"{msisdn}")
        api_response = None
        try:
            c = 0
            while c < 3:
                api_response = crm_api_object.cbs_query_cdr_get_product_id(msisdn=msisdn, recharge_time=recharge_time)
                if api_response is None:
                    c = c + 1
                else:
                    break

            service_category = False
            actual_charge_amount_verify = False
            offering_id = 0
            error_msg = ''
            print('API Response Service Layer =', api_response.status_code)

            if api_response.status_code == 200:
                print(f'200 Response : check_cbs_product_detail')
                res = json.loads(api_response.text)
                print('API check_cbs_product_detail =', res)
                #   validating response for offering ID Start
                if 'CDRInfo' in res['QueryCDRResult']:
                    print(f'CDRInfo In Response : check_cbs_product_detail')
                    cdr_info = res['QueryCDRResult']['CDRInfo']
                    print(cdr_info)
                    if 'ServiceCategory' in cdr_info:
                        print('Working First Cond')
                        if int(cdr_info['ServiceCategory']) == 7:
                            print('got category')
                            service_category = True
                            if 'TotalChargeInfo' in cdr_info:
                                print('TotalChargeInfo')
                                actual_charge_amount = int(cdr_info['TotalChargeInfo']['ActualChargeAmt'])
                                print('actual_charge_amount = ', actual_charge_amount)
                                compare_smart_script_price = int(smart_script_data.get('RECHARGE_PACK_PRICE'))
                                #   compare_smart_script_price = int(34 * 10000)
                                compare_smart_script_price_less = int(compare_smart_script_price - 1)
                                compare_smart_script_price_plus = int(compare_smart_script_price + 1)
                                compareable_variance = [compare_smart_script_price, compare_smart_script_price_less,
                                                        compare_smart_script_price_plus]
                                if compareable_variance.__contains__(actual_charge_amount):
                                    print('Compare 1 True')
                                    actual_charge_amount_verify = True
                                    if 'ChargeDetail' in cdr_info:
                                        if 'OfferingID' in cdr_info['ChargeDetail']:
                                            offering_id = cdr_info['ChargeDetail']['OfferingID']

                    else:
                        print('Working second Cond')
                        for x in cdr_info:
                            if int(x['ServiceCategory']) == 7:
                                print('got category')
                                service_category = True
                                if 'TotalChargeInfo' in x:
                                    print('TotalChargeInfo')
                                    actual_charge_amount = int(x['TotalChargeInfo']['ActualChargeAmt'])
                                    print('actual_charge_amount = ', actual_charge_amount)
                                    compare_smart_script_price = int(smart_script_data.get('RECHARGE_PACK_PRICE'))
                                    compare_smart_script_price_less = int(compare_smart_script_price - 1)
                                    compare_smart_script_price_plus = int(compare_smart_script_price + 1)
                                    compareable_variance = [compare_smart_script_price, compare_smart_script_price_less,
                                                            compare_smart_script_price_plus]
                                    if compareable_variance.__contains__(actual_charge_amount):
                                        print('Compare 1 True')
                                        actual_charge_amount_verify = True
                                        if 'ChargeDetail' in x:
                                            if 'OfferingID' in x['ChargeDetail']:
                                                offering_id = x['ChargeDetail']['OfferingID']

                #   validating response for offering ID End
                print(offering_id, actual_charge_amount_verify, service_category)
            else:
                print('Error check_cbs_product_detail= ', api_response.status_code)
                error_msg = 'unable to find bundle detail.'

            print('CDRInfo service_category = ', service_category)
            print('CDRInfo actual_charge_amount_verify = ', actual_charge_amount_verify)
            print('CDRInfo offering_id = ', offering_id)
            if (service_category is True) and (actual_charge_amount_verify is True):
                return offering_id, error_msg
            elif (service_category is True) and (actual_charge_amount_verify is False):
                return offering_id, error_msg
            else:
                error_msg = "Unable to find the bundle pack"
                return offering_id, error_msg
        except Exception as e:
            offering_id = 0
            error_msg = "Invalid API Response MSISDN not getting any info"
            return offering_id, error_msg

    def check_free_unit_get_voice_sms(self, msisdn, trade_time, smart_script_data):
        self.log.log_info(f"check_free_unit_get_voice_sms {msisdn}")
        api_response = None
        c = 0
        while c < 3:
            api_response = crm_api_object.cbs_query_free_unit_get_voice_sms(msisdn=msisdn)
            if api_response is None:
                c = c + 1
            else:
                break

        sms_bundle = False
        sms_bundle_unused = False
        sec_bundle = False
        sec_bundle_unused = False
        min_bundle = False
        min_bundle_unused = False
        hour_bundle = False
        hour_bundle_unused = False
        sms = True
        sec = True
        minutes = True
        hour = True
        unused = True
        error_msg = ''
        free_unit_data = {
            'sms': False,
            'sms_FreeUnitInstanceID': 0,
            'sms_TotalInitialAmount': 0,
            'sms_UsageAmount': 0,
            'sms_FreeUnitType': None,
            'sec': False,
            'sec_FreeUnitInstanceID': 0,
            'sec_TotalInitialAmount': 0,
            'sec_UsageAmount': 0,
            'sec_FreeUnitType': None,
            'minutes': False,
            'minutes_FreeUnitInstanceID': 0,
            'minutes_TotalInitialAmount': 0,
            'minutes_UsageAmount': 0,
            'minutes_FreeUnitType': None,
            'hour': False,
            'hour_FreeUnitInstanceID': 0,
            'hour_TotalInitialAmount': 0,
            'hour_UsageAmount': 0,
            'hour_FreeUnitType': None
        }
        print('API Response Service Layer =', api_response.status_code)
        try:
            if api_response.status_code == 200:
                res = json.loads(api_response.text)
                if 'QueryFreeUnitResult' in res:
                    free_unit_data, sms, sec, minutes, hour, error_msg = self.parsing_free_unit_data(res=res,
                                                                                                     trade_time=trade_time,
                                                                                                     smart_script_data=smart_script_data)
                    print(free_unit_data)
                    self.log.log_info(msg=f"First level Free Unit Data = {free_unit_data}")
                    if free_unit_data.get('sms') is False and free_unit_data.get('sec') is False and free_unit_data.get(
                            'minutes') is False and free_unit_data.get('hour') is False:
                        self.log.log_info(msg=f"First level Free Unit verification Failed")
                        print('Second level verification')
                        self.log.log_info(msg=f"Second level verification")
                        free_unit_data, sms, sec, minutes, hour, error_msg = self.sl_parsing_free_unit_data(res=res,
                                                                                                            trade_time=trade_time,
                                                                                                            smart_script_data=smart_script_data)
                        print(free_unit_data)
                    else:
                        print('Second level Skip verification due to first level already has product')
                else:
                    unused = False
                    return unused, free_unit_data, "Unable to find bundle detail."
            else:
                print('Error check_cbs_product_detail= ', api_response.status_code)
                error_msg = 'unable to find bundle detail.'

            print('check_free_unit_get_voice_sms sms_bundle = ', sms)
            print('check_free_unit_get_voice_sms sec_bundle = ', sec)
            print('check_free_unit_get_voice_sms min_bundle = ', minutes)
            print('check_free_unit_get_voice_sms hour_bundle = ', hour)

            if sms and sec and minutes and hour:
                return unused, free_unit_data, error_msg
            else:
                unused = False
                error_msg = "Der customer, Due to used bundle you are not eligible for rebate."
                return unused, free_unit_data, error_msg
        except Exception as e:
            error_msg = "Invalid API Response MSISDN not getting any info"
            return unused, free_unit_data, error_msg

    def parsing_free_unit_data(self, res, trade_time, smart_script_data):
        self.log.log_info(msg=f"parsing_free_unit_data {res}")
        sms_bundle = False
        sms_bundle_unused = False
        sec_bundle = False
        sec_bundle_unused = False
        min_bundle = False
        min_bundle_unused = False
        hour_bundle = False
        hour_bundle_unused = False
        sms = True
        sec = True
        minutes = True
        hour = True
        unused = True
        error_msg = ''
        free_unit_data = {
            'sms': False,
            'sms_FreeUnitInstanceID': 0,
            'sms_TotalInitialAmount': 0,
            'sms_UsageAmount': 0,
            'sms_FreeUnitType': None,
            'sec': False,
            'sec_FreeUnitInstanceID': 0,
            'sec_TotalInitialAmount': 0,
            'sec_UsageAmount': 0,
            'sec_FreeUnitType': None,
            'minutes': False,
            'minutes_FreeUnitInstanceID': 0,
            'minutes_TotalInitialAmount': 0,
            'minutes_UsageAmount': 0,
            'minutes_FreeUnitType': None,
            'hour': False,
            'hour_FreeUnitInstanceID': 0,
            'hour_TotalInitialAmount': 0,
            'hour_UsageAmount': 0,
            'hour_FreeUnitType': None
        }
        print("if 'QueryFreeUnitResult' in res")
        if 'FreeUnitItem' in res['QueryFreeUnitResult']:
            print("if 'FreeUnitItem' in res['QueryFreeUnitResult']")
            cdr_info = res['QueryFreeUnitResult']['FreeUnitItem']
            if 'FreeUnitItemDetail' in cdr_info:
                print(" if 'FreeUnitItemDetail' in cdr_info:")
                print(cdr_info['FreeUnitItemDetail'], trade_time)
                cdr_effective_time = cdr_info['FreeUnitItemDetail']
                if 'EffectiveTime' in cdr_effective_time:
                    print('Effective found')
                    print(cdr_info['FreeUnitItemDetail']['EffectiveTime'], trade_time)
                    if cdr_info['FreeUnitItemDetail']['EffectiveTime'] == trade_time:
                        if cdr_info['MeasureUnit'] == "1101":
                            print('Got SMS')
                            if 'FreeUnitItemDetail' in cdr_info:
                                if 'FreeUnitOrigin' in cdr_info['FreeUnitItemDetail']:
                                    if 'OfferingKey' in cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']:
                                        if int(cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey'][
                                                   'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                            sms_bundle = True
                                            sms = False
                                            free_unit_data['sms'] = False
                                            sms_usage_amount = int(cdr_info['TotalInitialAmount']) - int(
                                                cdr_info['TotalUnusedAmount'])
                                            free_unit_data.update(sms_UsageAmount=sms_usage_amount)
                                            if int(cdr_info['TotalInitialAmount']) == int(
                                                    cdr_info['TotalUnusedAmount']):
                                                sms = True
                                                free_unit_data['sms'] = True
                                                free_unit_data['sms_TotalInitialAmount'] = int(
                                                    cdr_info['TotalInitialAmount'])
                                                free_unit_data['sms_FreeUnitInstanceID'] = int(
                                                    cdr_info['FreeUnitItemDetail']['FreeUnitInstanceID'])
                                                free_unit_data['sms_FreeUnitType'] = cdr_info['FreeUnitType']
                        elif cdr_info['MeasureUnit'] == "1003":
                            print('Got SEC')
                            if 'FreeUnitItemDetail' in cdr_info:
                                if 'FreeUnitOrigin' in cdr_info['FreeUnitItemDetail']:
                                    if 'OfferingKey' in cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']:
                                        if int(cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey'][
                                                   'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                            sec_bundle = True
                                            sec = False
                                            free_unit_data['sec'] = False
                                            sec_usage_amount = int(cdr_info['TotalInitialAmount']) - int(
                                                cdr_info['TotalUnusedAmount'])
                                            free_unit_data.update(sec_UsageAmount=sec_usage_amount)
                                            if int(cdr_info['TotalInitialAmount']) == int(
                                                    cdr_info['TotalUnusedAmount']):
                                                sec = True
                                                free_unit_data['sec'] = True
                                                free_unit_data['sec_TotalInitialAmount'] = int(
                                                    cdr_info['TotalInitialAmount'])
                                                free_unit_data['sec_FreeUnitInstanceID'] = int(
                                                    cdr_info['FreeUnitItemDetail']['FreeUnitInstanceID'])
                                                free_unit_data['sec_FreeUnitType'] = cdr_info['FreeUnitType']
                        elif cdr_info['MeasureUnit'] == "1004":
                            print('Got Minutes')
                            if 'FreeUnitItemDetail' in cdr_info:
                                if 'FreeUnitOrigin' in cdr_info['FreeUnitItemDetail']:
                                    if 'OfferingKey' in cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']:
                                        if int(cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey'][
                                                   'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                            min_bundle = True
                                            minutes = False
                                            free_unit_data['minutes'] = False
                                            minutes_usage_amount = int(cdr_info['TotalInitialAmount']) - int(
                                                cdr_info['TotalUnusedAmount'])
                                            free_unit_data.update(minutes_UsageAmount=minutes_usage_amount)
                                            if int(cdr_info['TotalInitialAmount']) == int(
                                                    cdr_info['TotalUnusedAmount']):
                                                minutes = True
                                                free_unit_data['minutes'] = True
                                                free_unit_data['minutes_TotalInitialAmount'] = int(
                                                    cdr_info['TotalInitialAmount'])
                                                free_unit_data['minutes_FreeUnitInstanceID'] = int(
                                                    cdr_info['FreeUnitItemDetail']['FreeUnitInstanceID'])
                                                free_unit_data['minutes_FreeUnitType'] = cdr_info[
                                                    'FreeUnitType']
                        elif cdr_info['MeasureUnit'] == "1005":
                            print('Got Hours')
                            if 'FreeUnitItemDetail' in cdr_info:
                                if 'FreeUnitOrigin' in cdr_info['FreeUnitItemDetail']:
                                    if 'OfferingKey' in cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']:
                                        if int(cdr_info['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey'][
                                                   'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                            hour_bundle = True
                                            hour = False
                                            free_unit_data['hour'] = False
                                            hour_usage_amount = int(cdr_info['TotalInitialAmount']) - int(
                                                cdr_info['TotalUnusedAmount'])
                                            free_unit_data.update(hour_UsageAmount=hour_usage_amount)
                                            if int(cdr_info['TotalInitialAmount']) == int(
                                                    cdr_info['TotalUnusedAmount']):
                                                hour = True
                                                free_unit_data['hour'] = True
                                                free_unit_data['hour_TotalInitialAmount'] = int(
                                                    cdr_info['TotalInitialAmount'])
                                                free_unit_data['hour_FreeUnitInstanceID'] = int(
                                                    cdr_info['FreeUnitItemDetail']['FreeUnitInstanceID'])
                                                free_unit_data['hour_FreeUnitType'] = cdr_info['FreeUnitType']
                else:
                    print('Effective not found')
                    for x in cdr_effective_time:
                        print('x cdr_effective_time= ', x)
                        if x['EffectiveTime'] == trade_time:
                            print('cdr_info', cdr_info)
                            if cdr_info['MeasureUnit'] == "1101":
                                print('Got SMS')
                                if 'FreeUnitOrigin' in x:
                                    if 'OfferingKey' in x['FreeUnitOrigin']:
                                        if int(x['FreeUnitOrigin']['OfferingKey']['OfferingID']) == int(
                                                smart_script_data.get('PRODUCT_ID')):
                                            sms_bundle = True
                                            sms = False
                                            free_unit_data['sms'] = False
                                            sms_usage_amount = int(x['InitialAmount']) - int(x['CurrentAmount'])
                                            free_unit_data.update(sms_UsageAmount=sms_usage_amount)
                                            if int(x['InitialAmount']) == int(x['CurrentAmount']):
                                                sms = True
                                                free_unit_data['sms'] = True
                                                free_unit_data['sms_TotalInitialAmount'] = int(
                                                    x['InitialAmount'])
                                                free_unit_data['sms_FreeUnitInstanceID'] = int(
                                                    x['FreeUnitInstanceID'])
                                                free_unit_data['sms_FreeUnitType'] = cdr_info['FreeUnitType']
                            elif cdr_info['MeasureUnit'] == "1003":
                                print('Got SEC')
                                if 'FreeUnitOrigin' in x:
                                    if 'OfferingKey' in x['FreeUnitOrigin']:
                                        if int(x['FreeUnitOrigin']['OfferingKey']['OfferingID']) == int(
                                                smart_script_data.get('PRODUCT_ID')):
                                            sec_bundle = True
                                            sec = False
                                            free_unit_data['sec'] = False
                                            sec_usage_amount = int(x['InitialAmount']) - int(x['CurrentAmount'])
                                            free_unit_data.update(sec_UsageAmount=sec_usage_amount)
                                            if int(x['InitialAmount']) == int(x['CurrentAmount']):
                                                sec = True
                                                free_unit_data['sec'] = True
                                                free_unit_data['sec_TotalInitialAmount'] = int(
                                                    x['InitialAmount'])
                                                free_unit_data['sec_FreeUnitInstanceID'] = int(
                                                    x['FreeUnitInstanceID'])
                                                free_unit_data['sec_FreeUnitType'] = cdr_info['FreeUnitType']
                            elif cdr_info['MeasureUnit'] == "1004":
                                print('Got Minutes')
                                if 'FreeUnitOrigin' in x:
                                    if 'OfferingKey' in x['FreeUnitOrigin']:
                                        if int(x['FreeUnitOrigin']['OfferingKey']['OfferingID']) == int(
                                                smart_script_data.get('PRODUCT_ID')):
                                            min_bundle = True
                                            minutes = False
                                            free_unit_data['minutes'] = False
                                            minutes_usage_amount = int(x['InitialAmount']) - int(
                                                x['CurrentAmount'])
                                            free_unit_data.update(minutes_UsageAmount=minutes_usage_amount)
                                            if int(x['InitialAmount']) == int(x['CurrentAmount']):
                                                minutes = True
                                                free_unit_data['minutes'] = True
                                                free_unit_data['minutes_TotalInitialAmount'] = int(
                                                    x['InitialAmount'])
                                                free_unit_data['minutes_FreeUnitInstanceID'] = int(
                                                    x['FreeUnitInstanceID'])
                                                free_unit_data['minutes_FreeUnitType'] = cdr_info[
                                                    'FreeUnitType']
                            elif cdr_info['MeasureUnit'] == "1005":
                                print('Got Hours')
                                if 'FreeUnitOrigin' in x:
                                    if 'OfferingKey' in x['FreeUnitOrigin']:
                                        if int(x['FreeUnitOrigin']['OfferingKey']['OfferingID']) == int(
                                                smart_script_data.get('PRODUCT_ID')):
                                            hour_bundle = True
                                            hour = False
                                            free_unit_data['hour'] = False
                                            hour_usage_amount = int(x['InitialAmount']) - int(
                                                x['CurrentAmount'])
                                            free_unit_data.update(hour_UsageAmount=hour_usage_amount)
                                            if int(x['InitialAmount']) == int(x['CurrentAmount']):
                                                hour = True
                                                free_unit_data['hour'] = True
                                                free_unit_data['hour_TotalInitialAmount'] = int(
                                                    x['InitialAmount'])
                                                free_unit_data['hour_FreeUnitInstanceID'] = int(
                                                    x['FreeUnitInstanceID'])
                                                free_unit_data['hour_FreeUnitType'] = cdr_info['FreeUnitType']
            else:
                print("Else 'FreeUnitItemDetail' in cdr_info")
                print("Else 'FreeUnitItemDetail' in cdr_info", cdr_info)
                for x in cdr_info:
                    print('x = ', x)
                    if 'FreeUnitItemDetail' in x:
                        if isinstance(x['FreeUnitItemDetail'], list):
                            for xx in x['FreeUnitItemDetail']:
                                print(f"xx = {xx}")
                                if 'FreeUnitOrigin' in xx:
                                    if 'OfferingKey' in xx['FreeUnitOrigin']:
                                        if 'OfferingID' in xx['FreeUnitOrigin']['OfferingKey']:
                                            if 'EffectiveTime' in xx:
                                                print('normal')
                                                print(xx['EffectiveTime'], trade_time)
                                                print(xx, trade_time)
                                                if int(xx['EffectiveTime']) == int(trade_time) or int(
                                                        xx['FreeUnitOrigin']['OfferingKey'][
                                                            'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                                    print('trade_time Match in loop')
                                                    if x['MeasureUnit'] == "1101":
                                                        print('Got SMS')
                                                        if 'FreeUnitItemDetail' in x:
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        print(
                                                                            "Product ID Match for SMS Section")
                                                                        sms_bundle = True
                                                                        sms = False
                                                                        free_unit_data['sms'] = False
                                                                        sms_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            sms_UsageAmount=sms_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            sms = True
                                                                            free_unit_data['sms'] = True
                                                                            free_unit_data[
                                                                                'sms_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'sms_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['sms_FreeUnitType'] = x[
                                                                                'FreeUnitType']
                                                    elif x['MeasureUnit'] == "1003":
                                                        print('Got SEC')
                                                        if 'FreeUnitItemDetail' in x:
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        sec_bundle = True
                                                                        sec = False
                                                                        free_unit_data['sec'] = False
                                                                        sec_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            sec_UsageAmount=sec_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            sec = True
                                                                            free_unit_data['sec'] = True
                                                                            free_unit_data[
                                                                                'sec_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'sec_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['sec_FreeUnitType'] = x[
                                                                                'FreeUnitType']
                                                            else:
                                                                print('Expect else else')
                                                                #   exit(1)
                                                    elif x['MeasureUnit'] == "1004":
                                                        print('Got Minutes')
                                                        if 'FreeUnitItemDetail' in x:
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        min_bundle = True
                                                                        minutes = False
                                                                        free_unit_data['minutes'] = False
                                                                        minutes_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            minutes_UsageAmount=minutes_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            minutes = True
                                                                            free_unit_data['minutes'] = True
                                                                            free_unit_data[
                                                                                'minutes_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'minutes_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['minutes_FreeUnitType'] = x[
                                                                                'FreeUnitType']
                                                    elif x['MeasureUnit'] == "1005":
                                                        print('Got Hours')
                                                        if 'FreeUnitItemDetail' in x:
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        hour_bundle = True
                                                                        hour = False
                                                                        free_unit_data['hour'] = False
                                                                        hour_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            hour_UsageAmount=hour_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            hour = True
                                                                            free_unit_data['hour'] = True
                                                                            free_unit_data[
                                                                                'hour_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'hour_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['hour_FreeUnitType'] = x[
                                                                                'FreeUnitType']
                                            else:
                                                print('Normal find expected time in loop')
                                                print('Loop = xx', x['FreeUnitItemDetail'])
                                                for xx in x['FreeUnitItemDetail']:
                                                    print(xx['EffectiveTime'], trade_time)
                                                    if int(xx['EffectiveTime']) == int(trade_time) or int(
                                                            xx['FreeUnitOrigin']['OfferingKey'][
                                                                'OfferingID']) == int(
                                                        smart_script_data.get('PRODUCT_ID')):
                                                        print('trade_time Match in loop')
                                                        if x['MeasureUnit'] == "1101":
                                                            print('Got SMS')
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        print(xx)
                                                                        if sms_bundle and sms and \
                                                                                free_unit_data[
                                                                                    'sms_UsageAmount'] == 0:
                                                                            break
                                                                        sms_bundle = True
                                                                        sms = False
                                                                        free_unit_data['sms'] = False
                                                                        sms_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            sms_UsageAmount=sms_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            sms = True
                                                                            free_unit_data['sms'] = True
                                                                            free_unit_data[
                                                                                'sms_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'sms_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['sms_FreeUnitType'] = \
                                                                                x[
                                                                                    'FreeUnitType']
                                                        elif x['MeasureUnit'] == "1003":
                                                            print('Got SEC')
                                                            print(xx)
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    print(xx['FreeUnitOrigin']['OfferingKey'][
                                                                              'OfferingID'],
                                                                          smart_script_data.get('PRODUCT_ID'))
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        if sec_bundle and sec and \
                                                                                free_unit_data[
                                                                                    'sec_UsageAmount'] == 0:
                                                                            break
                                                                        sec_bundle = True
                                                                        sec = False
                                                                        free_unit_data['sec'] = False
                                                                        print(xx)
                                                                        sec_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            sec_UsageAmount=sec_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            sec = True
                                                                            free_unit_data['sec'] = True
                                                                            free_unit_data[
                                                                                'sec_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'sec_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data['sec_FreeUnitType'] = \
                                                                                x[
                                                                                    'FreeUnitType']
                                                        elif x['MeasureUnit'] == "1004":
                                                            print('Got Minutes')
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        if min_bundle and minutes and \
                                                                                free_unit_data[
                                                                                    'minutes_UsageAmount'] == 0:
                                                                            break
                                                                        min_bundle = True
                                                                        minutes = False
                                                                        free_unit_data['minutes'] = False
                                                                        minutes_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            minutes_UsageAmount=minutes_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            minutes = True
                                                                            free_unit_data['minutes'] = True
                                                                            free_unit_data[
                                                                                'minutes_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'minutes_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data[
                                                                                'minutes_FreeUnitType'] = \
                                                                                x[
                                                                                    'FreeUnitType']
                                                        elif x['MeasureUnit'] == "1005":
                                                            print('Got Hours')
                                                            if 'FreeUnitOrigin' in xx:
                                                                if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                    if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                               'OfferingID']) == int(
                                                                        smart_script_data.get('PRODUCT_ID')):
                                                                        if hour_bundle and hour and \
                                                                                free_unit_data[
                                                                                    'hour_UsageAmount'] == 0:
                                                                            break
                                                                        hour_bundle = True
                                                                        hour = False
                                                                        free_unit_data['hour'] = False
                                                                        hour_usage_amount = int(
                                                                            xx['InitialAmount']) - int(
                                                                            xx['CurrentAmount'])
                                                                        free_unit_data.update(
                                                                            hour_UsageAmount=hour_usage_amount)
                                                                        if int(xx['InitialAmount']) == int(
                                                                                xx['CurrentAmount']):
                                                                            hour = True
                                                                            free_unit_data['hour'] = True
                                                                            free_unit_data[
                                                                                'hour_TotalInitialAmount'] = int(
                                                                                xx['InitialAmount'])
                                                                            free_unit_data[
                                                                                'hour_FreeUnitInstanceID'] = int(
                                                                                xx['FreeUnitInstanceID'])
                                                                            free_unit_data[
                                                                                'hour_FreeUnitType'] = x[
                                                                                'FreeUnitType']
                        else:
                            if 'FreeUnitOrigin' in x['FreeUnitItemDetail']:
                                #   print(f"x['FreeUnitItemDetail'] {x['FreeUnitItemDetail']}")
                                if 'OfferingKey' in x['FreeUnitItemDetail']['FreeUnitOrigin']:
                                    if 'OfferingID' in x['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey']:
                                        if 'EffectiveTime' in x['FreeUnitItemDetail']:
                                            print('normal')
                                            print(x['FreeUnitItemDetail']['EffectiveTime'], trade_time)
                                            print(x['FreeUnitItemDetail'], trade_time)
                                            if int(x['FreeUnitItemDetail']['EffectiveTime']) == int(
                                                    trade_time) or int(
                                                x['FreeUnitItemDetail']['FreeUnitOrigin']['OfferingKey'][
                                                    'OfferingID']) == int(smart_script_data.get('PRODUCT_ID')):
                                                print('trade_time Match in loop')
                                                if x['MeasureUnit'] == "1101":
                                                    print('Got SMS')
                                                    if 'FreeUnitItemDetail' in x:
                                                        if 'FreeUnitOrigin' in x['FreeUnitItemDetail']:
                                                            if 'OfferingKey' in x['FreeUnitItemDetail'][
                                                                'FreeUnitOrigin']:
                                                                if int(x['FreeUnitItemDetail'][
                                                                           'FreeUnitOrigin'][
                                                                           'OfferingKey']['OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    print("Product ID Match for SMS Section")
                                                                    sms_bundle = True
                                                                    sms = False
                                                                    free_unit_data['sms'] = False
                                                                    sms_usage_amount = int(
                                                                        x['TotalInitialAmount']) - int(
                                                                        x['TotalUnusedAmount'])
                                                                    free_unit_data.update(
                                                                        sms_UsageAmount=sms_usage_amount)
                                                                    if int(x['TotalInitialAmount']) == int(
                                                                            x['TotalUnusedAmount']):
                                                                        sms = True
                                                                        free_unit_data['sms'] = True
                                                                        free_unit_data[
                                                                            'sms_TotalInitialAmount'] = int(
                                                                            x['TotalInitialAmount'])
                                                                        free_unit_data[
                                                                            'sms_FreeUnitInstanceID'] = int(
                                                                            x['FreeUnitItemDetail'][
                                                                                'FreeUnitInstanceID'])
                                                                        free_unit_data['sms_FreeUnitType'] = x[
                                                                            'FreeUnitType']
                                                elif x['MeasureUnit'] == "1003":
                                                    print('Got SEC')
                                                    if 'FreeUnitItemDetail' in x:
                                                        if 'FreeUnitOrigin' in x['FreeUnitItemDetail']:
                                                            if 'OfferingKey' in x['FreeUnitItemDetail'][
                                                                'FreeUnitOrigin']:
                                                                if int(x['FreeUnitItemDetail'][
                                                                           'FreeUnitOrigin'][
                                                                           'OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    sec_bundle = True
                                                                    sec = False
                                                                    free_unit_data['sec'] = False
                                                                    sec_usage_amount = int(
                                                                        x['TotalInitialAmount']) - int(
                                                                        x['TotalUnusedAmount'])
                                                                    free_unit_data.update(
                                                                        sec_UsageAmount=sec_usage_amount)
                                                                    if int(x['TotalInitialAmount']) == int(
                                                                            x['TotalUnusedAmount']):
                                                                        sec = True
                                                                        free_unit_data['sec'] = True
                                                                        free_unit_data[
                                                                            'sec_TotalInitialAmount'] = int(
                                                                            x['TotalInitialAmount'])
                                                                        free_unit_data[
                                                                            'sec_FreeUnitInstanceID'] = int(
                                                                            x['FreeUnitItemDetail'][
                                                                                'FreeUnitInstanceID'])
                                                                        free_unit_data['sec_FreeUnitType'] = x[
                                                                            'FreeUnitType']
                                                        else:
                                                            print('Expect else else')
                                                            #   exit(1)
                                                elif x['MeasureUnit'] == "1004":
                                                    print('Got Minutes')
                                                    if 'FreeUnitItemDetail' in x:
                                                        if 'FreeUnitOrigin' in x['FreeUnitItemDetail']:
                                                            if 'OfferingKey' in x['FreeUnitItemDetail'][
                                                                'FreeUnitOrigin']:
                                                                if int(x['FreeUnitItemDetail'][
                                                                           'FreeUnitOrigin'][
                                                                           'OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    min_bundle = True
                                                                    minutes = False
                                                                    free_unit_data['minutes'] = False
                                                                    minutes_usage_amount = int(
                                                                        x['TotalInitialAmount']) - int(
                                                                        x['TotalUnusedAmount'])
                                                                    free_unit_data.update(
                                                                        minutes_UsageAmount=minutes_usage_amount)
                                                                    if int(x['TotalInitialAmount']) == int(
                                                                            x['TotalUnusedAmount']):
                                                                        minutes = True
                                                                        free_unit_data['minutes'] = True
                                                                        free_unit_data[
                                                                            'minutes_TotalInitialAmount'] = int(
                                                                            x['TotalInitialAmount'])
                                                                        free_unit_data[
                                                                            'minutes_FreeUnitInstanceID'] = int(
                                                                            x['FreeUnitItemDetail'][
                                                                                'FreeUnitInstanceID'])
                                                                        free_unit_data['minutes_FreeUnitType'] = \
                                                                            x[
                                                                                'FreeUnitType']
                                                elif x['MeasureUnit'] == "1005":
                                                    print('Got Hours')
                                                    if 'FreeUnitItemDetail' in x:
                                                        if 'FreeUnitOrigin' in x['FreeUnitItemDetail']:
                                                            if 'OfferingKey' in x['FreeUnitItemDetail'][
                                                                'FreeUnitOrigin']:
                                                                if int(x['FreeUnitItemDetail'][
                                                                           'FreeUnitOrigin'][
                                                                           'OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    hour_bundle = True
                                                                    hour = False
                                                                    free_unit_data['hour'] = False
                                                                    hour_usage_amount = int(
                                                                        x['TotalInitialAmount']) - int(
                                                                        x['TotalUnusedAmount'])
                                                                    free_unit_data.update(
                                                                        hour_UsageAmount=hour_usage_amount)
                                                                    if int(x['TotalInitialAmount']) == int(
                                                                            x['TotalUnusedAmount']):
                                                                        hour = True
                                                                        free_unit_data['hour'] = True
                                                                        free_unit_data[
                                                                            'hour_TotalInitialAmount'] = int(
                                                                            x['TotalInitialAmount'])
                                                                        free_unit_data[
                                                                            'hour_FreeUnitInstanceID'] = int(
                                                                            x['FreeUnitItemDetail'][
                                                                                'FreeUnitInstanceID'])
                                                                        free_unit_data['hour_FreeUnitType'] = x[
                                                                            'FreeUnitType']
                                        else:
                                            print('Normal find expected time in loop')
                                            print('Loop = xx', x['FreeUnitItemDetail'])
                                            for xx in x['FreeUnitItemDetail']:
                                                print(xx['EffectiveTime'], trade_time)
                                                if int(xx['EffectiveTime']) == int(trade_time) or int(
                                                        xx['FreeUnitOrigin']['OfferingKey'][
                                                            'OfferingID']) == int(
                                                    smart_script_data.get('PRODUCT_ID')):
                                                    print('trade_time Match in loop')
                                                    if x['MeasureUnit'] == "1101":
                                                        print('Got SMS')
                                                        if 'FreeUnitOrigin' in xx:
                                                            if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    print(xx)
                                                                    if sms_bundle and sms and free_unit_data[
                                                                        'sms_UsageAmount'] == 0:
                                                                        break
                                                                    sms_bundle = True
                                                                    sms = False
                                                                    free_unit_data['sms'] = False
                                                                    sms_usage_amount = int(
                                                                        xx['InitialAmount']) - int(
                                                                        xx['CurrentAmount'])
                                                                    free_unit_data.update(
                                                                        sms_UsageAmount=sms_usage_amount)
                                                                    if int(xx['InitialAmount']) == int(
                                                                            xx['CurrentAmount']):
                                                                        sms = True
                                                                        free_unit_data['sms'] = True
                                                                        free_unit_data[
                                                                            'sms_TotalInitialAmount'] = int(
                                                                            xx['InitialAmount'])
                                                                        free_unit_data[
                                                                            'sms_FreeUnitInstanceID'] = int(
                                                                            xx['FreeUnitInstanceID'])
                                                                        free_unit_data['sms_FreeUnitType'] = x[
                                                                            'FreeUnitType']
                                                    elif x['MeasureUnit'] == "1003":
                                                        print('Got SEC')
                                                        print(xx)
                                                        if 'FreeUnitOrigin' in xx:
                                                            if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                print(xx['FreeUnitOrigin']['OfferingKey'][
                                                                          'OfferingID'],
                                                                      smart_script_data.get('PRODUCT_ID'))
                                                                if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    if sec_bundle and sec and free_unit_data[
                                                                        'sec_UsageAmount'] == 0:
                                                                        break
                                                                    sec_bundle = True
                                                                    sec = False
                                                                    free_unit_data['sec'] = False
                                                                    print(xx)
                                                                    sec_usage_amount = int(
                                                                        xx['InitialAmount']) - int(
                                                                        xx['CurrentAmount'])
                                                                    free_unit_data.update(
                                                                        sec_UsageAmount=sec_usage_amount)
                                                                    if int(xx['InitialAmount']) == int(
                                                                            xx['CurrentAmount']):
                                                                        sec = True
                                                                        free_unit_data['sec'] = True
                                                                        free_unit_data[
                                                                            'sec_TotalInitialAmount'] = int(
                                                                            xx['InitialAmount'])
                                                                        free_unit_data[
                                                                            'sec_FreeUnitInstanceID'] = int(
                                                                            xx['FreeUnitInstanceID'])
                                                                        free_unit_data['sec_FreeUnitType'] = x[
                                                                            'FreeUnitType']
                                                    elif x['MeasureUnit'] == "1004":
                                                        print('Got Minutes')
                                                        if 'FreeUnitOrigin' in xx:
                                                            if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    if min_bundle and minutes and \
                                                                            free_unit_data[
                                                                                'minutes_UsageAmount'] == 0:
                                                                        break
                                                                    min_bundle = True
                                                                    minutes = False
                                                                    free_unit_data['minutes'] = False
                                                                    minutes_usage_amount = int(
                                                                        xx['InitialAmount']) - int(
                                                                        xx['CurrentAmount'])
                                                                    free_unit_data.update(
                                                                        minutes_UsageAmount=minutes_usage_amount)
                                                                    if int(xx['InitialAmount']) == int(
                                                                            xx['CurrentAmount']):
                                                                        minutes = True
                                                                        free_unit_data['minutes'] = True
                                                                        free_unit_data[
                                                                            'minutes_TotalInitialAmount'] = int(
                                                                            xx['InitialAmount'])
                                                                        free_unit_data[
                                                                            'minutes_FreeUnitInstanceID'] = int(
                                                                            xx['FreeUnitInstanceID'])
                                                                        free_unit_data['minutes_FreeUnitType'] = \
                                                                            x[
                                                                                'FreeUnitType']
                                                    elif x['MeasureUnit'] == "1005":
                                                        print('Got Hours')
                                                        if 'FreeUnitOrigin' in xx:
                                                            if 'OfferingKey' in xx['FreeUnitOrigin']:
                                                                if int(xx['FreeUnitOrigin']['OfferingKey'][
                                                                           'OfferingID']) == int(
                                                                    smart_script_data.get('PRODUCT_ID')):
                                                                    if hour_bundle and hour and free_unit_data[
                                                                        'hour_UsageAmount'] == 0:
                                                                        break
                                                                    hour_bundle = True
                                                                    hour = False
                                                                    free_unit_data['hour'] = False
                                                                    hour_usage_amount = int(
                                                                        xx['InitialAmount']) - int(
                                                                        xx['CurrentAmount'])
                                                                    free_unit_data.update(
                                                                        hour_UsageAmount=hour_usage_amount)
                                                                    if int(xx['InitialAmount']) == int(
                                                                            xx['CurrentAmount']):
                                                                        hour = True
                                                                        free_unit_data['hour'] = True
                                                                        free_unit_data[
                                                                            'hour_TotalInitialAmount'] = int(
                                                                            xx['InitialAmount'])
                                                                        free_unit_data[
                                                                            'hour_FreeUnitInstanceID'] = int(
                                                                            xx['FreeUnitInstanceID'])
                                                                        free_unit_data['hour_FreeUnitType'] = x[
                                                                            'FreeUnitType']
        return free_unit_data, sms, sec, minutes, hour, error_msg

    def sl_parsing_free_unit_data(self, res, trade_time, smart_script_data):
        pass

    def terminate_adjust_voice_sms(self, msisdn, free_unit_type, free_unit_instance_id, adjustment_amount):
        print(f"Terminate Request for {msisdn},{free_unit_type},{free_unit_instance_id},{adjustment_amount}")
        self.log.log_info(f"{msisdn}")
        print(msisdn, free_unit_type, free_unit_instance_id, adjustment_amount)
        msisdn = AppUtils.mob_num_to_10_digit(mob=msisdn)
        api_response = None
        c = 0
        while c < 3:
            api_response = crm_api_object.cbs_free_unit_adjustment(msisdn=msisdn,
                                                                   free_unit_type=free_unit_type,
                                                                   free_unit_instance_id=free_unit_instance_id,
                                                                   adjustment_amount=adjustment_amount)
            if api_response is None:
                c = c + 1
            else:
                break
        pack_terminated = False
        trade_time = ''
        error_msg = ''
        print('API Response Service Layer =', api_response)
        if api_response.status_code == 200:
            try:
                res = json.loads(api_response.text)
                print(res)
                if 'ResultCode' in res['ResultHeader']:
                    response_code_string = res['ResultHeader']['ResultCode']
                    response_code = response_code_string.strip()
                    if int(response_code) == 0:
                        pack_terminated = True
                    else:
                        error_msg = res['ResultHeader']['ResultDesc']
                else:
                    error_msg = res['ResultHeader']['ResultDesc']
            except Exception as e:
                print(e)
                error_msg = 'API Bundle Terminate API not responding.'
        else:
            print('Error = ', api_response.status_code)
            error_msg = 'Bundle Terminate API not responding.'

        if pack_terminated:
            print(pack_terminated, error_msg)
            return pack_terminated, error_msg
        else:
            print(pack_terminated, error_msg)
            return pack_terminated, error_msg

    @staticmethod
    def check_download_file(file_name):
        flag = False
        file_name_ext = file_name.rstrip('.xlsx')
        for f in os.listdir(default_download_path):
            if f == f"{file_name_ext}.csv":
                flag = True
                break
        return flag

    def manual_report_generate(self):
        # Manual Mail trigger start
        data_robi_file = list()
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_postpaid_20211105_1.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_1.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_2.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_3.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_4.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_5.csv")
        data_robi_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_6.csv")
        for fileNameRObi in data_robi_file:
            self.get_success_failure_rates_robi(file_dir=fileNameRObi)

        data_at_file = list()
        data_at_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_postpaid_20211105_1.csv")
        data_at_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_prepaid_20211105_1.csv")
        data_at_file.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_prepaid_20211105_1.csv")
        for fileNameAT in data_at_file:
            self.get_success_failure_rates_airtel(file_dir=fileNameAT)

        all_files_list = list()
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_postpaid_20211105_1.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_1.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_2.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_3.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_4.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_5.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\robi\call_drop_rebate_prepaid_20211105_6.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_postpaid_20211105_1.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_prepaid_20211105_1.csv")
        all_files_list.append(
            r"E:\Rayhan_development\Call-Drop-RPA\files\manual\at\call_drop_air_rebate_prepaid_20211105_2.csv")
        final_zip_dir = self.compress_file_manual(all_files_list=all_files_list)
        self.send_mail(attach_file_dir=final_zip_dir)
        # Manual Mail trigger end

    @staticmethod
    def fetch_call_drop_rpa_status_flag():
        data_list = list()
        select_query = "select * from CALL_DROP_RPA_STATUS_FLAG where DEFAULT_EMAIL='calldrop@robi.com.bd'"
        call_drop_flag = db_object.select_query(query=select_query)
        for flag in call_drop_flag:
            call_drop_data = {
                "CALL_DROP_RPA_STATUS": str(flag[0]),
                "SENT_SMS_RPA_STATUS": str(flag[1]),
                "MAIL_STATUS": flag[2],
                "MAIL_SEND_DATE": str(flag[3]),
            }
            data_list.append(call_drop_data)
        return data_list

    def check_call_drop_rpa_pending_flag(self):
        pending_flag = False
        data_list = self.fetch_call_drop_rpa_status_flag()
        if data_list is not None:
            for data in data_list:
                if data.get("CALL_DROP_RPA_STATUS") == "Pending":
                    pending_flag = True
            return pending_flag

    def check_call_drop_send_sms_rpa_pending_flag(self):
        pending_flag = False
        data_list = self.fetch_call_drop_rpa_status_flag()
        if data_list is not None:
            for data in data_list:
                if data.get("SENT_SMS_RPA_STATUS") == "Pending":
                    pending_flag = True
            return pending_flag

    @staticmethod
    def get_file_name_with_status(previous_date):
        query = f"SELECT FILE_NAME,STATUS FROM CALL_DROP_FILE_LOG WHERE FILE_NAME LIKE '%{previous_date}%'"
        file_name_with_status_list = db_object.select_query(query=query)
        return file_name_with_status_list

    @staticmethod
    def send_file_status_email_to_developers(previous_date, file_name_with_status_list):
        mail = Mail()
        targets = AppUtils.conf['error_to'].split(',')
        mail_title = f"CALL DROP REBATE FILE STATUS - {previous_date}"
        table_row = ''
        for row in file_name_with_status_list:
            table_row += f"""
                          <tr>
                            <td>{row[0]}</td>
                            <td>{row[1]}</td>
                          </tr>"""
        mail_body = f"""<p>Dear Concern, <br><br> 

                        Please receive the considered PROVISIONING FILE NAME & STATUS.</p><br>

                        <table>
                          <tr>
                            <th>FILE NAME</th>
                            <th>STATUS</th>
                          </tr>{table_row}
                        </table>
                        <p>Regards, <br> Automated Call Drop Rebate System</p>"""

        mail.send_mail_to(targets, None, mail_title, mail_body)
        mail.send()

    @staticmethod
    def set_call_drop_send_sms_rpa_running_flag():
        update_query = "update CALL_DROP_RPA_STATUS_FLAG set SENT_SMS_RPA_STATUS='Running' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
        db_object.execute_query(query=update_query)

    @staticmethod
    def set_call_drop_send_sms_rpa_pending_flag():
        update_query = "update CALL_DROP_RPA_STATUS_FLAG set SENT_SMS_RPA_STATUS='Pending' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
        db_object.execute_query(query=update_query)

    @staticmethod
    def set_call_drop_rpa_running_flag():
        update_query = "update CALL_DROP_RPA_STATUS_FLAG set CALL_DROP_RPA_STATUS='Running' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
        db_object.execute_query(query=update_query)

    @staticmethod
    def set_call_drop_rpa_pending_flag():
        update_query = "update CALL_DROP_RPA_STATUS_FLAG set CALL_DROP_RPA_STATUS='Pending' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
        db_object.execute_query(query=update_query)

    def fetch_file_from_server(self, local_dir, remote_dir):
        # sftp server connection
        # host = "10.101.10.217"  # Enter host
        host = AppUtils.conf['sftp_host']
        # username = "sftp_rpa"  # Enter USERNAME
        username = AppUtils.conf['sftp_username']  # Enter USERNAME
        # password = "eFPoGS=q,tH5+\nR"  # Enter password
        password = AppUtils.conf['sftp_password']  # Enter password
        msisdn_list = AppUtils.conf['call_drop_sms_to_concern'].split(',')
        print(host, username, password, msisdn_list)
        try:
            # print(host, username, repr(password).strip("'"))
            with RemoteServerConnection(host=host, username=username,
                                        password=repr(password).strip("'")) as sftp_connection:
                print("Server connection established")
                self.log.log_info(msg="Server connection established")
                # remote server object
                remote_server = RemoteServerOperation(sftp_connection)
                # check validate file
                validation_found = remote_server.check_validation_file(remote_dir=remote_dir)
                if validation_found:
                    # copy files from remote server directory
                    files_name_list = remote_server.copy_file_from_remote(local_dir=local_dir,
                                                                          remote_dir=remote_dir)
                    return files_name_list
                else:
                    crm_api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop Rebate Validate File Not Found!")
                    print("Call Drop Rebate Validate File Not Found!")
                    self.set_call_drop_rpa_pending_flag()
                    self.webpage.driver.quit()
                    raise SystemExit
        except paramiko.ssh_exception.SSHException as e:
            print('SSH error, you need to add the public key of your remote in your local known_hosts file first.', e)
            self.log.log_warn(
                msg=f"SSH error, you need to add the public key of your remote in your local known_hosts file first - {e}")

    def compress_file_manual(self, all_files_list):
        # print(self.all_files_list)
        # print(all_files_list)
        # Select the compression mode ZIP_DEFLATED for compression
        # or zipfile.ZIP_STORED to just store the file
        compression = zipfile.ZIP_DEFLATED

        date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
        date_today = date_today.strftime("%Y%m%d")
        # create the zip file first parameter path/name, second mode

        # config upto zipfile

        zip_file_dir = AppUtils.conf['zip_file_dir']

        final_zip_dir = f"{zip_file_dir}\\call_drop_rebate_{date_today}.zip"

        zf = zipfile.ZipFile(final_zip_dir, mode="w")
        try:
            for file_name in all_files_list:
                # Add file to the zip file
                # first parameter file to zip, second filename in zip
                zf.write(file_name, file_name.split("\\")[-1], compress_type=compression)
            return final_zip_dir
        except FileNotFoundError:
            print("File not found")
            self.log.log_info(msg="File not found")
        finally:
            # Don't forget to close the file!
            zf.close()

    def fetch_all_file_call_drop(self, date):
        select_query = f"SELECT * FROM CALL_DROP_FILE_LOG WHERE STATUS='Done' AND FILE_NAME LIKE '%{date}%'"
        file_log_data = db_object.select_query(select_query)
        data_list = list()
        if file_log_data is not None:
            for file_log in file_log_data:
                file_log_dict = {
                    "FILE_NAME": str(file_log[0]),
                    "BRAND": str(file_log[1]),
                    "STATUS": str(file_log[2]),
                    "TOTAL_RECORD": str(file_log[4]),
                    "SUCCESS_RECORD": str(file_log[5]),
                    "FAILED_RECORD": str(file_log[6]),
                }
                data_list.append(file_log_dict)
        # print(data_list)
        self.log.log_info(msg=data_list)
        return data_list

    def fetch_today_file_call_drop(self, date):
        select_query = f"SELECT * FROM CALL_DROP_FILE_LOG WHERE FILE_NAME LIKE '%{date}%'"
        file_log_data = db_object.select_query(select_query)
        data_list = list()
        if file_log_data is not None:
            for file_log in file_log_data:
                file_log_dict = {
                    "FILE_NAME": str(file_log[0]),
                    "BRAND": str(file_log[1]),
                    "STATUS": str(file_log[2]),
                    "TOTAL_RECORD": str(file_log[4]),
                    "SUCCESS_RECORD": str(file_log[5]),
                    "FAILED_RECORD": str(file_log[6]),
                }
                data_list.append(file_log_dict)
        # print(data_list)
        self.log.log_info(msg=data_list)
        return data_list

    def compress_file(self):
        all_files_list = list()
        robi_dir = AppUtils.conf["downloaded_files_dir_robi"]
        airtel_dir = AppUtils.conf["downloaded_files_dir_airtel"]
        # Select the compression mode ZIP_DEFLATED for compression
        # or zipfile.ZIP_STORED to just store the file
        compression = zipfile.ZIP_DEFLATED

        date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
        date_today = date_today.strftime("%Y%m%d")
        # date_today = '20211106'

        data_list = self.fetch_all_file_call_drop(date=date_today)
        if data_list is not None:
            for data in data_list:
                if data.get("BRAND") == 'Robi':
                    file_name = data.get("FILE_NAME").split('.xlsx')
                    file_name_ext = f"{robi_dir}{file_name[0]}.csv"
                    all_files_list.append(file_name_ext)
                elif data.get("BRAND") == 'Airtel':
                    file_name = data.get("FILE_NAME").split('.xlsx')
                    file_name_ext = f"{airtel_dir}{file_name[0]}.csv"
                    all_files_list.append(file_name_ext)

        # print(all_files_list)
        # create the zip file first parameter path/name, second mode

        # config upto zipfile

        zip_file_dir = AppUtils.conf['zip_file_dir']

        final_zip_dir = f"{zip_file_dir}\\call_drop_rebate_{date_today}.zip"

        zf = zipfile.ZipFile(final_zip_dir, mode="w")
        try:
            for file_name in all_files_list:
                # Add file to the zip file
                # first parameter file to zip, second filename in zip
                zf.write(file_name, file_name.split("\\")[-1], compress_type=compression)
            return final_zip_dir
        except FileNotFoundError:
            print("File not found")
            self.log.log_info(msg="File not found")
        finally:
            # Don't forget to close the file!
            zf.close()

    def generate_xlsx_file(self, csv_files_name, csv_file_dir_name, xlsx_file_dir_name):
        xlsx_files_name_list = list()
        try:
            if csv_files_name is not None:
                for csv_file in csv_files_name:
                    csv_file_dir = AppUtils.conf['csv_file_dir']
                    csv_file_location = f"{csv_file_dir}/{csv_file_dir_name}/{csv_file}"
                    # Reading the csv file
                    df_new = pd.read_csv(csv_file_location)
                    xlsx_file = csv_file.replace(".csv", ".xlsx")
                    xlsx_files_name_list.append(xlsx_file)
                    xlsx_file_dir = AppUtils.conf['xlsx_file_dir']
                    xlsx_file_path = f"{xlsx_file_dir}/{xlsx_file_dir_name}/{xlsx_file}"
                    if not os.path.isfile(xlsx_file_path):
                        GFG = pd.ExcelWriter(xlsx_file_path)
                        df_new.to_excel(GFG, index=False)
                        GFG.save()
                    # os.remove(csv_file_location)
                return xlsx_files_name_list

        except Exception as e:
            print(e)

    def send_sms_check(self):
        date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
        date_today = date_today.strftime("%Y%m%d")
        robi_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
                                      f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
                                      f"FROM CALL_DROP_SMS_LOG_ROBI " \
                                      f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date_today}%'"
        robi_sms_status_count_query_response = db_object.select_query(query=robi_sms_status_count_query)
        robi_success_sms_count = robi_sms_status_count_query_response[0][0]
        robi_failed_sms_count = robi_sms_status_count_query_response[0][1]

        airtel_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
                                        f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
                                        f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
                                        f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date_today}%'"
        airtel_sms_status_count_query_response = db_object.select_query(query=airtel_sms_status_count_query)
        airtel_success_sms_count = airtel_sms_status_count_query_response[0][0]
        airtel_failed_sms_count = airtel_sms_status_count_query_response[0][1]

        print(f'Robi Success SMS\t- {robi_success_sms_count}')
        print(f'Robi Failed SMS\t\t- {robi_failed_sms_count}')
        print(f'Airtel Success SMS\t- {airtel_success_sms_count}')
        print(f'Airtel Failed SMS\t- {airtel_failed_sms_count}')

        self.log.log_info(f'Robi Success SMS - {robi_success_sms_count}')
        self.log.log_info(f'Robi Failed SMS - {robi_failed_sms_count}')
        self.log.log_info(f'Airtel Success SMS - {airtel_success_sms_count}')
        self.log.log_info(f'Airtel Failed SMS - {airtel_failed_sms_count}')

        return robi_success_sms_count, robi_failed_sms_count, airtel_success_sms_count, airtel_failed_sms_count

    @staticmethod
    def time_now():
        start_time = AppUtils.conf["mail_sent_start_time"]
        end_time = AppUtils.conf["mail_sent_end_time"]
        tz = pytz.timezone('Asia/Dhaka')
        date_time_today = datetime.now(tz)
        time_now = date_time_today.strftime("%H:%M:%S")
        # time_now = "23:00:00"

        if start_time < end_time:
            return start_time <= time_now <= end_time
        else:  # Over midnight
            return time_now >= start_time or time_now <= end_time

    @staticmethod
    def pending_sms_check():
        flag = False
        select_query = "select count(*) from CALL_DROP_SMS_LOG_ROBI where SMS_STATUS like 'Pending%'"
        count = db_object.select_query(query=select_query)
        robi_pending = 0 if count is None else int(count[0][0])

        select_query = "select count(*) from CALL_DROP_SMS_LOG_AIRTEL where SMS_STATUS like 'Pending%'"
        count = db_object.select_query(query=select_query)
        airtel_pending = 0 if count is None else int(count[0][0])

        if robi_pending == 0 and airtel_pending == 0:
            flag = True
        return flag

    @staticmethod
    def fetch_all_robi_sms_data(file_name):
        data_list = list()
        select_query = "select * from CALL_DROP_SMS_LOG_ROBI where FILE_NAME='" + file_name + "'"
        robi_data = db_object.select_query(query=select_query)
        print(robi_data)
        if robi_data is not None:
            for robi_data in robi_data:
                print(robi_data)
                sms_data = {
                    "MSISDN": str(robi_data[0]),
                    "AMOUNT": str(robi_data[1]),
                    "API_RESPONSE": str(robi_data[10]),
                    "SMS_STATUS": str(robi_data[11]),
                    "FILE_NAME": str(robi_data[12])
                }
                data_list.append(sms_data)
        return data_list

    @staticmethod
    def fetch_all_airtel_sms_data(file_name):
        data_list = list()
        select_query = "select * from CALL_DROP_SMS_LOG_AIRTEL where FILE_NAME='" + file_name + "'"
        airtel_data = db_object.select_query(query=select_query)
        if airtel_data is not None:
            for airtel_data in airtel_data:
                sms_data = {
                    "MSISDN": str(airtel_data[0]),
                    "AMOUNT": str(airtel_data[1]),
                    "API_RESPONSE": str(airtel_data[10]),
                    "SMS_STATUS": str(airtel_data[11]),
                    "FILE_NAME": str(airtel_data[12])
                }
                data_list.append(sms_data)
        return data_list

    def count_success_failure_rates(self, date):
        robi_postpaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
                              f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
                              f"FROM CALL_DROP_SMS_LOG_ROBI " \
                              f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%postpaid%'"
        robi_postpaid_query_response = db_object.select_query(query=robi_postpaid_query)
        robi_postpaid_success_count = int(robi_postpaid_query_response[0][0]) if robi_postpaid_query_response[0][
                                                                                     0] is not None else 0
        robi_postpaid_failure_count = int(robi_postpaid_query_response[0][1]) if robi_postpaid_query_response[0][
                                                                                     1] is not None else 0

        robi_prepaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
                             f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
                             f"FROM CALL_DROP_SMS_LOG_ROBI " \
                             f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%prepaid%'"
        robi_prepaid_query_response = db_object.select_query(query=robi_prepaid_query)
        robi_prepaid_success_count = int(robi_prepaid_query_response[0][0]) if robi_prepaid_query_response[0][
                                                                                   0] is not None else 0
        robi_prepaid_failure_count = int(robi_prepaid_query_response[0][1]) if robi_prepaid_query_response[0][
                                                                                   1] is not None else 0

        airtel_postpaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
                                f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
                                f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
                                f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%postpaid%'"
        airtel_postpaid_query_response = db_object.select_query(query=airtel_postpaid_query)
        airtel_postpaid_success_count = int(airtel_postpaid_query_response[0][0]) if airtel_postpaid_query_response[0][0] is not None else 0
        airtel_postpaid_failure_count = int(airtel_postpaid_query_response[0][1]) if airtel_postpaid_query_response[0][1] is not None else 0

        airtel_prepaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
                               f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
                               f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
                               f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%prepaid%'"
        airtel_prepaid_query_response = db_object.select_query(query=airtel_prepaid_query)
        airtel_prepaid_success_count = int(airtel_prepaid_query_response[0][0]) if airtel_prepaid_query_response[0][0] is not None else 0
        airtel_prepaid_failure_count = int(airtel_prepaid_query_response[0][1]) if airtel_prepaid_query_response[0][1] is not None else 0

        print(f'Robi Postpaid Success Minute\t- {robi_postpaid_success_count // 60}')
        print(f'Robi Postpaid Failure Minute\t- {robi_postpaid_failure_count // 60}')
        print(f'Robi Prepaid Success Minute\t\t- {robi_prepaid_success_count // 60}')
        print(f'Robi Prepaid Failure Minute\t\t- {robi_prepaid_failure_count // 60}')
        print(f'Airtel Postpaid Success Minute\t- {airtel_postpaid_success_count // 60}')
        print(f'Airtel Postpaid Failure Minute\t- {airtel_postpaid_failure_count // 60}')
        print(f'Airtel Prepaid Success Minute\t- {airtel_prepaid_success_count // 60}')
        print(f'Airtel Prepaid Failure Minute\t- {airtel_prepaid_failure_count // 60}')

        self.log.log_info(f'Robi Postpaid Success Minute - {robi_postpaid_success_count // 60}')
        self.log.log_info(f'Robi Postpaid Failure Minute - {robi_postpaid_failure_count // 60}')
        self.log.log_info(f'Robi Prepaid Success Minute - {robi_prepaid_success_count // 60}')
        self.log.log_info(f'Robi Prepaid Failure Minute - {robi_prepaid_failure_count // 60}')
        self.log.log_info(f'Airtel Postpaid Success Minute - {airtel_postpaid_success_count // 60}')
        self.log.log_info(f'Airtel Postpaid Failure Minute - {airtel_postpaid_failure_count // 60}')
        self.log.log_info(f'Airtel Prepaid Success Minute - {airtel_prepaid_success_count // 60}')
        self.log.log_info(f'Airtel Prepaid Failure Minute - {airtel_prepaid_failure_count // 60}')

        return robi_prepaid_success_count, robi_prepaid_failure_count, robi_postpaid_success_count, \
               robi_postpaid_failure_count, airtel_prepaid_success_count, airtel_prepaid_failure_count, \
               airtel_postpaid_success_count, airtel_postpaid_failure_count

    def check_mail_sent(self):
        date_today = dt.date.today()
        format_date = date_today.strftime("%Y%m%d")
        data_list = self.fetch_call_drop_rpa_status_flag()
        if data_list is not None:
            for data in data_list:
                if data.get("MAIL_STATUS") is None:
                    return True
                elif data.get("MAIL_STATUS") == "Mail_Sent" and data.get("MAIL_SEND_DATE") == format_date:
                    return False
                if int(data.get("MAIL_SEND_DATE")) < int(format_date):
                    update_query = "update CALL_DROP_RPA_STATUS_FLAG set MAIL_STATUS='' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
                    db_object.execute_query(query=update_query)

    def check_call_drop_all_file_status_done(self):
        file_list = list()
        done_status_file_list = list()
        equal_flag = False
        try:
            date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
            date_today = date_today.strftime("%Y%m%d")
            # date_today = '20211106'

            data_list = self.fetch_today_file_call_drop(date_today)

            if data_list is not None:
                for data in data_list:
                    if date_today in data.get("FILE_NAME"):
                        file_list.append(data.get("FILE_NAME"))
                    if date_today in data.get("FILE_NAME") and data.get("STATUS") == "Done":
                        done_status_file_list.append(data.get("FILE_NAME"))

                if len(file_list) == len(done_status_file_list):
                    equal_flag = True
            for file in file_list:
                print(file)
            return equal_flag
        except Exception as e:
            print(e)
            self.log.log_critical(e)

    def generate_report(self):
        pass

    def update_daily_mail_status_to_file(self, total_unique_subscribe_robi, total_unique_subscribe_airtel):
        month_name = datetime.now().strftime('%B')
        if dt.date.today().day == 1:
            current_date = dt.date.today()
            previous_month = current_date.replace(day=1) - dt.timedelta(days=1)
            previous_month_name = previous_month.strftime('%B')
            filename = f"monthly_report_{previous_month_name}.xlsx"
        else:
            filename = f"monthly_report_{month_name}.xlsx"
        if os.path.exists(filename):
            df = pd.read_excel(filename, index_col=False)
            dic = {
                "DATE": dt.date.today() - dt.timedelta(days=1),
                "ROBI": total_unique_subscribe_robi,
                "AIRTEL": total_unique_subscribe_airtel,
                "TOTAL": total_unique_subscribe_robi + total_unique_subscribe_airtel
            }
            df = pd.concat([df, pd.DataFrame([dic])])
            df.to_excel(filename, index=False)
        else:
            dic = {
                "DATE": dt.date.today() - dt.timedelta(days=1),
                "ROBI": total_unique_subscribe_robi,
                "AIRTEL": total_unique_subscribe_airtel,
                "TOTAL": total_unique_subscribe_robi + total_unique_subscribe_airtel
            }
            print("File Creating")
            self.log.log_info(f"Monthly report file creating for Month: {month_name}")
            df = pd.DataFrame([dic])
            df.to_excel(filename, index=False)

    @staticmethod
    def check_missing_dates(filename):
        df = pd.read_excel(filename, index_col=False)
        df['DATE'] = pd.to_datetime(df['DATE'])
        today = dt.date.today()
        start_date = dt.date(today.year, today.month - 1, 1)
        first_day_of_month = dt.date(today.year, today.month, 1)
        end_date = first_day_of_month - dt.timedelta(days=1)
        all_dates = pd.date_range(start=start_date, end=end_date)
        missing_dates = list(set(all_dates) - set(df['DATE']))
        missing_dates.sort()
        return missing_dates

    @staticmethod
    def send_final_reprot_mail():
        mail = Mail()
        conf = ConfigParser()
        current_date = dt.date.today()
        previous_month = current_date.replace(day=1) - dt.timedelta(days=1)
        previous_month_year = previous_month.year
        previous_month_name = previous_month.strftime('%B')
        filename = f"monthly_report_{previous_month_name}.xlsx"
        attachments = [filename]
        targets = conf.get_call_drop_report_email_to()
        mail_title = f"CALL DROP FINAL REPORT - {previous_month_name}, {previous_month_year}"
        mail_body = f"""<p>Dear Holy Apu, <br><br> I hope you are doing well. As per request, I have attached the Call Drop Rebate Report for the Month of {previous_month_name}, {previous_month_year} with this email.</p><br>
                    <p>Regards, <br> Automated Call Drop Rebate System</p>"""
        mail.send_mail_to(targets, None, mail_title, mail_body, attachments)
        mail.send()

    @staticmethod
    def send_missing_dates_to_dev(missing_dates):
        mail = Mail()
        conf = ConfigParser()
        current_date = dt.date.today()
        previous_month = current_date.replace(day=1) - dt.timedelta(days=1)
        previous_month_year = previous_month.year
        previous_month_name = previous_month.strftime('%B')
        filename = f"monthly_report_{previous_month_name}.xlsx"
        attachments = [filename]
        targets = conf.get_error_reporting_email()
        date_string = ', '.join(str(date.date()) for date in missing_dates)
        mail_title = f"CALL DROP FINAL REPORT - {previous_month_name}, {previous_month_year}"
        mail_body = f"""<p>Dear Concern, <br><br> RPA might not send email in those following days - {date_string}. Please check the attacment and do neccesary steps.</p><br>
                            <p>Regards, <br> Automated Call Drop Rebate System</p>"""
        mail.send_mail_to(targets, None, mail_title, mail_body, attachments)
        mail.send()

    @staticmethod
    def send_previous_missing_dates_to_dev(missing_dates):
        mail = Mail()
        conf = ConfigParser()
        current_date = dt.date.today()
        current_year = current_date.year
        month_name = current_date.strftime('%B')
        filename = f"monthly_report_{month_name}.xlsx"
        attachments = [filename]
        targets = conf.get_error_reporting_email()
        date_string = ', '.join(str(date.date()) for date in missing_dates)
        mail_title = f"CALL DROP FINAL REPORT - {month_name}, {current_year}"
        mail_body = f"""<p>Dear Concern, <br><br> RPA might not send email in those following days - {date_string}. Please check the attacment and do neccesary steps.</p><br>
                            <p>Regards, <br> Automated Call Drop Rebate System</p>"""
        mail.send_mail_to(targets, None, mail_title, mail_body, attachments)
        mail.send()

    def send_final_monthly_report(self):
        current_date = dt.date.today()
        previous_month = current_date.replace(day=1) - dt.timedelta(days=1)
        previous_month_name = previous_month.strftime('%B')
        filename = f"monthly_report_{previous_month_name}.xlsx"
        if os.path.exists(filename):
            missing_dates = self.check_missing_dates(filename)
            if len(missing_dates) == 0:
                self.send_final_reprot_mail()
            else:
                self.send_missing_dates_to_dev(missing_dates)
        else:
            self.log.log_error(f"FIle: {filename} not found")

    def check_previous_missing_dates(self):
        month_name = datetime.now().strftime('%B')
        filename = f"monthly_report_{month_name}.xlsx"
        try:
            df = pd.read_excel(filename, index_col=False)
            df['DATE'] = pd.to_datetime(df['DATE'])
            today = dt.date.today()
            start_date = dt.date(today.year, today.month, 1)
            end_date = today - dt.timedelta(days=1)
            all_dates = pd.date_range(start=start_date, end=end_date)
            missing_dates = list(set(all_dates) - set(df['DATE']))
            missing_dates.sort()
            return missing_dates
        except Exception as e:
            self.log.log_error(f"File: {filename} not found")

    def send_mail(self):
        mail = Mail()
        conf = ConfigParser()
        if self.check_mail_sent() and self.pending_sms_check() and self.time_now() and self.check_call_drop_all_file_status_done():
            attach_file_dir = self.compress_file()
            robi_success_sms_count, robi_failed_sms_count, airtel_success_sms_count, airtel_failed_sms_count = self.send_sms_check()

            targets = conf.get_call_drop_report_email_to()
            print(targets)

            attachments = attach_file_dir

            current_date = dt.date.today()
            date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
            format_date = date_today.strftime("%Y%m%d")
            mail_send_date = current_date.strftime("%Y%m%d")
            date_today = date_today.strftime("%d-%m-%Y")

            robi_prepaid_success_count, robi_prepaid_failure_count, \
            robi_postpaid_success_count, robi_postpaid_failure_count, \
            airtel_prepaid_success_count, airtel_prepaid_failure_count, \
            airtel_postpaid_success_count, airtel_postpaid_failure_count = self.count_success_failure_rates(format_date)

            total_min_success_posted_robi = robi_prepaid_success_count + robi_postpaid_success_count
            total_min_fail_posted_robi = robi_prepaid_failure_count + robi_postpaid_failure_count
            total_min_success_posted_airtel = airtel_prepaid_success_count + airtel_postpaid_success_count
            total_min_fail_posted_airtel = airtel_prepaid_failure_count + airtel_postpaid_failure_count
            total_success_fail_comp_min_robi = total_min_success_posted_robi + total_min_fail_posted_robi
            total_success_fail_comp_min_airtel = total_min_success_posted_airtel + total_min_fail_posted_airtel

            # robi data
            prepaid_min_success_robi = 0 if robi_prepaid_success_count == 0 else robi_prepaid_success_count // 60
            prepaid_min_fail_robi = 0 if robi_prepaid_failure_count == 0 else robi_prepaid_failure_count // 60
            postpaid_min_success_robi = 0 if robi_postpaid_success_count == 0 else robi_postpaid_success_count // 60
            postpaid_min_fail_robi = 0 if robi_postpaid_failure_count == 0 else robi_postpaid_failure_count // 60
            total_min_success_posted_robi = 0 if total_min_success_posted_robi == 0 else total_min_success_posted_robi // 60
            total_min_fail_posted_robi = 0 if total_min_fail_posted_robi == 0 else total_min_fail_posted_robi // 60
            total_compensation_min_robi = 0 if total_success_fail_comp_min_robi == 0 else total_success_fail_comp_min_robi // 60
            total_unique_subscribe_robi = robi_success_sms_count + robi_failed_sms_count

            # airtel data
            prepaid_min_success_airtel = 0 if airtel_prepaid_success_count == 0 else airtel_prepaid_success_count // 60
            prepaid_min_fail_airtel = 0 if airtel_prepaid_failure_count == 0 else airtel_prepaid_failure_count // 60
            postpaid_min_success_airtel = 0 if airtel_postpaid_success_count == 0 else airtel_postpaid_success_count // 60
            postpaid_min_fail_airtel = 0 if airtel_postpaid_failure_count == 0 else airtel_postpaid_failure_count // 60
            total_min_success_posted_airtel = 0 if total_min_success_posted_airtel == 0 else total_min_success_posted_airtel // 60
            total_min_fail_posted_airtel = 0 if total_min_fail_posted_airtel == 0 else total_min_fail_posted_airtel // 60
            total_compensation_min_airtel = 0 if total_success_fail_comp_min_airtel == 0 else total_success_fail_comp_min_airtel // 60
            total_unique_subscribe_airtel = airtel_success_sms_count + airtel_failed_sms_count

            mail_title = f"CALL DROP REBATE STATUS - {date_today}"
            mail_body = f"""<p>Dear Concern, <br><br> Prepaid & Postpaid bonus disbursement has been completed successfully
                        for Robi & Airtel subscribers for {date_today}. The below table contains the detailed overall status.</p><br>
            
                <table>
                  <tr>
                    <th>Brand</th>
                    <th>Prepaid Minutes (Success)</th>
                    <th>Prepaid Minutes (Failure)</th>
                    <th>Postpaid Minutes (Success)</th>
                    <th>Postpaid Minutes (Failure)</th>
                    <th>Total Minutes Successfully Posted</th>
                    <th>Total Minutes Posting Failure</th>
                    <th>Total Compensation Minutes</th>
                    <th>Successful SMS Submission</th>
                    <th>SMS Submission Failed</th>
                    <th>Total unique subscribers Received Call Drop Rebate</th>
                  </tr>
                  <tr>
                    <td>Robi</td>
                    <td>{str(prepaid_min_success_robi)}</td>
                    <td>{str(prepaid_min_fail_robi)}</td>
                    <td>{str(postpaid_min_success_robi)}</td>
                    <td>{str(postpaid_min_fail_robi)}</td>
                    <td>{str(total_min_success_posted_robi)}</td>
                    <td>{str(total_min_fail_posted_robi)}</td>
                    <td>{str(total_compensation_min_robi)}</td>
                    <td>{str(robi_success_sms_count)}
                    <td>{str(robi_failed_sms_count)}
                    <td>{str(total_unique_subscribe_robi)}
                  </tr>
                  <tr>
                    <td>Airtel</td>
                    <td>{str(prepaid_min_success_airtel)}</td>
                    <td>{str(prepaid_min_fail_airtel)}</td>
                    <td>{str(postpaid_min_success_airtel)}</td>
                    <td>{str(postpaid_min_fail_airtel)}</td>
                    <td>{str(total_min_success_posted_airtel)}</td>
                    <td>{str(total_min_fail_posted_airtel)}</td>
                    <td>{str(total_compensation_min_airtel)}</td>
                    <td>{str(airtel_success_sms_count)}
                    <td>{str(airtel_failed_sms_count)}
                    <td>{str(total_unique_subscribe_airtel)}
                  </tr>
                  <tr>
                    <td>Total</td>
                    <td>{str(prepaid_min_success_robi + prepaid_min_success_airtel)}</td>
                    <td>{str(prepaid_min_fail_robi + prepaid_min_fail_airtel)}</td>
                    <td>{str(postpaid_min_success_robi + postpaid_min_success_airtel)}</td>
                    <td>{str(postpaid_min_fail_robi + postpaid_min_fail_airtel)}</td>
                    <td>{str(total_min_success_posted_robi + total_min_success_posted_airtel)}</td>
                    <td>{str(total_min_fail_posted_robi + total_min_fail_posted_airtel)}</td>
                    <td>{str(total_compensation_min_robi + total_compensation_min_airtel)}</td>
                    <td>{str(robi_success_sms_count + airtel_success_sms_count)}
                    <td>{str(robi_failed_sms_count + airtel_failed_sms_count)}
                    <td>{str(total_unique_subscribe_robi + total_unique_subscribe_airtel)}
                  </tr>
                </table>
                <p>Total success and fail logs are attached herewith for your reference.</p>
                <p>Regards, <br> Automated Call Drop Rebate System</p>"""

            mail.send_mail_to(targets, None, mail_title, mail_body, attachments)
            mail.send()
            print("Final mail has been sent successfully to: ", targets)
            update_query = "update CALL_DROP_RPA_STATUS_FLAG set MAIL_STATUS='Mail_Sent', MAIL_SEND_DATE='" + mail_send_date + "' where DEFAULT_EMAIL='calldrop@robi.com.bd'"
            db_object.execute_query(query=update_query)
            # Send sms to concern
            msisdn_list = ['8801833184089', '8801833184087', '8801833183769']
            crm_api_object.smsapi(msisdn=msisdn_list, message="Call Drop Rebate Email Mail Sent!")
            self.update_daily_mail_status_to_file(total_unique_subscribe_robi, total_unique_subscribe_airtel)
            self.log.log_info("Checking Previous missing dates ")
            missing_dates = self.check_previous_missing_dates()
            if len(missing_dates) > 0:
                self.send_previous_missing_dates_to_dev(missing_dates)
            today_date = dt.date.today()
            final_report_date = AppUtils.conf['final_report_date']
            is_final_date = today_date.day == final_report_date
            if is_final_date:
                self.log.log_info("Sending final report mail")
                self.send_final_monthly_report()

    def upload_file_dcrm(self, xlsx_file_name, xlsx_file_dir_name):
        try:
            upload_file_path = AppUtils.conf['upload_abs_path']
            final_upload_file_path = f"{upload_file_path}\\{xlsx_file_dir_name}\\{xlsx_file_name}"
            print(f'upload_file = {final_upload_file_path}')
            self.log.log_info(msg=f'upload_file = {final_upload_file_path}')
            time.sleep(2)
            self.webpage.driver.find_element_by_xpath("//input[@name='file']").send_keys(final_upload_file_path)
            time.sleep(2)
            self.webpage.driver.find_element_by_xpath('//*[@id="uploadbtn"]').click()
            time.sleep(2)
        except Exception as e:
            print(e)
            self.log.log_critical(e)

    def download_file_from_dcrm(self, xlsx_file_name):
        download_flag = False
        try:
            dcrm_pagination_size = AppUtils.conf['dcrm_pagination_size']
            element = self.webpage.driver.find_element_by_css_selector('option[value = "25"]')
            self.webpage.driver.execute_script(f'arguments[0].setAttribute("value", "{dcrm_pagination_size}")', element)
            perPage = self.webpage.driver.find_element_by_id("perPage")
            Select(perPage).select_by_visible_text("25")
            time.sleep(5)
            table = self.webpage.driver.find_element_by_tag_name('tbody')
            table_row = table.find_elements_by_tag_name('tr')
            for row in table_row:
                file_name = row.find_element_by_css_selector("td:nth-child(3)").text
                print(f'Check File Name - {file_name}')
                if file_name == xlsx_file_name:
                    status = row.find_element_by_css_selector("td:nth-child(8)").text
                    if status == "Completed":
                        print(f'File Provisioning Completed - {file_name}')
                        file_id = row.find_element_by_css_selector("td:nth-child(2)").text
                        total_records = row.find_element_by_css_selector("td:nth-child(9)").text
                        success_records = row.find_element_by_css_selector("td:nth-child(10)").text
                        failure_records = row.find_element_by_css_selector("td:nth-child(11)").text
                        ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET TOTAL_RECORD='" + total_records + "', SUCCESS_RECORD='" + success_records + "', FAILED_RECORD='" + failure_records + "' WHERE FILE_NAME='" + xlsx_file_name + "'"
                        db_object.execute_query(query=ins_log_query)
                        print(f'File_name = {file_name}')
                        print(f'Status = {status}')
                        print(f'Total Records = {total_records}')
                        print(f'Success Records = {success_records}')
                        print(f'Failure Records = {failure_records}')
                        if int(total_records) == int(success_records) or (int(success_records) + int(failure_records)) == int(total_records):
                            print(f'Downloading File - {file_name}')
                            file_download_url = f'https://dcrm.robi.com.bd/dcrm/bulk/postpaid/ajx/downloadBulkReport?fileId={file_id}&filename={file_name}'
                            try:
                                self.driver.get(file_download_url)
                                print(f'File Download Successful - {file_name}')
                                self.log.log_info(f'File Download Successful - {file_name}')
                                download_flag = True
                                time.sleep(AppUtils.conf['file_download_delay'])
                                return download_flag
                            except Exception as e:
                                print(f'File Download Unsuccessful - {e}')
                                self.log.log_critical(f'File Download Unsuccessful - {e}')
                        else:
                            print('Conditions Do Not Match !!')
                            self.log.log_critical('Conditions Do Not Match !!')
                    else:
                        print(f'File Still Provisioning - {file_name}')
                        self.log.log_info(f'File Still Provisioning - {file_name}')
                        time.sleep(5)
                        return download_flag
                else:
                    print(f'File Searching in Table, Current File - {file_name}')
                    self.log.log_info(f'File Searching in Table, Current File - {file_name}')
            return download_flag
        except Exception as e:
            print(f'Exception Occurred While Downloading File - {e}')
            self.log.log_critical(f'Exception Occurred While Downloading File - {e}')

    @staticmethod
    def partition_flag_generator(length, divisor):
        partition = length // divisor
        partitions = []
        count = 1
        for i in range(divisor):
            if count != divisor:
                partitions.append(partition * count)
            else:
                partitions.append((partition * count) + (length % divisor))
            count += 1
        return partitions

    def dataframe_load_distribution(self, dataframe, partition):
        dataframe['SMS_STATUS'] = None
        partitions = self.partition_flag_generator(len(dataframe), partition)
        for i in range(len(partitions)):
            if i == 0:
                dataframe['SMS_STATUS'].iloc[:partitions[i]] = f'Pending_{i + 1}'
            else:
                dataframe['SMS_STATUS'].iloc[partitions[i - 1]:partitions[i]] = f'Pending_{i + 1}'
        return dataframe

    def store_data_into_robi_table(self, file_dir):
        ins_query_insert = ""
        ins_value = list()
        try:
            df = pd.read_csv(file_dir)
            print(len(df))
            for i in range(len(df)):
                amount = df.loc[i, "Amount"].strip('="')
                eff_date = df.loc[i, "Eff Date"].strip('="')
                balance_type = df.loc[i, "Balance Type"].strip('="')
                exp_date = df.loc[i, "Exp Date"].strip('="')
                op_type = df.loc[i, "Op Type"].strip('="')
                msisdn = df.loc[i, "Msisdn"].strip('="')
                adjust_type = df.loc[i, "Adjust Type"].strip('="')
                sp_id = df.loc[i, "Spid"].strip('="')
                additional_info = df.loc[i, "Additionalinfo"].strip('="')
                api_status = df.loc[i, "Api Status"].strip('="')
                api_response_message = df.loc[i, "Api Response Message"].strip('="')
                # success_sms_status = df.loc[i, "SMS_STATUS"]
                success_sms_status = 'Pending'
                failed_sms_status = "Failed"
                file_name = file_dir.split("\\")[-1]

                if api_response_message == "Success: Operation successfully.":
                    ins_value_tuple = (f'{str(msisdn)}', f'{str(amount)}', f'{str(eff_date)}',
                                       f'{str(balance_type)}', f'{str(exp_date)}', f'{str(op_type)}',
                                       f'{str(adjust_type)}',
                                       f'{str(sp_id)}', f'{str(additional_info)}',
                                       f'{str(api_status)}', f'{str(api_response_message)}',
                                       f'{str(success_sms_status)}',
                                       f'{str(file_name)}')
                    ins_value.append(ins_value_tuple)
                else:
                    ins_value_tuple = (f'{str(msisdn)}', f'{str(amount)}', f'{str(eff_date)}',
                                       f'{str(balance_type)}', f'{str(exp_date)}', f'{str(op_type)}',
                                       f'{str(adjust_type)}',
                                       f'{str(sp_id)}', f'{str(additional_info)}',
                                       f'{str(api_status)}', f'{str(api_response_message)}',
                                       f'{str(failed_sms_status)}',
                                       f'{str(file_name)}')
                    ins_value.append(ins_value_tuple)

            ins_query_insert += "INSERT INTO CALL_DROP_SMS_LOG_ROBI (MSISDN, AMOUNT, EFF_DATE, BALANCE_TYPE, EXP_DATE, OP_TYPE,ADJUST_TYPE, SPID, ADDITIONALINFO, API_STATUS, API_RESPONSE_MESSAGE, SMS_STATUS, FILE_NAME) VALUES "
            ins_query_insert += "(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
            # print(ins_query_insert)
            # print(insValue)
            db_object.execute_many_query(query=ins_query_insert, param=ins_value)
        except Exception as e:
            from traceback import print_exc
            print_exc()
            print(e)

    def store_data_into_airtel_table(self, file_dir):
        ins_query_insert = ""
        ins_value = list()
        try:
            df = pd.read_csv(file_dir)
            print(len(df))
            for i in range(len(df)):
                amount = df.loc[i, "Amount"].strip('="')
                eff_date = df.loc[i, "Eff Date"].strip('="')
                balance_type = df.loc[i, "Balance Type"].strip('="')
                exp_date = df.loc[i, "Exp Date"].strip('="')
                op_type = df.loc[i, "Op Type"].strip('="')
                msisdn = df.loc[i, "Msisdn"].strip('="')
                adjust_type = df.loc[i, "Adjust Type"].strip('="')
                sp_id = df.loc[i, "Spid"].strip('="')
                additional_info = df.loc[i, "Additionalinfo"].strip('="')
                api_status = df.loc[i, "Api Status"].strip('="')
                api_response_message = df.loc[i, "Api Response Message"].strip('="')
                # success_sms_status = df.loc[i, "SMS_STATUS"]
                success_sms_status = 'Pending'
                failed_sms_status = 'Failed'
                file_name = file_dir.split("\\")[-1]

                if api_response_message == "Success: Operation successfully.":
                    ins_value_tuple = (f'{str(msisdn)}', f'{str(amount)}', f'{str(eff_date)}',
                                       f'{str(balance_type)}', f'{str(exp_date)}', f'{str(op_type)}',
                                       f'{str(adjust_type)}',
                                       f'{str(sp_id)}', f'{str(additional_info)}',
                                       f'{str(api_status)}', f'{str(api_response_message)}',
                                       f'{str(success_sms_status)}',
                                       f'{str(file_name)}')
                    ins_value.append(ins_value_tuple)
                else:
                    ins_value_tuple = (f'{str(msisdn)}', f'{str(amount)}', f'{str(eff_date)}',
                                       f'{str(balance_type)}', f'{str(exp_date)}', f'{str(op_type)}',
                                       f'{str(adjust_type)}',
                                       f'{str(sp_id)}', f'{str(additional_info)}',
                                       f'{str(api_status)}', f'{str(api_response_message)}',
                                       f'{str(failed_sms_status)}',
                                       f'{str(file_name)}')
                    ins_value.append(ins_value_tuple)

            ins_query_insert += "INSERT INTO CALL_DROP_SMS_LOG_AIRTEL (MSISDN, AMOUNT, EFF_DATE, BALANCE_TYPE, EXP_DATE, OP_TYPE,ADJUST_TYPE, SPID, ADDITIONALINFO, API_STATUS, API_RESPONSE_MESSAGE, SMS_STATUS, FILE_NAME) VALUES "
            ins_query_insert += "(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
            # print(ins_query_insert)
            # print(insValue)
            db_object.execute_many_query(query=ins_query_insert, param=ins_value)
        except Exception as e:
            from traceback import print_exc
            print_exc()
            print(e)

    def logout_from_dcrm(self):
        print("Trying to logout")
        time.sleep(5)
        self.webpage.driver.get(url="https://dcrm.robi.com.bd/dcrm/logout")
        time.sleep(5)

    @staticmethod
    def fetch_call_drop_file_status_count(brand, previous_date, status):
        select_query = f"""
            SELECT COUNT(*) FROM CALL_DROP_FILE_LOG
            WHERE BRAND = '{brand}' AND FILE_NAME LIKE '%{previous_date}%' and STATUS = '{status}'
        """
        db_response = db_object.select_query(select_query)
        count = int(db_response[0][0])
        return count

    @staticmethod
    def fetch_call_drop_file_log_from_db(file_name):
        select_query = f"SELECT * FROM CALL_DROP_FILE_LOG WHERE FILE_NAME = '{file_name}'"
        file_log_data = db_object.select_query(select_query)
        if file_log_data == 0:
            return False
        else:
            file_log_dict = {
                "FILE_NAME": str(file_log_data[0][0]),
                "BRAND": str(file_log_data[0][1]),
                "STATUS": str(file_log_data[0][2]),
            }
            return file_log_dict

    @staticmethod
    def isBrowserAlive(driver):
        try:
            driver.get(AppUtils.conf['crm_link'])
            return True
        except Exception as e:
            print(f'Driver Unavailable - {e}')
            return False

    def _process_robi_files(self, xlsx_file_name, robi_xlsx_file_dir_name, process):
        is_found = False
        data = self.fetch_call_drop_file_log_from_db(file_name=xlsx_file_name)

        if data:
            is_found = True

            if data.get('STATUS') == 'Done':
                return

            if data.get('STATUS') == 'Pending' and process == 'Upload':
                print('Upload Section')
                if self.isBrowserAlive(self.webpage.driver):
                    self.webpage.driver.quit()
                wrapper = XMLWrapper()
                self.webpage = wrapper.webpage
                self.driver = self.webpage.driver
                self.upload_run_wrapper(wrapper=wrapper, wrapper_df=credentials)
                self.upload_file_dcrm(xlsx_file_name=xlsx_file_name, xlsx_file_dir_name=robi_xlsx_file_dir_name)
                table = self.webpage.driver.find_element_by_tag_name('tbody')
                table_row = table.find_elements_by_tag_name('tr')
                for row in table_row:
                    file_name = row.find_element_by_css_selector("td:nth-child(3)").text
                    if file_name == xlsx_file_name:
                        ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Upload Complete' WHERE FILE_NAME='" + xlsx_file_name + "'"
                        data['STATUS'] = 'Upload Complete'
                        db_object.execute_query(query=ins_log_query)
                time.sleep(5)

            if data.get('STATUS') == 'Upload Complete' and process == 'Download':
                print('Download Section')
                if self.isBrowserAlive(self.webpage.driver):
                    self.webpage.driver.quit()
                wrapper = XMLWrapper()
                self.webpage = wrapper.webpage
                self.driver = self.webpage.driver
                self.run_wrapper(wrapper=wrapper, wrapper_df=credentials)
                self.download_file_from_dcrm(xlsx_file_name=xlsx_file_name)
                download_flag = self.check_download_file(file_name=xlsx_file_name)
                if download_flag:
                    ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Download Complete' WHERE FILE_NAME='" + xlsx_file_name + "'"
                    data['STATUS'] = 'Download Complete'
                    db_object.execute_query(query=ins_log_query)
                time.sleep(5)

            if data.get('STATUS') == 'Download Complete' and process == 'Download':
                print('Move File Section')
                moved_file_name = AppUtils.move_call_drop_file_robi(default_download_path,
                                                                    xlsx_file_name.strip(".xlsx"))
                self.store_data_into_robi_table(file_dir=moved_file_name)
                ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='" + xlsx_file_name + "'"
                data['STATUS'] = 'Done'
                db_object.execute_query(query=ins_log_query)
                time.sleep(5)
            self.logout_from_dcrm()
        if not is_found and process == 'Upload':
            print(f"Insert_File_Name - {xlsx_file_name}")
            insert_query = f"INSERT INTO CALL_DROP_FILE_LOG (FILE_NAME,STATUS,BRAND) VALUES ('" + xlsx_file_name + "', 'Pending', 'Robi')"
            db_object.execute_query(query=insert_query)
            self._process_robi_files(xlsx_file_name, robi_xlsx_file_dir_name, process)

    def _process_airtel_files(self, xlsx_file_name, airtel_xlsx_file_dir_name, process):
        is_found = False
        data = self.fetch_call_drop_file_log_from_db(file_name=xlsx_file_name)

        if data:
            is_found = True

            if data.get('STATUS') == 'Done':
                return

            if data.get('STATUS') == 'Pending' and process == 'Upload':
                print('Upload Section')
                if self.isBrowserAlive(self.webpage.driver):
                    self.webpage.driver.quit()
                wrapper = XMLWrapper()
                self.webpage = wrapper.webpage
                self.driver = self.webpage.driver
                self.upload_run_wrapper(wrapper=wrapper, wrapper_df=credentials)
                self.upload_file_dcrm(xlsx_file_name=xlsx_file_name, xlsx_file_dir_name=airtel_xlsx_file_dir_name)
                table = self.webpage.driver.find_element_by_tag_name('tbody')
                table_row = table.find_elements_by_tag_name('tr')
                for row in table_row:
                    file_name = row.find_element_by_css_selector("td:nth-child(3)").text
                    if file_name == xlsx_file_name:
                        ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Upload Complete' WHERE FILE_NAME='" + xlsx_file_name + "'"
                        data['STATUS'] = 'Upload Complete'
                        db_object.execute_query(query=ins_log_query)
                time.sleep(5)

            if data.get('STATUS') == 'Upload Complete' and process == 'Download':
                print('Download Section')
                if self.isBrowserAlive(self.webpage.driver):
                    self.webpage.driver.quit()
                wrapper = XMLWrapper()
                self.webpage = wrapper.webpage
                self.driver = self.webpage.driver
                self.run_wrapper(wrapper=wrapper, wrapper_df=credentials)
                self.download_file_from_dcrm(xlsx_file_name=xlsx_file_name)
                download_flag = self.check_download_file(file_name=xlsx_file_name)
                if download_flag:
                    ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Download Complete' WHERE FILE_NAME='" + xlsx_file_name + "'"
                    data['STATUS'] = 'Download Complete'
                    db_object.execute_query(query=ins_log_query)
                time.sleep(5)

            if data.get('STATUS') == 'Download Complete' and process == 'Download':
                print('Move File Section')
                moved_file_name = AppUtils.move_call_drop_file_airtel(default_download_path,
                                                                      xlsx_file_name.strip(".xlsx"))
                self.store_data_into_airtel_table(file_dir=moved_file_name)
                ins_log_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='" + xlsx_file_name + "'"
                data['STATUS'] = 'Done'
                db_object.execute_query(query=ins_log_query)
                time.sleep(5)
            self.logout_from_dcrm()
        if not is_found and process == 'Upload':
            print(f"Insert_File_Name - {xlsx_file_name}")
            insert_query = f"INSERT INTO CALL_DROP_FILE_LOG (FILE_NAME,STATUS,BRAND) VALUES ('" + xlsx_file_name + "', 'Pending', 'Airtel')"
            db_object.execute_query(query=insert_query)
            self._process_airtel_files(xlsx_file_name, airtel_xlsx_file_dir_name, process)

    def robi_rebate(self):
        try:
            robi_xlsx_file_dir_name = "robi"  # robi xlsx file dir name
            robi_csv_file_dir_name = "calldrop_robi"  # robi csv file dir name

            local_dir_robi = AppUtils.conf['local_dir_robi']
            remote_dir_robi = AppUtils.conf['remote_dir_robi']

            files_name_list = self.fetch_file_from_server(local_dir=local_dir_robi,
                                                          remote_dir=remote_dir_robi)
            if files_name_list is not None:
                xlsx_files_name_list = self.generate_xlsx_file(csv_files_name=files_name_list,
                                                               csv_file_dir_name=robi_csv_file_dir_name,
                                                               xlsx_file_dir_name=robi_xlsx_file_dir_name)
                self.log.log_info(msg=f'File Name List: {xlsx_files_name_list}')

                previous_date = (
                        dt.datetime.now() - dt.timedelta(days=int(AppUtils.conf['previous_email_day']))
                ).strftime("%Y%m%d")

                uploaded_file_count = self.fetch_call_drop_file_status_count(
                    brand='Robi',
                    previous_date=previous_date,
                    status='Upload Complete'
                )
                if uploaded_file_count != len(xlsx_files_name_list):
                    # Upload File Into dCRM
                    print("Upload Section")
                    for xlsx_file_name in xlsx_files_name_list:
                        self.log.log_info(msg=f'File Name: {xlsx_file_name}')
                        print(f'File Name: {xlsx_file_name}')
                        self._process_robi_files(xlsx_file_name=xlsx_file_name,
                                                 robi_xlsx_file_dir_name=robi_xlsx_file_dir_name,
                                                 process='Upload')
                completed_file_count = self.fetch_call_drop_file_status_count(
                    brand='Robi',
                    previous_date=previous_date,
                    status='Done'
                )
                if completed_file_count != len(xlsx_files_name_list):
                    # Download File From dCRM & Store Data Into Database
                    print("Download Section")
                    for xlsx_file_name in xlsx_files_name_list:
                        self.log.log_info(msg=f'File Name: {xlsx_file_name}')
                        print(f'File Name: {xlsx_file_name}')
                        self._process_robi_files(xlsx_file_name=xlsx_file_name,
                                                 robi_xlsx_file_dir_name=robi_xlsx_file_dir_name,
                                                 process='Download')
        except Exception as e:
            print(e)
            self.log.log_critical(e)

    def airtel_rebate(self):
        try:
            airtel_xlsx_file_dir_name = "airtel"  # airtel xlsx file dir name
            airtel_csv_file_dir_name = "calldrop_airtel"  # airtel csv file dir name

            local_dir_airtel = AppUtils.conf['local_dir_airtel']
            remote_dir_airtel = AppUtils.conf['remote_dir_airtel']

            files_name_list = self.fetch_file_from_server(local_dir=local_dir_airtel,
                                                          remote_dir=remote_dir_airtel)
            if files_name_list is not None:
                xlsx_files_name_list = self.generate_xlsx_file(csv_files_name=files_name_list,
                                                               csv_file_dir_name=airtel_csv_file_dir_name,
                                                               xlsx_file_dir_name=airtel_xlsx_file_dir_name)

                self.log.log_info(msg=f'File Name List: {xlsx_files_name_list}')
                # Upload File Into dCRM
                previous_date = (
                        dt.datetime.now() - dt.timedelta(days=int(AppUtils.conf['previous_email_day']))
                ).strftime("%Y%m%d")

                uploaded_file_count = self.fetch_call_drop_file_status_count(
                    brand='Airtel',
                    previous_date=previous_date,
                    status='Upload Complete'
                )
                if uploaded_file_count != len(xlsx_files_name_list):
                    print("Upload Section")
                    for xlsx_file_name in xlsx_files_name_list:
                        print(f'File Name: {xlsx_file_name}')
                        self.log.log_info(msg=f'File Name: {xlsx_file_name}')
                        self._process_airtel_files(xlsx_file_name=xlsx_file_name,
                                                   airtel_xlsx_file_dir_name=airtel_xlsx_file_dir_name,
                                                   process='Upload')

                completed_file_count = self.fetch_call_drop_file_status_count(
                    brand='Airtel',
                    previous_date=previous_date,
                    status='Done'
                )
                if completed_file_count != len(xlsx_files_name_list):
                    # Download File From dCRM & Store Data Into Database
                    print("Download Section")
                    for xlsx_file_name in xlsx_files_name_list:
                        print(f'File Name: {xlsx_file_name}')
                        self.log.log_info(msg=f'File Name: {xlsx_file_name}')
                        self._process_airtel_files(xlsx_file_name=xlsx_file_name,
                                                   airtel_xlsx_file_dir_name=airtel_xlsx_file_dir_name,
                                                   process='Download')
        except Exception as e:
            print(e)
            self.log.log_critical(e)

    def is_now_in_time_period(self):
        start_time = AppUtils.conf['start_time']
        end_time = AppUtils.conf['end_time']
        tz = pytz.timezone('Asia/Dhaka')
        date_time_today = datetime.now(tz)
        time_now = date_time_today.strftime("%H:%M:%S")

        print(f"Start_time = {start_time}\nEnd_time = {end_time}\nTime_now = {time_now}\n")
        self.log.log_info(msg=f"Start_time = {start_time}\nEnd_time = {end_time}\nTime_now = {time_now}\n")

        if start_time < end_time:
            return start_time <= time_now <= end_time
        else:  # Over midnight
            return time_now >= start_time or time_now <= end_time

    @staticmethod
    def get_pending_status_robi():
        try:
            pending_sms_batch_size = AppUtils.conf['pending_sms_batch_size']
            call_drop_data_list_robi = list()
            query = f"SELECT * FROM CALL_DROP_SMS_LOG_ROBI WHERE SMS_STATUS='Pending' FETCH NEXT {pending_sms_batch_size} ROWS ONLY"
            call_drop_sms_log = db_object.select_query(query=query)
            if call_drop_sms_log != 0:
                for call_drop_sms in call_drop_sms_log:
                    call_drop_data = {
                        "MSISDN": call_drop_sms[0],
                        "FILE_NAME": call_drop_sms[12],
                        "AMOUNT": call_drop_sms[1]
                    }
                    call_drop_data_list_robi.append(call_drop_data)
                return call_drop_data_list_robi
        except Exception as e:
            print(e)

    @staticmethod
    def get_pending_status_airtel():
        try:
            pending_sms_batch_size = AppUtils.conf['pending_sms_batch_size']
            call_drop_data_list_airtel = list()
            query = f"SELECT * FROM CALL_DROP_SMS_LOG_AIRTEL WHERE SMS_STATUS = 'Pending' FETCH NEXT {pending_sms_batch_size} ROWS ONLY"
            call_drop_sms_log = db_object.select_query(query=query)
            if call_drop_sms_log != 0:
                for call_drop_sms in call_drop_sms_log:
                    call_drop_data = {
                        "MSISDN": call_drop_sms[0],
                        "FILE_NAME": call_drop_sms[12],
                        "AMOUNT": call_drop_sms[1]
                    }
                    call_drop_data_list_airtel.append(call_drop_data)
                return call_drop_data_list_airtel
        except Exception as e:
            print(e)

    @staticmethod
    def convert_english_to_bangla_time(message_content):
        dic = {'0': '০', '1': '১', '2': '২', '3': '৩', '4': '৪', '5': '৫', '6': '৬', '7': '৭', '8': '৮', '9': '৯',
               'hour': 'ঘন্টা', 'minute': 'মিনিট', 'second': 'সেকেন্ড'}
        for word, initial in dic.items():
            message_content = message_content.replace(word, initial)
        return message_content

    def convert_time(self, second):
        second = second % (24 * 3600)
        hour = second // 3600
        second %= 3600
        minute = second // 60
        second %= 60
        if hour != 0 and minute != 0 and second != 0:
            message = f"{hour} hour {minute} minute {second} second"
            message_bangla = self.convert_english_to_bangla_time(message)
            return message_bangla
        elif hour == 0 and minute != 0:
            message = f"{minute} minute {second} second"
            message_bangla = self.convert_english_to_bangla_time(message)
            return message_bangla
        elif hour == 0 and minute == 0 and second != 0:
            message = f"{second} second"
            message_bangla = self.convert_english_to_bangla_time(message)
            return message_bangla
        else:
            message = f"{hour} hour {minute} minute {second} second"
            message_bangla = self.convert_english_to_bangla_time(message)
            return message_bangla

    def send_sms_to_robi_msisdn(self):
        try:
            call_drop_data_list_robi = self.get_pending_status_robi()
            if call_drop_data_list_robi is not None and len(call_drop_data_list_robi) > 0:
                call_drop_db_obj = CallDropDB()
                call_drop_db_obj.connect()
                msisdn_list = []
                file_name_list = []
                for call_drop_data in call_drop_data_list_robi:
                    msisdn = call_drop_data.get("MSISDN")
                    file_name = call_drop_data.get("FILE_NAME")
                    amount_to_time = self.convert_time(int(call_drop_data.get("AMOUNT")))
                    robi_msg = f"কল ড্রপের জন্য রবি থেকে আপনি পেয়েছেন {amount_to_time} (মেয়াদ ১৫দিন)। ব্যবহার করতে পারবেন যেকোনো রবি নম্বর এ। ব্যালান্স চেক করতে ডায়াল *222*2# কলড্রপের বিস্তারিত জানতে ডায়াল *121*765#"
                    msisdn_list.append(msisdn)
                    file_name_list.append(file_name)
                    crm_api_object.call_drop_smsapi_robi(msisdn=msisdn, msg=robi_msg)
                msisdn_list = list(set(msisdn_list))
                msisdn_list_string = ','.join("'{0}'".format(msisdn) for msisdn in msisdn_list)
                file_name_list = list(set(file_name_list))
                file_name_list_string = ','.join("'{0}'".format(file_name) for file_name in file_name_list)
                query = f"UPDATE CALL_DROP_SMS_LOG_ROBI SET SMS_STATUS='Sent' WHERE FILE_NAME IN ({file_name_list_string}) AND MSISDN IN ({msisdn_list_string})"
                call_drop_db_obj.execute_query(query=query)
                time.sleep(AppUtils.conf['query_update_delay'])
                call_drop_db_obj.close_connection()
                self.send_sms_to_robi_msisdn()
        except Exception as e:
            print(e)

    def send_sms_to_airtel_msisdn(self):
        try:
            call_drop_data_list_airtel = self.get_pending_status_airtel()
            if call_drop_data_list_airtel is not None and len(call_drop_data_list_airtel) > 0:
                call_drop_db_obj = CallDropDB()
                call_drop_db_obj.connect()
                msisdn_list = []
                file_name_list = []
                for call_drop_data in call_drop_data_list_airtel:
                    msisdn = call_drop_data.get("MSISDN")
                    file_name = call_drop_data.get("FILE_NAME")
                    amount_to_time = self.convert_time(int(call_drop_data.get("AMOUNT")))
                    airtel_msg = f"কল ড্রপের জন্য আপনি পেয়েছেন {amount_to_time} (মেয়াদ ১৫দিন)। ব্যবহার করতে পারবেন যেকোনো এয়ারটেল নম্বর এ। ব্যালান্স চেক করতে ডায়াল *778*31# কলড্রপের বিস্তারিত জানতে ডায়াল *121*765#"
                    msisdn_list.append(msisdn)
                    file_name_list.append(file_name)
                    crm_api_object.call_drop_smsapi_airtel(msisdn=msisdn, msg=airtel_msg)
                msisdn_list = list(set(msisdn_list))
                msisdn_list_string = ','.join("'{0}'".format(msisdn) for msisdn in msisdn_list)
                file_name_list = list(set(file_name_list))
                file_name_list_string = ','.join("'{0}'".format(file_name) for file_name in file_name_list)
                query = f"UPDATE CALL_DROP_SMS_LOG_AIRTEL SET SMS_STATUS='Sent' WHERE FILE_NAME IN ({file_name_list_string}) AND MSISDN IN ({msisdn_list_string})"
                call_drop_db_obj.execute_query(query=query)
                time.sleep(AppUtils.conf['query_update_delay'])
                call_drop_db_obj.close_connection()
                self.send_sms_to_airtel_msisdn()
        except Exception as e:
            print(e)

    def check_running_process(self, process_name):
        try:
            running_flag = process_name in (p.name() for p in psutil.process_iter())
            return running_flag
        except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
            print(e)
            self.log.log_warn(msg=e)
