import os
import time
from pathlib import Path

from selenium.common.exceptions import NoSuchElementException, TimeoutException

from pages.pages2 import Pages
from utils.constants import LocatorType
from utils.custom_exception import ExpectedDataNotFoundException
from utils.logger import Logger


class RpaCRM:

    def __init__(self, browser, url):
        self.log = Logger.get_instance()
        self.pages = Pages(browser=browser)  # got error
        self.pages.navigate(url)
        self.browser = self.pages.driver
        self.pages.driver.maximize_window()

    locator_type = LocatorType()

    location = "C:\\Users\\crm_rpa\\Downloads\\"
    # location = "C:\\Users\\rashid.rayhan\\Downloads\\"

    login_user = '//*[@id="s_swepi_1"]'
    login_pass = '//*[@id="s_swepi_2"]'
    login_button = '//*[@id="s_swepi_22"]'

    category = '//*[@id="s_sctrl_tabScreen"]/button'
    complaint_management = '//*[@id="ui-id-106"]'  # //*[@id="ui-id-135"]
    all_service_management = '//*[@id="s_5_2_19_0_mb"]'
    setting_icon = 's_at_m_2'

    def login(self, username, password):
        self.pages.input_text(self.login_user, locator_type=self.locator_type.xpath, text=username)
        self.pages.input_text(self.login_pass, locator_type=self.locator_type.xpath, text=password)
        self.pages.click(self.login_button, locator_type=self.locator_type.xpath)

    def filter(self):
        # time.sleep(3)
        try:
            # self.pages.click(self.category, locator_type=self.locator_type.xpath)
            # time.sleep(1)
            # self.pages.click(self.complaint_management, locator_type=self.locator_type.xpath)
            # self.pages.click('Complaint Management', locator_type=self.locator_type.link_text)
            time.sleep(1)
            self.pages.click('All Service Requests', locator_type=self.locator_type.link_text)
            time.sleep(1)
            self.pages.click('//*[@id="s_2_1_14_0_Ctrl"]', locator_type=self.locator_type.xpath)
        except NoSuchElementException:
            self.log.log_critical(msg="Username and Password Failed")
            self.pages.end()
            exit(1)
        except TimeoutException:
            self.log.log_critical(msg="Username and Password Failed")
            self.pages.end()
            exit(1)

        # Area
        try:
            self.pages.click('//*[@id="1INSArea"]', locator_type=self.locator_type.xpath)
            time.sleep(1)
            self.pages.input_text('//*[@id="1_INSArea"]', locator_type=self.locator_type.xpath,
                                  text="unwillingly activated pack")
            # self.pages.input_text('//*[@id="1_INSArea"]', locator_type=self.locator_type.xpath,
            #                       text="Adjustment request")
            time.sleep(1)

            self.pages.click('/html/body/div[1]/div/div[5]/div/div[6]/ul[16]/li[1]',
                             locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-444"]', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-418"]', locator_type=self.locator_type.xpath)

        except NoSuchElementException as e:
            self.log.log_error(e)
            self.log.log_critical(msg="While Filtering Area is not found : Unwilling Data Pack Missing!")
            self.pages.end()
            exit()

        # Type
        try:
            self.pages.click('//*[@id="1INS_Product"]', locator_type=self.locator_type.xpath)
            time.sleep(1)
            self.pages.input_text('//*[@id="1_INS_Product"]', locator_type=self.locator_type.xpath, text="DATA")
            # self.pages.input_text('//*[@id="1_INS_Product"]', locator_type=self.locator_type.xpath,
            #                       text="Product & Services")
            time.sleep(1)
            self.pages.click(' /html/body/div[1]/div/div[5]/div/div[6]/ul[16]/li', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-438"]', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-468"]', locator_type=self.locator_type.xpath)

        except NoSuchElementException:
            self.log.log_error(msg="While Filtering Type is not found : DATA Missing!")
            self.pages.end()
            exit()

        # Sub Area
        try:
            self.pages.click('//*[@id="1INS_Sub-Area"]', locator_type=self.locator_type.xpath)
            time.sleep(1)
            self.pages.input_text('//*[@id="1_INS_Sub-Area"]', locator_type=self.locator_type.xpath,
                                  text="Adjustment given request")
            # self.pages.input_text('//*[@id="1_INS_Sub-Area"]', locator_type=self.locator_type.xpath,
            #                       text="Unwillingly buy Data pack")
            time.sleep(1)
            self.pages.click('/html/body/div[1]/div/div[5]/div/div[6]/ul[16]/li', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-443"]', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-471"]"]', locator_type=self.locator_type.xpath)

        except NoSuchElementException:
            self.log.log_error(msg="While Filtering Sub Area is not found : Adjustment Given!")
            self.pages.end()
            exit()

        # Status
        try:
            self.pages.click('//*[@id="1Status"]', locator_type=self.locator_type.xpath)
            time.sleep(1)
            self.pages.input_text('//*[@id="1_Status"]', locator_type=self.locator_type.xpath, text="Open")
            time.sleep(1)
            # self.pages.click('//*[@id="s_2_2_176_0_icon"]', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-453"]', locator_type=self.locator_type.xpath)
            self.pages.click('/html/body/div[1]/div/div[5]/div/div[6]/ul[16]/li', locator_type=self.locator_type.xpath)
            # self.pages.click('//*[@id="ui-id-481"]', locator_type=self.locator_type.xpath)
            # self.pages.click('Open', locator_type=self.locator_type.link_text)

        except NoSuchElementException:
            self.log.log_error(msg="While Filtering Status is not found : Open Missing!")
            self.pages.end()
            exit()

        # Service Request Go
        try:
            time.sleep(1)
            self.pages.click('//*[@id="s_2_1_11_0_Ctrl"]', locator_type=self.locator_type.xpath)
        except NoSuchElementException:
            self.log.log_error(msg="Search element Missing")
            self.pages.end()
            exit()

    def csv_download(self):
        path = self.location + 'output.txt'
        try:
            os.remove(path)
        except FileNotFoundError as e:
            self.log.log_warn(f"No File Found to be deleted LOCATION - {self.location}")
            pass

        # time.sleep(10)
        self.pages.click(self.setting_icon, locator_type=self.locator_type.id)
        # time.sleep(2)
        self.pages.click("//*[text()='Export...']", locator_type=self.locator_type.xpath)
        try:
            #
            # self.pages.click(
            #     '//*[@id="ui-id-499"]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[1]/nobr/input[4]',
            #     locator_type=self.locator_type.xpath)
            # time.sleep(2)

            self.pages.click(
                '/html/body/div[7]/div[2]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[1]/nobr/input[4]',
                locator_type=self.locator_type.xpath)
            # time.sleep(2)
            # self.pages.input_text('//*[@id="s_4_1_185_0"]', locator_type=self.locator_type.xpath, text=",")
            self.pages.input_text('//*[@id="s_4_1_185_0"]', locator_type=self.locator_type.xpath, text=",")
            # time.sleep(2)
            self.pages.click('//*[@id="s_4_1_186_0"]', locator_type=self.locator_type.xpath)
        except NoSuchElementException as e:
            self.log.log_error(e)
            self.log.log_critical(msg="No Complain list Found to Export")
            self.pages.end()
            exit()
        # time.sleep(2)

    def check_file(self, file_name):
        file_check_flag = 0
        for x in range(15):
            time.sleep(2)
            file = Path(self.location + file_name)

            if file.is_file():
                file_check_flag = 1
                print("found")
                self.pages.end()
                return True
        if file_check_flag == 0:
            print(
                "Please Check weather File Storage Location is correct or Server is Unable to provide File OUTPUT FILE NAME : " + file_name)
        self.pages.end()
        return False

    # def account(self, msisdn):
    #     # path = 'C:/Users/rashid.rayhan/Downloads/output (1).txt'
    #     path = 'C:/Users/crm_rpa/Downloads/output (1).txt'
    #     try:
    #         os.remove(path)
    #     except OSError:
    #         pass
    #     time.sleep(4)
    #     try:
    #         self.pages.click(self.category, locator_type=self.locator_type.xpath)
    #         self.pages.click('Accounts', locator_type=self.locator_type.link_text)
    #         self.pages.input_text(
    #             '//*[@id="s_S_A3_div"]/div/form/div/table/tbody/tr/td/table/tbody/tr[2]/td[2]/table/tbody/tr/td/input',
    #             locator_type=self.locator_type.xpath, text=str(msisdn))
    #         self.pages.click('//*[@id="s_3_1_16_0_Ctrl"]', locator_type=self.locator_type.xpath)
    #         time.sleep(3)
    #         self.pages.click('//*[@id="1Account_Number"]/a', locator_type=self.locator_type.xpath)
    #         self.pages.click('//*[@id="1Payment_Type"]/a', locator_type=self.locator_type.xpath)
    #         time.sleep(10)
    #         self.pages.click('//*[@id="s_vctrl_div_tabView"]/button', locator_type=self.locator_type.xpath)
    #         time.sleep(2)
    #         self.pages.click('Data Balance', locator_type=self.locator_type.link_text)
    #         time.sleep(8)
    #         self.pages.click('s_at_m_2', locator_type=self.locator_type.id)
    #
    #         self.pages.click('Export...', locator_type=self.locator_type.link_text)
    #         self.pages.click(
    #             '/html/body/div[7]/div[2]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[1]/nobr/input[4]',
    #             locator_type=self.locator_type.xpath)
    #         # self.pages.input_text('//*[@id="s_7_1_41_0"]', locator_type=self.locator_type.xpath, text=",")
    #         self.pages.input_text(
    #             '/html/body/div[7]/div[2]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[3]/nobr/input',
    #             locator_type=self.locator_type.xpath, text=",")
    #         self.pages.click('Next', locator_type=self.locator_type.link_text)
    #     except NoSuchElementException as e:
    #         self.log.log_warn(f"Unable to get Data inforamtion Detail : MSISDN - {msisdn} ")
    #         self.log.log_debug(f"Exception - {e.msg}")
    #     except BrowserWebElementMissingError as e:
    #         self.log.log_error(e)
    #     except Exception as e:
    #         self.log.log_error(e)

    def data_pack_details(self, sr_id):
        sr_id = str(sr_id)
        # time.sleep(5)
        path = self.location + "output (1).txt"
        try:
            os.remove(path)
        except OSError:
            pass
        time.sleep(5)
        try:
            # self.pages.click(self.category, locator_type=self.locator_type.xpath)
            # time.sleep(2)
            # self.pages.click('Complaint Management', locator_type=self.locator_type.link_text)
            # time.sleep(2)
            self.pages.input_text('s_6_1_19_0', locator_type=self.locator_type.name, text=str(sr_id))
            print(str(sr_id))
            # time.sleep(1)
            self.pages.click(
                '/html/body/div[1]/div/div[5]/div/div[6]/div/div[1]/div/div/div[2]/div[2]/div/div/div/form/div/table/tbody/tr/td/table/tbody/tr[16]/td[2]/table/tbody/tr/td[1]/button/span',
                locator_type=self.locator_type.xpath)
            x = f"// *[text() = '{sr_id}']"
            # time.sleep(2)
            self.pages.click(str(sr_id), locator_type=self.locator_type.link_text)
            # time.sleep(2)
            self.pages.click('/html/body/div[1]/div/div[5]/div/div[6]/div/div[1]/div/div[2]/div[2]/button',
                             locator_type=self.locator_type.xpath)
            # time.sleep(2)
            self.pages.click('Smart Script', locator_type=self.locator_type.link_text)
            # time.sleep(2)
            self.pages.click(
                '/html/body/div[1]/div/div[5]/div/div[6]/div/div[1]/div/div[3]/div[2]/div/form/span/div/div[1]/div[2]/span[1]/span/button',
                locator_type=self.locator_type.xpath)
            # time.sleep(2)
        except Exception as e:
            self.log.log_error(f"Smart Script Download Exception - {e}")
            raise ExpectedDataNotFoundException

        try:
            self.pages.click('Export...', locator_type=self.locator_type.link_text)
            self.pages.click(
                '/html/body/div[7]/div[2]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[1]/nobr/input[4]',
                locator_type=self.locator_type.xpath)

        except TimeoutException as e:
            self.log.log_error(f"Smart Script Not Found - {e}")
            self.pages.end()
            raise TimeoutException
        except NoSuchElementException as e:
            self.log.log_error(f"Smart Script Not Found - {e}")
            self.pages.end()
            raise NoSuchElementException

        try:
            self.pages.input_text(
                '/html/body/div[7]/div[2]/div/form/table/tbody/tr[1]/td/table/tbody/tr/td/table[3]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/table/tbody/tr/td[1]/nobr/input[4]',
                locator_type=self.locator_type.xpath, text=',')

            self.pages.click('Next', locator_type=self.locator_type.link_text)
        except Exception as e:
            self.log.log_warn(f"Unable to get Data inforamtion Detail : sr_id - {sr_id} ")
            self.log.log_debug(f"Exception - {e}")
            self.pages.end()
            raise ExpectedDataNotFoundException
