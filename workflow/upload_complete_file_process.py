from apps.api_helper import RPAApi
from utils.mail import Mail
from apps.database import DB
from apps.api_and_database import CRMAPI
from pages.pages_xml_wrapper import XMLWrapper
from selenium.webdriver.support.ui import Select
from apps.helper import Helper
from apps.config import ConfigParser
from apps.app_utils import AppUtils
from selenium.webdriver.common.keys import Keys
import time
import os
import shutil

crm_api_object = RPAApi()
api = CRMAPI()
mail = Mail()
db_obj = DB()
db_object = db_obj
conf = ConfigParser()
credentials = conf.get_credentials()
wrapper = XMLWrapper()
webpage = wrapper.webpage
driver = webpage.driver
helper = Helper(webpage=wrapper.webpage, driver=wrapper.webpage.driver)
helper.run_wrapper(wrapper=wrapper, wrapper_df=credentials)

query = "SELECT * from CALL_DROP_FILE_LOG WHERE STATUS='Upload Complete'"
result = db_object.select_query(query=query)
upload_complete_files = [row[0] for row in result]

dcrm_pagination_size = AppUtils.conf['dcrm_pagination_size']
webpage.driver.find_element_by_id("select2-searchType-container").click()
webpage.driver.find_element_by_xpath("//input[@type='search']").clear()
webpage.driver.find_element_by_xpath("//input[@type='search']").send_keys("File Name")
webpage.driver.find_element_by_xpath("//input[@type='search']").send_keys(Keys.ENTER)
element = webpage.driver.find_element_by_css_selector('option[value = "25"]')
webpage.driver.execute_script(f'arguments[0].setAttribute("value", "{dcrm_pagination_size}")', element)
perPage = webpage.driver.find_element_by_id("perPage")
Select(perPage).select_by_visible_text("25")

for file_name in upload_complete_files:
    print(f'File Name: {file_name}\n')
    webpage.driver.find_element_by_name("searchfield").clear()
    webpage.driver.find_element_by_name("searchfield").send_keys(file_name)
    webpage.driver.find_element_by_css_selector("button.btn.btn-danger.btn-xs.tblSearch").click()
    time.sleep(5)
    table = webpage.driver.find_element_by_tag_name('tbody')
    table_row = table.find_elements_by_tag_name('tr')
    for row in table_row:
        file_id = row.find_element_by_css_selector("td:nth-child(2)").text
        file_name = row.find_element_by_css_selector("td:nth-child(3)").text
        print(f'File Name: {file_name}')
        file_download_url = f'https://dcrm.robi.com.bd/dcrm/bulk/postpaid/ajx/downloadBulkReport?fileId={file_id}&filename={file_name}'
        driver.get(file_download_url)
        time.sleep(2)
        file_name_without_ext = file_name.split('.')[0]
        file_path = f'C:/Users/fin_rpa_admin/Downloads/{file_name_without_ext}.csv'
        if os.path.isfile(file_path):
            if '_air_' in file_name_without_ext:
                dst_path = f"E:/Rayhan_development/Call-Drop-RPA/files/download_file/airtel/{file_name_without_ext}.csv"
                if not os.path.isfile(dst_path):
                    try:
                        shutil.move(file_path, dst_path)
                        print(f'Downloaded & Moved - {file_name}')
                        helper.store_data_into_airtel_table(dst_path)
                        time.sleep(0.5)
                        update_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='{file_name}'"
                        db_object.execute_query(query=update_query)
                        print(f'Airtel File Data Stored in DB - {file_name}')
                    except Exception as e:
                        print(f'Unable to Move File - {file_name} - {e}')
                else:
                    print(f'File Already Exist in Destination Directory')
                    try:
                        helper.store_data_into_airtel_table(file_path)
                        time.sleep(0.5)
                        update_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='{file_name}'"
                        db_object.execute_query(query=update_query)
                        print(f'Airtel File Data Stored in DB - {file_name}')
                        os.remove(file_path)
                        print(f'File Removed - {file_name}')
                    except Exception as e:
                        print(f'Unable to Remove File - {file_name} - {e}')
            else:
                dst_path = f"E:/Rayhan_development/Call-Drop-RPA/files/download_file/robi/{file_name_without_ext}.csv"
                if not os.path.isfile(dst_path):
                    try:
                        shutil.move(file_path, dst_path)
                        print(f'Downloaded & Moved - {file_name}')
                        helper.store_data_into_robi_table(dst_path)
                        time.sleep(0.5)
                        update_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='{file_name}'"
                        db_object.execute_query(query=update_query)
                        print(f'Robi File Data Stored in DB - {file_name}')
                    except Exception as e:
                        print(f'Unable to Move File - {file_name} - {e}')
                else:
                    print(f'File Already Exist in Destination Directory')
                    try:
                        helper.store_data_into_robi_table(dst_path)
                        time.sleep(0.5)
                        update_query = f"UPDATE CALL_DROP_FILE_LOG SET STATUS='Done' WHERE FILE_NAME='{file_name}'"
                        db_object.execute_query(query=update_query)
                        print(f'Robi File Data Stored in DB - {file_name}')
                        os.remove(file_path)
                        print(f'File Removed - {file_name}')
                    except Exception as e:
                        print(f'Unable to Remove File - {file_name} - {e}')
        print('---------------------------------')






