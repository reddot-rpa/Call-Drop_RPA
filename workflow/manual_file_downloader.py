from pages.pages_xml_wrapper import XMLWrapper
from selenium.webdriver.support.ui import Select
from apps.helper import Helper
from apps.config import ConfigParser
from apps.app_utils import AppUtils
from selenium.webdriver.common.keys import Keys
import time
import os
import shutil

conf = ConfigParser()
credentials = conf.get_credentials()
wrapper = XMLWrapper()
webpage = wrapper.webpage
driver = webpage.driver
helper = Helper(webpage=wrapper.webpage, driver=wrapper.webpage.driver)

helper.run_wrapper(wrapper=wrapper, wrapper_df=credentials)

dcrm_pagination_size = AppUtils.conf['dcrm_pagination_size']
webpage.driver.find_element_by_id("select2-searchType-container").click()
webpage.driver.find_element_by_xpath("//input[@type='search']").clear()
webpage.driver.find_element_by_xpath("//input[@type='search']").send_keys("File Name")
webpage.driver.find_element_by_xpath("//input[@type='search']").send_keys(Keys.ENTER)
element = webpage.driver.find_element_by_css_selector('option[value = "25"]')
webpage.driver.execute_script(f'arguments[0].setAttribute("value", "{dcrm_pagination_size}")', element)
perPage = webpage.driver.find_element_by_id("perPage")
Select(perPage).select_by_visible_text("25")
start = 20230719
end = 20230728
for date in range(start, end, 1):
    print(f'Date: {date}\n')
    webpage.driver.find_element_by_name("searchfield").clear()
    webpage.driver.find_element_by_name("searchfield").send_keys(f"paid_{date}")
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
        time.sleep(1)
        file_name_without_ext = file_name.split('.')[0]
        file_path = f'C:/Users/fin_rpa_admin/Downloads/{file_name_without_ext}.csv'
        if os.path.isfile(file_path):
            if '_air_' in file_name_without_ext:
                dst_path = f"E:/Sunjid_development/Call-Drop-RPA/files/download_file/airtel/{file_name_without_ext}.csv"
                if not os.path.isfile(dst_path):
                    try:
                        shutil.move(file_path, dst_path)
                        print(f'Downloaded & Moved - {file_name}')
                    except Exception as e:
                        print(f'Unable to Move File - {file_name} - {e}')
                else:
                    print(f'File Already Exist in Destination Directory')
                    try:
                        os.remove(file_path)
                        print(f'File Removed - {file_name}')
                    except Exception as e:
                        print(f'Unable to Remove File - {file_name} - {e}')
            else:
                dst_path = f"E:/Sunjid_development/Call-Drop-RPA/files/download_file/robi/{file_name_without_ext}.csv"
                if not os.path.isfile(dst_path):
                    try:
                        shutil.move(file_path, dst_path)
                        print(f'Downloaded & Moved - {file_name}')
                    except Exception as e:
                        print(f'Unable to Move File - {file_name} - {e}')
                else:
                    print(f'File Already Exist in Destination Directory')
                    try:
                        os.remove(file_path)
                        print(f'File Removed - {file_name}')
                    except Exception as e:
                        print(f'Unable to Remove File - {file_name} - {e}')
        print('---------------------------------')




