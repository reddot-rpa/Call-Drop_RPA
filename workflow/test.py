# import datetime
# from apps.app_utils import AppUtils
# from apps.database import DB
# from utils.mail import Mail
#
# mail = Mail()
# db_obj = DB()
# db_object = db_obj
#
# previous_date = (datetime.datetime.now() - datetime.timedelta(days=int(AppUtils.conf['previous_email_day'])+1)).strftime("%Y%m%d")
# query = f"SELECT * FROM CALL_DROP_FILE_LOG WHERE BRAND='Robi' AND FILE_NAME LIKE '%{previous_date}%' ORDER BY CASE STATUS WHEN 'Pending' THEN 1 WHEN 'Upload Complete' THEN 2 WHEN 'Download Complete' THEN 3 ELSE 4 END"
# file_name_with_status_list = db_object.select_query(query=query)
# print(file_name_with_status_list)
#
# if file_name_with_status_list == 0:
#     print('Yes')
#
# targets = AppUtils.conf['error_to'].split(',')
# mail_title = f"CALL DROP REBATE FILE STATUS - {previous_date}"
# table_row = ''
# for row in file_name_with_status_list:
#     table_row += f"""
#                   <tr>
#                     <td>{row[0]}</td>
#                     <td>{row[1]}</td>
#                   </tr>"""
# mail_body = f"""<p>Dear Concern, <br><br>
#
#                 Please receive the FILE NAME & STATUS of the files that are being considered for PROVISIONING.</p><br>
#
#                 <table>
#                   <tr>
#                     <th>FILE NAME</th>
#                     <th>STATUS</th>
#                   </tr>{table_row}
#                 </table>
#                 <p>Regards, <br> Automated Call Drop Rebate System</p>"""
#
# mail.send_mail_to(targets, None, mail_title, mail_body)
# mail.send()

# import time
# import autoit
# from apps.helper import Helper
# from apps.app_utils import AppUtils
# from apps.config import ConfigParser
# from pages.pages_xml_wrapper import XMLWrapper
#
# conf = ConfigParser()
# credentials = conf.get_credentials()
# xml_wrapper_object = XMLWrapper()
# helper = Helper(webpage=xml_wrapper_object.webpage, driver=xml_wrapper_object.webpage.driver)
#
# location = r'E:\Rayhan_development\Call-Drop-RPA\files\download_file\airtel\call_drop_air_rebate_prepaid_20221011_9.csv'
# wrapper_xml_link = AppUtils.conf['upload_wrapper_path']
# xml_wrapper_object.automate_xml(location=wrapper_xml_link, df=credentials)
# xml_wrapper_object.webpage.driver.find_element_by_xpath("//input[@name='file']").send_keys(location)

# time.sleep(3)
# autoit.win_set_on_top("Open")
# time.sleep(5)
# autoit.control_set_text("Open", "Edit1", file_path)
# time.sleep(10)
# autoit.control_send("Open", "Edit1", "{ENTER}")

# import time
# from apps.database import DB
# import pandas as pd
#
# db = DB()
#
# data = []
# dates = ['20221004', '20221005', '20221006']
#
#
# for date in dates:
#     robi_prepaid_query = f"SELECT SUM(AMOUNT) FROM CALL_DROP_SMS_LOG_ROBI WHERE FILE_NAME LIKE '%prepaid_{date}%'"
#     robi_postpaid_query = f"SELECT SUM(AMOUNT) FROM CALL_DROP_SMS_LOG_ROBI WHERE FILE_NAME LIKE '%postpaid_{date}%'"
#     airtel_prepaid_query = f"SELECT SUM(AMOUNT) FROM CALL_DROP_SMS_LOG_AIRTEL WHERE FILE_NAME LIKE '%prepaid_{date}%'"
#     airtel_postpaid_query = f"SELECT SUM(AMOUNT) FROM CALL_DROP_SMS_LOG_AIRTEL WHERE FILE_NAME LIKE '%postpaid_{date}%'"
#
#     robi_prepaid_success_count = int(db.select_query(query=robi_prepaid_query)[0][0]) // 60
#     time.sleep(2)
#     robi_postpaid_success_count = int(db.select_query(query=robi_postpaid_query)[0][0]) // 60
#     time.sleep(2)
#     airtel_prepaid_success_count = int(db.select_query(query=airtel_prepaid_query)[0][0]) // 60
#     time.sleep(2)
#     airtel_postpaid_success_count = int(db.select_query(query=airtel_postpaid_query)[0][0]) // 60
#     time.sleep(2)
#
#     temp = [date, 'Robi', robi_prepaid_success_count, robi_postpaid_success_count,
#             robi_prepaid_success_count + robi_postpaid_success_count]
#     data.append(temp)
#     temp = [date, 'Airtel', airtel_prepaid_success_count, airtel_postpaid_success_count,
#             airtel_prepaid_success_count + airtel_postpaid_success_count]
#     data.append(temp)
#
# db.close_connection()
# df = pd.DataFrame(data, columns=['Date', 'Brand', 'Prepaid Minutes', 'Postpaid Minutes', 'Total Minutes'])
# df.to_csv('Report.csv', index=False)

# data_list = [
#     {'id': 1, 'name': 'A', 'status': 'Pending'},
#     {'id': 2, 'name': 'B', 'status': 'Pending'},
#     {'id': 3, 'name': 'C', 'status': 'Pending'},
#     {'id': 4, 'name': 'D', 'status': 'Pending'},
#     {'id': 5, 'name': 'E', 'status': 'Pending'}
# ]
#
# for data in data_list:
#     if data.get('status') == 'Pending':
#         print(f'Pending - {data}')
#         data['status'] = 'Upload'
#     if data.get('status') == 'Upload':
#         print(f'Upload - {data}')
#         data['status'] = 'Download'
#     if data.get('status') == 'Download':
#         print(f'Download - {data}')
#         data['status'] = 'Done'
#     print('----->>>')
# print(data_list)

# import os
# import pandas as pd
# download_file = os.listdir(r'E:\Rayhan_development\Call-Drop-RPA\files\download_file\robi')
# download_file_list = [file for file in download_file if '20221011' in file]
#
# df = pd.read_csv(r'E:\Rayhan_development\Call-Drop-RPA\files\download_file\DB_Log.csv')
# db_file_list = list(df['FILE_NAME'].iloc[:])
#
# for file in download_file_list:
#     if file not in db_file_list:
#         print(file)


# import requests
# from pages.pages_xml_wrapper import XMLWrapper
# from apps.helper import Helper
# from apps.config import ConfigParser
# import time
# from apps.app_utils import AppUtils
# from selenium.webdriver.support.ui import Select
# from apps.encryption import CryptoPassPhase
#
# xml_wrapper_object = XMLWrapper()
# conf = ConfigParser()
# helper = Helper(webpage=xml_wrapper_object.webpage, driver=xml_wrapper_object.webpage.driver)
# credentials = conf.get_credentials()
#
# xlsx_file_name = 'call_drop_air_rebate_postpaid_20221017_1.xlsx'
# # xlsx_file_name = 'call_drop_air_rebate_prepaid_20221016_9.xlsx'
# # xlsx_file_name = 'call_drop_rebate_postpaid_20221016_1.xlsx'
# # xlsx_file_name = 'call_drop_air_rebate_postpaid_20221016_1.xlsx'
#
# helper.run_wrapper(wrapper=xml_wrapper_object, wrapper_df=credentials)
# download_flag = False
# try:
#     element = helper.webpage.driver.find_element_by_css_selector('option[value = "25"]')
#     helper.webpage.driver.execute_script('arguments[0].setAttribute("value", "150")', element)
#     perPage = helper.webpage.driver.find_element_by_id("perPage")
#     Select(perPage).select_by_visible_text("25")
#     time.sleep(5)
#     table = helper.webpage.driver.find_element_by_tag_name('tbody')
#     table_row = table.find_elements_by_tag_name('tr')
#     for row in table_row:
#         file_id = row.find_element_by_css_selector("td:nth-child(2)").text
#         file_name = row.find_element_by_css_selector("td:nth-child(3)").text
#         if file_name == xlsx_file_name:
#             status = row.find_element_by_css_selector("td:nth-child(8)").text
#             total_records = row.find_element_by_css_selector("td:nth-child(9)").text
#             success_records = row.find_element_by_css_selector("td:nth-child(10)").text
#             failure_records = row.find_element_by_css_selector("td:nth-child(11)").text
#             time.sleep(5)
#             if status == "Completed":
#                 file_download_url = f'https://dcrm.robi.com.bd/dcrm/bulk/postpaid/ajx/downloadBulkReport?fileId={file_id}&filename={file_name}'
#                 print(f'File_name = {file_name}')
#                 print(f'Status = {status}')
#                 print(f'Total Records = {total_records}')
#                 print(f'Success Records = {success_records}')
#                 print(f'Failure Records = {failure_records}')
#                 if total_records == success_records or (success_records + failure_records) == total_records:
#                     crm_username = AppUtils.conf['crm_username']
#                     pass_phase = AppUtils.conf['PassPhase']
#                     crm_password = CryptoPassPhase.decrypt(pass_phase, AppUtils.conf['crm_password'])
#                     default_download_path = AppUtils.get_download_path()
#                     print(f'Downloading File - {file_name}')
#                     try:
#                         helper.webpage.driver.get(file_download_url)
#                         print(f'File Download Successful - {file_name}')
#                         helper.log.log_info(f'File Download Successful - {file_name}')
#                         download_flag = True
#                     except Exception as e:
#                         print(f'File Write Unsuccessful - {e}')
#                         helper.log.log_critical(f'File Write Unsuccessful - {e}')
#             else:
#                 helper.webpage.driver.refresh()
# except Exception as e:
#     print(e)
#     helper.log.log_critical(e)

# from apps.database import DB
# db_obj = DB()
# db_object = db_obj
#
#
# select_query = f"SELECT * FROM CALL_DROP_FILE_LOG WHERE FILE_NAME = 'call_drop_rebate_prepaid_20221025_1.xlsx'"
# file_log_data = db_object.select_query(select_query)
# print(file_log_data)
#
# file_log_dict = {
#     "FILE_NAME": str(file_log_data[0][0]),
#     "BRAND": str(file_log_data[0][1]),
#     "STATUS": str(file_log_data[0][2]),
# }
# print(file_log_dict)
#
# print(file_log_dict.get('FILE_NAME'))

import datetime as dt


# def send_sms_check():
#     date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
#     date_today = date_today.strftime("%Y%m%d")
#
#     robi_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
#                                   f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
#                                   f"FROM CALL_DROP_SMS_LOG_ROBI " \
#                                   f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date_today}%'"
#     robi_sms_status_count_query_response = db_object.select_query(query=robi_sms_status_count_query)
#     robi_success_sms_count = robi_sms_status_count_query_response[0][0]
#     robi_failed_sms_count = robi_sms_status_count_query_response[0][1]
#
#     airtel_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
#                                     f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
#                                     f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
#                                     f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date_today}%'"
#     airtel_sms_status_count_query_response = db_object.select_query(query=airtel_sms_status_count_query)
#     airtel_success_sms_count = airtel_sms_status_count_query_response[0][0]
#     airtel_failed_sms_count = airtel_sms_status_count_query_response[0][1]
#
#     return robi_success_sms_count, robi_failed_sms_count, airtel_success_sms_count, airtel_failed_sms_count
#
#
# print(send_sms_check())

# def count_success_failure_rates(date):
#     robi_postpaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
#                           f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
#                           f"FROM CALL_DROP_SMS_LOG_ROBI " \
#                           f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%postpaid%'"
#     robi_postpaid_query_response = db_object.select_query(query=robi_postpaid_query)
#     robi_postpaid_success_count = int(robi_postpaid_query_response[0][0]) if robi_postpaid_query_response[0][0] is not None else 0
#     robi_postpaid_failure_count = int(robi_postpaid_query_response[0][1]) if robi_postpaid_query_response[0][1] is not None else 0
#
#     robi_prepaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
#                          f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
#                          f"FROM CALL_DROP_SMS_LOG_ROBI " \
#                          f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%prepaid%'"
#     robi_prepaid_query_response = db_object.select_query(query=robi_prepaid_query)
#     robi_prepaid_success_count = int(robi_prepaid_query_response[0][0]) if robi_prepaid_query_response[0][0] is not None else 0
#     robi_prepaid_failure_count = int(robi_prepaid_query_response[0][1]) if robi_prepaid_query_response[0][1] is not None else 0
#
#     airtel_postpaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
#                             f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
#                             f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
#                             f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%postpaid%'"
#     airtel_postpaid_query_response = db_object.select_query(query=airtel_postpaid_query)
#     airtel_postpaid_success_count = int(airtel_postpaid_query_response[0][0]) if airtel_postpaid_query_response[0][0] is not None else 0
#     airtel_postpaid_failure_count = int(airtel_postpaid_query_response[0][1]) if airtel_postpaid_query_response[0][1] is not None else 0
#
#     airtel_prepaid_query = f"SELECT SUM(CASE SMS_STATUS WHEN 'Sent' THEN AMOUNT END) AS SUCCESS, " \
#                            f"SUM(CASE SMS_STATUS WHEN 'Failed' THEN AMOUNT END) AS FAILED " \
#                            f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
#                            f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{date}%' AND FILE_NAME LIKE '%prepaid%'"
#     airtel_prepaid_query_response = db_object.select_query(query=airtel_prepaid_query)
#     airtel_prepaid_success_count = int(airtel_prepaid_query_response[0][0]) if airtel_prepaid_query_response[0][0] is not None else 0
#     airtel_prepaid_failure_count = int(airtel_prepaid_query_response[0][1]) if airtel_prepaid_query_response[0][1] is not None else 0
#
#     return robi_prepaid_success_count, robi_prepaid_failure_count, robi_postpaid_success_count, \
#            robi_postpaid_failure_count, airtel_prepaid_success_count, airtel_prepaid_failure_count, \
#            airtel_postpaid_success_count, airtel_postpaid_failure_count
#
#
# date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
# date_today = date_today.strftime("%Y%m%d")
# print(count_success_failure_rates(date=date_today))
