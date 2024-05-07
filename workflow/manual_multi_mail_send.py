from pages.pages_xml_wrapper import XMLWrapper
from apps.helper import Helper
from apps.config import ConfigParser
from apps.api_helper import RPAApi
import zipfile
from utils.mail import Mail
from apps.database import DB
from apps.app_utils import AppUtils
from apps.api_and_database import CRMAPI

crm_api_object = RPAApi()
api = CRMAPI()
mail = Mail()
db_obj = DB()
db_object = db_obj
conf = ConfigParser()
wrapper = XMLWrapper()
webpage = wrapper.webpage
driver = webpage.driver
helper = Helper(webpage=wrapper.webpage, driver=wrapper.webpage.driver)


def compress_file(format_date):
    all_files_list = list()
    robi_dir = AppUtils.conf["downloaded_files_dir_robi"]
    airtel_dir = AppUtils.conf["downloaded_files_dir_airtel"]
    compression = zipfile.ZIP_DEFLATED
    data_list = helper.fetch_all_file_call_drop(date=format_date)
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
    zip_file_dir = AppUtils.conf['zip_file_dir']
    final_zip_dir = f"{zip_file_dir}\\call_drop_rebate_{format_date}.zip"
    zf = zipfile.ZipFile(final_zip_dir, mode="w")
    try:
        for file_name in all_files_list:
            zf.write(file_name, file_name.split("\\")[-1], compress_type=compression)
        return final_zip_dir
    except FileNotFoundError:
        print("File not found")
    finally:
        zf.close()


def send_sms_check(format_date):
    robi_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
                                  f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
                                  f"FROM CALL_DROP_SMS_LOG_ROBI " \
                                  f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{format_date}%'"
    robi_sms_status_count_query_response = db_object.select_query(query=robi_sms_status_count_query)
    robi_success_sms_count = robi_sms_status_count_query_response[0][0]
    robi_failed_sms_count = robi_sms_status_count_query_response[0][1]
    airtel_sms_status_count_query = f"SELECT COUNT(CASE SMS_STATUS WHEN 'Sent' THEN 1 END) AS SUCCESS, " \
                                    f"COUNT(CASE SMS_STATUS WHEN 'Failed' THEN 1 END) AS FAILED " \
                                    f"FROM CALL_DROP_SMS_LOG_AIRTEL " \
                                    f"WHERE SMS_STATUS IN ('Sent', 'Failed') AND FILE_NAME LIKE '%{format_date}%'"
    airtel_sms_status_count_query_response = db_object.select_query(query=airtel_sms_status_count_query)
    airtel_success_sms_count = airtel_sms_status_count_query_response[0][0]
    airtel_failed_sms_count = airtel_sms_status_count_query_response[0][1]
    print(f'Robi Success SMS\t- {robi_success_sms_count}')
    print(f'Robi Failed SMS\t\t- {robi_failed_sms_count}')
    print(f'Airtel Success SMS\t- {airtel_success_sms_count}')
    print(f'Airtel Failed SMS\t- {airtel_failed_sms_count}')
    return robi_success_sms_count, robi_failed_sms_count, airtel_success_sms_count, airtel_failed_sms_count


start = 12
end = 15
month = '04'
for date in range(start, end, 1):
    if date == 10:
        continue
    if len(str(date)) == 1:
        date = '0' + str(date)
    mail_date = f'{date}-{month}-2024'
    format_date = f"2024{month}{date}"
    print(f'Date: {mail_date}\n')
    robi_success_sms_count, robi_failed_sms_count, airtel_success_sms_count, airtel_failed_sms_count = send_sms_check(format_date)
    targets = conf.get_call_drop_report_email_to()
    print(targets)

    attachments = compress_file(format_date)

    robi_prepaid_success_count, robi_prepaid_failure_count, \
    robi_postpaid_success_count, robi_postpaid_failure_count, \
    airtel_prepaid_success_count, airtel_prepaid_failure_count, \
    airtel_postpaid_success_count, airtel_postpaid_failure_count = helper.count_success_failure_rates(format_date)

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

    mail_title = f"CALL DROP REBATE STATUS - {mail_date}"
    mail_body = f"""<p>Dear Concern, <br><br> Prepaid & Postpaid bonus disbursement has been completed successfully
                for Robi & Airtel subscribers for {mail_date}. The below table contains the detailed overall status.</p><br>
    
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
    # Send sms to concern
    msisdn_list = ['8801833184089']
    # crm_api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop Rebate Email Mail Sent - {mail_date}")