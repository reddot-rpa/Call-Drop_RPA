import traceback
import subprocess
from pages.pages_xml_wrapper import XMLWrapper
from utils.mail import Mail
from apps.helper import Helper
from apps.api_helper import RPAApi
from apps.config import ConfigParser
from apps.app_utils import AppUtils
from utils.logger import Logger
import datetime
from sys import exit

log = Logger.get_instance()
call_drop_api_object = RPAApi()
xml_wrapper_object = XMLWrapper()
conf = ConfigParser()
mail = Mail()
error_msg = ""
helper = None
msisdn_list = AppUtils.conf['call_drop_sms_to_concern'].split(',')

try:
    helper = Helper(webpage=xml_wrapper_object.webpage, driver=xml_wrapper_object.webpage.driver)
    AppUtils.rpa_running_flag(flag="Start")
    credentials = conf.get_credentials()

    # check process is running or not
    if helper.check_call_drop_rpa_pending_flag():
        # set RPA running
        helper.set_call_drop_rpa_running_flag()
        # For Robi File
        call_drop_api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop RPA Processing Started.")

        helper.robi_rebate()
        helper.airtel_rebate()
        # set RPA pending flag
        helper.set_call_drop_rpa_pending_flag()
        # Done
        call_drop_api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop RPA Processing Done.")
        helper.log.log_info(msg=f"All Valid Data Process Done")

        # Alerting About Files for Processing and Status
        previous_date = (datetime.datetime.now() - datetime.timedelta(days=int(AppUtils.conf['previous_email_day']))).strftime("%Y%m%d")
        file_name_with_status_list = helper.get_file_name_with_status(previous_date)
        helper.send_file_status_email_to_developers(previous_date, file_name_with_status_list)

        xml_wrapper_object.webpage.driver.quit()
        helper.webpage.driver.quit()
        print('Process Completed')
        log.log_info(msg="Process Completed!")
    else:
        print("Already one process is running!")
        log.log_info(msg="Already one process is running!")
        xml_wrapper_object.webpage.driver.quit()
        print('Process Complete')
except Exception as e:
    print("Closing Browser JOB")
    log.log_info(msg=f"Closing Browser Job")
    helper.set_call_drop_rpa_pending_flag()
    xml_wrapper_object.webpage.driver.quit()
    print(e)
    log.log_critical(msg=f"Call Drop RPA : There is issue in main method due to {str(e)}")
    call_drop_api_object.smsapi(msisdn=msisdn_list, message="Call Drop RPA : " + str(e))
    log.log_error(exception=e)
    targets = conf.get_error_reporting_email()
    mail_body = f'<p>Dear Concern,</p> There is an exception in RPA : <br> <br> {e}  <br> <br> {traceback.format_exc()}  <p>Thanks</p>'
    mail.send_mail_to(targets, None, 'Call Drop RPA', mail_body)
    mail.send()
    print('Process Complete')
