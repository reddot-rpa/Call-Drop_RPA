import traceback
from apps.api_helper import RPAApi
from utils.logger import Logger
from apps.config import ConfigParser
from traceback import print_exc
from apps.helper import Helper
from utils.mail import Mail
from apps.app_utils import AppUtils

log = Logger.get_instance()
conf = ConfigParser()
api_object = RPAApi()
mail = Mail()
helper = None
msisdn_list = AppUtils.conf['call_drop_sms_to_concern'].split(',')

try:
    helper = Helper(webpage=None, driver=None)
    # check process is running or not
    if helper.check_call_drop_send_sms_rpa_pending_flag():
        api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop SMS RPA Started.")
        # set rpa status running
        helper.set_call_drop_send_sms_rpa_running_flag()
        # Check time period Then sent sms to msisdn
        if helper.is_now_in_time_period():
            helper.send_sms_to_robi_msisdn()
            helper.send_sms_to_airtel_msisdn()
            # Send mail to the user
            helper.send_mail()
        else:
            print("Time is out of range")
            log.log_info(msg="Time is out of range")

        # if all process done set rpa status pending
        helper.set_call_drop_send_sms_rpa_pending_flag()
        # Done
        api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop SMS RPA Done.")
    else:
        log.log_info(msg="Already one process is running!")
        print("Already one process is running!")
except Exception as e:
    print_exc()
    print(e)
    log.log_critical(e)
    api_object.smsapi(msisdn=msisdn_list, message=f"Call Drop SMS RPA Done.")
    helper.set_call_drop_send_sms_rpa_pending_flag()
    log.log_error(exception=e)
    targets = conf.get_error_reporting_email()
    mail_body = f'<p>Dear Concern,</p> There is an exception in RPA : <br> <br> {e}  <br> <br> {traceback.format_exc()}  <p>Thanks</p>'
    mail.send_mail_to(targets, None, 'Call Drop SMS RPA', mail_body)
    mail.send()
