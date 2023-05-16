import json
import urllib
from datetime import datetime
import sys
import uuid
import requests
import time
import xmltodict
import ssl
import urllib.parse
from apps.database import DB
from utils.logger import Logger
from xml.etree import ElementTree
from apps.app_utils import AppUtils
from apps.config import ConfigParser

db_object = DB()
conf = ConfigParser()

class RPAApi:
    token = None
    token_genarate_url = None
    postpaid_recharge_url = None
    prepaid_recharge_url = None
    user_name = None
    password = None
    authorization_token = None
    adjust_balance_url = None

    def __init__(self):
        self.log = Logger.get_instance()
        self.user_name = "IT_RPA_AppS"
        self.password = "RoboTIC%24HerO)24"
        self.authorization_token = "UHpTdjBGc1RkRUQ3VXB4dmNMX3VWeXh4SG9BYTpqaW1xN0RTTkxzdkZpQlJaN0Q0Vjd1Z0xGQ0Fh"
        self.token_genarate_url = 'https://apigate.robi.com.bd/token'
        self.cbs_damage_card_recharge_url = 'https://apigate.robi.com.bd/cbs/cbsDamageCardRecharge/v1/somapi/postpaid/voucherRecharge'
        self.cbs_blacklist_remove_url = 'https://apigate.robi.com.bd/cbs/cbsBlacklisting/v1/somapi/postpaid/blacklist/msisdn'
        self.cbs_query_recharge_log_url = 'https://apigate.robi.com.bd/cbs/cbsQueryRechargeLog/v1/cbsQueryRechargeLog'
        self.cbs_query_cdr_product_id_url = 'https://apigate.robi.com.bd/cbs/CBSQueryCDR/v1/cbsQueryCDR'
        self.cbs_query_free_unit_url = 'https://apigate.robi.com.bd/cbs/cbsQueryFreeUnit/v1/cbsQueryFreeUnit'
        self.dcrm_complaint_status_update_url = "http://dcrmsoap.robi.com.bd:8080/updaterem2crm/ws/UpdateServiceRequest"
        self.sms_url = "http://10.101.11.164:8888/cgi-bin/sendsms"
        self.call_drop_sms_url = "https://api.appsmsc.robi.com.bd:8443/sms"
        self.token = self.access_token()

    def stop_execution(self):
        print('Stopping Execution')
        self.log.log_info(msg="Execution Stopped")
        sys.exit('Stopping Execution')

    def access_token(self):
        url = self.token_genarate_url
        payload = "grant_type=password&username=" + self.user_name + "&password=" + self.password + "&scope=PRODUCTION"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Basic " + self.authorization_token,
            'cache-control': "no-cache",
        }
        try:
            response = requests.post(url=url, data=payload, headers=headers, timeout=3)
            self.log.log_info(msg="Response received from MIFE")
            print(response.text)
            print(response.headers['Content-Type'])
            if response.headers['Content-Type'] == 'application/json; charset=UTF-8':
                return response.json()['access_token']
            else:
                return self.access_token()

        except requests.exceptions.RequestException as e:
            self.log.log_critical(msg="Unable to connect to apigate.robi.com.bd{e}")

    def mob_num_to_10_digit(self, mob=''):
        self.log.log_info(f"Converting MSISDN 10 For : {mob}")
        misdo_response = 0
        if len(mob) == 13 and mob.startswith('880'):
            misdo_response = mob[3:]
        elif len(mob) == 10 and mob.startswith('1'):
            misdo_response = mob
        elif len(mob) == 11 and mob.startswith('01'):
            misdo_response = mob[1:]

        return misdo_response

    def cbs_remove_msisdn_blacklist(self, msisdn):
        msisdn = self.mob_num_to_10_digit(mob=msisdn)
        self.log.log_info(f"Removing & CHecking Blacklist For : {msisdn}")
        cbs_response = {'responseCode': 1, 'responseDesc': 'Failed To Execute'}
        url = self.cbs_blacklist_remove_url + '/' + msisdn
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache",
            'Authorization': "Bearer " + self.token
        }
        try:
            response = requests.request("GET", url, headers=headers, timeout=3)
            self.log.log_info(f"Request Send to clear blacklist msisdn: {msisdn}")
            self.log.log_info(f"CBS unblock API response DETAILS : msisdn - {msisdn} response - {response}")
            print(f"Request Send to clear blacklist msisdn: {msisdn}")
            if response.status_code == 401:
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("GET", url, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                json_content = json.loads(response.content)
                print(json_content)
                return json_content
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            return None

    def cbs_damage_card_recharge(self, msisdn, card_serial, pin_no):
        recharge_id = "ERASE" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('TOKEN ID API = ', recharge_id)
        recharge_unique_serial = recharge_id
        missdn = self.mob_num_to_10_digit(msisdn)
        print('cbs_damage_card_recharge = MSISDN : ', missdn)
        card_serial = card_serial
        pin_no = pin_no
        i = 0
        id = recharge_id
        url = self.cbs_damage_card_recharge_url
        headers = {
            'Content-Type': "text/plain",
            'cache-control': "no-cache",
            'Authorization': "Bearer " + self.token,
            'Accept': "*/*"
        }

        payload = {
            'rechargeUniqueSerial': f"{recharge_unique_serial}",
            'missdn': f"{missdn}",
            'cardSerial': f"{card_serial}",
            'pinNo': f"{pin_no}"
        }

        print('payload', payload)

        headers = {'Content-Type': "application/json", 'cache-control': "no-cache",
                   'Authorization': "Bearer " + self.token, }

        try:
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            print(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            if response.status_code == 401:
                time.sleep(2)
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                print(response)
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    def crm_complain_update(self, msisdn, sr_id, status, error=''):
        now = datetime.now()
        current_date_time = now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
        id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")

        reason = "Adjustment Given"
        root_cause = "Other"
        root_casue_detail = "Other"
        escalation = ""
        Solution = "Adjustment Given"
        if status == "Cancelled":
            reason = error
            root_cause = "Invalid Complaint"
            root_casue_detail = "Invalid Complaint"
            escalation = "Unable to Adjust the requested amount"
            Solution = "Adjustment Given"
        url = "http://lb_osb.robi.com.bd:7777/RobiAxiataSOA/RequesterABCS/ProxyServices/UpdateServiceRequest/2.0/UpdateServiceRequestReqABCSPS"
        payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:upd=\"http://www.robi.com.bd/soa/xsd/UpdateServiceRequest/V2\" xmlns:com=\"http://www.robi.com.bd/soa/xsd/Common\">\n   <soapenv:Header/>\n   <soapenv:Body>\n      <upd:UpdateServiceRequest>\n         <upd:CommonComponents>\n            <com:IMSINum/>\n            <com:MSISDNNum/>\n            <com:ProcessingNumber>{id}</com:ProcessingNumber> <!--Mandatory Input:unique identifier to track the transaction-->\n            <com:RequestDateTime>{current_date_time}</com:RequestDateTime> <!--Conditional Mandatory: Holds the timestamp when the request came in. If it is being passed, it has to be in the exact format as mentioned. Else even the tag should also not be present. Empty tag will not be supported.-->\n            <com:SenderID>RPA</com:SenderID> <!-- Mnadatory Input. Valid Values \"Remedy\" -->\n\t    <!--Optional: Holds the Source IP or Host Name of the application giving the request or the Service Invoker-->\n            <com:SourceHostName>xxxx</com:SourceHostName>\n         </upd:CommonComponents>\n         <upd:RequestType>UpdateServiceRequest</upd:RequestType> <!--Mandatory Input, valid value \"UpdateServiceRequest\" -->\n         <upd:Identification>\n            <upd:ServiceID>{msisdn}</upd:ServiceID> <!-- Mandatory: Holds  Subscriber MSISDN, this is sample value-->\n         </upd:Identification>\n         <upd:ServiceRequestDetails>\n            <upd:ServiceRequestSpecification>\n               \t<upd:ServiceReqNum>{sr_id}</upd:ServiceReqNum> <!-- Mandatory: Holds CRM SRNumber,  this is a sample Value -->\n               \t<upd:IncidentStatus>{status}</upd:IncidentStatus> <!-- Mandatory : Holds Status value. Valid values are ..\"Pending\" , \"Open\" , \"In Progress\" , \"Completed\" , \"Closed\" , \"Cancelled\"-->\n\t\t<upd:Resolution>{reason}</upd:Resolution> <!-- Mandatory : Resolution for update service request -->\n\t\t<upd:StatusReason>{reason}</upd:StatusReason> <!-- Mandatory : Incident Status updated reason -->\n               \t<upd:RootCause>{root_cause}</upd:RootCause><!-- Optional: Root Cause -->\n               \t<upd:RootCauseDetails>{root_casue_detail}</upd:RootCauseDetails> <!-- Optional: Root Cause reason -->\n            </upd:ServiceRequestSpecification>\n         </upd:ServiceRequestDetails>\n      </upd:UpdateServiceRequest>\n   </soapenv:Body>\n</soapenv:Envelope>"
        headers = {
            'Content-Type': "application/xml"
        }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"api name: crm_complain_update,sr_number: {sr_id}, msisdn: {msisdn}, calling id : {id}, response: {response.text}")
            # print(response.text)
            tree = ElementTree.fromstring(response.text)
            ns = {"v2": "http://www.robi.com.bd/soa/xsd/UpdateServiceRequest/V2"}
            status_desc = tree.find('.//v2:StatusDesc', ns).text
            if status_desc == 'Success':
                self.log.log_info(
                    f"Response received from CRM Detail : MSISND - {msisdn} SR_ID - {sr_id} Status - {status}  ")
                print(response.text)
                return "Success"
            else:
                self.log.log_warn(
                    f"Unable to change status in CRM, CRM complain terminate not possible Details : For MSISDN - {msisdn} SR_ID - {sr_id} calling id : {id} Status - {status} Exception - {status_desc}")
                print(response.text)
                return "fail"

        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to CRM, CRM complain terminate not possible Details : For MSISDN - {msisdn} SR_ID - {sr_id} calling id : {id} Status - {status} Exception - {e}")
            print(
                f"Unable to connect to CRM, CRM complain terminate not possible Details : For MSISDN - {msisdn} SR_ID - {sr_id} calling id : {id} Status - {status} Exception - {e}")
            return "fail"

    def crm_complain_update_counter(self, msisdn, sr_id, current_status, target_status, error):
        time.sleep(1)
        init_msg = f"crm_complain_update_counter API Hit : (msisdn: {msisdn}, sr_id:{sr_id}, current_status:{current_status}, target_status:{target_status}, error:{error})"
        self.log.log_info(init_msg)
        print(init_msg)
        print(msisdn, sr_id, target_status)
        i = 0
        while i < 3:
            i = i + 1
            time.sleep(1)
            response = self.crm_complain_update(msisdn, sr_id, target_status, error=error)
            print(msisdn, sr_id, target_status)
            if response == "Success":
                self.log.log_info(response)
                print(response)
                return response
            else:
                if i == 3:
                    if response == "fail":
                        error_ = f"{error}, Complain's status can not be changed to {target_status}, Current status: {current_status}"
                        #   db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
                        print(error_)
                    return response

    def check_cbs_recharge_log(self, msisdn, start_time, end_time):
        message_seq = "UNWIILING" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('TOKEN ID API = ', message_seq)
        missdn = self.mob_num_to_10_digit(msisdn)
        print('mob_num_to_10_digit = MSISDN : ', missdn)
        i = 0
        url = self.cbs_query_recharge_log_url
        payload = f"Version=1&MessageSeq={message_seq}&PrimaryIdentity={missdn}&TotalRowNum=1000&BeginRowNum=0" \
                  f"&FetchRowNum=1&StartTime={start_time}&EndTime={end_time}"
        print('payload', payload)

        # headers = {
        #     'Content-Type': "text/plain",
        #     'cache-control': "no-cache",
        #     'Authorization': "Bearer " + self.token,
        #     'Accept': "*/*"
        # }
        headers = {'Content-Type': "application/x-www-form-urlencoded", 'cache-control': "no-cache",
                   'Authorization': "Bearer " + self.token, }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            print(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            if response.status_code == 401:
                time.sleep(2)
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                print(response)
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    def cbs_query_cdr_get_product_id(self, msisdn, recharge_time):
        message_seq = "UNWIILING" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('TOKEN ID API = ', message_seq)
        missdn = self.mob_num_to_10_digit(msisdn)
        print('mob_num_to_10_digit = MSISDN : ', missdn)
        i = 0
        url = self.cbs_query_cdr_product_id_url
        payload = f"Version=1&MessageSeq={message_seq}&PrimaryIdentity={missdn}&OperatorID=353&BEID=101&StartTime" \
                  f"={recharge_time}&TotalCDRNum=10&BeginRowNum=1&FetchRowNum=200"
        print('payload', payload)
        headers = {'Content-Type': "application/x-www-form-urlencoded", 'cache-control': "no-cache",
                   'Authorization': "Bearer " + self.token, }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            print(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            if response.status_code == 401:
                time.sleep(2)
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                print(response)
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    def cbs_query_free_unit_get_voice_sms(self, msisdn):
        message_seq = "UNWIILING" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('TOKEN ID API = ', message_seq)
        missdn = self.mob_num_to_10_digit(msisdn)
        print('mob_num_to_10_digit = MSISDN : ', missdn)
        i = 0
        url = self.cbs_query_free_unit_url
        payload = f"Version=test112qw&MessageSeq={message_seq}&PrimaryIdentity={missdn}"
        print('payload', payload)
        headers = {'Content-Type': "application/x-www-form-urlencoded", 'cache-control': "no-cache",
                   'Authorization': "Bearer " + self.token, }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            print(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            if response.status_code == 401:
                time.sleep(2)
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                print(response)
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    def terminate_request_voice_sms_plan(self, msisdn, plan_id, adjustment_amount):
        message_seq = "UNWIILING" + datetime.now().strftime("%Y%m%d%H%M%S")
        url = "https://apigate.robi.com.bd/cbs/CbsAdjustAccount/v1/cbsAdjustAccount"

        payload = f"Version=1&BusinessCode=Payment&MessageSeq={message_seq}&BEID=101&OperatorID=353&AccessMode=3" \
                  f"&AdjustmentSerialNo={plan_id}&PrimaryIdentity={msisdn}&OpType=1&BalanceType=C_3501" \
                  f"&AdjustmentType=1&AdjustmentAmt={adjustment_amount}"
        print(payload)
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Bearer " + self.token,
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "apigate.robi.com.bd",
            'Accept-Encoding': "gzip, deflate",
            'cache-control': "no-cache"
        }

        # Version: 1
        # BusinessCode: Payment
        # MessageSeq: 900212311141
        # BEID: 101
        # OperatorID: 353
        # AccessMode: 3
        # AdjustmentSerialNo: 20103041121123
        # PrimaryIdentity: 1642996501
        # OpType: 1
        # BalanceType: C_3501
        # AdjustmentType: 1
        # AdjustmentAmt: 20000000

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            print('Response terminate_request_voice_sms_plan = ', response)
            self.log.log_info(
                f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code},RESPONSE - {response.text} ")

            if response.status_code == 401:
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
                print('Response terminate_request_voice_sms_plan = ', response)
                self.log.log_info(
                    f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code},RESPONSE - {response.text} ")

            if response.status_code == 200:
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd, ADCS terminate not possible Details : For MSISDN - {msisdn} AMOUNT - {plan_id} EXCEPTION - {e}")
            return None

    def terminate_request_data_plan(self, msisdn, plan_id):
        url = "https://apigate.robi.com.bd/adcs/AdcsTerminatePlan/v1/updatePlan"
        payload = f"msisdn={msisdn}&planId={plan_id}&state=terminated"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Bearer " + self.token,
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "apigate.robi.com.bd",
            'Accept-Encoding': "gzip, deflate",
            'cache-control': "no-cache"
        }
        try:
            response = requests.request("PUT", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code}, RESPONSE - {response.text} ")

            if response.status_code == 401:
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("PUT", url, data=payload, headers=headers, timeout=3)
                self.log.log_info(
                    f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code}, RESPONSE - {response.text} ")

            if response.status_code == 200:
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd, ADCS terminate not possible Details : For MSISDN - {msisdn} AMOUNT - {plan_id} EXCEPTION - {e}")
            return None

    def rebate_balance(self, msisdn, amount):
        print('Token = ', self.token)
        id = "RPA_" + str(AppUtils.conf['rpa_name']) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('ID = ', id)
        rebate_amount = 0
        if type(amount) is str or type(amount) is int or type(amount) is float:
            rebate_amount = int(float(amount))
        else:
            self.log.log_critical(
                f"amount value is not required Type This might problem Details : MSISDN - {msisdn} AMOUNT - {amount} AMOUNT_TYPE - {type(amount)}")
            raise TypeError("Must be String Type Only Containing Numbers")

        # amount = amount * 10000
        url = "https://apigate.robi.com.bd/ocsadjustAccount/v1/adjustaccount"

        payload = f"CommandId=AdjustAccount&RequestType=Event&AccountType=2000&SubscriberNo={msisdn}&CurrAcctChgAmt={rebate_amount}&OperateType=2&LogID={id}&TransactionId={id}&SequenceId={id}&Version=1&SerialNo={id}&AdditionalInfo=RPA"
        print(payload)
        self.log.log_info(msg=f"{payload}")
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': f'Bearer {self.token}'
        }
        try:
            response = requests.post(url=url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")

            if response.status_code == 401:
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.post(url=url, data=payload, headers=headers, timeout=3)
                self.log.log_info(
                    f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")
            print('response = ', response)
            self.log.log_info(msg=f"{response}")
            if response.status_code == 200 and response.json()['ResultHeader']['ResultDesc'] == "Operation successful.":
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd, CBS rebate not possible Details : For  MSISDN - {msisdn} AMOUNT - {rebate_amount} EXCEPTION - {e}")
            return None

        # return response

    def cbs_free_unit_adjustment(self, msisdn, free_unit_type, free_unit_instance_id, adjustment_amount):
        message_seq = "UNWIILING" + datetime.now().strftime("%Y%m%d%H%M%S")
        print('TOKEN ID API = ', message_seq)
        missdn = self.mob_num_to_10_digit(msisdn)
        print('mob_num_to_10_digit = MSISDN : ', missdn)
        i = 0
        url = "https://apigate.robi.com.bd/cbs/cbsFreeUnitAdjustment/v1/cbsAdjustAccountFreeUnit"

        payload = f'Version=1&BusinessCode=1&AccessMode=3' \
                  f'&AdjustmentSerialNo={message_seq}&PrimaryIdentity={missdn}' \
                  f'&OpType=1&BalanceType=C_MAIN_ACCOUNT&AdjustmentType=2' \
                  f'&AdjustmentAmt=0&FreeUnitInstanceID={free_unit_instance_id}' \
                  f'&FreeUnitType={free_unit_type}&AdjustmentType=2' \
                  f'&AdjustmentAmt={adjustment_amount}&Code=C_WS_AdditionalInfo&Value=Test'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + self.token,
        }

        print('payload', payload)

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            print(
                f"msisdn: {missdn}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Response Body: {response.text}")
            if response.status_code == 401:
                time.sleep(2)
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            elif response.status_code == 500:
                return None
            elif response.status_code == 200:
                print(response)
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    def get_price(self, msisdn, trade_time, start_time, end_time):
        print('Get Price API Call based on ', msisdn, trade_time, start_time, end_time)
        self.log.log_info(msg=f'Get Price API Call based on , {msisdn}, {trade_time}, {start_time}, {end_time}')
        msisdn = str(msisdn)
        msisdn = AppUtils.mob_num_to_10_digit(mob=msisdn)
        i = 0
        price_flag = 0
        id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        payload = f"Version=1&MessageSeq={id}&PrimaryIdentity={msisdn}&TotalRowNum=1000&BeginRowNum=0&FetchRowNum=100&StartTime={start_time}&EndTime={end_time}"
        url = self.cbs_query_recharge_log_url
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
            'Authorization': f"Bearer {self.token}"
        }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Pack Price Response Body: {response.text}")
            print(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code} Pack Price Response Body: {response.text}")

            if response.status_code == 401:
                i = i + 1
                self.token = self.access_token()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)

            self.log.log_info(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Pack Price Response Body: {response.text}")
            print(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code} Pack Price Response Body: {response.text}")

            if response.status_code == 500:
                return None

            if response.status_code == 200 and response.json()['ResultHeader'][
                'ResultDesc'] == "Operation successfully.":
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_error(e)
            self.log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None

    # def get_price(self, msisdn, trade_time, start_time, end_time):
    #     print('Get Price API Call based on ', msisdn, trade_time, start_time, end_time)
    #     self.log.log_info(msg=f'Get Price API Call based on , {msisdn}, {trade_time}, {start_time}, {end_time}')
    #     msisdn = str(msisdn)
    #     msisdn = AppUtils.mob_num_to_10_digit(mob=msisdn)
    #     i = 0
    #     price_flag = 0
    #     id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
    #     payload = f"Version=1&MessageSeq={id}&PrimaryIdentity={msisdn}&TotalRowNum=1000&BeginRowNum=0&FetchRowNum=100&StartTime={start_time}&EndTime={end_time}"
    #     url = self.cbs_query_recharge_log_url
    #     headers = {
    #         'Content-Type': "application/x-www-form-urlencoded",
    #         'cache-control': "no-cache",
    #         'Authorization': f"Bearer {self.token}"
    #     }
    #
    #     try:
    #         response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
    #         self.log.log_info(
    #             f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Pack Price Response Body: {response.text}")
    #         print(
    #             f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code} Pack Price Response Body: {response.text}")
    #
    #         if response.status_code == 401:
    #             i = i + 1
    #             self.token = self.access_token()
    #             headers['Authorization'] = f'Bearer {self.token}'
    #             response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
    #
    #         self.log.log_info(
    #             f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Pack Price Response Body: {response.text}")
    #         print(
    #             f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code} Pack Price Response Body: {response.text}")
    #
    #         if response.status_code == 500:
    #             return None
    #
    #         if response.status_code == 200 and response.json()['ResultHeader'][
    #             'ResultDesc'] == "Operation successfully.":
    #             return response
    #         else:
    #             return None
    #     except requests.exceptions.RequestException as e:
    #         self.log.log_error(e)
    #         self.log.log_error_msg(
    #             f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
    #         print(f"Get Price Exception - {e}")
    #         return None

    def dcrm_complain_update(self, msisdn, sr_id, status, error=''):
        now = datetime.now()
        current_date_time = now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
        rpa_processing_id = "RPA" + datetime.now().strftime("%Y%m%d%H%M%S")
        api_status = False
        root_cause = "Other"
        root_cause_detail = "Other"
        solution = "Requested amount has been adjusted successfully."
        if status == "Cancelled":
            root_cause = str(error)
            root_casue_detail = str(error)
            solution = "Not eligible for adjustment."
        url = self.dcrm_complaint_status_update_url
        payload = """<x:Envelope
                    xmlns:x="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:v="http://www.robi.com.bd/soa/xsd/UpdateServiceRequest/V2"
                    xmlns:com="http://www.robi.com.bd/soa/xsd/Common">
                    <x:Header/>
                    <x:Body>
                        <v:UpdateServiceRequest>
                            <v:CommonComponents>
                                <com:IMSINum></com:IMSINum>
                                <com:MSISDNNum>""" + str(msisdn) + """</com:MSISDNNum>
                                <com:ProcessingNumber>""" + str(rpa_processing_id) + """</com:ProcessingNumber>
                                <com:RequestDateTime>""" + str(current_date_time) + """</com:RequestDateTime>
                                <com:SenderID>RPA</com:SenderID>
                                <com:SourceHostName>RPA</com:SourceHostName>
                            </v:CommonComponents>
                            <v:RequestType>UpdateServiceRequest</v:RequestType>
                            <v:Identification>
                                <v:ServiceID>""" + str(msisdn) + """</v:ServiceID>
                                <v:CustID></v:CustID>
                            </v:Identification>
                            <v:ServiceRequestDetails>
                                <v:ServiceRequestSpecification>
                                    <v:ServiceReqNum>""" + str(sr_id) + """</v:ServiceReqNum>
                                    <v:IncidentNum></v:IncidentNum>
                                    <v:ActionID></v:ActionID>
                                    <v:Description></v:Description>
                                    <v:IncidentStatus>""" + str(status) + """</v:IncidentStatus>
                                    <v:Resolution>""" + str(solution) + """</v:Resolution>
                                    <v:StatusReason>""" + str(root_cause) + """</v:StatusReason>
                                    <v:RootCause>""" + str(root_cause) + """</v:RootCause>
                                    <v:RootCauseDetails>""" + str(root_cause_detail) + """</v:RootCauseDetails>
                                </v:ServiceRequestSpecification>
                            </v:ServiceRequestDetails>
                        </v:UpdateServiceRequest>
                    </x:Body>
                </x:Envelope>"""
        #   print(payload)
        headers = {
            'SOAPAction': '"GET"',
            'Content-Type': 'text/xml; charset="utf-8"'
        }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=5)
            #   print(response)
            if response.status_code == 200:
                #   print('Response Text', response.text)
                result = xmltodict.parse(response.text)
                print(result)
                #   print(result["SOAP-ENV:Envelope"])
                #   print(result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"])
                #   print(result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["ns3:UpdateServiceRequestResponse"])
                print(
                    result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["ns3:UpdateServiceRequestResponse"]["ns3:StatusInfo"])
                status_info = result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["ns3:UpdateServiceRequestResponse"][
                    "ns3:StatusInfo"]
                self.log.log_info(msg=f"{status_info}")
                status_code = \
                result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["ns3:UpdateServiceRequestResponse"]["ns3:StatusInfo"][
                    "ns3:StatusCode"]
                status_desc = \
                result["SOAP-ENV:Envelope"]["SOAP-ENV:Body"]["ns3:UpdateServiceRequestResponse"]["ns3:StatusInfo"][
                    "ns3:StatusDesc"]
                if int(status_code) == 0 and str(status_desc) == "Success":
                    api_status = True
                    print(f"DCRM status {status} updated successfully for SR ID {sr_id} and MSISDN {msisdn}")
                    self.log.log_info(
                        msg=f"DCRM status {status} updated successfully for SR ID {sr_id} and MSISDN {msisdn}")
                else:
                    print(f"DCRM status {status} update failed for SR ID {sr_id} and MSISDN {msisdn}")
                    self.log.log_critical(
                        msg=f"DCRM status {status} update failed for SR ID {sr_id} and MSISDN {msisdn}")
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to CRM, CRM complain terminate not possible Details : For MSISDN - {msisdn} SR_ID - {sr_id} calling id : {id} Status - {status} Exception - {e}")
            print(
                f"Unable to connect to CRM, CRM complain terminate not possible Details : For MSISDN - {msisdn} SR_ID - {sr_id} calling id : {id} Status - {status} Exception - {e}")
        return api_status

    def smsapi(self, msisdn, message):
        ssl._create_default_https_context = ssl._create_unverified_context
        message = urllib.parse.quote_plus(message, encoding='UTF-8')
        msisdn_list = None
        if isinstance(msisdn, list):
            msisdn_list = msisdn
        elif isinstance(msisdn, str):
            msisdn_list = msisdn.split(',')
        for msisdn in msisdn_list:
            msisdn = msisdn.replace(' ', '')
            msisdn = AppUtils.msisdn_to_13_digit(msisdn=msisdn)
            url = f"https://api.appsmsc.robi.com.bd:8443/sms?user=RPAalert&password=RPA@l3rt1%23&src=RPA&dst={msisdn}&msg={message}"
            try:
                response = str(urllib.request.urlopen(url).read()).strip("b'")
                if response.split(":")[0] == "Operation success":
                    self.log.log_info(
                        f"Success Response Received from APPSMSC : MSISDN - {msisdn}, MSG - {message}, RESPONSE - {response}")
                else:
                    self.log.log_critical(
                        f"Failed Response Received from APPSMSC : MSISDN - {msisdn}, MSG - {message}, RESPONSE - {response}")
            except requests.exceptions.RequestException as e:
                self.log.log_critical(
                    f"Unable to Connect to APPSMSC API : MSISDN - {msisdn}, MESSAGE - {message}, EXCEPTION - {e}")

    def call_drop_smsapi_robi(self, msisdn, msg):
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            sms_url = self.call_drop_sms_url
            msg = urllib.parse.quote_plus(msg, encoding='UTF-8')
            final_url = f"{sms_url}?user=CallDropMin&password=C@11Dr0pM!n&src=Robi%20Rebate&dst={msisdn}&msg={msg}&dr=1&type=u"
            resource = urllib.request.urlopen(final_url)
            response = resource.read()
            response = str(response)
            response = response.strip("b'")
            print(f'Response - {response}')
            self.log.log_info(f'Response - {response}')
            time.sleep(AppUtils.conf['sms_send_delay'])
            if response.split(":")[0] == "Operation success":
                print(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
                self.log.log_info(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
            else:
                print(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
                self.log.log_critical(f"Failed Response received from SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg} RESPONSE - {response}")
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to SMS KENEL GATE WAY Details : For MSISDN - {msisdn} MESSAGE - {msg} EXCEPTION - {e}")

    def call_drop_smsapi_airtel(self, msisdn, msg):
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            sms_url = self.call_drop_sms_url
            msg = urllib.parse.quote_plus(msg, encoding='UTF-8')
            final_url = f"{sms_url}?user=CallDropMin&password=C@11Dr0pM!n&src=AT%20Rebate&dst={msisdn}&msg={msg}&dr=1&type=u"
            print(final_url)
            resource = urllib.request.urlopen(final_url)
            response = resource.read()
            response = str(response)
            response = response.strip("b'")
            print(f'Response - {response}')
            self.log.log_info(f'Response - {response}')
            time.sleep(AppUtils.conf['sms_send_delay'])
            if response.split(":")[0] == "Operation success":
                print(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
                self.log.log_info(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
            else:
                print(f"Response received from call drop SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg}")
                self.log.log_critical(f"Failed Response received from SMS KENEL Detail : MSISDN - {msisdn} MSG - {msg} RESPONSE - {response}")
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to SMS KENEL GATE WAY Details : For MSISDN - {msisdn} MESSAGE - {msg} EXCEPTION - {e}")
