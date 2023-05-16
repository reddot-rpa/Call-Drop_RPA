import datetime as dt
import re
import uuid
from datetime import datetime, timedelta
from xml.etree import ElementTree

import cx_Oracle
import requests
from dateutil.relativedelta import relativedelta

from utils.custom_exception import IllegalArgumentError
from utils.logger import Logger

log = Logger.get_instance()


class Formatter:

    @staticmethod
    def msisdn_to_13_digit(msisdn):
        msisdn = str(msisdn)
        if len(msisdn) == 13 and msisdn.startswith('880'):
            return msisdn
        elif len(msisdn) == 10 and msisdn.startswith('1'):
            return '880' + msisdn
        elif len(msisdn) == 11 and msisdn.startswith('01'):
            return '88' + msisdn
        else:
            log.log_warn(f"RPA Something wrong with the msisdn Details: MSISDN - {msisdn} TYPE - {type(msisdn)}")
            raise IllegalArgumentError

    @staticmethod
    def msisdn_to_10_digit(msisdn):
        msisdn = str(msisdn)
        if len(msisdn) == 13 and msisdn.startswith('880'):
            return msisdn[3:]
        elif len(msisdn) == 10 and msisdn.startswith('1'):
            return msisdn
        elif len(msisdn) == 11 and msisdn.startswith('01'):
            return msisdn[1:]
        else:
            log.log_warn(f"RPA Something wrong with the msisdn Details: MSISDN - {msisdn} TYPE - {type(msisdn)}")
            raise IllegalArgumentError

    @staticmethod
    def smart_script_get_value(key_and_value):
        value = key_and_value.split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
            ' ', '')
        return value

    @staticmethod
    def smart_script_fomrat_amount(unprocessed_amount):
        """
        regex Find All returns a list which then gets
        :param unprocessed_amount: str
        :return: str
        """
        processed_amount = re.findall(r'\d+.\d+|\d+', unprocessed_amount)[0]
        return processed_amount


class API:
    log = Logger.get_instance()
    token = None

    def __init__(self):
        token = self.accesstoken()

    def start_time(self):
        date = (str)((datetime.now() - timedelta(days=7)))
        date = date.split(".")
        x = date[0]
        x = x.replace(" ", "")
        x = x.replace("-", "")
        x = x.replace(":", "")
        return x

    def smsapi(self, msisdn, msg):
        msisdn = Formatter.msisdn_to_13_digit(msisdn)
        url = "http://10.101.11.164:8888/cgi-bin/sendsms"
        querystring = {"user": "tester", "pass": "foobar", "to": msisdn, "text": msg, "from": "8123"}
        try:
            response = requests.get(url=url, params=querystring, timeout=3)
            if response.status_code == 200:
                self.log.log_info(
                    f"Response received from SMS KENEL Detail : MSISND - {msisdn} MSG - {msg}")
            else:
                self.log.log_critical(
                    f"Failed Response received from SMS KENEL Detail : MSISND - {msisdn} MSG - {msg} RESPONSE - {response.text}")
            log.log_info(f"SMS API Response for msisdn = {msisdn}, messege = {msg}, RESPONSE - {response.text}")
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to SMS KENEL GATE WAY Details : For MSISDN - {msisdn} MESSAGE - {msg} EXCEPTION - {e}")

    def accesstoken(self):
        url = "https://apigate.robi.com.bd/token"
        payload = "grant_type=password&username=IT_RPA_AppS&password=RoboTIC%24HerO)24&scope=PRODUCTION"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Basic UHpTdjBGc1RkRUQ3VXB4dmNMX3VWeXh4SG9BYTpqaW1xN0RTTkxzdkZpQlJaN0Q0Vjd1Z0xGQ0Fh",
            'cache-control': "no-cache",
        }
        try:
            response = requests.post(url=url, data=payload, headers=headers, timeout=3)
            self.log.log_info(
                f"Response received from MIFE")
            print(response.text)
            print(response.headers['Content-Type'])
            if response.headers['Content-Type'] == 'application/json; charset=UTF-8':
                return response.json()['access_token']
            else:
                return self.accesstoken()

        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd , token not found DETAILS : EXCEPTION - {e}")

    def refund_balance(self, msisdn, amount):

        id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")

        msisdn = Formatter.msisdn_to_10_digit(msisdn)
        amount = Formatter.smart_script_fomrat_amount(amount)
        if type(amount) is str or type(amount) is int or type(amount) is float:
            amount = float(amount)
        else:
            self.log.log_critical(
                f"amount value is not required Type This might problem Details : MSISDN - {msisdn} AMOUNT - {amount} AMOUNT_TYPE - {type(amount)}")
            raise TypeError("Must be String Type Only Containing Numbers")

        # amount = amount * 10000
        url = "https://apigate.robi.com.bd/ocsadjustAccount/v1/adjustaccount"

        payload = f"CommandId=AdjustAccount&RequestType=Event&RequestType=Event&AccountType=2000&SubscriberNo={msisdn}&CurrAcctChgAmt={amount}&OperateType=2&LogID={id}&TransactionId={id}&SequenceId={id}&Version=1&SerialNo={id}&AdditionalInfo=RPA"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
            'Authorization': f'Bearer {self.token}'
        }
        try:
            response = requests.post(url=url, data=payload, headers=headers, timeout=3)
            log.log_info(f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")

            if response.status_code == 401:
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.post(url=url, data=payload, headers=headers, timeout=3)
                log.log_info(
                    f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")

            if response.status_code == 200 and response.json()['ResultHeader']['ResultDesc'] == "Operation successful.":
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd, CBS rebate not possible Details : For  MSISDN - {msisdn} AMOUNT - {amount} EXCEPTION - {e}")
            return None

        # return response

    def get_price(self, msisdn, trade_time):
        msisdn = str(msisdn)
        msisdn = Formatter.msisdn_to_10_digit(msisdn)
        # trade_time = str(trade_time)[:-2]
        i = 0
        price_flag = 0
        try:
            date = dt.datetime.strptime(trade_time, '%Y%m%d%H%M%S')
        except Exception as e:
            log.log_error(f"trade_time exception - {e}")
            return None
        print(date)
        start_param = date - timedelta(days=1)
        log.log_info(f"Start Time: {start_param} ")
        print(start_param)
        start_time = start_param.strftime("%Y%m%d%H%M%S")
        log.log_info(f"Start Time: {start_time} ")
        end_param = date + timedelta(days=1)
        log.log_info(f"End Time: {end_param} ")
        end_time = end_param.strftime("%Y%m%d%H%M%S")
        log.log_info(f"End Time: {end_time} ")
        print(end_param)

        id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")

        payload = f"Version=1&MessageSeq={id}&PrimaryIdentity={msisdn}&TotalRowNum=1000&BeginRowNum=0&FetchRowNum=100&StartTime={start_time}&EndTime={end_time}"

        url = "https://apigate.robi.com.bd/cbs/cbsQueryRechargeLog/v1/cbsQueryRechargeLog"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
            'Authorization': f"Bearer {self.token}"
        }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)

            # print(response.status_code)
            log.log_info(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code}, Pack Price Response Body: {response.text}")
            print(
                f"msisdn: {msisdn}, trade_time: {trade_time}, try_count: {i}, ID: {id} -> Status code: {response.status_code} Pack Price Response Body: {response.text}")

            if response.status_code == 401:
                i = i + 1
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)

            log.log_info(
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
            log.log_error(e)
            log.log_error_msg(
                f"get price list failed, can't connect to apigate.robi.com.bd. SequenceID = {id} Error: {e}")
            print(f"Get Price Exception - {e}")
            return None


    def adcs_terminate_plan(self, msisdn, plan_id):
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
            log.log_info(
                f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code}, RESPONSE - {response.text} ")

            if response.status_code == 401:
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("PUT", url, data=payload, headers=headers, timeout=3)
                log.log_info(
                    f"Pack terminate API : MSISND - {msisdn}, Plan_ID - {plan_id},Status_Code: {response.status_code}, RESPONSE - {response.text} ")

            if response.status_code == 200:
                return response
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to apigate.robi.com.bd, ADCS terminate not possible Details : For MSISDN - {msisdn} AMOUNT - {plan_id} EXCEPTION - {e}")
            return None

    def crm_complain_update(self, msisdn, sr_id, status):

        now = datetime.now()
        current_date_time = now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
        id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")

        reason = "Adjustment Given"
        root_cause = "Other"
        root_casue_detail = "Other"
        escalation = ""
        Solution = "Adjustment Given"
        if status == "Cancelled":
            reason = "Not Eligible for Adjustment"
            root_cause = "Invalid Complaint"
            root_casue_detail = "Invalid Complaint"
            escalation = "Unable to Adjust the requested amount"
            Solution = "Adjustment Given"
        # url = "http://10.101.26.34:26810/RobiAxiataSOA/RequesterABCS/ProxyServices/UpdateServiceRequest/2.0/UpdateServiceRequestReqABCSPS"
        # url = "http://prdlsoaan2.robi.com.bd:26810/RobiAxiataSOA/RequesterABCS/ProxyServices/UpdateServiceRequest/2.0/UpdateServiceRequestReqABCSPS"
        url = "http://lb_osb.robi.com.bd:7777/RobiAxiataSOA/RequesterABCS/ProxyServices/UpdateServiceRequest/2.0/UpdateServiceRequestReqABCSPS"
        payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:upd=\"http://www.robi.com.bd/soa/xsd/UpdateServiceRequest/V2\" xmlns:com=\"http://www.robi.com.bd/soa/xsd/Common\">\n   <soapenv:Header/>\n   <soapenv:Body>\n      <upd:UpdateServiceRequest>\n         <upd:CommonComponents>\n            <com:IMSINum/>\n            <com:MSISDNNum/>\n            <com:ProcessingNumber>{id}</com:ProcessingNumber> <!--Mandatory Input:unique identifier to track the transaction-->\n            <com:RequestDateTime>{current_date_time}</com:RequestDateTime> <!--Conditional Mandatory: Holds the timestamp when the request came in. If it is being passed, it has to be in the exact format as mentioned. Else even the tag should also not be present. Empty tag will not be supported.-->\n            <com:SenderID>REM</com:SenderID> <!-- Mnadatory Input. Valid Values \"Remedy\" -->\n\t    <!--Optional: Holds the Source IP or Host Name of the application giving the request or the Service Invoker-->\n            <com:SourceHostName>xxxx</com:SourceHostName>\n         </upd:CommonComponents>\n         <upd:RequestType>UpdateServiceRequest</upd:RequestType> <!--Mandatory Input, valid value \"UpdateServiceRequest\" -->\n         <upd:Identification>\n            <upd:ServiceID>{msisdn}</upd:ServiceID> <!-- Mandatory: Holds  Subscriber MSISDN, this is sample value-->\n         </upd:Identification>\n         <upd:ServiceRequestDetails>\n            <upd:ServiceRequestSpecification>\n               \t<upd:ServiceReqNum>{sr_id}</upd:ServiceReqNum> <!-- Mandatory: Holds CRM SRNumber,  this is a sample Value -->\n               \t<upd:IncidentStatus>{status}</upd:IncidentStatus> <!-- Mandatory : Holds Status value. Valid values are ..\"Pending\" , \"Open\" , \"In Progress\" , \"Completed\" , \"Closed\" , \"Cancelled\"-->\n\t\t<upd:Resolution>{reason}</upd:Resolution> <!-- Mandatory : Resolution for update service request -->\n\t\t<upd:StatusReason>Technical Problem</upd:StatusReason> <!-- Mandatory : Incident Status updated reason -->\n               \t<upd:RootCause>{root_cause}</upd:RootCause><!-- Optional: Root Cause -->\n               \t<upd:RootCauseDetails>{root_casue_detail}</upd:RootCauseDetails> <!-- Optional: Root Cause reason -->\n            </upd:ServiceRequestSpecification>\n         </upd:ServiceRequestDetails>\n      </upd:UpdateServiceRequest>\n   </soapenv:Body>\n</soapenv:Envelope>"
        headers = {
            'Content-Type': "application/xml"
        }

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            log.log_info(
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

    def customer_pack_details(self, msisdn):
        msisdn = Formatter.msisdn_to_13_digit(msisdn)
        url = "https://apigate.robi.com.bd/adcsQueryCurrentPlans/v1/queryCurrentPlans"
        querystring = {"msisdn": f"{msisdn}"}
        headers = {
            'Authorization': f"Bearer {self.token}",
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "apigate.robi.com.bd",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        try:
            response = requests.request("GET", url, headers=headers, params=querystring, timeout=3)

            if response.status_code == 401:
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=3)

            if response.status_code == 200:
                self.log.log_info(
                    f"All active Pack Information Found form ADCS Details : MSISDN - {msisdn}, RESPONSE - {response.text}")
                print(response.text, "Printing Response")
                return response
            else:
                return None

        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to ADCS, ADCS pack details not found Details : For MSISDN - {msisdn} Exception - {e}")
            return None

    def prepaid_postpaid_check(self, msisdn):
        msisdn = str(msisdn)
        msisdn = msisdn[-10:]
        print("Enter prepaid_postpaid_check API")
        url = "https://apigate.robi.com.bd/cbsQueryCustomerInfo/v1/cbsQueryCustomerInfo"
        payload = f"Version=1&MessageSeq=201901271801001&PrimaryIdentity={msisdn}&OperatorID=353&BEID=101&BusinessCode=1"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': f"Bearer {self.token}"
        }
        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
            log.log_info(f"Prepaid Postpaid Check Response {msisdn}: {response.text}")

            if response.status_code == 401:
                self.token = self.accesstoken()
                headers['Authorization'] = f'Bearer {self.token}'
                response = requests.request("POST", url, data=payload, headers=headers, timeout=3)
                log.log_info(f"Prepaid Postpaid Check Response {msisdn}: {response.text}")

            print(f"Prepaid Postpaid Check Response {msisdn}: {response.text}")

            if response.status_code == 200 and response.json()['ResultHeader'][
                'ResultDesc'] == "Operation successfully.":
                return response
            else:
                return None

        except requests.exceptions.RequestException as e:
            self.log.log_critical(
                f"Unable to connect to ADCS, ADCS pack details not found Details : For MSISDN - {msisdn} Exception - {e}")
            return None


class DB:

    def history_validation(self, msisdn):
        try:
            dsn_tns = cx_Oracle.makedsn('raxdw-scan', 1628, service_name='DBC3')
            conn = cx_Oracle.connect(user='rpa', password='bi4#VnPh', dsn=dsn_tns)
            cursor = conn.cursor()
            date = dt.date.today() - relativedelta(months=6)
            print(date)
            sql = f"SELECT * FROM RPA.CRM_UNWILLING_DATA_PACK_PURCHASE WHERE MSISDN = '{msisdn}' AND STATUS = 'Closed' AND COMPLAIN_TYPE = 'Data' AND COMPLAIN_DATE > DATE '{date}'"
            cursor.execute(sql)
            res = cursor.fetchall()
            print(res)
            if not res:
                cursor.close()
                return 1
            else:
                cursor.close()
                return 0
        except cx_Oracle.DatabaseError as e:
            log.log_critical("There is a problem with Oracle DETAILS : EXCEPTION - {e}")

    def data_record(self, dataframe, table):
        try:
            dsn_tns = cx_Oracle.makedsn('raxdw-scan', 1628, service_name='DBC3')

            # oracle_db = sa.create_engine('oracle://rpa:bi4#VnPh' + dsn_tns)
            # connection = oracle_db.connect()
            # dataframe.to_sql(table, connection, index=False, if_exists='append')
            # conn.commit()

        except cx_Oracle.DatabaseError as e:
            log.log_critical("There is a problem with Oracle DETAIL: EXCEPTION - {e}")

    def single_data_record(self, SR_NUMBER, MSISDN, PACK='None', USAGE='None', PACK_ID='None', BONUS_PACK='None',
                           BONUS_PACK_ID='None', DB_VALIDATION='None', PRICE='None', ERROR='None', LIST_TYPE='None',
                           COUNTER_LOG='None'):


        try:
            dsn_tns = cx_Oracle.makedsn('raxdw-scan', 1628, service_name='DBC3')
            conn = cx_Oracle.connect(user='rpa', password='bi4#VnPh', dsn=dsn_tns)
            cursor = conn.cursor()
            sql = f"""INSERT INTO crm_complain_list( SR_NUMBER, MSISDN, PACK, USAGE, PACK_ID, BONUS_PACK, BONUS_PACK_ID, DB_VALIDATION, PRICE, ERROR, LIST_TYPE, CREATE_TIME, COUNTER_LOG) 
            VALUES ('{SR_NUMBER}','{MSISDN}','{PACK}','{USAGE}','{PACK_ID}','{BONUS_PACK}','{BONUS_PACK_ID}','{DB_VALIDATION}','{PRICE}','{ERROR}','{LIST_TYPE}',SYSDATE,'{COUNTER_LOG}')"""
            cursor.execute(sql)
            conn.commit()
            print('Insert Done')

        except Exception as e:
            log.log_critical(f"There is a problem with Oracle DETAIL(single_data_record): EXCEPTION - {e}")
            log.log_error(e)
            print(e)

    def single_data_update(self, SR_NUMBER, MSISDN, ERROR='None'):
        try:
            dsn_tns = cx_Oracle.makedsn('raxdw-scan', 1628, service_name='DBC3')
            conn = cx_Oracle.connect(user='rpa', password='bi4#VnPh', dsn=dsn_tns)
            cursor = conn.cursor()
            sql = f"UPDATE crm_complain_list set ERROR = '{ERROR}' where MSISDN = '{MSISDN}' and SR_NUMBER = '{SR_NUMBER}'"
            print(sql)
            cursor.execute(sql)
            conn.commit()
            print('Update Done')

        except Exception as e:
            log.log_critical(f"There is a problem with Oracle DETAIL: EXCEPTION(single_data_update) - {e}")
            log.log_error(e)
            print(e)

    def check_sr_exsitance(self, MSISDN, SR_NUMBER):
        try:
            dsn_tns = cx_Oracle.makedsn('raxdw-scan', 1628, service_name='DBC3')
            conn = cx_Oracle.connect(user='rpa', password='bi4#VnPh', dsn=dsn_tns)
            cursor = conn.cursor()
            sql = f"SELECT count(MSISDN) FROM crm_complain_list where MSISDN = '{MSISDN}' and SR_NUMBER = '{SR_NUMBER}'"
            log.log_info(sql)
            cursor.execute(sql)
            list = cursor.fetchone()
            number_of_rows = list[0]
            row = int(number_of_rows)
            log.log_info(row)
            if row > 0:
                log.log_info("Found in processed list")

            return row

        except Exception as e:
            log.log_critical(f"There is a problem with Oracle DETAIL(check_sr_exsitance): EXCEPTION - {e}")
            log.log_error(e)
            print(e)
            return None
