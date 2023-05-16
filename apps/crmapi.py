import datetime
import sys
import uuid
import requests
from utils.custom_exception import IllegalArgumentError
from utils.logger import Logger


class CRMApi:
    token = None
    token_genarate_url = None
    postpaid_recharge_url = None
    prepaid_recharge_url = None
    user_name = None
    password = None
    authorization_token = None
    adjust_balance_url = None

    def __init__(self):
        self.token = self.generate_access_token()
        self.log = Logger.get_instance()
        self.user_name = "IT_RPA_AppS"
        self.password = "RoboTIC%24HerO)24"
        self.authorization_token = "UHpTdjBGc1RkRUQ3VXB4dmNMX3VWeXh4SG9BYTpqaW1xN0RTTkxzdkZpQlJaN0Q0Vjd1Z0xGQ0Fh"
        self.token_genarate_url = 'https://apigate.robi.com.bd/token'
        self.postpaid_recharge_url = 'https://apigate.robi.com.bd/cbsPerformPayment/v1/performPayment'
        self.prepaid_recharge_url = 'https://apigate.robi.com.bd/cbsprepaidRecharge/v1/prepaidRecharge'
        self.adjust_balance_url = 'https://apigate.robi.com.bd/ocsadjustAccount/v1/adjustaccount'

    def stop_execution(self):
        print('Stopping Execution')
        self.log_info(msg="Execution Stopped")
        sys.exit('Stopping Execution')

    def access_token(self):
        url = self.token_genarate_url
        payload = "grant_type=password&username="+self.user_name+"&password="+self.password+"&scope=PRODUCTION"
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Basic "+self.authorization_token,
            'cache-control': "no-cache",
        }
        try:
            response = requests.post(url=url, data=payload, headers=headers, timeout=3)
            self.log.log_info(f"Response received from MIFE")
            print(response.text)
            print(response.headers['Content-Type'])
            if response.headers['Content-Type'] == 'application/json; charset=UTF-8':
                return response.json()['access_token']
            else:
                return self.access_token()

        except requests.exceptions.RequestException as e:
            self.log.log_critical(f"Unable to connect to apigate.robi.com.bd , token not found DETAILS : EXCEPTION - {e}")

    # def msisdn_to_13_digit(self, msisdn):
    #     msisdn = str(msisdn)
    #     if len(msisdn) == 13 and msisdn.startswith('880'):
    #         return msisdn
    #     elif len(msisdn) == 10 and msisdn.startswith('1'):
    #         return '880' + msisdn
    #     elif len(msisdn) == 11 and msisdn.startswith('01'):
    #         return '88' + msisdn
    #     else:
    #         self.log.log_warn(f"RPA Something wrong with the msisdn Details: MSISDN - {msisdn} TYPE - {type(msisdn)}")
    #         raise IllegalArgumentError
    #
    # def msisdn_to_10_digit(self, msisdn=''):
    #     msisdn = str(msisdn)
    #     if len(msisdn) == 13 and msisdn.startswith('880'):
    #         return msisdn[3:]
    #     elif len(msisdn) == 10 and msisdn.startswith('1'):
    #         return msisdn
    #     elif len(msisdn) == 11 and msisdn.startswith('01'):
    #         return msisdn[1:]
    #     else:
    #         self.log.log_warn(f"RPA Something wrong with the msisdn Details: MSISDN - {msisdn} TYPE - {type(msisdn)}")
    #         raise IllegalArgumentError
    #
    # def generate_access_token(self):
    #     url = self.token_genarate_url
    #     payload = "grant_type=password&username=" + self.user_name + "&password=" + self.password + "&scope=PRODUCTION"
    #     headers = {
    #         'Content-Type': "application/x-www-form-urlencoded",
    #         'Authorization': "Basic " + self.authorization_token,
    #         'cache-control': "no-cache",
    #     }
    #     try:
    #         response = requests.post(url=url, data=payload, headers=headers, timeout=3)
    #         self.log.log_info(
    #             f"Response received from MIFE")
    #         print(response.text)
    #         print(response.headers['Content-Type'])
    #         if response.headers['Content-Type'] == 'application/json; charset=UTF-8':
    #             return response.json()['access_token']
    #         else:
    #             return self.generate_access_token()
    #
    #     except requests.exceptions.RequestException as e:
    #         self.log.log_critical(
    #             f"Unable to connect to apigate.robi.com.bd , token not found DETAILS : EXCEPTION - {e}")
    #
    # def validate_balance_code(self, amount=0, balance_code=0):
    #     balance_code_array = [3001, 3000, 3105, 2515, 3104, 3102, 3101, 3103, 2513, 2514, 2505, 2503, 2502, 2240,
    #                           130959, 2500, 2504, 2501, 2000, 3501, 2550]
    #     if balance_code in balance_code_array:
    #         return_amount = 10000 * amount
    #         return return_amount
    #     else:
    #         return amount
    #
    # def recharge_balance(self, msisdn=None):
    #     id = "RPA_" + str(uuid.uuid4()) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
    #     msisdn = self.msisdn_to_10_digit(msisdn)
    #     url = self.adjust_balance_url
    #     payload = f"CommandId=AdjustAccount&RequestType=Event&RequestType=Event&AccountType=2000&SubscriberNo={msisdn}&CurrAcctChgAmt={amount}&OperateType=2&LogID={id}&TransactionId={id}&SequenceId={id}&Version=1&SerialNo={id}&AdditionalInfo=RPA"
    #     headers = {
    #         'Content-Type': "application/x-www-form-urlencoded",
    #         'cache-control': "no-cache",
    #         'Authorization': f'Bearer {self.token}'
    #     }
    #
    #     try:
    #         response = requests.post(url=url, data=payload, headers=headers, timeout=3)
    #         self.log.log_info(f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")
    #
    #         if response.status_code == 401:
    #             self.token = self.generate_access_token()
    #             headers['Authorization'] = f'Bearer {self.token}'
    #             response = requests.post(url=url, data=payload, headers=headers, timeout=3)
    #             self.log.log_info(
    #                 f"api name: refund_balance, msisdn: {msisdn}, calling id : {id}, response: {response.text}")
    #
    #         if response.status_code == 200 and response.json()['ResultHeader']['ResultDesc'] == "Operation successful.":
    #             return response
    #         else:
    #             return None
    #     except requests.exceptions.RequestException as e:
    #         self.log.log_critical(
    #             f"Unable to connect to apigate.robi.com.bd, CBS rebate not possible Details : For  MSISDN - {msisdn} AMOUNT - {amount} EXCEPTION - {e}")
    #         return None


# data = Mief()
# amount = data.validate_balance_code(amount=20, balance_code=3000)
# print("Amount =", amount)
