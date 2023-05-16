import datetime as dt
import re

from apps.api_and_database2 import API, DB
from utils.custom_exception import ExpectedDataNotFoundException, NoneResponseException
from utils.logger import Logger

log = Logger.get_instance()
log.log_start()
api = API()
db = DB()


class APIValidation:

    def pre_porst_check(self, msisdn):
        str_msisdn = str(msisdn)
        msisdn_without_zero = str_msisdn[-10:]
        c = 0
        response = None
        prepaid = True
        while c < 3:
            response = api.prepaid_postpaid_check(msisdn_without_zero)
            if response is None:
                c = c + 1
            else:
                break

        if response is None:
            raise NoneResponseException
        else:
            try:
                prepost = response.json()['QueryCustomerInfoResult']['Subscriber']['SubscriberInfo']['Brand']
            except Exception as e:
                log.log_error(f"Exception: {e}")
                raise NoneResponseException

            if prepost == '301' or prepost == '302':
                raise ExpectedDataNotFoundException
            else:
                return True

    def terminate_pack(self, msisdn, plan_id):
        counter = 3
        adcs_terminate = False
        response = None
        result = None
        c = 0
        while c < 3:
            response = api.adcs_terminate_plan(msisdn, plan_id)
            if response is None:
                c = c + 1
            else:
                break

        if response is not None:
            if response.status_code == 200:
                result = response.json()['message']
                if result.upper() == 'OPERATION SUCCESSFUL' and response.json()['status'] == 0:
                    log.log_info(f"ADCS - Termination Success! DETAILS : MSISDN - {msisdn} PLAN_ID - {plan_id}")
                    adcs_terminate = True
                    return adcs_terminate

        # print(adcs_terminate)
        if not adcs_terminate:
            error = f"ADCS - Unable to Terminate, DETAILS : MSISDN - {msisdn}, PLAN_ID - {plan_id}, RESULT - {result}"
            log.log_critical(error)
            raise ExpectedDataNotFoundException

    def rebate(self, msisdn, pack_price):
        response = None
        result = None
        amount = pack_price
        rebate_flag = False
        if amount is None:
            log.log_warn(f"Unable_to_rebate_amount_is_None MSISDN: {msisdn}")
        numpy_msisdn = str(msisdn)
        cbs_msisdn = numpy_msisdn[3:]

        c = 0
        while c < 3:
            try:
                response = api.refund_balance(cbs_msisdn, amount=amount)
            except TypeError as e:
                error = f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount} EXCEPTION - {e}"
                log.log_error(e)
                log.log_critical(error)
                break

            if response is None:
                c = c + 1
            else:
                break

        if response is not None:
            if response.status_code == 200:
                result = response.json()['ResultHeader']['ResultDesc']
                print(response.text)
                print(result)
                if result == 'Operation successful.':
                    log.log_info(f"CBS - Rebate Success, DETAILS : MSISDN {msisdn} AMOUNT - {amount}")
                    rebate_flag = True
                    return rebate_flag

        if not rebate_flag:
            error = f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount} RESULT - {result}"
            log.log_critical(error)
            raise ExpectedDataNotFoundException

    def smsapi(self, msisdn, msg):
        log.log_info(f"sms api called: {msg}")
        msg = str(msg)
        api.smsapi(msisdn=msisdn, msg=msg)

    def find_pack_id(self, msisdn, pack, bonus_pack):
        c = 0
        response = None
        while c < 3:
            response = api.customer_pack_details(msisdn=msisdn)
            if response is None:
                c = c + 1
            else:
                break

        if response is None:
            raise ExpectedDataNotFoundException

        pack_details = {"pack_id": None, "plan_usage": None, "pack_found": False, "bonus_pack_id": None,
                        "bonus_pack_found": False}
        pack_found = False
        if response.status_code == 200:
            plan_list = response.json()['plan']
            log.log_debug(plan_list)
            print(len(plan_list))
            pack_found = False
            for plans in plan_list:
                plan_name = plans['planDefinition']['name']
                plan_name = plan_name.replace(' ', "").replace('\s', "")
                print(plan_name)
                log.log_debug(f"Comparing pack from list of pack from API {plan_name} to {pack} from SMART SCRIPT")
                if plan_name == pack:
                    print("Pack Found")
                    pack_found = True
                    pack_id = plans['id']
                    plan_usage = plans['usage']
                    pack_details.update(pack_id=pack_id)
                    pack_details.update(plan_usage=plan_usage)
                    pack_details.update(pack_found=pack_found)
                    if plan_usage == 0:
                        bonus_pack_found = False
                        bonus_pack_id = None
                        if bonus_pack is not None:
                            for bonus_plan in plan_list:
                                print(bonus_plan)
                                log.log_debug(
                                    f"Comparing Bonus pack from list of pack from API {bonus_plan['planDefinition']['name']} to {bonus_pack} from SMART SCRIPT")
                                api_bonus_plan = bonus_plan['planDefinition']['name']
                                api_bonus_plan = api_bonus_plan.replace(' ', "").replace('\s', "")
                                if api_bonus_plan == bonus_pack:
                                    bonus_pack_found = True
                                    bonus_pack_id = bonus_plan['id']
                                    pack_details.update(bonus_pack_id=bonus_pack_id)
                                    pack_details.update(bonus_pack_found=bonus_pack_found)
                                    break
                            break
                        else:
                            pack_details.update(error="bonus pack is None")
                            break
                    else:
                        pack_details.update(error="pack_has_been_used")
                        return pack_details
        else:
            pack_details.update(error="pack_not_found")

        if not pack_found:
            raise ExpectedDataNotFoundException

        print(pack_details)
        log.log_info(f"{pack_details}")
        return pack_details

    def crm_complain_update_counter(self, msisdn, sr_id, current_status, target_status, error):

        i = 0
        while i < 3:
            i = i + 1
            response = api.crm_complain_update(msisdn, sr_id, target_status)
            print(msisdn, sr_id, target_status)
            if response == "Success":
                log.log_info(response)
                return response
            else:
                if i == 3:
                    if response == "fail":
                        error_ = f"{error}, Complain's status can not be changed to {target_status}, Current status: {current_status}"
                        db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
                    return response

    def get_price(self, msisdn, trade_time):
        c = 0
        response = None
        prepaid = True
        while c < 3:
            response = api.get_price(msisdn, trade_time)
            if response is None:
                c = c + 1
            else:
                break

        if response is None:
            raise ExpectedDataNotFoundException

        pack_price = DataValidation.get_price_amount(response=response, trade_time=trade_time)
        log.log_info(f"{pack_price}")

        if pack_price == "pack_price_not_found" or pack_price is None or pack_price == "not_easyload":
            raise ExpectedDataNotFoundException

        return pack_price


class DataValidation:

    def get_price_amount(response, trade_time):
        date = dt.datetime.strptime(trade_time, '%Y%m%d%H%M%S')
        if response.status_code == 200 and response.json()['ResultHeader'][
            'ResultDesc'] == "Operation successfully.":
            if 'RechargeInfo' in response.json()['QueryRechargeLogResult']:
                data_list = response.json()['QueryRechargeLogResult']['RechargeInfo']
            else:
                # log.log_info(f"Rechare history not found. MSISDN - {msisdn}, TRADETIME - {trade_time}")
                return "pack_price_not_found"

            if isinstance(data_list, list):
                for row in data_list:
                    print(row, type(row), 188)
                    # print(row['TradeTime'],type(row['TradeTime']),189)
                    trade_time2 = str(row['TradeTime'])
                    date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                    diff = int(abs((date - date2).total_seconds()))
                    print(f"time diff: {diff}")
                    log.log_info(f"time diff: {diff}")

                    if diff <= 5:
                        # print(row['AdditionalProperty'])
                        for value in row['AdditionalProperty']:
                            print(value)
                            log.log_info(f"{value}")
                            # print(value['Value'])
                            if value['Code'] == "OperatorID":
                                if value['Value'] == "easyload":
                                    # print(value['Code'], value['Value'])
                                    pack_price = row['RechargeAmount']
                                    pack_price = str(pack_price)
                                    log.log_info(f"pack price: {pack_price}")
                                    return pack_price
                                else:
                                    return "not_easyload"

            else:
                # trade_time_new = str(data_list['TradeTime'])
                trade_time2 = str(data_list['TradeTime'])
                date2 = dt.datetime.strptime(trade_time2, '%Y%m%d%H%M%S')
                diff = int(abs((date - date2).total_seconds()))
                print(f"time diff: {diff}, msisdn:")
                # log.log_info(f"time diff: {diff}, msisdn: {msisdn}")

                if diff <= 5:
                    print(data_list['AdditionalProperty'])
                    for value in data_list['AdditionalProperty']:
                        print(value)
                        log.log_info(f"{value['Value']}")
                        if value['Code'] == "OperatorID":
                            if value['Value'] == "easyload":
                                # print(value['Code'], value['Value'])
                                pack_price = data_list['RechargeAmount']
                                pack_price = str(pack_price)
                                log.log_info(f"pack price: {pack_price}")
                                return pack_price
                            else:
                                return "not_easyload"

        else:
            return "pack_price_not_found"

        return "pack_price_not_found"

    def smart_scripts_info(self):
        log.log_info("Enter smart_script_file_read")
        file = open("C:/Users/crm_rpa/Downloads/output (1).txt", 'r', encoding='utf-16')
        # file = open("C:/Users/rashid.rayhan/Downloads/output (1).txt", 'r', encoding='utf-16')
        lines = file.readlines()
        pack = None
        bonus_pack = None
        price = None
        trade_time = None
        log.log_info(lines)
        for line in lines:
            print(line)
            log.log_info(line)
            if ('Data' in line.split(',')[0]):
                pack = line.split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                       '').replace(' ',
                                                                                                                   '')

            elif ('Bonus' in line.split(',')[0]):
                bonus_pack = line.split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                             '').replace(
                    ' ',
                    '')
            elif ('Price' in line.split(',')[0]):
                price = line.split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                        '').replace(' ',
                                                                                                                    '')

            elif ('Date & Time' in line.split(',')[0]):
                trade_time = line.split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                             '').replace(
                    ' ',
                    '')
                trade_time = trade_time.split(".")
                trade_time = re.sub("\D", "", trade_time[0])
        file.close()

        # if price is not None:
        #     price = price + '0000'

        smart_script_info = {'pack': pack, 'bonus_pack': bonus_pack, 'price': price, 'trade_time': trade_time}
        log.log_info(f"smart_script_info - {smart_script_info}")
        print(f"smart_script_info - {smart_script_info}")
        return smart_script_info

    def history_validation(self, msisdn, sr_id):

        db_check = db.history_validation(msisdn=msisdn)

        if (db_check == 1):
            return db_check
        elif (db_check == 0):
            error = "The Complain by the MSISDN Existed Previously with in 6 months"
            log.log_info(
                f"Unable to Process for the following complain - {sr_id}  MSISDN - {msisdn} DB_CHECK - {db_check}")
            # counter_log = f"{counter} out of {len(complain_list.index)}"
            raise ExpectedDataNotFoundException
        else:
            raise ExpectedDataNotFoundException
