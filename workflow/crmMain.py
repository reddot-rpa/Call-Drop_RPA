# Region: Imports
import time
from sys import exit

import pandas as pd
from selenium.common.exceptions import WebDriverException, NoSuchElementException

from apps.api_and_database import API, DB
from apps.crm import RpaCRM
from utils.logger import Logger

# End Region

# Region: Initialization Static
log = Logger.get_instance()
log.log_start()
api = API()
db = DB()
username = 'rpa'
password = 'Windows@10'
# link = 'https://10.101.26.30/ecommunications_enu/start.swe?SWECmd=Login&SWECM=S&SRN=&SWEHo=10.101.26.30'
brows = 'chrome'
link = 'https://c1.robi.com.bd/ecommunications_enu/start.swe?SWECmd=GotoView&SWEView=CUT+Home+Page+View+(CME)&SWERF=1&SWEHo=c1.robi.com.bd&SWEBU=1'
# link = 'https://c1.robi.com.bd/ecommunications_enu/start.swe?SWECmd=GotoView&SWEView=Service+Request+Screen+Homepage+View&SWERF=1&SWEHo=c1.robi.com.bd&SWEBU=1'

# Region: Data Frame

df_rebate_fail = pd.DataFrame(
    columns=['SR_NUMBER', 'MSISDN', 'PACK', 'PACK_ID', 'BONUS_PACK', 'BONUS_PACK_ID', 'PRICE', 'USAGE', 'DB_VALIDATION',
             'ERROR'])

# End Region
# End Region


def rebate_and_terminate_pack(msisdn, amount, prod_id):
    counter = 3
    adcs_terminate = False
    response = None
    result = None

    while True:
        # terminate pack
        response = api.adcs_terminate_plan(msisdn, prod_id)
        if response is not None:
            print(response.text)
            print(response.status_code)
            if response.status_code == 200:
                result = response.json()['message']
                if result.upper() == 'OPERATION SUCCESSFUL' and response.json()['status'] == 0:
                    log.log_info(f"ADCS - Termination Success! DETAILS : MSISDN - {msisdn} PROD_ID - {prod_id}")
                    adcs_terminate = True
                    break
                elif counter == 0:
                    log.log_warn(f"ADCS - Unable to Terminate, DETAILS : MSISDN {msisdn} PROD_ID - {prod_id}")
                    return "Unable_to_terminate_pack"
                else:
                    counter = counter - 1
            elif counter == 0:
                log.log_warn(f"ADCS - Unable to Terminate, DETAILS : MSISDN {msisdn} PROD_ID - {prod_id}")
                return "Unable_to_terminate_pack"
            else:
                counter = counter - 1
        elif counter == 0:
            log.log_warn(f"ADCS - Unable to Terminate, DETAILS : MSISDN {msisdn} PROD_ID - {prod_id}")
            return "Unable_to_terminate_pack"
        else:
            counter = counter - 1
    # print(adcs_terminate)
    if not adcs_terminate:
        log.log_warn(f"Unable_to_terminate_pack MSISDN: {msisdn}")
        return "Unable_to_terminate_pack"

    if amount is None:
        log.log_warn(f"Unable_to_rebate_amount_is_None MSISDN: {msisdn}")
        # return "Unable_to_rebate"

    # Rebate pack price
    if adcs_terminate and amount is not None:
        print(type(msisdn), msisdn)
        numpy_msisdn = str(msisdn)
        cbs_msisdn = numpy_msisdn[3:]
        counter = 3
        while True:
            try:
                response = api.refund_balance(cbs_msisdn, amount=amount)
            except TypeError as e:
                error = f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount} EXCEPTION - {e}"
                log.log_error(e)
                log.log_critical(error)
            if response is not None:
                if response.status_code == 200:
                    result = response.json()['ResultHeader']['ResultDesc']
                    print(response.text)
                    print(result)
                    if result == 'Operation successful.':
                        log.log_info(f"CBS - Rebate Success, DETAILS : MSISDN {msisdn} AMOUNT - {amount}")
                        return "Operation successful"
                    elif counter == 0:
                        error = f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount} RESULT - {result}"
                        log.log_critical(error)
                        global df_rebate_fail
                        df_rebate_fail = df_rebate_fail.append(
                            other={'SR_NUMBER': sr_id, 'MSISDN': msisdn,
                                   'PACK': pack, 'USAGE': None,
                                   'PACK_ID': None, 'BONUS': bonus_pack,
                                   'BONUS_PACK_ID': None, 'DB_VALIDATION': None,
                                   'PRICE': amount, 'ERROR': error},
                            ignore_index=True)
                        return "Unable_to_rebate"
                    else:
                        counter = counter - 1
                elif counter == 0:
                    log.log_warn(f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount}")
                    return "Unable_to_rebate"
                else:
                    counter = counter - 1
            elif counter == 0:
                log.log_warn(f"CBS - Unable to Rebate, DETAILS : MSISDN {msisdn} AMOUNT - {amount}")
                return "Unable_to_rebate"
            else:
                counter = counter - 1



# End Region
# RPA
crm = RpaCRM(brows, link)
crm.login(username, password)
crm.filter()
crm.csv_download()
complain_list_file = crm.check_file('output.txt')
# api.smsapi('8801833182430','fount output file')

if not complain_list_file:
    log.log_warn("No new Complain Found File Closing RPA")
    exit(1)
# complain_list = pd.read_csv("C:/Users/rashid.rayhan/Downloads/output.txt", encoding='utf-16')
complain_list = pd.read_csv("C:/Users/crm_rpa/Downloads/output.txt", encoding='utf-16')
complain_list = complain_list.sort_values(by='Log Date')

log.log_debug(complain_list)
# crm.pages.end()
# crm = None
counter = 0
for row in range(len(complain_list.index)):
    counter = counter + 1

    log.log_info(f"\n\n{counter} out of {len(complain_list.index)}")
    print(f"\n\n{counter} out of {len(complain_list.index)}")

    msisdn = complain_list.iat[row, 2]
    msisdn = str(msisdn)
    sr_id = complain_list.iat[row, 1]
    sr_id = str(sr_id)
    row = db.check_sr_exsitance(MSISDN=msisdn, SR_NUMBER=sr_id)
    if row is None or row > 0:
        if row is None:
            log.log_warn(f"Unable to find databse response for SR_ID : {sr_id}")
            print(f"Unable to find databse response for SR_ID : {sr_id}")
        if row > 0:
            log.log_warn(f"This complain handled before SR_ID : {sr_id}")
            print(f"This complain handled before SR_ID : {sr_id}")
        continue




    str_msisdn = str(msisdn)
    msisdn_without_zero = str_msisdn[3:]
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
        error = "Unable to Find number catagory for Prepaid/Postpaid Therefore not processing"
        log.log_critical(error)
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
        continue
    else:
        print(msisdn)
        print(response.text)
        prepost = response.json()['QueryCustomerInfoResult']['Subscriber']['SubscriberInfo']['Brand']
        if prepost == '301' or prepost == '302':
            error = f"Unable to Process as provided phone number is Postpaid : {msisdn} response - {prepost}"
            log.log_warn(error)
            counter_log = f"{counter} out of {len(complain_list.index)}"
            db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                                  ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
            continue

    # print(type(msisdn), msisdn)
    try:
        crm_smart_script = RpaCRM(brows, link)
        crm_smart_script.login(username, password)
    except NoSuchElementException as e:
        log.log_warn(f"Login Exception - {e}")
        continue
    except Exception as e:
        log.log_warn(f"Login Exception - {e}")
        continue
    try:
        crm_smart_script.data_pack_details(sr_id)
    except crm_smart_script.pages.NoSuchElementException as e:
        log.log_error(e)
        log.log_warn(f"Probably No Smart Script Found here Details : MSISDN - {msisdn} SR_ID - {sr_id}")
        msg = "Dear Customer, we are unable to adjust the requested amount."
        api.smsapi(msisdn=msisdn, msg=msg)
        error = "No Smart Script Found"
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")

        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
        if complain_status_response == "fail":
            error_ = f"{error}, Complain can not Cancel, Current status: Open"
            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        continue
    except WebDriverException as e:
        log.log_error(e)
        print(e)
        continue
    data_info_file = crm_smart_script.check_file('output (1).txt')
    if not data_info_file:
        log.log_warn(f"No Smart Script Found Details : SR_ID - {sr_id} MSISDN - {msisdn}")
        continue
    # file = open("C:/Users/rashid.rayhan/Downloads/output (1).txt", 'r', encoding='utf-16')
    file = open("C:/Users/crm_rpa/Downloads/output (1).txt", 'r', encoding='utf-16')

    lines = file.readlines()
    pack = lines[1].split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t', '').replace(' ',
                                                                                                                 '')
    bonus_pack = lines[2].split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t', '').replace(
        ' ', '')
    try:
        trade_time = lines[4].split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
            ' ', '')

    except IndexError as e:
        trade_time = lines[3].split(',')[1].replace('"', '').replace('\s', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
            ' ', '')
        pass
    file.close()

    trade_time = trade_time.split(".")
    trade_time = trade_time[0].replace("-", "").replace("T", "").replace(":", "").replace('"', "").replace("/", "")
    log.log_info(f"msisdn: {msisdn}, sr: {sr_id}, Trade time: {trade_time}")
    print(pack, bonus_pack, trade_time)
    # trade_time = re.findall(r'\d+.\d+|\d+', trade_time)[0]
    print(trade_time, type(trade_time))
    trade_time = str(trade_time)

    # pack_price = api.get_price(msisdn_without_zero, trade_time)

    try:
        pack_price = api.get_price(msisdn_without_zero, trade_time)
    except Exception as e:
        # log.log_critical(f"GET PRICE EXCEPTION - {e}, Trade Time - {trade_time}")
        error = f"GET PRICE EXCEPTION - {e}, Trade Time - {trade_time}"
        log.log_critical(error)
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
        if complain_status_response == "fail":
            error_ = f"{error}, Complain can not Cancel, Current status: Open"
            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        continue

    print(pack_price, type(pack_price))
    log.log_info(f"pack price: {pack_price}, sr_id: {sr_id}, msisdn: {msisdn}")

    if pack_price == "api_failure":
        error = f"cbsQueryRechargeLog API failure, Trade Time - {trade_time}"
        log.log_critical(error)
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
        if complain_status_response == "fail":
            error_ = f"{error}, Complain can not Cancel, Current status: Open"
            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        continue

    if pack_price == "pack_price_not_found" or pack_price is None:
        error = f"Unable to Find Pack Price, Trade Time - {trade_time}"
        log.log_critical(error)
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
        msg = "Dear Customer, we are unable to adjust the requested amount."
        api.smsapi(msisdn, msg=msg)
        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
        if complain_status_response == "fail":
            error_ = f"{error}, Complain can not Cancel, Current status: Open"
            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        continue
    elif pack_price == "not_easyload":
        error = "Not Recharged from Easyload or MFS, Trade Time - {trade_time}"
        log.log_info(
            f"Unable to Process for the following complain - {sr_id}  MSISDN - {msisdn} DETAILS - Not From Easyload")
        counter_log = f"{counter} out of {len(complain_list.index)}"
        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
        msg = "Dear Customer, we are unable to adjust the requested amount."
        api.smsapi(msisdn, msg=msg)
        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
        if complain_status_response == "fail":
            error_ = f"{error}, Complain can not Cancel, Current status: Open"
            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        continue

    # crm_smart_script.pages.end()
    if not bonus_pack or bonus_pack.upper() == 'NA' or bonus_pack.upper() == 'N/A' or bonus_pack.upper() == 'N\A' or bonus_pack.upper() == 'NONE' or bonus_pack.upper() == 'NO':
        bonus_pack = None
    log.log_info(f"Bonsu Pack Details : {bonus_pack}")
    response = api.customer_pack_details(msisdn)
    # print(response.status_code)
    if response == None:
        continue
    if response.status_code == 200:
        # print(200,'works')
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
                pack_found = True
                pack_id = plans['id']
                plan_usage = plans['usage']
                if plan_usage == 0:
                    db_check = db.history_validation(msisdn)
                    rebate_and_terminate_flag = "ok"
                    if db_check == 1:
                        bonus_pack_id = None
                        if bonus_pack is not None:
                            bonus_pack_found = False
                            for bonus_plan in plan_list:
                                print(bonus_plan)
                                log.log_debug(
                                    f"Comparing Bonus pack from list of pack from API {bonus_plan['planDefinition']['name']} to {bonus_pack} from SMART SCRIPT")
                                api_bonus_plan = bonus_plan['planDefinition']['name']
                                api_bonus_plan = api_bonus_plan.replace(' ', "").replace('\s', "")
                                if api_bonus_plan == bonus_pack:
                                    bonus_pack_found = True
                                    bonus_pack_id = bonus_plan['id']
                            log.log_info(f"Bonus Pack Found Details : PACK - {bonus_pack} PACK_ID - {bonus_pack_id}")
                            rebate_and_terminate_flag = rebate_and_terminate_pack(msisdn=msisdn, prod_id=pack_id,
                                                                                  amount=pack_price)
                            rebate_and_terminate_pack(msisdn=msisdn, prod_id=bonus_pack_id, amount=None)
                        else:
                            log.log_info(f"Bonus Pack Not Found")
                            rebate_and_terminate_flag = rebate_and_terminate_pack(msisdn=msisdn, prod_id=pack_id,
                                                                                  amount=pack_price)
                        print(f"rebate_and_terminate_flag: {rebate_and_terminate_flag}")
                        log.log_info(f"\nrebate_and_terminate_flag: {rebate_and_terminate_flag}")

                        if rebate_and_terminate_flag != "Operation successful":
                            counter_log = f"{counter} out of {len(complain_list.index)}"
                            db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn, PACK=pack, USAGE=plan_usage,
                                                  PACK_ID=pack_id, BONUS_PACK=bonus_pack, BONUS_PACK_ID=bonus_pack_id,
                                                  DB_VALIDATION=db_check, PRICE=pack_price,
                                                  COUNTER_LOG=counter_log, ERROR=rebate_and_terminate_flag,
                                                  LIST_TYPE="Off_White")
                        else:
                            counter_log = f"{counter} out of {len(complain_list.index)}"
                            db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn, PACK=pack, USAGE=plan_usage,
                                                  PACK_ID=pack_id, BONUS_PACK=bonus_pack, BONUS_PACK_ID=bonus_pack_id,
                                                  DB_VALIDATION=db_check, PRICE=pack_price,
                                                  COUNTER_LOG=counter_log, LIST_TYPE="white")

                            msg = "Dear Customer, Your requested amount has been adjusted. To check your account balance, please dial *1#. Thank you. "
                            api.smsapi(msisdn, msg)

                        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="In Progress")
                        if complain_status_response == "fail":
                            error = f"Complain can not close, {rebate_and_terminate_flag}, Current status: Open"
                            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error)
                            break
                        time.sleep(1)
                        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Completed")
                        if complain_status_response == "fail":
                            error = f"Complain can not close, {rebate_and_terminate_flag}, Current status: In Progress"
                            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error)
                            break
                        time.sleep(1)
                        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Closed")
                        if complain_status_response == "fail":
                            error = f"Complain can not close, {rebate_and_terminate_flag}, Current status: Completed"
                            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error)

                        break
                    else:
                        error = "The Complain by the MSISDN Existed Previously with in 6 months"
                        log.log_info(
                            f"Unable to Process for the following complain - {sr_id}  MSISDN - {msisdn} USAGE - {plan_usage} DB_CHECK - {db_check}")
                        counter_log = f"{counter} out of {len(complain_list.index)}"
                        db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                                              USAGE=plan_usage, PACK_ID=pack_id, BONUS_PACK=bonus_pack,
                                              DB_VALIDATION=db_check, PRICE=pack_price,
                                              ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
                        msg = "Dear Customer, we are unable to adjust the requested amount."
                        api.smsapi(msisdn, msg=msg)
                        complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
                        if complain_status_response == "fail":
                            error_ = f"{error}, Complain can not Cancel, Current status: Open"
                            db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
                else:
                    error = "The User has used data from the pack"
                    log.log_info(
                        f"Unable to Process for the following complain - {sr_id}  MSISDN - {msisdn} USAGE - {plan_usage}")
                    counter_log = f"{counter} out of {len(complain_list.index)}"
                    db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                                          USAGE=plan_usage, PACK_ID=pack_id, BONUS_PACK=bonus_pack,
                                          PRICE=pack_price,
                                          ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
                    msg = "Dear Customer, we are unable to adjust the requested amount."
                    api.smsapi(msisdn, msg=msg)
                    complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
                    if complain_status_response == "fail":
                        error_ = f"{error}, Complain can not Cancel, Current status: Open"
                        db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
        if not pack_found:
            error = "Unable to find pack from customer pack list"
            counter_log = f"{counter} out of {len(complain_list.index)}"
            db.single_data_record(SR_NUMBER=sr_id, MSISDN=msisdn,
                                  BONUS_PACK=bonus_pack,
                                  PRICE=pack_price,
                                  ERROR=error, COUNTER_LOG=counter_log, LIST_TYPE="black")
            log.log_info(
                f"Unable to Process for the following complain - {sr_id}  MSISDN - {msisdn} ERROR - {error}")
            complain_status_response = api.crm_complain_update_counter(msisdn, sr_id, status="Cancelled")
            if complain_status_response == "fail":
                error_ = f"{error}, Complain can not Cancel, Current status: Open"
                db.single_data_update(MSISDN=msisdn, SR_NUMBER=sr_id, ERROR=error_)
            msg = "Dear Customer, we are unable to adjust the requested amount."
            api.smsapi(msisdn=msisdn, msg=msg)
    time.sleep(5)

api.smsapi('8801833182430', 'All done')
df_rebate_fail.to_csv("C:/Users/crm_rpa/Downloads/rebate_fail_list.csv", index=False, mode='a', header=False)
log.log_end()
