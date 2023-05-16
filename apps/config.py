import json
import sys
import pandas as pd
from apps.encryption import CryptoPassPhase


class ConfigParser:

    def __init__(self):
        with open("../env_config.json", encoding="utf-8") as config_file:
            conf = json.load(config_file)
        self.conf = conf

    @staticmethod
    def stop_execution():
        print('Stopping Execution')
        sys.exit('Stopping Execution')

    def encrypt_key(self, key):
        pass_phase = self.conf['PassPhase']
        encrypted = CryptoPassPhase.encrypt(pass_phase, key)
        print(encrypted)
        self.stop_execution()

    def get_credentials(self):
        link = self.conf['crm_link']
        pass_phase = self.conf['PassPhase']
        interaction_type = self.conf['interaction_type']
        ins_product = self.conf['ins_product']
        ins_area = self.conf['ins_area']
        ins_sub_area = self.conf['ins_sub_area']
        ins_status = self.conf['ins_status']
        crm_username = self.conf['crm_username']
        crm_password = self.conf['crm_password']
        credentials = pd.DataFrame(
            columns=[
                "link",
                "username",
                "password",
                "interaction_type",
                "ins_product",
                "ins_area",
                "ins_sub_area",
                "ins_status"
            ],
            data=[
                [
                    link,
                    crm_username,
                    crm_password,
                    interaction_type,
                    ins_product,
                    ins_area,
                    ins_sub_area,
                    ins_status
                ]
            ]
        )
        return credentials

    def set_workflow_config(self, app_id, del_index=0, rpa_status='', remarks=''):
        credentials = pd.DataFrame(
            columns=[
                "username",
                "password",
                "app_id",
                "del_index",
                "rpa_status",
                "remarks",
            ],
            data=[
                [
                    "finance_rpa",
                    "Pass*12345",
                    app_id,
                    del_index,
                    rpa_status,
                    remarks,
                ]
            ]
        )
        return credentials

    def get_reporting_email(self):
        pass

    def get_error_reporting_email(self):
        to = self.conf['error_to']
        return to.split(',')

    def get_call_drop_report_email_to(self):
        to = self.conf['call_drop_email_to']
        return to.split(',')

    def get_call_drop_sms_to_concern(self):
        to = self.conf['call_drop_sms_to_concern']
        return to.split(',')

    def get_sms_sucsess_message(self):
        to = self.conf['success_sms']
        return to

    def get_sms_error_message(self):
        to = self.conf['not_success_sms']
        return to

    def get_config(self):
        return self.conf
