import os
import datetime
import datetime as dt
from utils.logger import Logger
from apps.app_utils import AppUtils

log = Logger.get_instance()


class RemoteServerOperation:
    def __init__(self, sftp_connection):
        self.sftp = sftp_connection

    @staticmethod
    def formatting_date():
        previous_date = datetime.date.today() - datetime.timedelta(days=1)
        date_str = str(previous_date)
        date_split = date_str.split('-')
        return "".join(date_split)
        # return '20221012'

    def check_validation_file(self, remote_dir):
        validation_found = False
        if self.sftp.isdir(remote_dir):
            # list all the files and dir in remote
            files_list = self.sftp.listdir(remote_dir)
            # check if file in not empty
            date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
            date_today = date_today.strftime("%Y%m%d")
            if len(files_list) != 0:
                if remote_dir.split('/')[-1] == "calldrop":
                    for file in files_list:
                        if 'validation' in file.lower() and date_today in file:
                            validation_found = True
                else:
                    # for airtel validation
                    validation_found = True
                return validation_found

    def copy_file_from_remote(self, local_dir, remote_dir):
        file_name_not_to_consider = AppUtils.conf['file_name_not_to_consider']
        date_today = dt.date.today() - dt.timedelta(days=AppUtils.conf['previous_email_day'])
        date_today = date_today.strftime("%Y%m%d")
        try:
            files_name_list = list()
            # check local directory exists
            if os.path.exists(local_dir):
                # check remote directory exist
                if self.sftp.isdir(remote_dir):
                    # list all the files and dir in remote
                    files_list = self.sftp.listdir(remote_dir)
                    # check if file in not empty
                    if len(files_list) != 0:
                        for file_name in files_list:
                            if date_today in file_name and 'csv' in file_name and file_name_not_to_consider not in file_name:
                                files_name_list.append(file_name)
                                # join local file path
                                local_file_path = os.path.join(local_dir, file_name)
                                # Check file already exists in local
                                if not os.path.exists(local_file_path):
                                    # create file in local
                                    with open(local_file_path, 'w') as file:
                                        pass
                                    self.sftp.get(remotepath=f'{remote_dir}/{file_name}',
                                                  localpath=f'{local_dir}/{file_name}',
                                                  preserve_mtime=True)
                                    print(
                                        f"Copy file from remote successful. [REMOTE_PATH] - {remote_dir}. [FILE_NAME] - {file_name}.")
                                    log.log_info(msg=f"Copy file from remote successful. [REMOTE_PATH] - {remote_dir}. [FILE_NAME] - {file_name}.")
                        return files_name_list
                    else:
                        print("File list is empty")
                        log.log_info(msg="File list is empty")
                else:
                    print("Remote directory is not exists")
                    log.log_info(msg="Remote directory is not exists")
            else:
                # create local directory if not exists
                os.mkdir(local_dir)
                self.copy_file_from_remote(local_dir, remote_dir)

        except Exception as e:
            from traceback import print_exc
            print_exc()
            print(e)
            log.log_critical(e)
