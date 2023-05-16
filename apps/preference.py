
class Preference:
    def __init__(self, web_driver):
        self.web_driver = web_driver

    def grid_reader(self):
        record_table_available = self.web_driver.check_table_record(output_data="count")

        self.web_driver.log_info(msg="CRM Complain Found = " + str(record_table_available))
        if record_table_available > 1:
            pull_primary_data = self.web_driver.primary_data_initiate_smart_script(pull_primary_data=[])

            pull_primary_data = self.web_driver.initiate_smart_script_data(pull_primary_data=pull_primary_data, index=0)

            pull_primary_data = self.web_driver.push_more_smart_script_data(pull_primary_data=pull_primary_data)

            self.web_driver.log_info(
                msg="Preparing Final Grid Data for Saving Into DB and Execute Other Action" + str(pull_primary_data))
            for item in pull_primary_data:
                self.web_driver.log_info(msg="Individual Final Data Row = " + str(item))

        else:
            self.web_driver.log_info(msg="CRM No Complaint Row Found")

        self.web_driver.rpa_running_flag(flag="End")
        #   xml_wrapper_object.webpage.driver.close()
