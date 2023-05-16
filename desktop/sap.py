from desktop.window import Window
import time


# "C:\\Program Files\\SAP\\FrontEnd\\SAPgui\\saplogon.exe"

class SAP:
    quality_module = 'QAS'
    production_module = 'PROD'

    def __init__(self, sap_location, sap_module='QAS'):
        self.sap = Window(application=sap_location, app_class="[TITLE:SAP Logon 730;CLASS:#32770]",
                          app_title="SAP Logon 730", run_type=Window.autoit_text)
        if sap_module == 'QAS':
            self.sap.list_iterator(6)
        else:
            self.sap.list_iterator(1)

    def login(self, username, password):
        SAP_front_end = self.sap.use_sub_ui(app_class="[TITLE:SAP;CLASS:SAP_FRONTEND_SESSION]")
        self.sap.wait_for_window(SAP_front_end)
        self.sap.write_on_edit_box(username)
        self.sap.press_key("TAB")
        self.sap.write_on_edit_box(password)
        self.sap.press_key("ENTER")
        SAP_front_end = self.sap.use_sub_ui(app_class="[CLASS:SAP_FRONTEND_SESSION]")
        self.sap.wait_for_window(SAP_front_end)

    def enable_scripting(self,filename):
        self.sap.press_key("!{F12}n", mode=1)
        time.sleep(2)
        self.sap.press_key("SPACE")
        time.sleep(2)
        self.sap.write_on_edit_box(filename)
        self.sap.press_key("TAB")
        self.sap.click_button()
        time.sleep(2)
        self.sap.press_key("!o", mode=1)

    def check_secondary_login(self):
        secondary_login = self.sap.use_sub_ui(app_class="[TITLE:License Information for Multiple Logon;CLASS:#32770]")
        if self.sap.wait_for_window(secondary_login) == 1:
            self.sap.press_key("TAB")
            self.sap.press_key("UP")
            self.sap.press_key("ENTER")
            time.sleep(5)
            
            
            
    def close(self):
        self.sap.close_window()
        time.sleep(2)
        self.sap.press_key("!{F4}", mode=1)
        time.sleep(2)
        self.sap.press_key("+{F3}", mode=1)
        time.sleep(2)
        self.sap.press_key("{TAB}", mode=1)
        time.sleep(2)
        self.sap.press_key("ENTER")
        time.sleep(2)
        self.sap.close_window()
        time.sleep(2)
        self.sap.press_key("!{F4}", mode=1)


    def error_handle(self):
        error_window = self.sap.wait_for_window("[Title:SAP Frontend Server;Class:	#32770]")
        if(error_window is not None):
            self.sap.click_button()
            return True
        else:
            return False

