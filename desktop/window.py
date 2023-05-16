import autoit
import os
import threading
import time


class Window:
    os = 'os'
    autoit_text = 'autoit'
    os_thread = 'os_thread'

    __handle = None
    __app_class = None

    def __init__(self, application, app_class, app_title=None, run_type='os'):
        """
        Definition of Desktop App Open and store instances of the desktop app for further useage
        :param application: application executable file name
        :param app_instance: application class name
        """
        self.__app_class = app_class
        if run_type == 'autoit':
            if application is not None:
                autoit.run(application)
                instance = self.wait_for_window()
                if instance == 0:
                    print("unable to create instance")
                print(self.autoit_text)
        elif run_type == 'os':
            os.system(application)
            instance = self.wait_for_window()
            if instance == 0:
                print("unable to create instance")
            print(self.os)
        elif run_type == 'os_thread':
            print("Run by thread run")

    thread_flag = True

    def thread_run(self, application):
        while self.thread_flag:
            os.system(application)

    def write_on_edit_box(self, text, edit_box_id=None, sub_class=None):
        app_class = self.get_app_class(sub_class)
        if edit_box_id != None:
            autoit.control_set_text(app_class, edit_box_id, text)
        else:
            autoit.send(text, mode=1)

    def close_window(self, sub_class=None):
        app_class = self.get_app_class(sub_class)
        print(app_class)
        autoit.win_close(app_class)

    def click_button(self, button=None, sub_class=None):
        app_class = self.get_app_class(sub_class)
        if button != None:
            autoit.control_click(app_class, button)
        else:
            autoit.send(send_text="{ENTER}", mode=0)

    def get_app_class(self, sub_class=None):
        app_class = self.__app_class
        if sub_class != None:
            app_class = sub_class.get_app_class()
        return app_class

    def wait_for_window(self, sub_class=None):
        app_class = self.get_app_class(sub_class)
        found = 0
        try:
            found = autoit.win_wait_active(app_class, 15)
        except Exception:
            print(Exception)
            return None
        return found

    def use_sub_ui(self, app_class):
        sub_ui = self.Sub_UI(app_class)
        return sub_ui

    CTRL = "LCTRL"
    ENTER = "ENTER"
    ALT = "LALT"
    UP = "UP"
    DOWN = "DOWN"
    TAB = "TAB"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    ESC = "ESC"
    BACKSAPCE = "BS"

    def press_key(self, key, count=1,mode = 0):
        if mode == 0:
            if count > 1:
                key = "{" + key + " " + str(count) + "}"
            else:
                key = "{" + key + "}"
            print(key)
        autoit.send(key, mode=0)

    def list_iterator(self, line_number):
        line_number = line_number - 1
        self.press_key("DOWN", line_number)
        self.press_key("ENTER")

    class Sub_UI:
        __sub_ui_app_class = None

        def __init__(self, app_class):
            self.__sub_ui_app_class = app_class

        def get_app_class(self):
            return self.__sub_ui_app_class
