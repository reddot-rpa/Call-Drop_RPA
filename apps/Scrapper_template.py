from pages.pages import Pages
from utils.constants import LocatorType
from utils.logger import Logger


class Scrapper:

    def __init__(self, browser, url):
        self.pages = Pages(browser=browser)
        self.pages.navigate(url)
        self.browser = self.pages.driver

    locator_type = LocatorType()


    def snippet(self):
        self.elem = self.pages.find_all_elements(locator="""//*[@href and @class="result_title_link"]""",type='xpath')

        self.ref_len = len(self.elem)
        list =[]

        for i in range(self.ref_len):
            self.el = self.elem[i]
            self.x = self.el.get_attribute('href')

            self.pages.new_tab(self.x)
            self.para = self.pages.find_all_elements('p',type='tags')

            for p in self.para:
                print(p.text)
                list.append(p.text)
            list.append('END')
            self.pages.end()
            self.pages.switch_to_main()

        return list