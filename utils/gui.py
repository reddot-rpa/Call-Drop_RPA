from tkinter import *
from utils.dates import Dates


class GUI:
    ok_clicked_flag = False

    def __init__(self, x_axis=200, y_axis=350, title='RPA Framework'):
        self.window = Tk()
        self.window.title(title)
        self.window.geometry(str(x_axis) + 'x' + str(y_axis))
        self.dates = Dates()
        self.to_date = self.dates.get_current_date()
        self.from_date = self.dates.get_old_date(2)

    def login(self):
        lbl = Label(self.window, text="Username")
        lbl.grid(column=0, row=0)

        username_text_box = Entry(self.window, width=20)
        username_text_box.grid(column=1, row=0)

        lbl = Label(self.window, text="Password")
        lbl.grid(column=0, row=1)

        password_text_box = Entry(self.window, show="*", width=20)
        password_text_box.grid(column=1, row=1)

        btn = Button(self.window, text="ok",
                     command=lambda: self._clicked_login_button(
                         username_text_box,
                         password_text_box,
                     )
                     )
        btn.grid(column=4, row=4)
        self.window.mainloop()

    def login_and_date_range(self):
        lbl = Label(self.window, text="Username")
        lbl.grid(column=0, row=0)

        username_text_box = Entry(self.window, width=20)
        username_text_box.grid(column=1, row=0)

        lbl = Label(self.window, text="Password")
        lbl.grid(column=0, row=1)

        password_text_box = Entry(self.window, show="*", width=20)
        password_text_box.grid(column=1, row=1)

        lbl = Label(self.window, text="From Date")
        lbl.grid(column=0, row=2)

        from_date_text_box = Entry(self.window, width=20)
        from_date_text_box.insert(END, self.from_date)
        from_date_text_box.grid(column=1, row=2)

        lbl = Label(self.window, text="To Date")
        lbl.grid(column=0, row=3)

        to_date_text_box = Entry(self.window, width=20)
        to_date_text_box.insert(END, self.to_date)
        to_date_text_box.grid(column=1, row=3)

        btn = Button(text="ok",
                     command=lambda: self._clicked_login_and_range_button(
                         username_text_box,
                         password_text_box,
                         from_date_text_box,
                         to_date_text_box,
                     )
                     )
        btn.grid(column=4, row=4)
        self.window.mainloop()

    def _clicked_login_and_date_range_button(self, username_text_box, password_text_box, from_date_text_box,
                                        to_date_text_box):
        self.username = username_text_box.get()
        self.password = password_text_box.get()

        self.from_date = from_date_text_box.get()
        self.to_date = to_date_text_box.get()

        self.to_date = self.dates.format_date(self.to_date)
        self.from_date = self.dates.format_date(self.from_date)

        self.ok_clicked_flag = True
        self.window.destroy()

    def _clicked_login_button(self, username_text_box, password_text_box):
        self.username = username_text_box.get()
        self.password = password_text_box.get()

        self.ok_clicked_flag = True
        self.window.destroy()
