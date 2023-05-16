from time import strftime
from datetime import date, timedelta, datetime


class Dates:

    def get_current_date(self):
        to_date = strftime("%d-%b-%Y")
        return to_date

    def get_old_date(self, days=2):
        yesterday = date.today() - timedelta(days)
        from_date = yesterday.strftime("%d-%b-%Y")
        return from_date

    def format_date(self, date):
        formatted_date = datetime.strptime(date, "%d-%b-%Y")
        formatted_date = formatted_date.strftime("%d-%b-%Y")
        return formatted_date
