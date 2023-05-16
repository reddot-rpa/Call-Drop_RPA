import os.path
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from tabulate import tabulate

from apps.config import ConfigParser
from utils.logger import Logger


class Mail:
    """
    Mail Class where all the smtp servers, port no, username, password is given for both google and outlook.

    """
    conf = ConfigParser().conf
    google_smtp_server = 'smtp.gmail.com'
    # google_smtp_port = 465
    google_smtp_port = 587
    google_user_name = 'rpa.devops.ui@gmail.com'
    google_email = 'rpa.devops.ui@gmail.com'
    google_password = 'robi@1234'

    # outlook_smtp_server = 'gateway5.robi.com.bd'
    # outlook_smtp_server = '123.231.118.153'
    outlook_smtp_server = conf['mail_server']
    # outlook_smtp_port = 1003
    outlook_smtp_port = conf['mail_port']
    # outlook_user_name = 'ecs.rpa@robi.com.bd'
    # outlook_user_name = 'rpa'
    # outlook_password = 'Windows@10'
    outlook_email = conf['mail_sender']

    logger = Logger.get_instance()
    # outlook_password = 'Windows@09~~'

    targets = None
    CC = None
    mail = None
    smtp_server = None
    smtp_port = None
    smtp_email = None
    smtp_user_name = None
    smtp_password = None
    channel = None
    body = None

    google = 'Google'
    outlook = 'Outlook'

    mail_body_text = None
    mail_body_html = None
    mail_body_template_plain_text = """
{body}

Regards,
This is a system generated mail, do not reply!"""

    mail_body_template_html = """
<html>
    <head>
        <style> 
          table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
          th, td {{ padding: 5px; text-align:center;}}
          p {{ font-weight: normal;}}
        </style>
    </head>
    <body>
        <p>{body}</p>
        <p>This is a system generated mail, do not reply!</p>
    </body>
</html>
"""

    def __init__(self, channel='Google'):

        config = ConfigParser().conf

        """
        :param channel:Chose the mail send from google or outlook to open the connection
        """

        # if channel == self.google:
        #     self.smtp_server = self.google_smtp_server
        #     self.smtp_port = self.google_smtp_port
        #     self.smtp_user_name = self.google_user_name
        #     self.smtp_password = self.google_password
        #     self.smtp_email = self.google_email
        # elif channel == self.outlook:
        #     self.smtp_server = self.outlook_smtp_server
        #     self.smtp_port = self.outlook_smtp_port
        #     self.smtp_user_name = self.outlook_user_name
        #     self.smtp_password = self.outlook_password
        #     self.smtp_email = self.outlook_email

        if config['mail_config_flag'] == "prod":
            self.smtp_server = self.outlook_smtp_server
            self.smtp_port = self.outlook_smtp_port
            # self.smtp_user_name = self.outlook_user_name
            # self.smtp_password = self.outlook_password
            self.smtp_email = self.outlook_email
        if config['mail_config_flag'] == "dev":
            self.smtp_server = self.google_smtp_server
            self.smtp_port = self.google_smtp_port
            self.smtp_user_name = self.google_user_name
            self.smtp_password = self.google_password
            self.smtp_email = self.google_email

    def send_mail_to(self, targets, cc, mail_subject, mail_body, mail_attachment=None, in_mail_table=None):
        """
        This function manage to collect different types of information as a parameter to create the email .
        If mail attachment is not none it will show mail attachment in the mail body.

        :param targets: Mail id of the person you want to send mail
        :param cc: Mail id of the persons you want to keep in cc li
        :param mail_subject: Mail Subject
        :param mail_body: Message or values in mail body
        :param mail_attachment: Attachments for any mail
        :param in_mail_table: Read files and Insert into mail body table
        :return:None
        """

        self.logger.log_info(f'Sending Mail to : {targets} cc : {cc}')

        if in_mail_table is not None:
            list_for_table = in_mail_table
            mail_body_html = mail_body.format(table=tabulate(list_for_table, headers="firstrow", tablefmt="html"))
            mail_body_plain = mail_body.format(table=tabulate(list_for_table, headers="firstrow", tablefmt="grid"))
            self.mail_body_text = self.mail_body_template_plain_text.format(body=mail_body_plain)
            self.mail_body_html = self.mail_body_template_html.format(body=mail_body_html)
        else:
            self.mail_body_text = self.mail_body_template_plain_text.format(body=mail_body)
            self.mail_body_html = self.mail_body_template_html.format(body=mail_body)

        self.targets = targets
        self.CC = cc
        # self.mail = MIMEMultipart()
        self.mail = MIMEMultipart("alternative", None,
                                  [MIMEText(self.mail_body_text), MIMEText(self.mail_body_html, 'html')])
        # self.mail.attach(MIMEText(self.mail_body_text))
        self.mail['Subject'] = mail_subject
        self.mail['To'] = ', '.join(self.targets)
        if self.CC is not None:
            self.mail['CC'] = ', '.join(self.CC)
        if mail_attachment is not None:
            self.attachment(mail_attachment)

    def send(self):
        """
        Send mail to target
        :return:None
        """

        config = ConfigParser().conf

        if config['mail_config_flag'] == "dev":
            print("Sending mail in development server")
            self.logger.log_info('Sending Mail')
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.sendmail(self.smtp_email, self.targets, self.mail.as_string())
            server.quit()

        if config['mail_config_flag'] == "prod":
            print("Sending mail in production server")
            self.logger.log_info('Sending Mail')
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.sendmail(self.smtp_email, self.targets, self.mail.as_string())
            server.quit()
        # files = [
        #     r'E:\auc_fixed_asset_prod\rpaframework\Scripts\order_error_final_asset.csv',
        #     r'E:\auc_fixed_asset_prod\rpaframework\Scripts\order_status_iw33.xlsx',
        # ]
        #
        # for a_file in files:
        #     attachment = open(a_file, 'rb')
        #     file_name = os.path.basename(a_file)
        #     part = MIMEBase('application', 'octet-stream')
        #     part.set_payload(attachment.read())
        #     part.add_header('Content-Disposition',
        #                     'attachment',
        #                     filename=file_name)
        #     encoders.encode_base64(part)
        #     self.mail.attach(part)

        # sends email

        # server.sendmail(self.smtp_email, self.targets, self.mail.as_string())
        # server.quit()

    def attachment(self, mail_attachment):
        """
        :param mail_attachment:  If there is any attachment
        :return:None
        """

        self.logger.log_info(f'Attaching Attachment - {mail_attachment}')

        if type(mail_attachment) == str:
            with open(mail_attachment, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            attachment_name = os.path.basename(mail_attachment)

            part.add_header(
                "Content-Disposition",
                f"attachment; filename = {attachment_name}",
            )
            self.mail.attach(part)
        elif type(mail_attachment) == list:
            for attachment in mail_attachment:
                with open(attachment, "rb") as attachment_reader:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment_reader.read())

                encoders.encode_base64(part)
                attachment_name = os.path.basename(attachment)

                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename = {attachment_name}",
                )
                self.mail.attach(part)
        else:
            raise TypeError("mail_attachment type in not string or list of string")
