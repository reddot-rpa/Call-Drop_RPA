import requests
import json


class AlertModule:
    def send_sms(self, msisdns: list, sender_name, message):
        try:
            status_dict = {}
            msisdns = msisdns
            trimmed_message = message
            # if len(message) > <character limit of your account smsc account (int)>
            # else message
            trimmed_message = trimmed_message.encode('utf-8').decode('unicode-escape')
            for msisdn in msisdns:
                # REST request URL
                url = "https://smsc.robi.com.bd:18312/1/smsmessaging/outbound/RPAalert/requests"  #RPA
                # url = "https://smsc.robi.com.bd:18312/1/smsmessaging/outbound/CallDMin/requests"   # Call Drop
                # structured JSON
                payload = {
                    "outboundSMSMessageRequest":
                        {
                            "address": [str(msisdn)],
                            "senderAddress": str(sender_name),
                            "receiptRequest":
                                {
                                    "notifyURL": "http://10.135.192.142:8080/notifications/DeliveryInfoNotification",
                                    "callbackData": "12345",
                                    "notificationFormat": "json"
                                },
                            "outboundSMSTextMessage":
                                {
                                    "message": str(trimmed_message)
                                }
                        }
                }
                # ============================== headers For RPA USER ==========================
                # headers = {
                #     'X-WSSE': 'UsernameToken Username="RPAalert", PasswordDigest="35f0HjBzi6yNtGoZxCSKyTnL/NbFWHD5vvKeXZdqD20=", Nonce="2024012223595900001", Created="2024-01-22T23:59:59Z"',
                #     'Authorization': 'WSSE realm="SDP",profile="UsernameToken"',
                #     'X-RequestHeader': 'request ServiceId="35000000000018", LinkId="", FA="", OA="", PresentId=""',
                #     'Accept': 'application/json',
                #     'Content-Type': 'application/json; charset="UTF-8"',
                #     'Content-Length': str(len(trimmed_message))
                # }

                # ============================== headers For CallDrop USER ==========================
                headers = {
                    'X-WSSE': 'UsernameToken Username="CallDMin", PasswordDigest="AirE0ESSBZ1Aw4EAL8D+Muc1DsP2kNkpWOBfwlcfav0=", Nonce="2024012223595900001", Created="2024-01-22T23:59:59Z"',
                    'Authorization': 'WSSE realm="SDP",profile="UsernameToken"',
                    'X-RequestHeader': 'request ServiceId="35000000000027", LinkId="", FA="", OA="", PresentId=""',
                    'Accept': 'application/json',
                    'Content-Type': 'application/json; charset="UTF-8"',
                    'Content-Length': str(len(trimmed_message))
                }
                # POST request
                response = requests.post(url, headers=headers, data=json.dumps(payload,  ensure_ascii=False))
                print(response.content)
                status_dict.update({msisdn: response.status_code})

            return status_dict
        except Exception as e:
            return {"error": 1, "message": "Could not deliver", "exception": str(e)}


# Create an instance of the AlertModule class
alert_module = AlertModule()
result = alert_module.send_sms(["01833184087"], "RPA", "ব্যবহার করতে পারবেন যেকোনো এয়ারটেল নম্বর এ। ব্যালান্স চেক করতে ডায়াল *778*31# কলড্রপের বিস্তারিত জানতে ডায়াল *121*765#")

print(result)