import psutil

from apps.service_validation import APIValidation

apivalid = APIValidation()
# gives a single float value
# print(psutil.cpu_percent())
# gives an object with many fields
# print(psutil.virtual_memory())
# you can convert that object to a dictionary
print(dict(psutil.virtual_memory()._asdict()))
print(dict(psutil.virtual_memory()._asdict()).get('percent'))
percent = dict(psutil.virtual_memory()._asdict()).get('percent')
percent = float(percent)
print(percent)
msisdn = "8801833182430"  # Rayhan vai
# msisdn = "8801817183413"#Rafat vai
# msisdn = "8801642081428"#Shawon
if (percent >= 70):
    msg = f"Current memory uses of CRM-RPA server is {percent}%."
    print(msg)
    apivalid.smsapi(msisdn=msisdn, msg=msg)
