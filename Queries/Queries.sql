INSERT INTO CALL_DROP_FILE_LOG
(FILE_NAME, BRAND, STATUS, TOTAL_RECORD, SUCCESS_RECORD, FAILED_RECORD)
VALUES
('call_drop_rebate_postpaid_20220831_1.xlsx', 'Robi', 'Done', '418', '418', '0');


DELETE FROM CALL_DROP_FILE_LOG
WHERE FILE_NAME = 'call_drop_air_rebate_postpaid_20221015_1.xlsx';


select * from CALL_DROP_SMS_LOG_ROBI
where FILE_NAME = 'call_drop_rebate_postpaid_20220831_1.csv' and MSISDN = '8801894742586'

SELECT FILE_NAME,STATUS FROM CALL_DROP_FILE_LOG
WHERE FILE_NAME LIKE '%20221012%'

SELECT COUNT(*) FROM CALL_DROP_FILE_LOG
WHERE FILE_NAME LIKE '%rebate_%_20221005%' 


SELECT COUNT(*) FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE FILE_NAME LIKE 'call_drop_air_rebate_prepaid_20221005_9.csv'

SELECT FILE_NAME, COUNT(*)
FROM CALL_DROP_SMS_LOG_ROBI
WHERE FILE_NAME LIKE '%20221016%'
GROUP BY FILE_NAME

SELECT FILE_NAME, COUNT(*)
FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE FILE_NAME LIKE '%20221004%' AND SMS_STATUS = 'Sent'
GROUP BY FILE_NAME


SELECT SUM(AMOUNT) / 60
FROM CALL_DROP_SMS_LOG_ROBI
WHERE FILE_NAME LIKE '%prepaid_20221006%'

SELECT SUM(AMOUNT) / 60
FROM CALL_DROP_SMS_LOG_ROBI
WHERE FILE_NAME LIKE '%postpaid_20221004%'


SELECT SUM(AMOUNT) / 60
FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE FILE_NAME LIKE '%prepaid_20221004%'

SELECT SUM(AMOUNT) / 60
FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE FILE_NAME LIKE '%postpaid_20221004%'

SELECT DISTINCT * FROM CALL_DROP_SMS_LOG_AIRTEL  WHERE SMS_STATUS='Sent' AND FILE_NAME LIKE '%20221007%' FETCH NEXT 1000 ROWS ONLY

SELECT * FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE MSISDN = '8801619448120'

SELECT * FROM CALL_DROP_SMS_LOG_ROBI
WHERE MSISDN = '8801860688817'

SELECT COUNT(*) FROM CALL_DROP_SMS_LOG_AIRTEL
WHERE FILE_NAME = 'call_drop_air_rebate_prepaid_20221015_1.csv'

SELECT COUNT(*) FROM CALL_DROP_FILE_LOG
WHERE FILE_NAME LIKE '%20221010%'