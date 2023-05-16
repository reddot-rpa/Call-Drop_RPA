mode = 'development'
# mode = 'production'

SAP_DEV = {
    'location': 'C:/Program Files (x86)/SAP/FrontEnd/SAPgui/saplogon.exe',
    'username': 'aziz2863',
    'password': 'Mdmmmm@1984',
    'mode': 'QAS'
}
SAP_PRD = {
    'location': "C:/Program Files (x86)/SAP/FrontEnd/SAPgui/saplogon.exe",
    'username': 'rpa',
    'password': 'Rpa@1984',
    'mode': 'PRD'
}

VBScript_Generator_DEV = {
    'location': r'E:\Python Projects Robi\claimRPA',
    'filename': 'claim_script',
    'csv': r'E:\Python Projects Robi\claimRPA\claim_rpa_sample_data.csv'
    # 'location': r'C:\Users\automation\Documents',
    # 'filename': 'advance_script',
    # 'csv': r'C:\rpa\advance\advance_data.csv'
}

VBScript_Generator_PRD = {
    'location': r'C:\Users\automation\Documents\SAP\SAP GUI\ClaimRPA',
    'filename': 'claim_script',
    'csv': r'C:\rpa\claim\claim_file\claim_data.csv'
}
