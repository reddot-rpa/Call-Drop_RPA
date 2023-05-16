from config import config


class ConfigControllerSAP:
    devlopment = 'devlopment'
    production = 'prodcution'

    def __init__(self, type='devlopment'):
        if type == self.devlopment:
            self.SAP = config.SAP_DEV
        elif type == self.production:
            self.SAP = config.SAP_PRD

    def get_instance(self):
        return self.SAP


class ConfigControllerVBScript:
    devlopment = 'devlopment'
    production = 'prodcution'

    def __init__(self, type='devlopment'):
        if type == self.devlopment:
            self.VBScript = config.VBScript_Generator_DEV
        elif type == self.production:
            self.VBScript = config.VBScript_Generator_PRD

    def get_instance(self):
        return self.VBScript
