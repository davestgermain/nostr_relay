import yaml

class ConfigClass:
    DEBUG = True

    def __init__(self):
        self.uvicorn = {}

    def __str__(self):
        s = 'Config(\n'
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            s += f'\t{k}={v} \n'
        s += ')'
        return s

    def load(self, filename):
        with open(filename, 'r') as fp:
            conf = yaml.load(fp, yaml.FullLoader)

        for k, v in conf.items():
            setattr(self, k, v)

    def __getattr__(self, attrname):
        return None


Config = ConfigClass()
