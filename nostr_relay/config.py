import yaml

class Config:
    DEBUG = True

    @classmethod
    def to_string(cls):
        s = 'Config(\n'
        for k, v in cls.__dict__.items():
            if k.startswith('_') or k == 'to_string':
                continue
            s += f'\t{k}={v} \n'
        s += ')'
        return s


def load_configuration(filename):
    with open(filename, 'r') as fp:
        conf = yaml.load(fp, yaml.FullLoader)

    for k, v in conf.items():
        setattr(Config, k, v)

    return Config
