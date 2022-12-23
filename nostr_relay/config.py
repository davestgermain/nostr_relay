import yaml

class ConfigClass:
    DEBUG = True
    max_event_size = 4096
    nip05_verification = 'disabled'
    verification_blacklist = None
    verification_whitelist = None
    verification_expiration = 86400 * 30
    verification_update_frequency = 3600

    def __init__(self):
        self.gunicorn = {}
        self.logging = {
                'version': 1,
                'formatters': {
                    'simple': {
                        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    }
                },
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler', 
                        'level': 'DEBUG', 
                        'formatter': 'simple', 
                        'stream': 'ext://sys.stdout'
                    }
                }, 
                'loggers': {
                    'nostr_relay': {
                        'level': 'INFO', 
                        'handlers': ['console'], 
                        'propagate': False
                    }
                }, 
                'root': {
                    'level': 'INFO', 
                    'handlers': ['console']
                }
        } 

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
