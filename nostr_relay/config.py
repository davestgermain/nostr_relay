import os.path
import yaml

class ConfigClass:
    DEBUG = True
    max_event_size = 4096
    nip05_verification = 'disabled'
    verification_blacklist = None
    verification_whitelist = None
    verification_expiration = 86400 * 30
    verification_update_frequency = 3600
    oldest_event = 31536000
    redirect_homepage = ''

    def __init__(self):
        self.gunicorn = {}
        self.garbage_collector = {
            'class': 'nostr_relay.db.QueryGarbageCollector',
            'collect_interval': 300,
        }
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
        self._is_loaded = False

    def __str__(self):
        s = 'Config(\n'
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            s += f'\t{k}={v} \n'
        s += ')'
        return s

    def load(self, filename=None):
        if self._is_loaded:
            return

        filename = filename or os.path.abspath(os.path.join(os.path.dirname(__file__), 'config.yaml'))
        with open(filename, 'r') as fp:
            conf = yaml.load(fp, yaml.FullLoader)

        for k, v in conf.items():
            setattr(self, k, v)

        proc_name = self.gunicorn.get('proc_name', '')
        if proc_name:
            import multiprocessing
            multiprocessing.current_process().name = proc_name
        self._is_loaded = True

    def __getattr__(self, attrname):
        return None

    def __contains__(self, key):
        return key in self.__dict__

    def get(self, key, default=None):
        return self.__dict__.get(key, default)


Config = ConfigClass()
