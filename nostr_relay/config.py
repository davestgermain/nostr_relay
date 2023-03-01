import os.path
import yaml


class ConfigClass:
    DEBUG = False
    max_event_size = 4096
    oldest_event = 31536000
    redirect_homepage = ""
    subscription_limit = 32
    max_limit = 6000

    def __init__(self):
        self.authentication = {}
        self.gunicorn = {"bind": "127.0.0.1:6969"}
        self.storage = {
            "sqlalchemy.url": "sqlite+aiosqlite:///:memory:",
        }
        self.verification = {
            "nip05_verification": "disabled",
            "blacklist": None,
            "whitelist": None,
            "expiration": 86400 * 30,
            "update_frequency": 3600,
        }
        self.garbage_collector = {
            "collect_interval": 300,
        }
        self.logging = {
            "version": 1,
            "formatters": {
                "simple": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                "sqlalchemy.engine": {
                    "level": "WARNING",
                },
                "nostr_relay": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
            "root": {"level": "INFO", "handlers": ["console"]},
        }
        self._is_loaded = False

    def __str__(self):
        s = "Config(\n"
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            s += f"\t{k}={v} \n"
        s += ")"
        return s

    def load(self, filename=None, reload=False):
        if self._is_loaded and not reload:
            return

        filename = filename or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "config.yaml")
        )
        with open(filename, "r") as fp:
            conf = yaml.load(fp, yaml.FullLoader)

        for k, v in conf.items():
            setattr(self, k, v)

        proc_name = self.gunicorn.get("proc_name", "")
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

    @property
    def should_run_notifier(self):
        return (self.gunicorn.get("workers", 1) > 1) or self.get("run_notifier", False)


Config = ConfigClass()
