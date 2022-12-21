from .web import create_app
from .config import Config


def main():
    import sys
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None
    app = create_app(config_file)
    
    from gunicorn.app.base import Application

    class ASGIApplication(Application):
        def load_config(self):
            self.cfg.set('worker_class', 'uvicorn.workers.UvicornWorker')
            for k, v in Config.gunicorn.items():
                self.cfg.set(k.lower(), v)

        def load(self):
            return app

    ASGIApplication().run()
