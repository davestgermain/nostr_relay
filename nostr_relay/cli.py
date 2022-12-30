import click
from .web import create_app
from .config import Config


@click.command()
@click.argument('config_file', required=False)
def main(config_file):
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

