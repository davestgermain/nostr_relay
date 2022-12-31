import asyncio
import click
from functools import wraps
from .web import create_app
from .config import Config


def async_cmd(func):
  @wraps(func)
  def wrapper(*args, **kwargs):
    return asyncio.run(func(*args, **kwargs))
  return wrapper


@click.group()
@click.option('--config', '-c', required=False, help="Config file")
@click.pass_context
def main(ctx, config):
    ctx.ensure_object(dict)

    ctx.obj['config'] = config


@main.command()
@click.pass_context
def serve(ctx):
    """
    Start the http relay server 
    """
    app = create_app(ctx.obj['config'])
    
    from gunicorn.app.base import Application

    class ASGIApplication(Application):
        def load_config(self):
            self.cfg.set('worker_class', 'uvicorn.workers.UvicornWorker')
            for k, v in Config.gunicorn.items():
                self.cfg.set(k.lower(), v)

        def load(self):
            return app

    ASGIApplication().run()


@main.command()
@click.option("--identifier", '-i', help="Identifier (name@domain)")
@click.option("--pubkey", '-p', help="Public key")
@click.option("--relay", '-r', multiple=True, help='Relay address (can be added multiple times)')
@click.pass_context
@async_cmd
async def adduser(ctx, identifier='', pubkey='', relay=None):
    """
    Add a user to the NIP-05 identity table
    """
    if not identifier:
        click.echo("Identifier required")
    else:
        click.echo(f'Adding {identifier} = {pubkey} with relays: {relay}')

        from .db import get_storage
        Config.load(ctx.obj['config'])
        storage = get_storage()
        await storage.setup_db()
        await storage.set_identified_pubkey(identifier, pubkey, relays=relay)
        await storage.close()
