import asyncio
import click
from functools import wraps
from .web import run_with_gunicorn
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

    Config.load(config)


@main.command()
@click.pass_context
def serve(ctx):
    """
    Start the http relay server 
    """
    run_with_gunicorn()


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
        async with get_storage() as storage:
            await storage.set_identified_pubkey(identifier, pubkey, relays=relay)


@main.command()
@click.option("--query", '-q', help="Query", prompt="Enter REQ filters")
@click.option('--results/--no-results', default=True)
@click.pass_context
@async_cmd
async def query(ctx, query, results):
    """
    Run a REQ query and display results
    """
    import rapidjson
    import asyncio
    from .db import get_storage, Subscription
    if not query:
        click.echo("query is required")
        return -1
    query = query.strip()
    if not query.startswith('['):
        query = f'[{query}]'
    query = rapidjson.loads(query)
    queue = asyncio.Queue()
    async with get_storage() as storage:
        sub = Subscription(storage.db, 'cli', query, queue=queue, default_limit=60000)
        sub.prepare()
        click.echo(click.style('Query:', bold=True))
        click.echo(click.style(sub.query, fg="green"))
        await sub.run_query()

    if results:
        click.echo(click.style('Results:', fg='black', bold=True))

        while True:
            sub, event = await queue.get()
            if event:
                click.echo(click.style(rapidjson.dumps(rapidjson.loads(event), indent=4), fg="red"))
                click.echo('')
            else:
                break


@main.command()
@click.option("--query", '-q', help="Query", prompt="Enter REQ filters", default='[{"kinds":[0,1,2,3,4,5,5,6,7,8,9]}]')
@click.pass_context
@async_cmd
async def update_tags(ctx, query):
    """
    Update the tags in the tag table, from a REQ query
    """
    import rapidjson
    import asyncio
    from .event import Event
    from .db import get_storage, Subscription

    if not query:
        query = '[{}]'

    query = rapidjson.loads(query)
    queue = asyncio.Queue()

    async with get_storage() as storage:
        sub = Subscription(storage.db, 'cli', query, queue=queue, default_limit=600000)
        sub.prepare()
        click.echo(click.style('Query:', bold=True))
        click.echo(click.style(sub.query, fg="green"))
        await sub.run_query()

        count = 0
        async with storage.db.cursor() as cursor:

            while True:
                sub, event_json = await queue.get()
                if event_json:
                    event = Event(**rapidjson.loads(event_json))
                    await storage.process_tags(cursor, event)
                    count += 1
                else:
                    break

        await storage.db.commit()
        click.echo("Processed %d events" % count)
