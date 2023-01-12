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
@click.option("--event/--no-event", '-e', help="Return as EVENT message JSON", default=True)
@click.pass_context
@async_cmd
async def dump(ctx, event):
    """
    Dump all events
    """
    query = [{'since': 1}]
    as_event = event
    from .db import get_storage
    async with get_storage() as storage:
        async for event_json in storage.run_single_query(query):
            if as_event:
                print(f'["EVENT", {event_json}]')
            else:
                print(event_json)


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


@click.group()
@click.pass_context
def role(ctx):
    """
    View/Set authentication roles
    """
    pass


@role.command()
@click.option('--pubkey', '-p', help='Public Key', default='')
@click.option('--roles', '-r', help='Roles', default='')
@async_cmd
async def set(pubkey, roles):
    """
    Set roles in the authentication table
    """
    from .db import get_storage

    if not pubkey:
        click.echo('public key is required')
        return -1

    async with get_storage() as storage:
        await storage.authenticator.set_roles(pubkey, roles)
        click.echo(f"Set roles for pubkey: {pubkey} to {roles}")


@role.command()
@click.option('--pubkey', '-p', help='Public Key', default='')
@async_cmd
async def get(pubkey):
    """
    Get roles from the authentication table
    """
    from .db import get_storage
    async with get_storage() as storage:
        if not pubkey:
            async for pubkey, role in storage.authenticator.get_all_roles():
                click.echo(f'{pubkey}: {role}')
        else:
            roles = await storage.authenticator.get_roles(pubkey)
            click.echo(roles)

main.add_command(role)


@main.command()
@click.option('--ids', help='ids')
@click.option('--authors', help='authors')
@click.option('--kinds', help='kinds')
@click.option('--etags', help='etags')
@click.option('--ptags', help='ptags')
@click.option('--since', help='since')
@click.option('--until', help='until')
@click.option('--limit', help='limit')
@click.option('--source', help='source relay', default='ws://localhost:6969')
@click.option('--target', help='to relay', default='ws://localhost:6969')
def mirror(ids, authors, kinds, etags, ptags, since, until, limit, source, target):
    """
    Mirror REQ from source relay(s) to target relay(s)

    (this just calls the commands: nostreq, jq, and nostcat, which must exist on the PATH)
    """
    cmd = ['nostreq']
    if ids:
        cmd.append(f'--ids {ids}')
    if authors:
        cmd.append(f'--authors {authors}')
    if kinds:
        cmd.append(f'--kinds {kinds}')
    if etags:
        cmd.append(f'--etags {etags}')
    if ptags:
        cmd.append(f'--ptags {ptags}')
    if since:
        cmd.append(f'--since {since}')
    if until:
        cmd.append(f'--until {until}')
    if limit:
        cmd.append(f'--limit {limit}')

    cmd.append('|')
    cmd.append("nostcat")
    cmd.extend(source.split(','))
    cmd.append('|')
    cmd.append("jq -c 'del(.[1])'")
    cmd.append('|')
    cmd.append("nostcat --stream")
    cmd.extend(target.split(','))

    command = ' '.join(cmd)
    print(command)
    import subprocess, sys
    proc = subprocess.Popen(command, shell=True, bufsize=0, stdout=sys.stdout, stderr=subprocess.PIPE)
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.kill()
        return 0

