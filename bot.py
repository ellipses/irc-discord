import threading
import discord
import logging
import asyncio
import janus
import yaml
import irc3
import os

logging.basicConfig(level=logging.INFO)

server_cache = {}
client = discord.Client()
irc_to_discord = janus.Queue()
discord_to_irc = janus.Queue()


def format_irc_message(message):
    msg = "\x02 <%s>: \x02 %s" % (message.author.name, message.clean_content)
    for attachment in message.attachments:
        msg += ' ' + attachment['url']
    return msg


def format_discord_message(user, msg):
    # Prepend the message with zero width white space char to
    # avoid bot loops.
    return "\u200B**%s** %s" % (user, msg)


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('--------')
    servers = client.servers
    for server in servers:
        print('Joined server %s' % server)


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    await discord_to_irc.async_q.put(
        (format_irc_message(message), message.channel.id)
    )


def get_discord_server(channel_mapping, channel):
    server_id = channel_mapping[channel]
    if server_id not in server_cache:
        server_cache[server_id] = discord.Server(id=server_id)

    return server_cache[server_id]


async def dequeue_irc(config):
    await client.wait_until_ready()
    while True:
        msg, channel = await irc_to_discord.async_q.get()
        try:
            server = get_discord_server(config['channel_mapping'], channel)
        except KeyError:
            logging.debug(
                'Skipping %s because %s isnt in the mapping' % (msg, channel)
            )
        else:
            await client.send_message(server, msg)


@irc3.event(irc3.rfc.PRIVMSG)
async def on_privmsg(bot, mask, data, target, *args, **kw):
    await irc_to_discord.async_q.put(
        (format_discord_message(mask.split('!')[0], data), target)
    )


def dequeue_discord(bot, mapping, *args, **kwargs):
    while True:
        msg, channel = discord_to_irc.sync_q.get()
        try:
            bot.privmsg(mapping[channel], msg, nowait=True)
        except KeyError:
            logging.debug(
                'Skipping %s because %s isnt in the mapping' % (msg, channel)
            )


def start_irc(config):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    irc_config = dict(
        autojoins=config['channels'],
        host=config['host'], port=config['port'], ssl=False,
        includes=[
            'irc3.plugins.core',
            __name__,  # this register this module
        ],
        loop=loop)

    bot = irc3.IrcBot(nick=config['nick'], **irc_config)
    loop.run_in_executor(None, dequeue_discord, bot, config['channel_mapping'])
    bot.run()
    loop.run_forever()


def start_discord(loop, config):
    asyncio.set_event_loop(loop)
    client.loop.create_task(dequeue_irc(config))
    client.run(config['token'])


def main():
    config_path = os.path.join(os.path.dirname(__file__),
                               'conf/bot.yaml')
    config = yaml.load(open(config_path).read())
    discord_thread = threading.Thread(
        target=start_discord, args=(
            asyncio.get_event_loop(), config['discord']
        )
    )
    irc_thread = threading.Thread(target=start_irc, args=(config['irc'],))

    discord_thread.start()
    irc_thread.start()
    discord_thread.join()
    irc_thread.join()


if __name__ == '__main__':
    main()
