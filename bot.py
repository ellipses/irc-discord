import os
import sys
import yaml
import irc3
import time
import redis
import discord
import logging
import asyncio
import argparse
import asyncio_redis


logging.basicConfig(level=logging.INFO)

server_cache = {}
client = discord.Client()


def format_redis_value(message, timestamp, channel):
    return '{}--{}--{}'.format(channel, timestamp, message)


def format_irc_message(message, channel):
    msg = "\x02 <%s>: \x02 %s" % (message.author.name, message.clean_content)
    for attachment in message.attachments:
        msg += ' ' + attachment['url']
    return format_redis_value(msg, message.timestamp.timestamp(), channel)


def format_discord_message(user, msg, channel):
    """
    Prepend the message with zero width white space char to
    avoid bot loops.
    """
    msg = "\u200B**%s** %s" % (user, msg)
    return format_redis_value(msg, time.time(), channel)


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('--------')
    servers = client.servers
    for server in servers:
        print('Joined server %s' % server)


def get_discord_server(channel_mapping, channel):
    server_id = channel_mapping[channel]
    if server_id not in server_cache:
        server_cache[server_id] = discord.Server(id=server_id)

    return server_cache[server_id]


def get_channel_mapping(config, to_irc=True):
    if to_irc:
        return config['channel_mapping']
    else:
        return {
            value: key for key, value in config['channel_mapping'].items()
        }


@irc3.event(irc3.rfc.PRIVMSG)
async def on_privmsg(bot, mask, data, target, *args, **kw):
    await bot.redis_connection.lpush(
        bot.irc_config['irc_key'],
        [format_discord_message(mask.split('!')[0], data, target)]
    )


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    try:
        client.redis_conn
    except AttributeError:
        client.redis_conn = await create_redis_connection()

    await client.redis_conn.lpush(
        client.config['discord_key'],
        [format_irc_message(message, message.channel.id)]
    )


def dequeue_discord(bot, config, *args, **kwargs):
    mapping = get_channel_mapping(config, False)
    sync_redis = redis.Redis()
    while True:
        _, value = sync_redis.brpop(config['discord_key'])
        channel, timestamp, msg = value.decode('utf-8').split('--', 2)

        if (time.time() - float(timestamp)) > config.get('ttl', 3600):
            logging.info('Skipping %s since ttl has past.', msg)
            continue

        try:
            bot.privmsg(mapping[channel], msg, nowait=True)
        except KeyError:
            logging.debug(
                'Skipping %s because %s isnt in the mapping' % (msg, channel)
            )


async def dequeue_irc(config):
    await client.wait_until_ready()
    redis_connection = await create_redis_connection()

    while True:
        message = await redis_connection.brpop(
            [config['irc_key']])
        channel, timestamp, msg = message.value.split('--', 2)

        if (time.time() - float(timestamp)) > config.get('ttl', 3600):
            logging.info('Skipping %s since ttl has past.', msg)
            continue

        try:
            server = get_discord_server(config['channel_mapping'], channel)
        except KeyError:
            logging.debug(
                'Skipping %s because %s isnt in the mapping', msg, channel
            )
        else:
            for _ in range(client.max_retries):
                try:
                    await client.send_message(server, msg)
                    break
                except discord.HTTPException:
                    await asyncio.sleep(1)
            else:
                logging.exception(
                    'Failed to send message %s to %s after %s retries',
                    msg, server, config.max_retries
                )


def start_irc(config):
    loop = asyncio.get_event_loop()
    redis_connection = loop.run_until_complete(create_redis_connection())

    irc_config = config['irc']
    irc_settings = dict(
        autojoins=irc_config['channels'],
        host=irc_config['host'], port=irc_config['port'], ssl=False,
        includes=[
            'irc3.plugins.core',
            __name__,  # this register this module
        ],
        loop=loop)

    bot = irc3.IrcBot(nick=irc_config['nick'], **irc_settings)
    loop.run_in_executor(
        None, dequeue_discord, bot, config
    )
    bot.redis_connection = redis_connection
    bot.irc_config = config
    bot.run()
    loop.run_forever()


def start_discord(config):
    client.loop.create_task(dequeue_irc(config))
    client.max_retries = config.get('max_retries', 3)
    client.config = config
    client.run(config['discord']['token'])


async def create_redis_connection():
    return await asyncio_redis.Connection.create()


def main():
    parser = argparse.ArgumentParser(
        description="Irc to discord bridge."
    )
    parser.add_argument(
        '-c', dest='client', type=str,
        choices=['irc', 'discord'], default='irc',
        help='Select the client to start, irc or discord.'
    )
    args = parser.parse_args()
    config_path = os.path.join(os.path.dirname(__file__),
                               'conf/bot.yaml')
    config = yaml.load(open(config_path).read())

    client_mapping = {
        'irc': start_irc,
        'discord': start_discord
    }
    client_mapping[args.client](config)


if __name__ == '__main__':
    main()
