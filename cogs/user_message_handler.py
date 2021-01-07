import typing
from datetime import datetime as dt, timedelta
import collections

import discord
from discord.ext import tasks
import voxelbotutils as utils


class UserMessageHandler(utils.Cog):

    def __init__(self, bot:utils.Bot):
        super().__init__(bot)
        self.last_message: typing.Dict[discord.Member, dt] = collections.defaultdict(lambda: dt(2000, 1, 1, 0, 0))
        self.cached_for_saving: typing.List[discord.Message] = list()
        self.user_message_databaser.start()

    def cog_unload(self):
        """
        Stop the databaser loop very gently so it stores everything in cache first.
        """

        self.user_message_databaser.stop()

    @tasks.loop(minutes=1)
    async def user_message_databaser(self):
        """
        Saves all messages stored in self.cached_for_saving to db.
        """

        # Only save messages if there _were_ any
        if len(self.cached_for_saving) == 0:
            self.logger.info("Storing 0 cached messages in database")
            return

        # Get the messages we want to save
        currently_saving = self.cached_for_saving.copy()  # Make a copy to fend off the race conditions
        for m in currently_saving:
            try:
                self.cached_for_saving.remove(m)
            except ValueError:
                pass

        # Get members with their respective guilds
        members_of_cached_messages = [[message.author.id, message.guild.id] for message in currently_saving if message.author.bot is False and message.guild is not None]

        members = []
        for x in members_of_cached_messages:
            if x[0] not in [x[0] for x in members]:
                members.append(x)

        member_points = {}

        for member in members:
            # Get Data
            member_id = member[0]
            guild_id = member[1]
            point_data = self.bot.guild_settings[guild_id].setdefault('role_multiplier', dict())
            guild = self.bot.get_guild(guild_id)
            member_roles_ids = set(guild.get_member(member_id)._roles)

            points = next((multiplier for role_id, multiplier in point_data.items() if role_id in member_roles_ids), 1)
            member_points[member_id] = points

        # Sort them into a nice easy tuple
        records = [(i.created_at, i.author.id, i.guild.id, i.channel.id) for i in currently_saving if i.author.bot is False and i.guild is not None]

        # Recreate records with points
        new_records = []

        for (created_at, author_id, guild_id, channel_id) in records:

            try:
                points = member_points[author_id]
            except IndexError:
                points = 1

            new_records.append((created_at, author_id, guild_id, channel_id, points))

        # Copy the records into the db
        self.logger.info(f"Storing {len(new_records)} cached messages in database")
        async with self.bot.database() as db:
            await db.conn.copy_records_to_table(
                'user_messages',
                columns=('timestamp', 'user_id', 'guild_id', 'channel_id', 'points'),
                records=new_records
            )

    @utils.Cog.listener("on_message")
    async def user_message_cacher(self, message:discord.Message):
        """
        Listens for a user sending a message, and then saves that message as a point
        into the db should their last message be long enough ago.
        """

        # Filter out DMs
        if not isinstance(message.author, discord.Member):
            return

        # Filter out blacklisted roles
        blacklisted_roles = self.bot.guild_settings[message.guild.id].setdefault('blacklisted_text_roles', list())
        if set(message.author._roles).intersection(blacklisted_roles):
            return

        # Filter blacklisted channels
        if message.channel.id in self.bot.guild_settings[message.guild.id].setdefault('blacklisted_channels', list()):
            return

        # Make sure it's in the time we want
        last_message_from_user = self.last_message[message.author]
        if last_message_from_user < dt.utcnow() - timedelta(minutes=1):
            self.last_message[message.author] = message.created_at
        else:
            return

        # Cache for dynamic role handles
        self.cached_for_saving.append(message)

        # Dispatch points event
        self.bot.dispatch('user_points_receive', message.author)


def setup(bot:utils.Bot):
    x = UserMessageHandler(bot)
    bot.add_cog(x)
