import discord
from discord import Status
from discord.ext import commands
import sqlite3
import time
import config

intents = discord.Intents.all()
intents.members = True
intents.presences = True
bot = commands.Bot(command_prefix='!', intents=intents)
TOKEN = config.TOKEN
ALLOWED_USERS = config.ALLOWED_USERS
GUILD_ID = config.GUILD_ID  # Replace YOUR_SERVER_ID with the actual ID of your server in the config file
CHANNEL_ID = config.CHANNEL_ID  # Replace the channel ID with the channel ID where to bot should report leave messages.
MEMBER_ROLE = config.MEMBER_ROLE  # Replace Member Role name with your specific tracked role name in the config file.
INACTIVE_DAYS = config.INACTIVE_DAYS  # Number of days until a member in considered Inactive.

conn = sqlite3.connect('members.db')
c = conn.cursor()

# Create the members table if it doesn't exist
c.execute('''CREATE TABLE IF NOT EXISTS members
             (userid TEXT PRIMARY KEY, nickname TEXT, role TEXT, timestamp INTEGER, username TEXT, joindate INTEGER)''')

@bot.event
async def on_ready():
    print('Bot is ready.')
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="you"))

@bot.event
async def on_member_join(member):
    print('Member joined:', member)
    # Insert the user ID into the database when a user joins Discord
    c.execute("INSERT OR IGNORE INTO members (userid, nickname, username, joindate) VALUES (?, ?, ?, ?)", (str(member.id), str(member.nick), str(member), int(time.time())))
    conn.commit()

@bot.event
async def on_member_remove(member):
    if member.guild.id == GUILD_ID:
        channel = bot.get_channel(CHANNEL_ID)
        user_id = str(member.id)
        await channel.send(f"User {user_id} has left the server.")

        # Execute the code from !whois for the user
        c.execute("SELECT * FROM members WHERE userid = ?", (user_id,))
        result = c.fetchone()

        if result:
            nickname = result[1] if result[1] else "Not set"
            role = result[2] if result[2] else "Not set"
            #timestamp = convert_timestamp(result[3]) if result[3] else "Not available"
            await channel.send(f"Nickname: {nickname}\nRole: {role}")
        else:
            await channel.send("User not found in the database.")

@bot.event
async def on_member_update(before, after):
    nickname_before = before.nick
    nickname_after = after.nick
    userid_before = before.id
    userid_after = after.id
    username_before = before
    username_after =  after

    if nickname_after is not None:
        if nickname_before != nickname_after:
            print('Nickname changed from:', nickname_before, ' to: ', nickname_after)  # Debug print
            # Check if the record exists
            c.execute("SELECT * FROM members WHERE userid = ?", (str(userid_after),))
            result = c.fetchone()
            if result:
                print('Update the existing record with the new nickname: ', str(before.id))
                c.execute("UPDATE members SET nickname = ? WHERE userid = ?", (str(nickname_after), str(userid_after)))
                c.execute("UPDATE members SET username = ? WHERE userid = ?", (str(username_after), str(userid_after)))
            else:
                print('Insert a new record with the user ID and nickname')
                c.execute("INSERT INTO members (userid, nickname) VALUES (?, ?)", (str(userid_before), str(nickname_after)))
                c.execute("INSERT INTO members (userid, username) VALUES (?, ?)", (str(userid_before), str(username_after)))
            conn.commit()

    if after is not None:
        if before != after:
            print('Username changed from:', username_before, ' to: ', username_after)  # Debug print
            # Update the existing record with the new username
            c.execute("UPDATE members SET username = ? WHERE userid = ?", (str(username_after), str(userid_after)))
            conn.commit()

    if discord.utils.get(after.roles, name=MEMBER_ROLE) is not None:
        print(MEMBER_ROLE,'role assigned:', after)  # Debug print
        # Insert or replace the Member role in the database when assigned
        c.execute("UPDATE members SET role = ? WHERE userid = ?", (MEMBER_ROLE, str(userid_after)))
        conn.commit()

@bot.event
async def on_presence_update(before, after):
    if before.status != after.status and after.status == discord.Status.offline:
        print('User', after, 'is now offline')  # Debug print
        # Insert or replace the timestamp in the database when a user goes offline
        c.execute("UPDATE members SET timestamp = ? WHERE userid = ?", (int(time.time()), str(after.id)))
        conn.commit()
    else:
        # Check if the record exists
        c.execute("SELECT * FROM members WHERE userid = ?", (str(after.id),))
        result = c.fetchone()
        if result:
            print(after, 'changed status from', before.status, 'to', after.status)
            c.execute("UPDATE members SET timestamp = ? WHERE userid = ?", (int(0), str(after.id)))
            conn.commit()
        else:
            #User does not yet exist in db
            print(after, 'changed status from', before.status, 'to', after.status, '(Adding user to DB)')
            c.execute("INSERT INTO members (userid, username, nickname) VALUES (?, ?, ?)", (str(before.id), str(before), str(before.nick)))
            conn.commit()
            if discord.utils.get(after.roles, name=MEMBER_ROLE) is not None:
                print(MEMBER_ROLE, 'role assigned:', after)  # Debug print
                # Insert or replace the Member role in the database when assigned
                c.execute("UPDATE members SET role = ? WHERE userid = ?", (MEMBER_ROLE, str(after.id)))
                conn.commit()

@bot.event
async def on_command(ctx):
    print("Command triggered:", ctx.command)  # Debug print

@bot.event
async def on_command_error(ctx, error):
    print("Command error:", error)  # Debug print

@bot.command()
async def whois(ctx, *, username):
    if ctx.author.id not in ALLOWED_USERS:
        await ctx.send("Sorry, you are not allowed to use this command.")
        return
    # Remove the '<@' and '>' symbol from the input if present
    if username.startswith('<@') and username.endswith('>'):
        username = username[2:-1]
    username = username.lower()
 
    # Retrieve member information from the database
    c.execute("SELECT * FROM members WHERE lower(username) = ? OR userid = ? OR lower(nickname) = ?", (username, username, username))
    member_data = c.fetchone()

    if member_data is None:
        await ctx.send("User not found.")
        return

    embed = discord.Embed(title="User Information", color=discord.Color.blue())
    embed.add_field(name="User ID", value=member_data[0], inline=False)
    if member_data[1] == "None":
        embed.add_field(name="Nickname", value=username[0:-5])
    else:
        embed.add_field(name="Nickname", value=member_data[1] if member_data[1] else "Not set")
    embed.add_field(name="Username", value=member_data[4] if member_data[4] else "Not set", inline=False)
    embed.add_field(name="Role", value=member_data[2] if member_data[2] else "Guest", inline=False)
    embed.add_field(name="Member since", value=convert_timestamp(member_data[5]) if member_data[5] else "Not available", inline=False)
    embed.add_field(name="Offline since", value=convert_timestamp(member_data[3]) if member_data[3] else "Not available")
    await ctx.send(embed=embed)

def convert_timestamp(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

@bot.command(aliases=['seen'])
async def lastseen(ctx, *, arg):
    if ctx.author.id not in ALLOWED_USERS:
        await ctx.send("Sorry, you are not allowed to use this command.")
        return
    # Check if the argument starts with '<@' and ends with '>'
    if arg.startswith('<@') and arg.endswith('>'):
        arg = arg[2:-1]  # Remove the '<@' and '>' symbols

    # Convert the argument to lowercase for case-insensitive search
    arg = arg.lower()

    # Find the member in the database by nickname (case-insensitive)
    c.execute("SELECT * FROM members WHERE lower(nickname) = ? or lower(username) = ? OR userid = ?", (arg, arg, arg))
    result = c.fetchone()

    if result:
        userid = result[0]
        nickname = result[1] if result[1] else "Not set"
        role = result[2] if result[2] else "Not set"
        timestamp = convert_timestamp(result[3]) if result[3] else "Not available"

        # Determine the user's online status
        member = ctx.guild.get_member(userid)
        online_status = "Offline" if member and member.status == discord.Status.offline else "Right meow"

        # Create an embed to display the last seen information
        embed = discord.Embed(title="Last Seen Information", color=0x00ff00)
        embed.add_field(name="Nickname", value=nickname, inline=False)
        embed.add_field(name="Role", value=role, inline=False)
        embed.add_field(name="Last Seen", value=timestamp, inline=False)
        embed.add_field(name="Online Status", value=online_status, inline=False)

        await ctx.send(embed=embed)
    else:
        await ctx.send("User not found in the database.")

@bot.command()
async def inactive(ctx):
    if ctx.author.id not in ALLOWED_USERS:
        await ctx.send("Sorry, you are not allowed to use this command.")
        return
    # Retrieve inactive members from the database
    c.execute("SELECT * FROM members WHERE timestamp IS NOT NULL AND timestamp != 0 AND timestamp <= ? AND role = ?",
              (int(time.time()) - (INACTIVE_DAYS * 24 * 60 * 60), MEMBER_ROLE))
    inactive_members = c.fetchall()

    if not inactive_members:
        await ctx.send("No inactive members found.")
        return

    embeds = []
    chunk_size = 8  # Number of members per embed
    for i in range(0, len(inactive_members), chunk_size):
        embed = discord.Embed(title="Inactive Members (>10 days)", color=discord.Color.blue())
        for member_data in inactive_members[i:i + chunk_size]:
            embed.add_field(name="Username", value=member_data[4])
            embed.add_field(name="Nickname", value=member_data[1] if member_data[1] else "Not set")
            #embed.add_field(name="Role", value=member_data[2] if member_data[2] else "Not set")
            embed.add_field(name="Offline Since", value=convert_timestamp(member_data[3]) if member_data[3] else "Not available")
            #embed.add_field(name="\u200b", value="\u200b")  # Add a blank field for spacing
        embeds.append(embed)

    for embed in embeds:
        await ctx.send(embed=embed)

def convert_timestamp(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

# Run the bot
bot.run(TOKEN)

