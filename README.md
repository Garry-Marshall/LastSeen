# LastSeen Discord Bot

A modular Discord bot for monitoring and tracking user activity across guilds. Tracks user joins, leaves, nickname changes, role updates, and presence status.

## Features

- **Multi-Guild Support**: Works across multiple Discord servers simultaneously
- **User Tracking**: Monitors joins, leaves, nickname changes, and role updates
- **Presence Monitoring**: Tracks when users go offline/online
- **Database Storage**: SQLite database with proper connection management
- **Slash Commands**: Modern Discord slash command interface
- **Admin Panel**: Interactive configuration with buttons and modals
- **Logging**: Daily log files with configurable log levels
- **Modular Design**: Clean separation of concerns with cogs

## Commands

### User Commands

- `/whois <user>` - Display detailed information about a user
- `/lastseen <user>` - Check when a user was last seen online
- `/seen <user>` - Alias for `/lastseen`
- `/inactive` - List all members inactive for more than configured days

### Admin Commands (Requires "Bot Admin" role or Administrator permission)

- `/config` - Open interactive configuration panel
  - Set notification channel for leave messages
  - Set inactive days threshold
  - Update all members in database
  - View current configuration

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- A Discord bot token

### Setup

1. **Clone or download this repository**

2. **Create Virtual Environment** (recommended)
   ```bash
   python -m venv venv

   # On Linux/Mac:
   source venv/bin/activate

   # On Windows:
   venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the bot**
   - On first run, a `.env` file will be created automatically
   - Edit `.env` and add your Discord bot token:
     ```env
     DISCORD_BOT_TOKEN=your-actual-bot-token-here
     ```

5. **Create Discord Bot Application**
   - Go to https://discord.com/developers/applications
   - Create a new application
   - Go to "Bot" section and create a bot
   - Copy the bot token and paste it into `.env`
   - Enable these Privileged Gateway Intents:
     - ✅ PRESENCE INTENT
     - ✅ SERVER MEMBERS INTENT
     - ✅ MESSAGE CONTENT INTENT

6. **Invite bot to your server**
   - Go to OAuth2 > URL Generator
   - Select scopes: `bot`, `applications.commands`
   - Select permissions:
     - Read Messages/View Channels
     - Send Messages
     - Embed Links
     - Read Message History
     - View Server Members (required)
   - Copy and open the generated URL to invite the bot

7. **Run the bot**
   ```bash
   python main.py
   ```

## Configuration

### Environment Variables (.env)

```env
# REQUIRED: Your Discord bot token
DISCORD_BOT_TOKEN=your-bot-token-here

# Database filename (default: lastseen_bot.db)
DB_FILE=lastseen_bot.db

# Logging level: debug, info, warning, error (default: info)
DEBUG_LEVEL=info

# Bot admin role name (default: Bot Admin)
BOT_ADMIN_ROLE_NAME=Bot Admin

# Default inactive days threshold (default: 10)
DEFAULT_INACTIVE_DAYS=10
```

### Per-Guild Configuration

Use `/config` command in your Discord server to set:
- **Notification Channel**: Where member leave messages are posted
- **Inactive Days**: Threshold for `/inactive` command

## Project Structure

```
lastseen_bot/
├── main.py                 # Entry point
├── .env                    # Configuration (created on first run)
├── .env.template           # Configuration template
├── requirements.txt        # Python dependencies
├── bot/
│   ├── __init__.py
│   ├── client.py          # Bot client setup
│   ├── config.py          # Configuration loader
│   └── utils.py           # Helper functions
├── cogs/
│   ├── __init__.py
│   ├── tracking.py        # Event listeners (join/leave/update)
│   ├── commands.py        # User commands (whois/lastseen/inactive)
│   └── admin.py           # Admin commands (/config)
├── database/
│   ├── __init__.py
│   └── db_manager.py      # Database connection & schema
└── logs/                  # Daily log files (auto-created)
    ├── 2024-01-17.log
    └── ...
```

## Database Schema

### Guilds Table
- `guild_id` (PRIMARY KEY) - Discord guild ID
- `guild_name` - Guild name
- `notification_channel_id` - Channel for leave notifications
- `inactive_days` - Threshold for inactive command
- `added_at` - When bot joined guild

### Members Table
- `guild_id, user_id` (COMPOSITE PRIMARY KEY) - Guild and user IDs
- `username` - Discord username
- `nickname` - Server nickname
- `join_date` - When user joined guild
- `last_seen` - Last offline timestamp (0 = currently online)
- `is_active` - 1 = active, 0 = left server
- `roles` - JSON array of role names

## Usage Examples

### Check when a user was last seen
```
/lastseen @username
/seen username
/lastseen 123456789012345678
```

### Get user information
```
/whois @username
/whois nickname
```

### List inactive members
```
/inactive
```
Shows all members inactive for more than the configured threshold.

### Configure the bot (Admin only)
```
/config
```
Opens an interactive panel with buttons for configuration.

## Features Explained

### Presence Tracking
- When a user goes **offline**, their `last_seen` timestamp is recorded
- When a user comes **online**, `last_seen` is set to 0 (indicating current activity)
- This allows accurate "last seen" tracking while showing online users as "Currently online"

### Multi-Guild Support
- Each guild has its own configuration (notification channel, inactive days)
- User data is tracked separately per guild
- Same user in multiple guilds = separate records for privacy

### Rejoining Members
- When a user rejoins a guild, their existing record is updated (not duplicated)
- `is_active` flag is set back to 1
- Username, nickname, and roles are refreshed

## Logging

Logs are stored in the `logs/` directory with daily rotation:
- Filename format: `YYYY-MM-DD.log`
- Log levels: debug, info, warning, error
- Configure via `DEBUG_LEVEL` in `.env`

## Troubleshooting

### Bot not responding to commands
- Ensure bot has proper permissions in your server
- Check that slash commands are synced (restart bot to re-sync)
- Verify bot token is correct in `.env`

### Presence tracking not working
- Enable **PRESENCE INTENT** in Discord Developer Portal
- Bot must be in fewer than 100 servers (or be verified)

### Database errors
- Ensure database file has write permissions
- Check logs for specific error messages

### Commands not showing up
- Wait a few minutes for Discord to sync commands globally
- Try kicking and re-inviting the bot

## Privacy & Data

This bot stores:
- User IDs, usernames, nicknames
- Join dates and last seen timestamps
- Role names
- Guild information

All data is stored locally in SQLite database. No data is sent to external services.

## Support

For issues or questions:
- Check the logs in `logs/` directory
- Review the error messages in console
- Ensure all configuration is correct

This is a personal project to keep track of Guild Members in Discord servers.
Feel free to use and modify to your liking.

## License

MIT License - See LICENSE file for details
