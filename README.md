# LastSeen Discord Bot

A modular Discord bot for monitoring and tracking user activity across guilds. Tracks user joins, leaves, nickname changes, role updates, and presence status.

## Features

- **Multi-Guild Support**: Works across multiple Discord servers simultaneously
- **User Tracking**: Monitors joins, leaves, nickname changes, and role updates
- **Presence Monitoring**: Tracks when users go offline/online
- **Activity Statistics**: Detailed server activity metrics with visual charts
- **Role-Based Visibility**: Optionally track only members with specific roles
- **Channel Restrictions**: Limit bot commands to specific channels
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
- `/server-stats` - View detailed server activity statistics with visual charts

### Admin Commands (Requires "LastSeen Admin" role or Administrator permission)

- `/config` - Open interactive configuration panel
  - Set notification channel for leave messages
  - Set inactive days threshold
  - Set bot admin role name
  - Configure user command permissions (toggle role requirement, set user role)
  - **Set track only roles** - Only track members with specific roles (optional)
  - **Set allowed channels** - Restrict bot commands to specific channels (optional)
  - Update all members in database
  - View current configuration
- `/health` - Check bot health status (uptime, latency, database, guild stats)
- `/help` - Show bot information and available commands

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- A Discord bot token

### Setup

1. **Clone or download this repository**
   ```bash
   git clone https://github.com/Garry-Marshall/LastSeen
   ```
   
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
     DISCORD_BOT_TOKEN=your-bot-token-here
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

# Log retention: Number of days to keep log files (default: 5)
# Older logs are automatically deleted on bot startup
DEBUG_LOGS_DAYS_TO_KEEP=5
```

### Per-Guild Configuration

Use `/config` command in your Discord server to configure:

#### Basic Settings
- **Notification Channel**: Where member leave messages are posted
- **Inactive Days**: Threshold for `/inactive` command (default: 10 days)
- **Bot Admin Role**: Which role can manage bot settings (default: "LastSeen Admin")

#### Command Access Control
- **User Role Required**: Toggle whether a specific role is required to use bot commands
- **User Role Name**: The role required to use bot commands (when enabled)
- **Allowed Channels**: Restrict bot commands to specific channels (optional)

#### Tracking Filter (Advanced)
- **Track Only Roles**: Only track members with specific roles (optional)

##### How Track Only Roles Works

**Overview:**
When you configure specific roles to track, the bot will **only monitor and store activity data** for members who have at least one of those designated roles. Members without any of the specified roles will be completely ignored by the tracking system.

**Configuration:**
- Via `/config` command → Click "🎯 Set Track Only Roles"
- Enter comma-separated role names: `Member, Verified, VIP`
- Leave empty to track all members (default behavior)

**What Gets Tracked:**
- **When roles ARE configured:**
  - ✅ Members WITH the specified roles → Fully tracked
  - ❌ Members WITHOUT any specified roles → Not tracked
- **When NO roles configured (empty):**
  - ✅ All members → Fully tracked (default)

**Example:**
```
Config: Track only "Member" and "Verified" roles

User A: Has "Member" role → ✅ Tracked
User B: Has "Verified" role → ✅ Tracked
User C: Has both roles → ✅ Tracked
User D: Has "Guest" only → ❌ Not tracked
User E: No roles → ❌ Not tracked
```

**Use Cases:**
- Large servers: Only track verified/active members, ignore guests
- Private communities: Only track "Member" role, ignore bots and visitors
- Gaming clans: Only track "Clan Member" role, ignore casual visitors
- Resource optimization: Reduce database size by ignoring inactive/unverified users

**Important Notes:**
- Role names are **case-sensitive** and must match exactly as they appear in Discord
- If a member gains a required role, tracking starts automatically
- If a member loses all required roles, tracking stops
- Existing database records remain but stop receiving updates

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

## Usage Examples

### Open help dialog
```
/help
```

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

### View server activity statistics
```
/server-stats
```
Displays comprehensive server activity metrics including:
- Current online/offline counts
- Activity breakdown by time period (last hour, 24h, 7d, 30d, 30d+)
- Activity percentages and engagement rates
- Visual ASCII chart of activity distribution

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

### Activity Statistics
- View comprehensive server activity metrics with `/server-stats`
- See online/offline distribution across different time periods
- Visual ASCII charts showing activity breakdown
- Track engagement rates and recent activity percentages

### Role-Based Visibility
- Optionally configure specific roles to track (via `/config`)
- Only members with designated roles will have their activity monitored
- Useful for tracking specific member groups (e.g., "Verified", "Member")
- Leave empty to track all members (default behavior)

### Channel Restrictions
- Limit bot commands to specific channels for better server organization
- Configure via `/config` with channel names or IDs
- Helps prevent command spam in general chat
- Leave empty to allow commands in all channels (default)

### Multi-Guild Support
- Each guild has its own configuration (notification channel, inactive days, role/channel filters)
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
- Automatic cleanup: Logs older than `DEBUG_LOGS_DAYS_TO_KEEP` days are automatically deleted on bot startup (default: 5 days)

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
