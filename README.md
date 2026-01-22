# LastSeen Discord Bot

A modular Discord bot for monitoring and tracking user activity across guilds. Tracks user joins, leaves, nickname changes, role updates, and presence status.


 - Invite LastSeen into your Discord Server: [Invite Bot](https://discord.com/oauth2/authorize?client_id=1068871708322320445) 
 - Get support in our Community Discord Server: [Get Support](https://discord.gg/d3N5sd58fh)

## Core Features

### 🚀 Quick Setup
Guided step-by-step wizard for first-time configuration. No guesswork—just follow the prompts to set up notification channels, inactive thresholds, timezones, and more.

### 👁️ LastSeen Tracking
Know exactly when members were last online. Track presence changes, join/leave times, and activity patterns across your entire server.

### 💤 Inactive Members
Instantly identify members who've been offline beyond your threshold. Perfect for engagement campaigns, role cleanup, or understanding server health.

### 👤 WhoIs (Member Profiles)
Comprehensive member information at a glance: join date, roles, nickname history, last seen, and message activity stats.

### 📊 Analytics Dashboard
Interactive statistics with retention reports, growth trends, activity leaderboards, and heatmaps. Export to CSV for deeper analysis.

### 📅 Scheduled Reports
Automated weekly/monthly reports delivered to your chosen channel. Track activity, new members, and departures without lifting a finger.

---

## Features

- **Multi-Guild Support**: Works across multiple Discord servers simultaneously
- **User Tracking**: Monitors joins, leaves, nickname changes, and role updates
- **Presence Monitoring**: Tracks when users go offline/online
- **Activity Statistics**: Detailed server activity metrics with visual charts
- **Scheduled Reports**: Automated weekly/monthly reports with activity, new members, and departures
- **Timezone Support**: Guild-specific timezone configuration for accurate timestamp display
- **Role-Based Visibility**: Optionally track only members with specific roles
- **Channel Restrictions**: Limit bot commands to specific channels
- **Database Storage**: SQLite database with connection pooling for optimal performance
- **Message Activity Tracking**: Monitor message posting patterns and trends with buffered writes
- **Slash Commands**: Modern Discord slash command interface
- **Admin Panel**: Interactive configuration with buttons and modals
- **Quick Setup Wizard**: Step-by-step guided configuration for first-time users
- **Data Retention**: Configurable message activity retention periods per guild
- **Logging**: Daily log files with configurable log levels and automatic cleanup
- **Modular Design**: Clean separation of concerns with cogs
- **Performance Optimized**: Connection pooling, buffered writes, indexed queries for high-load servers

## Commands

### User Commands (Requires "LastSeen User" if "User Role Required" = ON)

- `/whois <user>` - Display detailed information about a user
- `/lastseen <user>` - Check when a user was last seen online
- `/seen <user>` - Alias for `/lastseen`
- `/inactive` - List all members inactive for more than configured days
- `/chat-history <user>` - Show message posting stats for the last year
- `/server-stats` - View detailed server activity statistics with visual charts
- `/help` - Show available commands
- `/about` - Show bot information

### Admin Commands (Requires "LastSeen Admin" role or Administrator permission)

- `/config` - Open interactive configuration panel
  - **🚀 Quick Setup** - Guided wizard for first-time setup (recommended for new users)
  - Set notification channel for leave messages
  - Set inactive days threshold
  - Set bot admin role name
  - Configure user command permissions (toggle role requirement, set user role)
  - Set track only roles - Only track members with specific roles (optional)
  - Set allowed channels - Restrict bot commands to specific channels (optional)
  - **Configure timezone** - Set guild-specific timezone for accurate timestamps (e.g., America/New_York, Europe/London)
  - **Set message retention** - Configure how long to keep message activity data (default: 365 days)
  - **Configure scheduled reports** - Set up automated weekly/monthly reports
    - Choose report channel
    - Select frequency (weekly, monthly, or both)
    - Pick report types (activity, new members, departures)
    - Set delivery day (day of week for weekly, day of month for monthly)
  - Update all members in database
  - View current configuration
- `/search` - Advanced member search and filtering
  - Search with multiple filters: roles, status, inactive days, activity, join date, username
  - Export results to CSV or TXT format
  - Paginated results with interactive navigation
- `/user-stats` - User statistics and analytics dashboard
  - Interactive buttons for detailed reports
  - Retention analysis, growth trends, activity leaderboard
  - Activity heatmap and comprehensive CSV export
- `/role-history <user>` - Show the last 20 role changes for a member
- `/health` - Check bot health status (uptime, latency, database, guild stats)
- `/help` - Show available commands

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

#### 🚀 Quick Setup (Recommended for First-Time Users)

The **Quick Setup wizard** provides a step-by-step guided walkthrough of essential configuration:

1. **Notification Channel** - Where to post member leave alerts
2. **Inactive Days Threshold** - When members appear in `/inactive` command
3. **Bot Admin Role** - Which role can manage bot settings (optional)
4. **Server Timezone** - For accurate timestamp display (optional)
5. **Summary** - Review all settings at once

**Features:**
- Interactive buttons to configure each step
- Clear explanations of why each setting matters
- Recommended values and examples
- Skip optional steps
- Progress tracking (Step X/5)
- 10-minute timeout for careful configuration

**How to Access:**
- Use `/config` command
- Click the **🚀 Quick Setup** button
- Follow the prompts to configure essential settings

#### Manual Configuration

Prefer to configure individual settings? Use the other buttons in `/config`:

#### Basic Settings
- **Notification Channel**: Where member leave messages are posted
- **Inactive Days**: Threshold for `/inactive` command (default: 10 days)
- **Bot Admin Role**: Which role can manage bot settings (default: "LastSeen Admin")

#### Command Access Control
- **User Role Required**: Toggle whether a specific role is required to use bot commands
- **User Role Name**: The role required to use bot commands (when enabled)
- **Allowed Channels**: Restrict bot commands to specific channels (optional)

#### Timezone Configuration
- **Server Timezone**: Set your guild's timezone for accurate timestamp display
  - All timestamps in `/lastseen`, `/whois`, and reports will be shown in your local time
  - Supports all IANA timezone names (e.g., `America/New_York`, `Europe/London`, `Asia/Tokyo`)
  - Defaults to UTC if not configured
  - Timezone names are validated against the official pytz timezone database

#### Message Activity & Retention
- **Message Retention Days**: Control how long message activity data is stored
  - Configure per guild (default: 365 days)
  - Automatic cleanup runs daily to remove old records
  - Helps manage database size for large servers
  - Separate retention for daily and hourly activity data

#### Scheduled Reports
- **Automated Reports**: Configure weekly and/or monthly automated reports
  - **Report Channel**: Choose which channel receives the reports
  - **Frequency**: Weekly, monthly, or both
  - **Report Types**:
    - **Activity Report**: Message statistics, peak day, top 5 contributors
    - **New Members**: List of members who joined during the period
    - **Departures**: Members who left during the period (with last seen info)
  - **Delivery Schedule**:
    - Weekly: Choose day of week (0=Monday to 6=Sunday)
    - Monthly: Choose day of month (1-28)
  - **Smart Features**:
    - Duplicate prevention (won't send same report twice on restarts)
    - Rate limiting (60-second minimum between reports)
    - Automatic retry on failures (up to 3 attempts with exponential backoff)
    - Pagination for large departure lists (25 members per embed)

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
│   └── admin/             # Admin cog (modular structure)
│       ├── __init__.py            # Package setup & exports
│       ├── admin_cog.py           # Admin slash commands
│       ├── config_view.py         # Interactive config UI
│       ├── permissions.py         # Permission checking utilities
│       ├── channel_config.py      # Channel & timing modals
│       ├── role_config.py         # Role-based access modals
│       ├── channel_filter.py      # Channel restriction modal
│       └── member_mgmt.py         # Member sync logic
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

### Search and filter members (Admin only)
```
/search
```
Advanced member search with multiple filter options. Combine filters to find specific members.

**Available Filters:**
- `roles` - Filter by one or more roles (e.g., `@Moderator,@Admin`)
- `status` - Filter by presence: `online`, `offline`, `idle`, `dnd`, or `all`
- `inactive` - Days since last seen (e.g., `>30`, `<7`, `=14`)
- `activity` - Message count in last 30 days (e.g., `>100`, `<10`)
- `joined` - Filter by join date (e.g., `>2025-01-01`, `<2024-06-01`)
- `username` - Search username (partial match, case-insensitive)
- `export` - Export results as `csv` or `txt` format

**Examples:**

Find all Moderators who are currently online:
```
/search roles:@Moderator status:online
```

Find members inactive for more than 30 days with less than 10 messages:
```
/search inactive:>30 activity:<10
```

Find all members who joined after January 1, 2025 and export to CSV:
```
/search joined:>2025-01-01 export:csv
```

Find members with "admin" in their username:
```
/search username:admin
```

Combine multiple filters for precise results:
```
/search roles:@Member status:offline inactive:>7 activity:<50
```

**Features:**
- Paginated results (15 members per page)
- Interactive navigation with Previous/Next buttons
- Export buttons in pagination view for CSV or TXT
- Results are ephemeral (only visible to you)
- Supports up to 1000 results with warning

### View server statistics and analytics (Admin only)
```
/user-stats
```
Interactive statistics dashboard with comprehensive server analytics.

**Overview Display:**
- Total members with growth percentage
- Active vs inactive member breakdown
- This month's joins, leaves, and net growth
- Activity metrics (total messages, average per member)
- Most active member identification

**Interactive Buttons:**

📊 **Retention Report**
- Cohort analysis for members who joined in last 30/60/90 days
- Retention rates and active member percentages
- Helps identify drop-off patterns

📈 **Server Growth**
- Growth trends for 30/90/365 day periods
- Join vs leave comparison
- Growth rate percentages
- Net growth tracking

🏆 **Leaderboard**
- Top 10 most active members by message count
- Period selection: 7 days, 30 days, 90 days, or all-time
- Medal rankings for top 3 members

🔥 **Activity Heatmap**
- Day-of-week activity distribution
- Visual bar charts showing peak days
- Message count breakdown by day

📋 **Export CSV**
- Comprehensive stats report
- Includes overview, growth metrics, and top 25 members
- Ephemeral delivery for privacy

**Example Usage:**
```
/user-stats
```
Opens the interactive dashboard. Click any button to view detailed reports, then use "Back to Overview" to return.

## Features Explained

### Presence Tracking
- When a user goes **offline**, their `last_seen` timestamp is recorded
- When a user comes **online**, `last_seen` is set to 0 (indicating current activity)
- This allows accurate "last seen" tracking while showing online users as "Currently online"

### Message Activity Tracking
- **Efficient Buffered Writes**: Messages are batched and written every 30 seconds
- **Dual Granularity**: Tracks both daily aggregates and hourly breakdowns
- **Retry Logic**: Failed writes are automatically retried on next flush
- **Buffer Protection**: Automatic flush when buffer size reaches 10,000 entries
- **Performance Optimized**: Uses single database calls with count parameters

### Activity Statistics
- View comprehensive server activity metrics with `/server-stats`
- See online/offline distribution across different time periods
- Visual ASCII charts showing activity breakdown
- Track engagement rates and recent activity percentages
- Message activity tracked with hourly and daily granularity

### Scheduled Reports
- **Automated Delivery**: Reports are sent automatically based on your configured schedule
- **Smart Scheduling**: 
  - Weekly reports check day of week (Monday=0, Sunday=6)
  - Monthly reports check day of month (1-28 for reliability across all months)
  - Hourly check task ensures reports are sent within 1 hour of scheduled time
- **Duplicate Prevention**: Date-based tracking ensures reports are only sent once per day
- **Graceful Failures**: Missing permissions or unavailable channels are logged without crashing
- **Content Filtering**: Empty reports (no new members, no departures) are automatically omitted

### Timezone Support
- **Guild-Specific Timezones**: Each server can set its own timezone
- **Automatic Conversion**: All timestamps are converted to guild timezone
- **Validation**: Invalid timezone strings fallback to UTC with logging
- **Comprehensive Coverage**: Supports all IANA timezone names
- **Consistent Display**: Timestamps in commands and reports use same timezone

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

### Performance & Reliability
- **Connection Pooling**: Database connection pool (5 connections) reduces overhead by ~90%
- **Buffered Writes**: Message activity is batched every 30 seconds for efficiency
- **Async-Safe Shutdown**: Gracefully flushes all buffers using thread pool during shutdown
- **Failed Write Retry**: Automatically retries failed database writes on next flush cycle
- **Input Sanitization**: Role names are sanitized (length limits, control character removal)
- **Database Indexes**: Optimized queries with strategic indexes on frequently accessed columns
- **Rate Limiting**: Built-in protection against Discord API rate limits with exponential backoff

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
- Database migrations run automatically on startup
- Connection pool closes gracefully on shutdown

### Commands not showing up
- Wait a few minutes for Discord to sync commands globally
- Try kicking and re-inviting the bot

### Scheduled reports not sending
- Verify report channel exists and bot has send permissions
- Check timezone configuration matches your expectations
- Review logs for "Sending weekly/monthly report" messages
- Reports only send if there's content (empty reports are skipped)
- Duplicate prevention: reports won't send twice on same calendar day

### Timezone issues
- Ensure timezone string is valid IANA format (e.g., `America/New_York`)
- Check logs for "Invalid timezone" warnings
- Invalid timezones automatically fallback to UTC
- Test with `/lastseen` command to verify timezone display

## Privacy & Data

This bot stores:
- User IDs, usernames, nicknames
- Join dates, last seen timestamps, message counts
- Role names
- Guild information

All data is stored locally in SQLite database. No data is sent to external services.

## Support

For issues or questions:
- Check the logs in `logs/` directory
- Review the error messages in console
- Ensure all configuration is correct
- Get in touch through GitHub or Discord.

This is a personal project to keep track of Guild Members in Discord servers.
Feel free to use and modify to your liking.

## License

MIT License - See LICENSE file for details
