# 🚀 Ultimate Telegram Sync & Bypass Bot 🚀
> **An Advanced, Multi-Instance Scalable Telegram Bot for Bypassing Restricted Content, Smart Renaming, and Syncing Millions of Files.**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![MongoDB](https://img.shields.io/badge/MongoDB-Powered-green?style=for-the-badge&logo=mongodb)
![Pyrogram](https://img.shields.io/badge/Pyrogram-V2-red?style=for-the-badge&logo=telegram)
![License](https://img.shields.io/badge/License-MIT-purple?style=for-the-badge)

Welcome to the most powerful Telegram Restricted Content bypasser and file synchronizer. Designed to handle **massive datasets (3+ Million files)** using a parallel multi-bot architecture, intelligent state-saving, and automated metadata extraction.

---

## 🔥 Supercharged Features

*   **🔓 Restricted Content Bypass:** Download or forward files from private/restricted channels effortlessly.
*   **🧠 Smart Auto-Resume (Stateful Sync):** Powered by MongoDB. If the server crashes or restarts, the bot remembers exactly which message ID it processed last and resumes instantly. **Zero duplicates. Zero data loss.**
*   **🤖 Multi-Instance Ready:** Run 5, 10, or 50 bots simultaneously! Use the same MongoDB URI but different `DB_NAME` per instance to achieve insane parallel processing speeds.
*   **✨ Extreme Smart Renaming Engine:** 
    *   Automatically extracts Year, Resolution (480p, 720p, 1080p, 4K), Codec (HEVC, x264), and Audio Languages.
    *   Destroys junk words, promotional links, and spam tags from filenames.
*   **⚡ Live Watcher (Auto-Forwarding):** Set up a live monitor. The second a new file is uploaded to the source, it is instantly renamed and routed to multiple target channels.
*   **🎛 Content Filtering:** Only want Videos and Documents? The built-in filter ignores spam text, stickers, and photos.
*   **🛡 Anti-Flood Protection:** Built-in dynamic sleep and Pyrogram FloodWait handlers to keep your Telegram accounts safe from bans.

---

## 🛠️ Environment Variables (Config)

To run this bot on Render, Koyeb, Heroku, or VPS, you need to set the following Environment Variables:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `API_ID` | Your Telegram API ID from my.telegram.org | `1234567` |
| `API_HASH` | Your Telegram API Hash | `0123456789abcdef0123456789abcdef` |
| `BOT_TOKEN` | Your Bot Token from @BotFather | `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11` |
| `DB_URI` | Your MongoDB Connection String | `mongodb+srv://admin:pass@cluster...` |
| `DB_NAME` | Database Name (Unique for each bot instance!) | `SyncBot_1` |
| `ADMINS` | List of Admin User IDs (Comma separated) | `12345678, 87654321` |
| `LOG_CHANNEL` | Channel ID for system logs & errors | `-1001234567890` |

---

## 💻 Bot Commands

### 👑 Admin / Setup
*   `/login` - Login via Phone Number to generate a session (Required for private channels).
*   `/logout` - Safely terminate and wipe the current session.
*   `/botstats` - View detailed system stats, active users, and download ETA.
*   `/status` - Check RAM, CPU, Disk space, and Uptime.

### 📥 Processing & Syncing
*   `/dl` - Reply to a link or send `/dl [link]` to start a batch/single download.
*   `/cancel` - Open the interactive menu to stop any ongoing task safely.

### 👀 Live Watcher Engine
*   `/watch [link]` - Setup a 24/7 live monitor for a channel.
*   `/watchers` - List all active watchers.
*   `/unwatch [source_id]` - Stop monitoring a specific source.
*   `/removetarget` - Remove a specific destination from a watcher.

---

## 🏗️ Multi-Bot Deployment Strategy (For Massive Data)
Transferring millions of files with one account will hit Telegram's limits. 
**The Solution:**
1. Create multiple Render/Koyeb apps (e.g., Bot1, Bot2, Bot3).
2. Connect them all to this exact same GitHub Repository.
3. Give each app a unique `BOT_TOKEN` and a unique `DB_NAME` (e.g., `DB_NAME=Cluster1`, `DB_NAME=Cluster2`).
4. Divide your workload (e.g., Bot 1 handles files 1-50,000, Bot 2 handles 50,001-100,000).
5. Watch them sync at lightning speed! ⚡

---
<p align="center">
  <b>Built with ❤️ for High-Performance Telegram Automation.</b>
</p>
