# -*- coding: utf-8 -*-
import os
import psutil
import time
import asyncio
import re
import shutil
import subprocess
import gc
import datetime
import uuid
from pathlib import Path
from collections import defaultdict
import motor.motor_asyncio
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated, UserAlreadyParticipant,
    InviteHashExpired, UsernameNotOccupied, FileReferenceExpired, UserNotParticipant,
    ApiIdInvalid, PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, PasswordHashInvalid, PeerIdInvalid, AuthKeyUnregistered, UserDeactivated
)

from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message, 
    BotCommand, BotCommandScopeDefault, BotCommandScopeChat, CallbackQuery
)
from concurrent.futures import ThreadPoolExecutor
import sys

# --- LOGGER SETUP ---
class LoggerWriter:
    def __init__(self, is_stderr=False):
        self.terminal = sys.stderr if is_stderr else sys.stdout
        self.log = open("bot.log", "a", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    def flush(self):
        self.terminal.flush()
        self.log.flush()

# Redirect all prints and tracebacks to bot.log as well as the console
sys.stdout = LoggerWriter(is_stderr=False)
sys.stderr = LoggerWriter(is_stderr=True)

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

API_ID = int(os.environ.get("API_ID", "") or 0)
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_URI = os.environ.get("DB_URI", "")
DB_NAME = os.environ.get("DB_NAME", "")
STRING_SESSION = os.environ.get("STRING_SESSION", None)

# Error Log Channel (Optional)
# Usage: "-100xxxx" for channel, or "-100xxxx/5" for Group Topic
LOG_CHANNEL = os.environ.get("LOG_CHANNEL", "") 

# Queue System
TASK_QUEUE = defaultdict(list) # Stores pending tasks: user_id -> [task_data, ...]

# Create a thread pool for blocking tasks
io_executor = ThreadPoolExecutor(max_workers=4)

LOGIN_SYSTEM = os.environ.get("LOGIN_SYSTEM", "True").lower() == "true"
ERROR_MESSAGE = os.environ.get("ERROR_MESSAGE", "True").lower() == "true"
WAITING_TIME = int(os.environ.get("WAITING_TIME", 3))

admin_str = os.environ.get("ADMINS", "")
ADMINS = [int(x) for x in admin_str.split(",") if x.strip().isdigit()]

sudo_str = os.environ.get("SUDOS", "")
SUDOS = [int(x) for x in sudo_str.split(",") if x.strip().isdigit()]

HELP_TXT = """<b>📚 BOT'S USAGE GUIDE</b>

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬✘▬
<blockquote expandable>
<b>🟢 1. SINGLE & BATCH DOWNLOADS</b>
• Send a single link to process one post.
• Send links in a "From - To" format to process multiple files at once.
• Works for both Public and Private links.
• <b>Examples:</b>
  ├ <code>https://t.me/xxxx/1001</code>
  └ <code>https://t.me/c/xxxx/101 - 120</code>

<b>👀 2. LIVE WATCHERS (AUTO-FORWARDING)</b>
• Automatically monitor a source and forward new messages to targets.
• Supports routing to <b>Multiple Targets</b> simultaneously!
• Features built-in <b>Content Filtering</b> (e.g., Only Videos & Docs).
• <b>Setup:</b> Send <code>/watch https://t.me/channel/123</code>
• <b>Manage:</b> Use <code>/watchers</code> to view and delete mappings.

<b>🤖 3. BOT CHATS & RESTRICTED CONTENT</b>
• Send the link with <code>/b/</code>, the bot's username, and message ID.
• <b>Format:</b> <code>https://t.me/b/botusername/4321</code>
• Bypasses "Saving Restricted Content" limits automatically!

<b>🛠 4. USEFUL COMMANDS</b>
• <code>/dl</code> - Reply to a link to process it.
• <code>/watch</code> - Setup a new live auto-forwarder.
• <code>/watchers</code> (or <code>/list</code>) - View active watchers & filters.
• <code>/removetarget</code> - Remove a specific destination.
• <code>/removesource</code> (or <code>/unwatch</code>) - Stop watching a source.
• <code>/cancel</code> - Cancel ongoing tasks.
</blockquote>
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬✘▬"""

# ==============================================================================
# --- DATABASE ---
# ==============================================================================

class Database:
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            session = None,
            api_id = None,
            api_hash = None,
        )

    async def add_user(self, id, name):
        user = self.new_user(id, name)
        if not await self.is_user_exist(id):
            await self.col.insert_one(user)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    async def get_all_users(self):
        cursor = self.col.find({})
        return cursor

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def set_session(self, id, session):
        await self.col.update_one({'id': int(id)}, {'$set': {'session': session}})

    async def get_session(self, id):
        user = await self.col.find_one({'id': int(id)})
        if user:
            return user.get('session')
        return None

    async def set_api_id(self, id, api_id):
        await self.col.update_one({'id': int(id)}, {'$set': {'api_id': api_id}})

    async def get_api_id(self, id):
        user = await self.col.find_one({'id': int(id)})
        if user:
            return user.get('api_id')
        return None

    async def set_api_hash(self, id, api_hash):
        await self.col.update_one({'id': int(id)}, {'$set': {'api_hash': api_hash}})

    async def get_api_hash(self, id):
        user = await self.col.find_one({'id': int(id)})
        if user:
            return user.get('api_hash')
        return None

    async def total_session_users_count(self):
        count = await self.col.count_documents({"session": {"$ne": None}})
        return count

    # --- WATCHER METHODS ---
    async def add_watcher(self, user_id, source_id, dest_id, source_thread=None, dest_thread=None, delay=0, is_restricted=False, source_title=None, dest_title=None, allowed_types=None):
        if allowed_types is None:
            allowed_types = ["Video", "Document"] # Default strict filter

        query = {
            'source_id': int(source_id),
            'source_thread': int(source_thread) if source_thread else None
        }
        
        new_target = {
            "dest_id": int(dest_id),
            "dest_thread": int(dest_thread) if dest_thread else None,
            "dest_title": dest_title
        }

        existing = await self.db.watchers.find_one(query)
        if existing:
            targets = existing.get("targets", [])
            # Convert legacy dest_id to targets automatically
            if not targets and 'dest_id' in existing:
                targets = [{"dest_id": existing['dest_id'], "dest_thread": existing.get('dest_thread'), "dest_title": existing.get('dest_title')}]
                
            target_exists = any(t["dest_id"] == new_target["dest_id"] and t.get("dest_thread") == new_target["dest_thread"] for t in targets)
            
            if not target_exists:
                targets.append(new_target)
                await self.db.watchers.update_one(query, {
                    "$set": {"targets": targets, "allowed_types": allowed_types, "delay": int(delay)}
                })
            else:
                await self.db.watchers.update_one(query, {"$set": {"allowed_types": allowed_types, "delay": int(delay)}})
        else:
            watcher = {
                "user_id": user_id,
                "source_id": int(source_id),
                "source_thread": int(source_thread) if source_thread else None,
                "delay": int(delay),
                "is_restricted": is_restricted,
                "source_title": source_title,
                "allowed_types": allowed_types,
                "targets": [new_target],
                "created_at": datetime.datetime.now()
            }
            await self.db.watchers.insert_one(watcher)

    async def get_watcher(self, source_id, source_thread=None):
        query = {'source_id': int(source_id)}
        if source_thread:
            query['source_thread'] = int(source_thread)
        return await self.db.watchers.find_one(query)

    async def get_all_watchers(self):
        return self.db.watchers.find({})
        
    async def remove_watcher(self, source_id):
        result = await self.db.watchers.delete_many({'source_id': int(source_id)})
        return result.deleted_count > 0

    async def remove_watcher_target(self, source_id, dest_id, source_thread=None):
        query = {'source_id': int(source_id)}
        if source_thread is not None:
            query['source_thread'] = int(source_thread)
            
        result = await self.db.watchers.update_one(
            query,
            {"$pull": {"targets": {"dest_id": int(dest_id)}}}
        )
        
        # If targets array becomes empty, delete the whole watcher automatically
        doc = await self.db.watchers.find_one(query)
        if doc and not doc.get("targets"):
            await self.db.watchers.delete_one(query)
            
        return result.modified_count > 0

db = Database(DB_URI, DB_NAME)

# ==============================================================================
# --- CLIENT & GLOBAL STATE ---
# ==============================================================================

app = Client(
    "RestrictedBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50,                 
    sleep_threshold=20,
    # max_concurrent_transmissions=10, 
    ipv6=False                    
)

# ==============================================================================
# --- NATIVE ASK IMPLEMENTATION (REPLACES PYROMOD) ---
# ==============================================================================
ASK_FUTURES = {}

async def custom_ask(self, chat_id: int, text: str, filters=None, timeout: int = 300, **kwargs):
    """Native implementation of bot.ask() to bypass pyromod bugs."""
    msg = await self.send_message(chat_id, text, **kwargs)
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    ASK_FUTURES[chat_id] = (future, filters)
    
    try:
        return await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        ASK_FUTURES.pop(chat_id, None)
        
        # Simulate a /cancel message if the user times out
        class MockMessage:
            text = "/cancel"
            async def reply(self, reply_text, *args, **kw):
                return await self._client.send_message(chat_id, reply_text, *args, **kw)
                
        mock_msg = MockMessage()
        mock_msg._client = self
        return mock_msg

# Bind the custom method to all Pyrogram Clients
Client.ask = custom_ask

# A specialized background listener to catch the user's reply
@app.on_message(filters.private, group=-1)
async def ask_listener(client, message):
    chat_id = message.chat.id
    if chat_id in ASK_FUTURES:
        future, filter_cond = ASK_FUTURES[chat_id]
        
        if filter_cond:
            is_match = filter_cond(client, message)
            if asyncio.iscoroutine(is_match):
                is_match = await is_match
            if not is_match:
                return # Ignore this message, wait for a matching one
                
        ASK_FUTURES.pop(chat_id, None)
        if not future.done():
            future.set_result(message)
        message.stop_propagation()
# ==============================================================================

BOT_START_TIME = time.time()

ACTIVE_PROCESSES = defaultdict(dict)  # user_id -> { task_uuid: info_dict, ... }
CANCEL_FLAGS = {}  # task_uuid -> True when cancelled

batch_temp = type("BT", (), {})()
batch_temp.ACTIVE_TASKS = defaultdict(int)
# IS_BATCH removed — cancellation is handled per task_uuid via CANCEL_FLAGS

# --- ROBUST CONCURRENCY SETTINGS ---
# 1. Server Limit: Max 30 uploads total (Protects your server CPU/Bandwidth)
SERVER_UPLOAD_LIMIT = asyncio.Semaphore(30) 

# 2. User Limit: Max 3 uploads per user (Protects user from FloodWait)
USER_SEMAPHORE_LIMIT = 3 
USER_SEMAPHORES = defaultdict(lambda: asyncio.Semaphore(USER_SEMAPHORE_LIMIT))
# -----------------------------------

PENDING_TASKS = {}
PROGRESS = {}
SESSION_STRING_SIZE = 351

MAX_CONCURRENT_TASKS_PER_USER = int(os.environ.get("MAX_TASKS_PER_USER", "3"))

USER_CLIENTS = {} # Dictionary: {user_id: Client_Object}

ALL_MSG_TYPES = ["Video", "Document", "Text", "Audio", "Photo", "Voice", "Animation", "Sticker"]

# ==============================================================================
# --- HELPERS ---
# ==============================================================================

def _pretty_bytes(n: float) -> str:
    try:
        n = float(n)
    except Exception:
        return "0 B"
    if n == 0: return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    unit = units[i]
    if unit == "B": return f"{int(n)} {unit}"
    else: return f"{n:.1f} {unit}"

def get_readable_time(seconds: int) -> str:
    try:
        seconds = int(seconds)
    except Exception:
        seconds = 0
    if seconds <= 0: return "0s"
    time_parts = []
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: time_parts.append(f"{h}h")
    if m > 0: time_parts.append(f"{m}m")
    if not time_parts or s > 0: time_parts.append(f"{s}s")
    return " ".join(time_parts)

def generate_bar(percent: float, length: int = 12) -> str:
    """Generates a status bar in the style: 〘⬤⬤⬤⬤◔○○○○○○○〙 34.3%"""
    filled_length = int(length * percent / 100)
    # Determine if we need a half-filled circle (◔)
    fraction = (percent / 100 * length) - filled_length
    has_half = fraction >= 0.5
    
    bar = '⬤' * filled_length
    if has_half and filled_length < length:
        bar += '◔'
        bar += '○' * (length - filled_length - 1)
    else:
        bar += '○' * (length - filled_length)
        
    return f"〘{bar}〙 {percent:.1f}%"
    
def sanitize_filename(filename: str) -> str:
    if not filename: return "unnamed_file"
    filename = re.sub(r'[:]', "-", filename)
    filename = re.sub(r'[\\/*?"<>|\[\]]', "", filename)
    name, ext = os.path.splitext(filename)
    if len(name) > 60:
        name = name[:60]
    if not ext:
        ext = ".dat"
    return f"{name}{ext}"

async def check_link_restriction(user_id, link_text):
    """
    Analyzes the link to determine if the source content is restricted.
    Handles both Post Links (t.me/c/xx/100) and Channel Links (t.me/c/xx).
    """
    # 1. Standardize the link
    clean_text = link_text.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("c/", "")
    
    # Remove any range "100-200"
    if "-" in clean_text:
        clean_text = clean_text.split("-")[0].strip()
        
    parts = clean_text.split("/")
    
    is_private = False
    chat_id = None
    msg_id = None

    try:
        if "t.me/b/" in link_text:
            return False, "🤖 **Bot Link:** Content availability depends on the bot."
            
        # LOGIC FIX: Distinguish between Channel ID and Message ID
        if "t.me/c/" in link_text:
            # Private Link Format: ID / (Optional Topic) / (Optional MsgId)
            is_private = True
            chat_id = int("-100" + parts[0])
            
            # If there's more than 1 part, the last part MIGHT be a message ID
            if len(parts) > 1 and parts[-1].isdigit():
                msg_id = int(parts[-1])
        else:
            # Public Link Format: Username / (Optional MsgId)
            chat_id = parts[0]
            if len(parts) > 1 and parts[-1].isdigit():
                msg_id = int(parts[-1])
            
    except Exception as e:
        return None, f"⚠️ **Could not analyze link.** Error: {e}"

    # 2. Select the Client (User vs Bot)
    is_temp_client = False
    check_client = app 
    
    if is_private:
        user_session = await db.get_session(user_id)
        if not user_session:
            return None, "🔒 **Private Link:** Please /login to verify restrictions."
        
        api_id = await db.get_api_id(user_id)
        api_hash = await db.get_api_hash(user_id)
        check_client = Client(f"check_{user_id}_{int(time.time())}", session_string=user_session, api_id=api_id, api_hash=api_hash, no_updates=True, ipv6=False, in_memory=True)
        is_temp_client = True

    # 3. Check the Content
    is_restricted = False
    status_msg = ""
    
    try:
        if is_temp_client:
            await check_client.connect()
            
        # If we have a specific message ID, check that message
        if msg_id:
            msg = await check_client.get_messages(chat_id, msg_id)
            if getattr(msg.chat, "has_protected_content", False) or getattr(msg, "has_protected_content", False):
                is_restricted = True
                status_msg = "🔒 **Source is RESTRICTED** (Will use Download Mode)"
            else:
                is_restricted = False
                status_msg = "🔓 **Source is PUBLIC/UNRESTRICTED** (Will use Fast Forward)"
        
        # If no message ID (Whole Channel Link), check the Chat info
        else:
            chat = await check_client.get_chat(chat_id)
            if getattr(chat, "has_protected_content", False):
                is_restricted = True
                status_msg = "🔒 **Channel is RESTRICTED** (Will use Download Mode)"
            else:
                is_restricted = False
                status_msg = "🔓 **Channel is PUBLIC/UNRESTRICTED**"
            
    except Exception as e:
        if "CHANNEL_PRIVATE" in str(e) or "USER_NOT_PARTICIPANT" in str(e):
            status_msg = "⚠️ **Private Chat:** I can't check yet (You need to join first)."
        else:
            status_msg = f"⚠️ **Check Failed:** `{str(e)[:30]}...`"
    finally:
        if is_temp_client:
            try: await check_client.disconnect()
            except: pass
        
    return is_restricted, status_msg
    
async def split_file_python(file_path, chunk_size=2000*1024*1024):
    """
    Async wrapper that runs the blocking smart split function in a separate thread.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(io_executor, _split_file_smart, file_path, chunk_size)

def _split_file_smart(file_path, chunk_size):
    """
    Tiered Splitting Logic:
    1. Try Linux 'split' (Fastest, Zero RAM)
    2. Try '7z' (Fast, Low RAM, Archive format)
    3. Fallback to Python (Safe, Optimized Buffer)
    """
    file_path = Path(file_path)
    if not file_path.exists():
        return []

    file_size = os.path.getsize(file_path)
    if file_size <= chunk_size:
        return [file_path]

    # --- TIER 1: LINUX 'SPLIT' (Preferred) ---
    if shutil.which("split"):
        try:
            output_prefix = f"{file_path}.part"
            cmd = ["split", "-b", str(chunk_size), "-d", "-a", "3", str(file_path), output_prefix]
            subprocess.run(cmd, check=True, capture_output=True)
            parts = sorted(list(file_path.parent.glob(f"{file_path.name}.part*")))
            if parts: return parts
        except Exception: pass

    # --- TIER 2: 7-ZIP (Store Mode) ---
    seven_z_exe = shutil.which("7z") or shutil.which("7za")
    if seven_z_exe:
        try:
            output_archive = f"{file_path}.7z"
            cmd = [seven_z_exe, "a", f"-v{chunk_size}b", "-mx0", output_archive, str(file_path)]
            subprocess.run(cmd, check=True, capture_output=True)
            parts = sorted(list(file_path.parent.glob(f"{file_path.name}.7z.*")))
            if parts: return parts
        except Exception: pass

    # --- TIER 3: PYTHON (Fallback / Low RAM) ---
    part_num = 0
    parts = []
    # [OPTIMIZATION] Low Buffer for Koyeb Free Tier (2MB)
    buffer_size = 2 * 1024 * 1024 
    
    with open(file_path, 'rb') as source:
        while True:
            part_name = file_path.parent / f"{file_path.name}.part{part_num:03d}"
            current_chunk_size = 0
            with open(part_name, 'wb') as dest:
                while current_chunk_size < chunk_size:
                    read_size = min(buffer_size, chunk_size - current_chunk_size)
                    data = source.read(read_size)
                    if not data: break
                    dest.write(data)
                    current_chunk_size += len(data)
            if current_chunk_size == 0:
                if part_name.exists(): part_name.unlink()
                break
            parts.append(part_name)
            part_num += 1
    return parts
    
def progress(current, total, message, typ, task_uuid=None):
    if task_uuid and CANCEL_FLAGS.get(task_uuid):
        raise Exception("CANCELLED_BY_USER")

    try:
        msg_id = int(message.id)
    except:
        try:
            msg_id = int(message)
        except:
            return
    key = f"{msg_id}:{typ}"
    now = time.time()
    if key not in PROGRESS:
        PROGRESS[key] = {
            "current": 0, "total": int(total), "percent": 0.0,
            "last_time": now, "last_current": 0, "speed": 0.0, "eta": None
        }
    rec = PROGRESS[key]
    rec["current"] = int(current)
    rec["total"] = int(total)
    if total > 0:
        rec["percent"] = (current / total) * 100.0
    dt = now - rec["last_time"]
    if dt >= 1 or current == total:
        delta_bytes = current - rec["last_current"]
        if dt <= 0: dt = 0.1
        speed = delta_bytes / dt
        rec["speed"] = speed
        rec["last_time"] = now
        rec["last_current"] = current
        if speed > 0 and total > current:
            rec["eta"] = (total - current) / speed
            
async def downstatus(client: Client, status_message: Message, chat, index: int, total_count: int, header_text: str = ""):
    msg_id = status_message.id
    key = f"{msg_id}:down"
    last_text = ""
    while True:
        rec = PROGRESS.get(key)
        if not rec:
            await asyncio.sleep(1)
            continue
        if rec["current"] == rec["total"] and rec["total"] > 0:
            break
            
        # Add the Header Text (Filter info) if it exists
        header_section = f"{header_text}\n" if header_text else ""

        status = (
            f"📥 **Downloading File ({index}/{total_count})**\n"
            f"└ 📂 `{max(0, total_count-index)}` remaining\n\n"
            f"**{rec.get('percent', 0):.1f}%** │ `{generate_bar(rec.get('percent', 0), length=12)}`\n\n"
            f"{header_section}"
            f"🚀 **Speed:** `{_pretty_bytes(rec.get('speed', 0))}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(rec.get('current', 0))} / {_pretty_bytes(rec.get('total', 0))}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(rec.get('eta', 0)) if rec.get('eta') else 0)}`"
        )

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except Exception:
                pass
        
        # --- DYNAMIC SLEEP LOGIC ---
        total_size = rec.get("total", 0)
        if total_size > 0 and total_size < 50 * 1024 * 1024:
            await asyncio.sleep(9) 
        else:
            await asyncio.sleep(20)
            
async def upstatus(client: Client, status_message: Message, chat, index: int, total_count: int, header_text: str = ""):
    msg_id = status_message.id
    key = f"{msg_id}:up"
    last_text = ""
    while True:
        rec = PROGRESS.get(key)
        if not rec:
            await asyncio.sleep(1)
            continue
        if rec["current"] == rec["total"] and rec["total"] > 0:
            break
            
        header_section = f"{header_text}\n" if header_text else ""

        status = (
            f"☁️ **Uploading File ({index}/{total_count})**\n"
            f"└ 📤 `{max(0, total_count-index)}` remaining\n\n"
            f"**{rec.get('percent', 0):.1f}%** │ `{generate_bar(rec.get('percent', 0), length=12)}`\n\n"
            f"{header_section}"
            f"🚀 **Speed:** `{_pretty_bytes(rec.get('speed', 0))}/s`\n"
            f"💾 **Size:** `{_pretty_bytes(rec.get('current', 0))} / {_pretty_bytes(rec.get('total', 0))}`\n"
            f"⏳ **ETA:** `{get_readable_time(int(rec.get('eta', 0)) if rec.get('eta') else 0)}`"
        )

        if status != last_text:
            try:
                await client.edit_message_text(chat, msg_id, status)
                last_text = status
            except Exception:
                pass
        
        # --- DYNAMIC SLEEP LOGIC ---
        total_size = rec.get("total", 0)
        if total_size > 0 and total_size < 50 * 1024 * 1024:
            await asyncio.sleep(9) 
        else:
            await asyncio.sleep(20)
            
def get_message_type(msg: Message):
    if msg.document: return "Document"
    if msg.video: return "Video"
    if msg.animation: return "Animation"
    if msg.sticker: return "Sticker"
    if msg.voice: return "Voice"
    if msg.audio: return "Audio"
    if msg.photo: return "Photo"
    if msg.text: return "Text"
    return None

# ==============================================================================
# --- HANDLERS (START/HELP/STATUS/CANCEL/etc.) ---
# ==============================================================================

async def test_destination_access(client: Client, chat_id, thread_id=None):
    """Sends a dummy message to verify write permissions, then deletes it."""
    try:
        test_msg = await client.send_message(
            chat_id=chat_id,
            text="🔄 **Testing Destination Accessibility...**\n*(This message will self-destruct)*",
            reply_to_message_id=thread_id
        )
        await test_msg.delete()
        return True, "Success"
    except Exception as e:
        return False, str(e)
        
@app.on_message(filters.command(["start"]) & (filters.private | filters.group))
async def send_start(client: Client, message: Message):
    # --- 1. Log and Save User (Database) ---
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    
    try:
        if not await db.is_user_exist(user_id):
            await db.add_user(user_id, user_name)
            print(f"New user {user_id} saved to database.") # Simple logging
    except Exception as e:
        print(f"Failed to save user {user_id}: {e}")

    # --- 2. Send Welcome Video & Text ---
    welcome_video_url = "https://files.catbox.moe/o9azww.mp4"
    welcome_text = (
        f"<b>👋 Hi {message.from_user.mention}, I am Save Restricted Content Bot.</b>\n\n"
        "<b>For downloading restricted content /login first.</b>\n\n"
        "<b>Know how to use bot by - /help</b>"
    )
    
    buttons = [
        [InlineKeyboardButton("❣️ Developer", url = "https://t.me/thanuj66")],
        [InlineKeyboardButton('🔍 sᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ', url='https://t.me/telegram'), InlineKeyboardButton('🤖 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ', url='https://t.me/telegram')]
    ]

    # Try sending video, fall back to message if video fails/is invalid
    try:
        await client.send_video(
            chat_id=message.chat.id, 
            video=welcome_video_url, 
            caption=welcome_text, 
            reply_markup=InlineKeyboardMarkup(buttons),
            reply_to_message_id=message.id
        )
    except Exception as e:
        # Fallback if video link dies or fails
        await client.send_message(
            chat_id=message.chat.id,
            text=welcome_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            reply_to_message_id=message.id
        )

@app.on_message(filters.command(["help"]) & (filters.private | filters.group))
async def send_help(client: Client, message: Message):
    await client.send_message(
        message.chat.id, 
        text=HELP_TXT,
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True
    )

@app.on_message(filters.command(["cancel"]) & (filters.private | filters.group))
async def send_cancel(client: Client, message: Message):
    user_id = message.from_user.id

    # 1. Check if user is stuck in "Setup Mode" (waiting for ID or Delay)
    if user_id in PENDING_TASKS:
        del PENDING_TASKS[user_id]
        await message.reply("✅ **Setup process cancelled.** You can send a new link now.")
        return

    # 2. Check if user has active downloads running
    user_tasks = ACTIVE_PROCESSES.get(user_id, {})
    if not user_tasks:
        await message.reply("✅ **No active tasks to cancel.**")
        return

    # 3. Show menu to cancel active downloads
    buttons = []
    for tid, info in list(user_tasks.items()):
        label = info.get("item", "Task")
        label_short = (label[:26] + "...") if len(label) > 29 else label
        buttons.append([InlineKeyboardButton(f"🛑 {label_short}", callback_data=f"cancel_task:{tid}")])
    buttons.append([InlineKeyboardButton("🛑 Cancel ALL My Tasks", callback_data="cancel_all")])
    buttons.append([InlineKeyboardButton("❌ Close Menu", callback_data="close_menu")])

    await message.reply(
        "**🚫 Cancel Tasks**\n\nSelect the task you want to cancel:",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_callback_query(filters.regex(r"^cancel_") | filters.regex(r"^cancel_task:"))
async def cancel_callback(client: Client, query):
    user_id = query.from_user.id
    data = query.data

    # --- FIX: Handle "cancel_setup" here because the regex ^cancel_ catches it ---
    if data == "cancel_setup":
        if user_id in PENDING_TASKS:
            del PENDING_TASKS[user_id]
        await query.message.edit("❌ **Task Setup Cancelled.**")
        return
    # --------------------------------------------------------------------------

    if data == "cancel_all":
        # --- ADD THIS: Clear pending queue first ---
        if user_id in TASK_QUEUE:
            TASK_QUEUE[user_id].clear()
        # -------------------------------------------
        
        user_tasks = list(ACTIVE_PROCESSES.get(user_id, {}).keys())
        if not user_tasks:
            await query.answer("No active tasks to cancel.", show_alert=True)
            try: await query.message.delete()
            except: pass
            return
        for tid in user_tasks:
            CANCEL_FLAGS[tid] = True  # Per-task flag — does NOT affect other users
        await query.message.edit("**🛑 Cancelling ALL your tasks...**\n(This may take a moment to stop current downloads)")
        return

    if data.startswith("cancel_task:"):
        task_uuid = data.split(":",1)[1]
        user_tasks = ACTIVE_PROCESSES.get(user_id, {})
        if task_uuid not in user_tasks:
            await query.answer("Task not found or already finished.", show_alert=True)
            try: await query.message.delete()
            except: pass
            return
        CANCEL_FLAGS[task_uuid] = True
        await query.message.edit(f"🛑 **Task cancelled:** `{user_tasks[task_uuid].get('item','Task')}`\nIt will stop shortly.")
        return
        
@app.on_callback_query(filters.regex("^close_menu"))
async def close_menu(client, query):
    try:
        await query.message.delete()
    except:
        await query.answer("Menu closed.")

@app.on_message(filters.command(["pixel"]) & (filters.user(ADMINS) | filters.user(SUDOS)))
async def pixel_bypass_handler(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply(
            "**Usage:**\n`/pixel https://pixeldrain.com/u/xxxx`\n\n"
            "Or multiple comma-separated links:\n"
            "`/pixel link1,link2,link3`"
        )

    # Extract the input string after the command
    input_text = message.text.split(None, 1)[1]
    
    # Extract all Pixeldrain IDs using Regex
    # This gracefully handles commas, spaces, or mixed formatting
    matches = re.findall(r"pixeldrain\.com/u/([a-zA-Z0-9_-]+)", input_text)
    
    if not matches:
        return await message.reply(
            "❌ **No valid Pixeldrain links found.**\n"
            "Please ensure the links follow the format: `https://pixeldrain.com/u/XXXX`"
        )

    # Convert IDs to bypassed links
    bypassed_links = [f"https://cdn.pixeldrain.eu.cc/{match}" for match in matches]
    
    # Join with commas and wrap in backticks for 1-tap copying
    bypassed_text = ",".join(bypassed_links)
    
    reply_text = (
        "✨ **Pixeldrain Bypass Successful!** ✨\n\n"
        "**📥 Bypassed Links (Tap to copy):**\n"
        f"`{bypassed_text}`\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "🌐 **Original Bypass Website:** [Click Here](https://pixeldrain-bypass.gamedrive.org/)\n"
        "📜 **Userscript:** [Install Script](https://pixeldrain-bypass.gamedrive.org/pixeldrain-bypass.user.js)"
    )
    
    await message.reply(reply_text, disable_web_page_preview=True)

@app.on_message(filters.command(["status"]) & (filters.user(ADMINS) | filters.user(SUDOS)))
async def status_style_handler(client, message):
    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_str = get_readable_time(uptime_seconds)
    mem = psutil.virtual_memory().percent
    cpu = psutil.cpu_percent()
    
    # Get Disk Usage
    total, used, free = shutil.disk_usage(".")
    disk_free = free / (1024**3)
    
    active_count = 0
    queue_list = []
    
    for uid, tasks in ACTIVE_PROCESSES.items():
        for t_id, info in tasks.items():
            active_count += 1
            src = info.get("source_title", "Source")
            dst = info.get("dest_title_name", "Destination")
            queue_list.append(f"• {src} → {dst}")
    
    # Count Active Watchers
    watcher_count = await db.db.watchers.count_documents({})
    
    queue_text = "\n".join(queue_list) if queue_list else "😴 No active downloads."

    msg = (
        f"🔰 **SYSTEM DASHBOARD**\n\n"
        f"⏱ **Uptime:** `{uptime_str}`\n"
        f"🧠 **RAM:** `{mem}%`  │  ⚙️ **CPU:** `{cpu}%` \n"
        f"💿 **Disk Free:** `{disk_free:.1f} GB` \n\n"
        f"👀 **Live Watchers:** `{watcher_count}` running\n"
        f"📉 **Active Downloads ({active_count})**\n"
        f"{queue_text}"
    )
    await message.reply(msg, quote=True)
    
@app.on_message(filters.command(["log"]) & (filters.user(ADMINS) | filters.user(SUDOS)))
async def send_log_handler(client: Client, message: Message):
    if os.path.exists("bot.log"):
        await message.reply_document("bot.log", caption="📄 **Bot Logs**\n(Updates automatically)")
    else:
        await message.reply("⚠️ Log file not found yet.")

@app.on_message(filters.command(["botstats"]) & filters.user(ADMINS))
async def bot_stats_handler(client: Client, message: Message):
    wait = await message.reply("📊 **Generating detailed stats...**")
    total_users = await db.total_users_count()
    all_users_cursor = await db.get_all_users()
    
    logged_in_list = []
    async for user in all_users_cursor:
        if user.get("session"):
            user_id = user['id']
            name = user.get("name") or f"User:{user_id}"
            user_tasks = ACTIVE_PROCESSES.get(user_id, {})
            
            if user_tasks:
                task_details = []
                for t_id, info in user_tasks.items():
                    src = info.get("source_title", "Source")
                    dst = info.get("dest_title", "Dest")
                    tot = info.get("total", 0)
                    curr = info.get("current", 0)
                    start_t = info.get("started", time.time())
                    
                    percent = (curr / tot * 100) if tot > 0 else 0
                    
                    # ETA Math
                    elapsed = time.time() - start_t
                    eta_str = "Calculating..."
                    if curr > 0 and elapsed > 0:
                        eta_str = get_readable_time(int(((tot - curr) / (curr / elapsed))))

                    task_details.append(
                        f"      └ 🏃 {src} → {info.get('dest_title_name', 'Destination')}"
                    )
                    
                tasks_str = "\n" + "\n".join(task_details)
                logged_in_list.append(f"• **{name}** [`{user_id}`]{tasks_str}")
            else:
                logged_in_list.append(f"• **{name}** [`{user_id}`] (IDLE 😴)")

    logged_in_text = "\n\n".join(logged_in_list) if logged_in_list else "No users logged in."
    stats_msg = (
        "📊 **DETAILED BOT STATISTICS**\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"🔑 **Logged-in Users:** `{len(logged_in_list)}`\n\n"
        f"📝 **User & Task Breakdown:**\n\n{logged_in_text}"
    )
    await wait.edit(stats_msg)
        
# ==============================================================================
# --- LOGIN / LOGOUT (async login handler inserted) ---
# ==============================================================================

@app.on_message(filters.private & ~filters.forwarded & filters.command(["logout"]))
async def logout(client, message):
    user_id = message.from_user.id
    if not await db.is_user_exist(user_id):
        return await message.reply_text("You are not logged in.")

    status_msg = await message.reply("📡 **Connecting to Telegram to terminate session...**")

    # 1. Get session details needed to connect
    session_string = await db.get_session(user_id)
    api_id = await db.get_api_id(user_id)
    api_hash = await db.get_api_hash(user_id)

    # 2. Perform Remote Logout (Remove from Devices)
    if session_string:
        user_client = None
        try:
            # Use stored keys or fallback to global env
            use_api_id = int(api_id) if api_id else API_ID
            use_api_hash = api_hash if api_hash else API_HASH
            
            user_client = Client(
                f"logout_{user_id}_{int(time.time())}", 
                session_string=session_string, 
                api_id=use_api_id, 
                api_hash=use_api_hash,
                no_updates=True,
                in_memory=True
            )
            
            await user_client.connect()
            
            # Try to logout, ignoring "Already Terminated" errors
            try:
                await user_client.log_out()
                await status_msg.edit("✅ **Session successfully removed from Telegram Devices.**")
            except Exception as e:
                # If the session dies instantly, Pyrogram might complain. We consider this a success.
                if "terminated" in str(e) or "Connection" in str(e):
                    await status_msg.edit("✅ **Session terminated successfully.**")
                else:
                    raise e
            
        except AuthKeyUnregistered:
            # This happens if the user already manually removed it from devices
            await status_msg.edit("⚠️ **Session was already invalid.** Cleaning local database...")
        except Exception as e:
            # For any other real error, we just log it but still clean local DB
            print(f"Remote logout warning: {e}")
            await status_msg.edit("✅ **Local session cleared.** (Remote session might already be gone)")
        finally:
            try:
                if user_client and user_client.is_connected:
                    await user_client.disconnect()
            except: pass

    # 3. Stop and remove live watcher client if running
    if user_id in USER_CLIENTS:
        try:
            await USER_CLIENTS[user_id].stop()
        except Exception:
            pass
        del USER_CLIENTS[user_id]

    # 4. Clean up Local Database
    await db.set_session(user_id, session=None)
    await db.set_api_id(user_id, api_id=None)
    await db.set_api_hash(user_id, api_hash=None)
    
    await message.reply("**Logout Complete** ♦\n(You are now disconnected)")

@app.on_message(filters.private & ~filters.forwarded & filters.command(["login"]))
async def login_handler(bot: Client, message: Message):
    
    if not await db.is_user_exist(message.from_user.id):
        await db.add_user(message.from_user.id, message.from_user.first_name)
        
    user_data = await db.get_session(message.from_user.id)
    if user_data is not None:
        await message.reply("**You Are Already Logged In. First /logout Your Old Session. Then Do Login.**")
        return  
    user_id = int(message.from_user.id)

    # --- Check Env Variables First ---
    if API_ID != 0 and API_HASH:
        await message.reply("**🔑 Specific API ID and HASH found in variables. Using them automatically...**")
        api_id = API_ID
        api_hash = API_HASH
    else:
        # YouTube Link Removed Here
        api_id_msg = await bot.ask(user_id, "<b>Send Your API ID.</b>", filters=filters.text)
        if api_id_msg.text == '/cancel':
            return await api_id_msg.reply('<b>process cancelled !</b>')
        try:
            api_id = int(api_id_msg.text)
            if api_id < 1000000 or api_id > 99999999:
                 await api_id_msg.reply("**❌ Invalid API ID**\n\nPlease start again with /login.", quote=True)
                 return
        except ValueError:
            await api_id_msg.reply("**Api id must be an integer, start your process again by /login**", quote=True)
            return
        
        api_hash_msg = await bot.ask(user_id, "**Now Send Me Your API HASH**", filters=filters.text)
        if api_hash_msg.text == '/cancel':
            return await api_hash_msg.reply('<b>process cancelled !</b>')
        api_hash = api_hash_msg.text

        if len(api_hash) != 32:
             await api_hash_msg.reply("**❌ Invalid API HASH**\n\nPlease start again with /login.", quote=True)
             return

    # --- NEW STYLED TEXT ---
    login_text = (
        "🔐 **Login Process Initiated**\n\n"
        "Please send your **Phone Number** in international format.\n"
        "Example: `+1234567890`\n\n"
        "🛡️ *Your session is stored securely locally.*"
    )
    # -----------------------

    phone_number_msg = await bot.ask(chat_id=user_id, text=login_text, filters=filters.text)
    if phone_number_msg.text=='/cancel':
        return await phone_number_msg.reply('<b>process cancelled !</b>')
    phone_number = phone_number_msg.text
    
    # Connect for auth
    client_auth = Client(f"login_{user_id}_{int(time.time())}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await client_auth.connect()
    
    await phone_number_msg.reply("Sending OTP...")
    try:
        code = await client_auth.send_code(phone_number)
        phone_code_msg = await bot.ask(user_id, "Please check for an OTP in official telegram account. If you got it, send OTP here after reading the below format. \n\nIf OTP is `12345`, **please send it as** `1 2 3 4 5`.\n\n**Enter /cancel to cancel The Procces**", filters=filters.text, timeout=600)
    except PhoneNumberInvalid:
        await phone_number_msg.reply('`PHONE_NUMBER` **is invalid.**')
        await client_auth.disconnect()
        return
        
    if phone_code_msg.text=='/cancel':
        await client_auth.disconnect()
        return await phone_code_msg.reply('<b>process cancelled !</b>')
        
    try:
        phone_code = phone_code_msg.text.replace(" ", "")
        await client_auth.sign_in(phone_number, code.phone_code_hash, phone_code)
    except PhoneCodeInvalid:
        await phone_code_msg.reply('**OTP is invalid.**')
        await client_auth.disconnect()
        return
    except PhoneCodeExpired:
        await phone_code_msg.reply('**OTP is expired.**')
        await client_auth.disconnect()
        return
    except SessionPasswordNeeded:
        two_step_msg = await bot.ask(user_id, '**Your account has enabled two-step verification. Please provide the password.\n\nEnter /cancel to cancel The Procces**', filters=filters.text, timeout=300)
        if two_step_msg.text=='/cancel':
            await client_auth.disconnect()
            return await two_step_msg.reply('<b>process cancelled !</b>')
        try:
            password = two_step_msg.text
            await client_auth.check_password(password=password)
        except PasswordHashInvalid:
            await two_step_msg.reply('**Invalid Password Provided**')
            await client_auth.disconnect()
            return
            
    string_session = await client_auth.export_session_string()
    await client_auth.disconnect()
    
    if len(string_session) < SESSION_STRING_SIZE:
        return await message.reply('<b>invalid session sring</b>')
    try:
        user_data = await db.get_session(message.from_user.id)
        if user_data is None:
            # Verification check
            uclient = Client(f"verify_{message.from_user.id}_{int(time.time())}", session_string=string_session, api_id=api_id, api_hash=api_hash, in_memory=True)
            await uclient.connect()
            
            await db.set_session(message.from_user.id, session=string_session)
            await db.set_api_id(message.from_user.id, api_id=api_id)
            await db.set_api_hash(message.from_user.id, api_hash=api_hash)
            
            try:
                await uclient.disconnect()
            except:
                pass
    except Exception as e:
        return await message.reply_text(f"<b>ERROR IN LOGIN:</b> `{e}`")
    await bot.send_message(message.from_user.id, "<b>Account Login Successfully.\n\nIf You Get Any Error Related To AUTH KEY Then /logout first and /login again</b>")

# ==============================================================================
# --- BROADCAST ---
# ==============================================================================

async def broadcast_messages(user_id, message):
    start_time = time.time()
    try:
        await message.copy(chat_id=user_id)
        # Calculates sleep based on work time
        elapsed = time.time() - start_time
        await asyncio.sleep(max(0, 1.5 - elapsed)) 
        return True, "Success"
    except FloodWait as e:
        # If floodwait is huge, just skip this user to save the broadcast
        if e.value > 60:
            return False, "Error"
        await asyncio.sleep(e.value)
        return await broadcast_messages(user_id, message)
    except InputUserDeactivated:
        await db.delete_user(int(user_id))
        return False, "Deleted"
    except UserIsBlocked:
        await db.delete_user(int(user_id))
        return False, "Blocked"
    except PeerIdInvalid:
        await db.delete_user(int(user_id))
        return False, "Error"
    except Exception as e:
        return False, "Error"

@app.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    if not b_msg:
        return await message.reply_text("**Reply This Command To Your Broadcast Message**")
    sts = await message.reply_text(text='Broadcasting your messages...')
    start_time = time.time()
    total_users = await db.total_users_count()
    done = 0
    blocked = 0
    deleted = 0
    failed = 0
    success = 0
    async for user in users:
        if 'id' in user:
            pti, sh = await broadcast_messages(int(user['id']), b_msg)
            if pti:
                success += 1
            elif pti == False:
                if sh == "Blocked":
                    blocked += 1
                elif sh == "Deleted":
                    deleted += 1
                elif sh == "Error":
                    failed += 1
            done += 1
            if not done % 20:
                await sts.edit(f"Broadcast in progress:\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")
        else:
            done += 1
            failed += 1
            if not done % 20:
                await sts.edit(f"Broadcast in progress:\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

    time_taken = str(datetime.timedelta(seconds=int(time.time()-start_time)))
    await sts.edit(f"Broadcast Completed:\nCompleted in {time_taken} seconds.\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

# ==============================================================================
# --- WATCHER SETUP WIZARD ---
# ==============================================================================

@app.on_message(filters.command(["watch"]) & filters.private)
async def watch_setup(client: Client, message: Message):
    user_id = message.from_user.id
    
    # Usage: /watch [LINK]
    if len(message.command) < 2:
        return await message.reply("**Usage:**\n`/watch https://t.me/channel/123`\n(Supports Topics too!)")
    
    link_text = message.command[1]
    
    # 1. Analyze Link (Restriction & Topic)
    wait_msg = await message.reply("🔎 **Analyzing Source...**", quote=True)
    is_restricted, status_text = await check_link_restriction(user_id, link_text)
    
    # Extract Source Topic (if any)
    source_thread_id = None
    if "t.me/c/" in link_text:
        parts = link_text.split("t.me/c/")[1].split("/")
        # Changed to >= 2 to catch direct Topic Links (t.me/c/12345/5)
        if len(parts) >= 2 and parts[1].isdigit():
            source_thread_id = int(parts[1])
    elif "t.me/" in link_text:
        parts = link_text.split("t.me/")[1].split("/")
        if len(parts) >= 2 and parts[1].isdigit():
            source_thread_id = int(parts[1])
    
    await wait_msg.delete()
    
    # 2. Save State
    PENDING_TASKS[user_id] = {
        "mode": "WATCHER", # <--- Important Flag
        "link": link_text,
        "source_thread_id": source_thread_id,
        "is_restricted": is_restricted,
        "status": "waiting_choice"
    }
    
    # 3. Ask Destination
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]
    ]
    
    await message.reply(
        f"👀 **Watcher Setup**\n\n"
        f"{status_text}\n"
        f"{(f'🔹 **Source Topic:** `{source_thread_id}` detected!' if source_thread_id else '')}\n\n"
        "**Where should new messages go?**",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )

@app.on_message(filters.command(["removetarget", "remove_target"]) & filters.private)
async def remove_target_cmd(client, message):
    if len(message.command) < 3:
        return await message.reply("**Usage:** `/removetarget [source_id] [dest_id]`")
    src = message.command[1]
    dst = message.command[2]
    
    if await db.remove_watcher_target(src, dst):
        await message.reply("✅ **Target Removed Successfully!**\nNo more messages will route here from that source.")
    else:
        await message.reply("⚠️ Target or Source not found.")

@app.on_message(filters.command(["removesource", "unwatch"]) & filters.private)
async def remove_source_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply("**Usage:** `/unwatch [source_id]`")
    
    try:
        src = int(message.command[1])
    except ValueError:
        return await message.reply("⚠️ **Invalid format!**\nPlease pass the numerical **Source ID** (e.g. `-100123...`). You can find this ID by sending `/watchers`.")
        
    if await db.remove_watcher(src):
        await message.reply("✅ **Source Route Removed Successfully!**")
    else:
        await message.reply("⚠️ Source not found.")

@app.on_message(filters.command(["watchers", "list"]) & filters.private)
async def list_watchers(client, message):
    user_id = message.from_user.id
    cursor = await db.get_all_watchers()
    watchers = await cursor.to_list(length=100)
    
    # Removed the ADMINS check so everyone only sees their own setups
    user_watchers = [w for w in watchers if w.get('user_id') == user_id]
    
    if not user_watchers:
        return await message.reply("💤 **No active watchers found.**")
    
    text = "📊 **Your Multi-Source Channel Mappings:**\n\n"
    
    for idx, w in enumerate(user_watchers, 1):
        src_id = w['source_id']
        src_display = w.get('source_title') or str(src_id)
        if w.get('source_thread'): src_display += f" (Topic: {w['source_thread']})"
        
        targets = w.get('targets', [])
        if not targets and 'dest_id' in w: # Load Legacy Layout 
            targets = [{"dest_id": w['dest_id'], "dest_thread": w.get('dest_thread'), "dest_title": w.get('dest_title')}]
            
        text += f"**{idx}. 📥 {src_display}**\n"
        text += f"   <code>{src_id}</code>\n"
        text += f"   🎛 **Filters:** `{', '.join(w.get('allowed_types', ['Legacy']))}`\n"
        text += f"   ⤵️ **Targets ({len(targets)}):**\n"
        
        for t in targets:
            dst_id = t['dest_id']
            dst_display = t.get('dest_title') or str(dst_id)
            if t.get('dest_thread'): dst_display += f" (Topic: {t['dest_thread']})"
            text += f"   • {dst_display} (<code>{dst_id}</code>)\n"
        text += "\n"
        
    await message.reply(text)
    
@app.on_callback_query(filters.regex("^unwatch_"))
async def unwatch_callback(client, query):
    # --- HANDLER 1: CANCEL ALL ---
    if query.data == "unwatch_all":
        user_id = query.from_user.id
        # Delete all watchers belonging to this user
        result = await db.db.watchers.delete_many({'user_id': user_id})
        
        # Edit message to show success (Closes menu)
        await query.message.edit(f"✅ **Success!**\n\n🗑 Removed `{result.deleted_count}` active watchers.")
        return

    # --- HANDLER 2: CANCEL SPECIFIC ---
    data = query.data.split("_")
    source_id = int(data[1])
    topic_id = int(data[2])
    
    # 1. Fetch details (Titles) BEFORE deleting, for the success message
    query_db = {'source_id': source_id}
    if topic_id != 0:
        query_db['source_thread'] = topic_id
        
    watcher = await db.db.watchers.find_one(query_db)
    
    src_name = str(source_id)
    dest_name = "Unknown"
    
    if watcher:
        src_name = watcher.get('source_title') or str(source_id)
        dest_name = watcher.get('dest_title') or str(watcher.get('dest_id'))

    # 2. Delete from DB
    if topic_id == 0:
        await db.db.watchers.delete_many({'source_id': source_id, 'source_thread': None})
        # Fallback for old format
        await db.db.watchers.delete_many({'source_id': source_id, 'source_thread': {'$exists': False}})
    else:
        await db.db.watchers.delete_many({'source_id': source_id, 'source_thread': topic_id})
        
    # 3. Edit Message (This replaces the menu, effectively closing it)
    await query.message.edit(
        f"🗑 **Active Watcher Task Removed**\n\n"
        f"From: **{src_name}**\n"
        f"To: **{dest_name}**"
    )
    
# ==============================================================================
# --- CORE: receive links / start tasks / processing / cancel checks ---
# ==============================================================================

@app.on_message((filters.text | filters.caption) & filters.private & ~filters.command(["dl", "start", "help", "cancel", "botstats", "login", "logout", "broadcast", "status", "watch", "unwatch", "watchers", "removetarget", "removesource", "log"]))
async def save(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in PENDING_TASKS:
        # (Keep existing setup logic for waiting_id / waiting_speed)
        if PENDING_TASKS[user_id].get("status") == "waiting_id":
            await process_custom_destination(client, message)
            return
        if PENDING_TASKS[user_id].get("status") == "waiting_speed":
            await process_speed_input(client, message)
            return

    link_text = message.text or message.caption
    if not link_text or "https://t.me/" not in link_text:
        return

    # --- NEW: CHECK RESTRICTION FIRST ---
    wait_msg = await message.reply("🔎 **Analyzing Link...**", quote=True)
    is_restricted, status_text = await check_link_restriction(user_id, link_text)
    await wait_msg.delete()
    # ------------------------------------

    PENDING_TASKS[user_id] = {
        "link": link_text, 
        "status": "waiting_choice",
        "is_restricted": is_restricted 
    }
    
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]
    ]
    
    # Add the status text to the reply
    await message.reply(
        f"✨ **Link Detected!**\n\n"
        f"{status_text}\n\n"
        "Where should I send the files?",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_message(filters.command(["dl"]) & (filters.private | filters.group))
async def dl_handler(client: Client, message: Message):
    user_id = message.from_user.id
    link_text = ""
    
    # 1. Extract Link (from Reply or Command Argument)
    reply = message.reply_to_message
    if reply and (reply.text or reply.caption):
        link_text = reply.text or reply.caption
    elif len(message.command) > 1:
        link_text = message.text.split(None, 1)[1]
        
    # 2. Validate Link
    if not link_text or "https://t.me/" not in link_text:
        await message.reply_text(
            "**Usage:**\n"
            "• Reply to a link with `/dl`\n"
            "• Or send `/dl https://t.me/...`"
        )
        return

    # --- NEW: CHECK RESTRICTION FIRST ---
    wait_msg = await message.reply("🔎 **Analyzing Link...**", quote=True)
    is_restricted, status_text = await check_link_restriction(user_id, link_text)
    await wait_msg.delete()
    # ------------------------------------

    # 3. Handle Group Chat (Directly ask for Speed)
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        PENDING_TASKS[user_id] = {
            "link": link_text,
            "dest_chat_id": message.chat.id,
            "dest_thread_id": message.message_thread_id,
            "dest_title": message.chat.title or "This Group",
            "status": "waiting_speed",
            "is_restricted": is_restricted # <--- ADDED THIS
        }
        # Send the status info before showing the speed menu
        await message.reply(f"✨ **Link Analyzed!**\n{status_text}", quote=True)
        await ask_for_speed(message)
        return

    # 4. Handle Private Chat (Show Destination Menu)
    # CHANGE: Added "is_restricted" here too
    PENDING_TASKS[user_id] = {
        "link": link_text, 
        "status": "waiting_choice",
        "is_restricted": is_restricted
    }
    
    buttons = [
        [InlineKeyboardButton("📂 Send to DM (Here)", callback_data="dest_dm")],
        [InlineKeyboardButton("📢 Send to Channel/Group", callback_data="dest_custom")],
        [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]  # <-- Added this button
    ]
    
    await message.reply(
        f"✨ **Link Detected!**\n\n"
        f"{status_text}\n\n"
        "I am ready to process this content. Please tell me where you want the files sent:",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True
    )
    
@app.on_callback_query(filters.regex("^dest_"))
async def destination_callback(client: Client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS:
        return await query.answer("❌ Task expired. Send link again.", show_alert=True)
    choice = query.data
    if choice == "dest_dm":
        PENDING_TASKS[user_id]["dest_chat_id"] = user_id
        PENDING_TASKS[user_id]["dest_thread_id"] = None
        await ask_for_speed(query.message)
    elif choice == "dest_custom":
        PENDING_TASKS[user_id]["status"] = "waiting_id"
        buttons = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]]
        await query.message.edit(
            "📝 **Send the Target Chat ID**\n\n"
            "Examples:\n"
            "• Channel/Group: `-100123456789`\n"
            "• Specific Topic: `-100123456789/5`\n\n"
            "⚠️ __Make sure I am an admin in that chat!__",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

def get_filter_keyboard(current_types):
    buttons = []
    row = []
    for t in ALL_MSG_TYPES:
        icon = "✅" if t in current_types else "❌"
        row.append(InlineKeyboardButton(f"{icon} {t}", callback_data=f"filter_toggle:{t}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    buttons.append([
        InlineKeyboardButton("✅ Select All", callback_data="filter_all"), 
        InlineKeyboardButton("❌ Clear All", callback_data="filter_none")
    ])
    buttons.append([InlineKeyboardButton("🚀 Proceed / Save Setup", callback_data="filter_start")])
    buttons.append([InlineKeyboardButton("🛑 Cancel Setup", callback_data="cancel_setup")])
    return InlineKeyboardMarkup(buttons)

async def show_filter_menu(message_or_query, user_id):
    task_data = PENDING_TASKS.get(user_id)
    if not task_data: return
    
    if "allowed_types" not in task_data:
        task_data["allowed_types"] = ["Video", "Document"] # Default strict settings!
    task_data["status"] = "waiting_filter"
    
    kb = get_filter_keyboard(task_data["allowed_types"])
    text = "🎛 **Content Filter**\n\nSelect the media types you want to forward or download.\n*(Default: Strictly Videos & Documents)*"
    
    if isinstance(message_or_query, CallbackQuery):
        await message_or_query.message.edit_text(text, reply_markup=kb)
    else:
        await message_or_query.reply(text, reply_markup=kb, quote=True)

@app.on_callback_query(filters.regex("^filter_toggle:(.+)"))
async def filter_toggle_cb(client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS: return await query.answer("Expired.", show_alert=True)
    mtype = query.data.split(":")[1]
    allowed = PENDING_TASKS[user_id].get("allowed_types", ["Video", "Document"])
    if mtype in allowed: allowed.remove(mtype)
    else: allowed.append(mtype)
    PENDING_TASKS[user_id]["allowed_types"] = allowed
    try: await query.message.edit_reply_markup(get_filter_keyboard(allowed))
    except: pass
    await query.answer()

@app.on_callback_query(filters.regex("^filter_all$"))
async def filter_all_cb(client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS: return await query.answer("Expired.", show_alert=True)
    PENDING_TASKS[user_id]["allowed_types"] = ALL_MSG_TYPES.copy()
    try: await query.message.edit_reply_markup(get_filter_keyboard(ALL_MSG_TYPES))
    except: pass
    await query.answer()

@app.on_callback_query(filters.regex("^filter_none$"))
async def filter_none_cb(client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS: return await query.answer("Expired.", show_alert=True)
    PENDING_TASKS[user_id]["allowed_types"] = []
    try: await query.message.edit_reply_markup(get_filter_keyboard([]))
    except: pass
    await query.answer()

@app.on_callback_query(filters.regex("^filter_start$"))
async def filter_start_cb(client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS: return await query.answer("Expired.", show_alert=True)
    
    task_data = PENDING_TASKS.pop(user_id)
    allowed_types = task_data.get("allowed_types", ["Video", "Document"])
    
    if not allowed_types: 
        PENDING_TASKS[user_id] = task_data # put back
        return await query.answer("❌ Select at least one type!", show_alert=True)
        
    delay = task_data.get("delay", 3)
    if task_data.get("mode") == "WATCHER":
        await finalize_watcher_setup(client, query.message, task_data, delay, user_id=user_id)
    else:
        await start_task_final(client, query.message, task_data, delay, user_id=user_id)

async def process_custom_destination(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text.strip()
    try:
        if message.reply_to_message and message.reply_to_message.from_user.is_self:
            await message.reply_to_message.delete()
    except:
        pass
    try:
        dest_chat_id = None
        dest_thread_id = None
        
        # Parse Links & IDs
        if "t.me/c/" in text:
            parts = text.split("t.me/c/")[1].split("/")
            dest_chat_id = int("-100" + parts[0])
            if len(parts) > 1 and parts[1].isdigit():
                dest_thread_id = int(parts[1])
        elif "t.me/" in text:
            parts = text.split("t.me/")[1].split("/")
            dest_chat_id = parts[0] # username
            if len(parts) > 1 and parts[1].isdigit():
                dest_thread_id = int(parts[1])
        elif "/" in text:
            parts = text.split("/")
            dest_chat_id = int(parts[0])
            dest_thread_id = int(parts[1])
        else:
            dest_chat_id = int(text) if text.lstrip('-').isdigit() else text
            
        try:
            chat = await client.get_chat(dest_chat_id)
            title = chat.title or "Target Chat"
            dest_chat_id = chat.id # Force conversion to proper strict ID
            
            # Prevent bot from endlessly replying if the chat isn't a Forum Topic
            if not getattr(chat, "is_forum", False):
                dest_thread_id = None
                
        except Exception as e:
            await message.reply(f"❌ **Invalid Chat ID/Link** or I am not an admin there.\nError: `{e}`")
            return
            
        # --- DESTINATION ACCESS TEST ---
        wait_msg = await message.reply("🔄 **Testing destination access...**", quote=True)
        success, error_msg = await test_destination_access(client, dest_chat_id, dest_thread_id)
        
        if not success:
            await wait_msg.edit(f"❌ **Setup Failed! Cannot write to destination.**\n\nPlease ensure I am an admin in `{title}` with posting permissions.\n**Error:** `{error_msg}`")
            return
            
        await wait_msg.delete()
        # -------------------------------
            
        PENDING_TASKS[user_id]["dest_chat_id"] = dest_chat_id
        PENDING_TASKS[user_id]["dest_thread_id"] = dest_thread_id
        PENDING_TASKS[user_id]["dest_title"] = title
        PENDING_TASKS[user_id]["status"] = "waiting_speed"
        await ask_for_speed(message)
    except Exception as e:
        await message.reply(f"❌ Invalid format. Please send a valid link or ID.\nError: `{e}`")

async def ask_for_speed(message: Message):
    buttons = [
        [InlineKeyboardButton("⚡ Default (3s)", callback_data="speed_default")],
        [InlineKeyboardButton("⚙️ Manual Speed", callback_data="speed_manual")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]
    ]
    text = "**🚀 Select Forwarding Speed**\n\nHow fast should I process messages?"
    if isinstance(message, Message) and message.from_user.is_bot:
        await message.edit(text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons), quote=True)

@app.on_callback_query(filters.regex("^speed_"))
async def speed_callback(client: Client, query):
    user_id = query.from_user.id
    if user_id not in PENDING_TASKS:
        await query.answer("❌ Task expired.", show_alert=True)
        return
    
    choice = query.data
    task_data = PENDING_TASKS[user_id]
    
    if choice == "speed_manual":
        PENDING_TASKS[user_id]["status"] = "waiting_speed"
        await query.message.edit(
            "⏱ **Enter Delay (Seconds)**\n\n"
            "Every time a new message arrives, I will wait this long before forwarding it.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_setup")]])
        )
        return

    # Default Speed selected
    if choice == "speed_default":
        PENDING_TASKS[user_id]["delay"] = 0 if task_data.get("mode") == "WATCHER" else 3
        await show_filter_menu(query, user_id)

async def process_speed_input(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text.strip()
    if not text.isdigit(): return await message.reply("❌ Numbers only.")
    
    delay = int(text)
    if user_id in PENDING_TASKS:
        PENDING_TASKS[user_id]["delay"] = delay
        await show_filter_menu(message, user_id)

async def finalize_watcher_setup(client, message, data, delay, user_id=None):
    if user_id is None:
        user_id = message.from_user.id if message.from_user else None
    if not user_id:
        return  # Can't proceed without a user_id
    src_link = data['link']
    source_id = None
    source_title = "Unknown Source"
    
    # 1. CHECK SESSION
    user_session = await db.get_session(user_id)
    api_id = await db.get_api_id(user_id)
    api_hash = await db.get_api_hash(user_id)
    
    if not user_session:
        return await message.reply("❌ **You are not logged in.**\n\nWatcher mode requires your account to 'listen' for new messages.\nPlease /login first.")

    # 2. START CLIENT ON-DEMAND (If not already running)
    if user_id not in USER_CLIENTS:
        status_msg = await message.reply("🔄 **Starting your Listener Client...**")
        try:
            u_api = api_id or API_ID
            u_hash = api_hash or API_HASH
            
            new_client = Client(
                f"watcher_setup_{user_id}", 
                session_string=user_session, 
                api_id=u_api, 
                api_hash=u_hash, 
                workers=4, 
                ipv6=False
                # Removed in_memory=True so Pyrogram tracks the update state physically
            )
            # Important: Add the handler without filters to ensure Pyrogram catches ALL channel updates!
            new_client.add_handler(MessageHandler(user_watcher_handler))
            
            await new_client.start()
            USER_CLIENTS[user_id] = new_client
            await status_msg.delete()
            
        except Exception as e:
            return await status_msg.edit(f"❌ **Session Error:** `{e}`\n\nTry /logout and /login again.")

    # 3. VERIFY LINK (Using the now-running client)
    user_client = USER_CLIENTS[user_id]
    try:
        join_target = None
        if "t.me/c/" in src_link:
            # Private Link Logic
            chat_str = src_link.split("t.me/c/")[1].split("/")[0]
            source_id = int("-100" + chat_str)
            join_target = source_id
            try:
                chat = await user_client.get_chat(source_id)
                source_title = chat.title or str(source_id)
            except:
                source_title = str(source_id)
        else:
            # Public Link Logic
            # Extract clean username (e.g. getfileshere5)
            username = src_link.replace("https://", "").replace("http://", "").replace("t.me/", "").split("/")[0]
            chat = await user_client.get_chat(username)
            source_id = chat.id
            source_title = chat.title or str(source_id)
            join_target = username # Use username to join public chats!
            
        # --- AGGRESSIVE AUTO-JOIN & VERIFICATION ---
        try: 
            await user_client.join_chat(join_target)
        except UserAlreadyParticipant:
            pass # All good!
        except Exception as e:
            print(f"⚠️ Auto-join warning for {join_target}: {e}")
            
        # Telegram WILL NOT send live updates if the account isn't a member. Force a check.
        try:
            await user_client.get_chat_member(source_id, "me")
        except Exception:
            return await message.reply(f"❌ **Watcher Setup Blocked!**\n\nThe account you logged in with (`/login`) is **NOT** a member of `{source_title}`. Telegram blocks live updates to non-members.\n\n**Fix:** Please manually open Telegram on the account you logged into the bot with, and join the source channel first!")
        
    except Exception as e:
        return await message.reply(f"❌ **Could not access Source.**\nMake sure your User Account is a member of that channel.\nError: `{e}`")

    dest_chat_id = data.get('dest_chat_id')
    
    # --- DESTINATION ACCESS TEST ---
    if dest_chat_id and dest_chat_id != user_id:  # Skip test if sending to DM
        success, error_msg = await test_destination_access(app, dest_chat_id, data.get('dest_thread_id'))
        if not success:
            return await message.reply(f"❌ **Watcher Setup Failed!**\nCannot write to the target destination.\n**Error:** `{error_msg}`")
    # -------------------------------

    # 4. SAVE TO DB
    await db.add_watcher(
        user_id=user_id,
        source_id=source_id,
        dest_id=dest_chat_id,
        source_thread=data.get('source_thread_id'),
        dest_thread=data.get('dest_thread_id'),
        delay=delay,
        is_restricted=data['is_restricted'],
        source_title=source_title,
        dest_title=data.get('dest_title', str(data.get('dest_chat_id'))),
        allowed_types=data.get('allowed_types')
    )
    
    # 5. REPLY WITH YOUR EXACT FORMAT
    await message.reply(
        f"✅ **Watcher/Routing Active!**\n\n"
        f"👀 Source: `{source_title}`\n"
        f"📂 Source Topic: `{data.get('source_thread_id', 'All')}`\n"
        f"🎯 Destination: `{data.get('dest_title')}`\n"
        f"📂 Dest Topic: `{data.get('dest_thread_id', 'None')}`\n"
        f"⏱ Delay: `{delay}s`\n"
        f"🔒 Restricted Mode: `{'Yes' if data['is_restricted'] else 'No'}`\n"
        f"🎛 Filter: `{', '.join(data.get('allowed_types', []))}`"
    )
    
# ==============================================================================
# --- NEW ROBUSTNESS HELPERS ---
# ==============================================================================

async def send_log(text):
    """Sends errors/alerts to the Configured Log Channel/Topic"""
    if not LOG_CHANNEL:
        return
    try:
        chat_id = LOG_CHANNEL
        topic_id = None
        
        # Check if "ID/TOPIC" format
        if "/" in LOG_CHANNEL:
            parts = LOG_CHANNEL.split("/")
            chat_id = int(parts[0])
            topic_id = int(parts[1])
        else:
            chat_id = int(LOG_CHANNEL)

        await app.send_message(chat_id, text, reply_to_message_id=topic_id)
    except Exception as e:
        print(f"❌ Failed to send log: {e}")

async def check_disk_space():
    """Returns False if free space is < 500MB"""
    try:
        total, used, free = shutil.disk_usage(".")
        free_mb = free / (1024 * 1024)
        if free_mb < 500: # Limit: 500MB
            return False
        return True
    except:
        return True

async def cleanup_watchdog():
    """Runs every 10 mins to clean stuck download folders older than 2 hours"""
    while True:
        await asyncio.sleep(600) # Check every 10 mins
        try:
            download_path = Path("./downloads")
            if not download_path.exists(): continue
            
            current_time = time.time()
            # 2 hours in seconds
            max_age = 2 * 60 * 60 
            
            for user_folder in download_path.iterdir():
                if user_folder.is_dir():
                    for task_folder in user_folder.iterdir():
                        if task_folder.is_dir():
                            folder_time = task_folder.stat().st_mtime
                            if (current_time - folder_time) > max_age:
                                await asyncio.to_thread(shutil.rmtree, task_folder, ignore_errors=True)
                                await send_log(f"🧹 **Auto-Cleanup:** Deleted stuck folder `{task_folder.name}` (Older than 2h)")
        except Exception as e:
            print(f"Watchdog Error: {e}")
            
async def start_task_final(client: Client, message_context: Message, task_data: dict, delay: int, user_id: int):
    # 1. DISK SPACE PRE-CHECK
    if not await check_disk_space():
        msg = "⚠️ **Server Busy:** Disk is almost full. Please wait for other tasks to finish."
        if isinstance(message_context, Message):
             await message_context.reply(msg, quote=True)
        await send_log("🚨 **Critical:** Disk Space Low (Under 500MB). Tasks rejected.")
        return

    # 2. QUEUE SYSTEM
    # If user has hit their limit (e.g., 2 tasks), queue this one.
    if user_id not in ADMINS and batch_temp.ACTIVE_TASKS[user_id] >= MAX_CONCURRENT_TASKS_PER_USER:
        TASK_QUEUE[user_id].append({
            "client": client,
            "message": message_context,
            "data": task_data,
            "delay": delay
        })
        position = len(TASK_QUEUE[user_id])
        await message_context.reply(f"⏳ **Added to Queue:** Position #{position}\nTask will start automatically when your current tasks finish.", quote=True)
        return

    # 3. START TASK (Standard Logic)
    task_uuid = uuid.uuid4().hex
    dest = task_data.get("dest_title", "Direct Message")
    
    batch_temp.ACTIVE_TASKS[user_id] += 1
    # IS_BATCH removed — no longer needed

    start_msg = f"✅ **Task Started!**\nDestination: `{dest}`\nSpeed: `{delay}s` delay\nTask ID: `{task_uuid[:8]}`"
    try:
        if isinstance(message_context, Message):
            if message_context.from_user.is_bot:
                await message_context.edit(start_msg)
            else:
                await message_context.reply(start_msg)
    except: pass
    
    # Log to Channel
    await send_log(f"▶️ **Task Started**\nUser: `{user_id}`\nLink: `{task_data['link'][:40]}...`")

    if user_id not in ACTIVE_PROCESSES:
        ACTIVE_PROCESSES[user_id] = {}
    ACTIVE_PROCESSES[user_id][task_uuid] = {
        "user": task_data.get("dest_title", f"User({user_id})"),
        "dest_title_name": task_data.get("dest_title", "Direct Message"), # Add this
        "item": task_data.get("link", "Unknown"),
        "started": time.time()
    }
    
    # CHANGE: Get the flag we saved earlier
    is_restricted = task_data.get("is_restricted", False)

    targets = [{"dest_id": task_data.get("dest_chat_id"), "dest_thread": task_data.get("dest_thread_id"), "dest_title": dest}]
    asyncio.create_task(
        process_links_logic(
            client,
            message_context,
            task_data["link"],
            targets=targets,
            dest_title=dest, 
            delay=delay,
            acc_user_id=user_id,
            task_uuid=task_uuid,
            is_restricted=is_restricted,
            allowed_types=task_data.get('allowed_types')
        )
    )   
    
# CHANGE: Upgraded to multi-target & filter logic
async def process_links_logic(client: Client, message: Message, text: str, targets=None, dest_title="Direct Message", delay=3, acc_user_id=None, task_uuid=None, is_restricted=False, allowed_types=None):
    # --- 1. SETUP USER & LOGGING ---
    user_id = acc_user_id or (message.from_user.id if message.from_user else 0)
    user_mention = message.from_user.mention if message.from_user else f"User({user_id})"
    
    if user_id not in ACTIVE_PROCESSES: ACTIVE_PROCESSES[user_id] = {}
    if not task_uuid: task_uuid = uuid.uuid4().hex
    
    ACTIVE_PROCESSES[user_id][task_uuid] = {
        "user": user_mention, 
        "dest_title_name": dest_title,
        "item": text[:50]+"...", 
        "started": time.time()
    }

    # --- 2. BATCH PROCESSING ---
    if "https://t.me/" in text:
        acc = None
        is_temp_client = False  # <--- ADD THIS HERE
        success_count = 0
        failed_count = 0
        total_count = 0
        status_message = None
        filter_thread_id = None # Store the Topic ID here
        
        start_time = time.time()
        source_title = "Unknown Source"

        try:
            was_cancelled = False
            clean_text = text.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("c/", "")
            parts = clean_text.split("/")

            # --- DETECT SOURCE TOPIC ID ---
            # If link is format: chat_id/topic_id/msg_id
            if len(parts) >= 3 and parts[1].isdigit(): 
                filter_thread_id = int(parts[1])

            # Parse range
            last_segment = parts[-1].strip()
            range_match = re.search(r"(\d+)\s*-\s*(\d+)", text)
            if range_match:
                fromID, toID = int(range_match.group(1)), int(range_match.group(2))
            else:
                fromID = toID = int(last_segment)

            total_count = max(1, toID - fromID + 1)

            # Session login
            user_data = await db.get_session(user_id)
            if not user_data:
                await message.reply("**/login First.**")
                return
            
            api_id = await db.get_api_id(user_id)
            api_hash = await db.get_api_hash(user_id)
            
            # Reuse existing client if running (Watcher), otherwise create temp
            is_temp_client = False
            if user_id in USER_CLIENTS and USER_CLIENTS[user_id].is_connected:
                acc = USER_CLIENTS[user_id]
            else:
                acc = Client(
                    f"task_{user_id}_{task_uuid}", 
                    session_string=user_data, 
                    api_hash=api_hash, 
                    api_id=api_id, 
                    no_updates=True,
                    workers=4,
                    sleep_threshold=60,
                    ipv6=False,
                    in_memory=True
                )
                await acc.start()
                is_temp_client = True
            
            try:
                chatid_check = int("-100" + parts[0]) if "https://t.me/c/" in text else parts[0]
                source_chat = await acc.get_chat(chatid_check)
                source_title = source_chat.title or "Private Chat"
            except: pass

            ACTIVE_PROCESSES[user_id][task_uuid].update({"source_title": source_title, "total": total_count, "current": 0})

            # --- STATUS MESSAGE SETUP ---
            status_text_header = f"**Batch Task Started!** 🚀\n"
            if filter_thread_id:
                status_text_header += f"**Filter:** `Topic {filter_thread_id} Only` 🎯\n"

            if is_restricted:
                status_message = await client.send_message(
                    message.chat.id,
                    f"⚡ **Initializing Task...**\n{status_text_header}\nSource: {source_title}\nTotal Files: {total_count}",
                    reply_to_message_id=message.id
                )
            else:
                status_message = await client.send_message(
                    message.chat.id,
                    f"{status_text_header}\n\n{generate_bar(0)}\n\n"
                    f"**Source:** {source_title}\n**Destination :** {dest_title}\n"
                    f"**Total:** {total_count}\n**Processed:** 0\n**Success:** 0\n**Failed:** 0\n**ETA:** ...",
                    reply_to_message_id=message.id
                )

            last_update_time = time.time()
            
            # Prepare header text for inner status
            inner_header = f"Filter: Topic {filter_thread_id} Only 🎯" if filter_thread_id else ""

            for index, msgid in enumerate(range(fromID, toID+1), start=1):
                loop_start_time = time.time()

                if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                    ACTIVE_PROCESSES[user_id][task_uuid]["current"] = index

                if CANCEL_FLAGS.get(task_uuid):
                    was_cancelled = True; break

                is_success = False
                try:
                    chatid = int("-100" + parts[0]) if "https://t.me/c/" in text else parts[0]
                    
                    is_success = await handle_private(
                        client, acc, message, chatid, msgid, index, total_count, 
                        status_message, targets, delay, 
                        user_id, task_uuid, 
                        is_restricted=is_restricted, 
                        header_text=inner_header,
                        filter_thread_id=filter_thread_id,
                        allowed_types=allowed_types
                    )
                
                except FloodWait as e:
                    if e.value > 300:
                        print(f"FloodWait too long ({e.value}s). Stopping task.")
                        await status_message.edit_text(f"❌ **Task Cancelled automatically**\nReason: FloodWait too long ({e.value}s).")
                        was_cancelled = True
                        break

                    wait_msg = f"⏳ **Rate Limiting Detected**\nSleeping for {e.value} seconds..."
                    try: 
                        if not is_restricted: await status_message.edit_text(wait_msg)
                    except: pass
                    await asyncio.sleep(e.value + 5)
                    
                except Exception as e: 
                    print(f"Error processing {msgid}: {e}")
                    pass

                if is_success: success_count += 1
                else: failed_count += 1

                # --- 3. UNIVERSAL SMART SLEEP ---
                if index < total_count:
                    if is_success:
                        # Success: Wait delay
                        elapsed_time = time.time() - loop_start_time
                        actual_sleep = max(0, delay - elapsed_time)
                        await asyncio.sleep(actual_sleep)
                    else:
                        # Failed/Skipped (Wrong Topic): Skip FAST (0.05s)
                        await asyncio.sleep(0.05)

                # --- UPDATE DASHBOARD ---
                if not is_restricted:
                    current_now = time.time()
                    if (index % 20 == 0) or (current_now - last_update_time >= 60) or index == total_count:
                        elapsed = current_now - start_time
                        percent = (index / total_count) * 100
                        eta_str = get_readable_time(int(((total_count - index) / (index / elapsed)))) if index > 0 else "..."
                        
                        try:
                            await status_message.edit_text(
                                f"{status_text_header}\n\n{generate_bar(percent)}\n\n"
                                f"**Source:** {source_title}\n**Destination :** {dest_title}\n"
                                f"**Total:** {total_count}\n**Processed:** {index}\n"
                                f"**Success:** {success_count}\n**Failed:** {failed_count}\n**ETA:** {eta_str}"
                            )
                            last_update_time = current_now
                        except: pass
                    
        except Exception as e:
            await send_log(f"❌ **Task Crashed**\nUser: `{user_id}`\nError: `{e}`")

        finally:
            if task_uuid in ACTIVE_PROCESSES.get(user_id, {}):
                try: del ACTIVE_PROCESSES[user_id][task_uuid]
                except: pass
            
            # Clean up cancel flag so it never bleeds into any future task
            CANCEL_FLAGS.pop(task_uuid, None)

            batch_temp.ACTIVE_TASKS[user_id] -= 1
            if batch_temp.ACTIVE_TASKS[user_id] < 0: batch_temp.ACTIVE_TASKS[user_id] = 0
            if TASK_QUEUE[user_id]:
                next_item = TASK_QUEUE[user_id].pop(0)
                asyncio.create_task(start_task_final(next_item["client"], next_item["message"], next_item["data"], next_item["delay"], user_id))

            if acc and is_temp_client:  # <--- ONLY STOP IF TEMP
                try: await acc.stop()
                except: pass

            duration = time.time() - start_time
            time_taken_str = get_readable_time(int(duration))
            
            if 'was_cancelled' in locals() and was_cancelled:
                header = f"Batch was Cancelled! 🛑 {user_mention} ✨"
            else:
                header = f"Batch was Completed! ✅ {user_mention} ✨"

            final_text = (
                f"{header}\n"
                f"📝 **Task :** {source_title} → {dest_title}\n"
                f"⏱ **Time Taken:** `{time_taken_str}`\n"
                f"📊 **Statistics:**\n"
                f"├ 📥 **Total Requested:** `{total_count}`\n"
                f"├ ✅ **Successful:** `{success_count}`\n"
                f"└ ❌ **Failed/Skipped:** `{failed_count}`"
            )
            
            try: await client.send_message(message.chat.id, final_text, reply_to_message_id=message.id)
            except: pass
            try: await status_message.delete()
            except: pass

# ==============================================================================
# --- handle_private: downloads & uploads with per-task cancel checks ---
# ==============================================================================

async def handle_private(client: Client, acc, message: Message, chatid, msgid: int, index: int, total_count: int, status_message: Message, targets: list, delay, user_id, task_uuid=None, is_restricted=False, header_text="", filter_thread_id=None, allowed_types=None):
    if not task_uuid:
        task_uuid = "default"
        
    msg = None
    try:
        msg = await acc.get_messages(chatid, msgid)
    except UserNotParticipant: return False
    except Exception: return False

    if not msg or msg.empty: return False
    
    # --- TOPIC FILTER CHECK ---
    if filter_thread_id is not None:
        if getattr(msg, "message_thread_id", None) != filter_thread_id:
            return False

    msg_type = get_message_type(msg)
    if not msg_type: return False

    # --- CONTENT FILTER CHECK ---
    if allowed_types is not None and msg_type not in allowed_types:
        return False

    if task_uuid and CANCEL_FLAGS.get(task_uuid): return False

    # --- CLEAR OLD STATUS ---
    try:
        if status_message:
            m_id = status_message.id
            if f"{m_id}:down" in PROGRESS: del PROGRESS[f"{m_id}:down"]
            if f"{m_id}:up" in PROGRESS: del PROGRESS[f"{m_id}:up"]
    except Exception: pass

    # 1. FAST FORWARD (Copy to Multiple Targets)
    if not is_restricted and not getattr(msg, "has_protected_content", False) and not getattr(msg.chat, "has_protected_content", False):
        forward_success = False
        for dest in targets:
            dest_chat_id = dest['dest_id']
            dest_thread_id = dest.get('dest_thread')
            try:
                await client.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, reply_to_message_id=dest_thread_id)
                forward_success = True
            except Exception:
                try:
                    await acc.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, reply_to_message_id=dest_thread_id)
                    forward_success = True
                except FloodWait as e:
                    if e.value > 300: raise e
                    await asyncio.sleep(e.value + 2)
                    await acc.copy_message(chat_id=dest_chat_id, from_chat_id=chatid, message_id=msgid, reply_to_message_id=dest_thread_id)
                    forward_success = True
                except Exception as e:
                    print(f"Task Fast-Copy blocked: {e}")
        if forward_success: return True
        # NOTE: 'return False' removed so it can drop down to download mode

    if "Text" == msg_type:
        for dest in targets:
            text_content = msg.text.html if msg.text else ""
            try: await client.send_message(dest['dest_id'], text_content, parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True, reply_to_message_id=dest.get('dest_thread'))
            except: pass
        return True

    # 2. PATHS & FILENAME
    task_folder_path = Path(f"./downloads/{user_id}/{task_uuid}/{msgid}/")
    task_folder_path.mkdir(parents=True, exist_ok=True)

    original_filename = "unknown_file"
    if msg.document and msg.document.file_name: original_filename = msg.document.file_name
    elif msg.video and msg.video.file_name: original_filename = msg.video.file_name
    elif msg.audio and msg.audio.file_name: original_filename = msg.audio.file_name
    elif msg_type == "Photo": original_filename = f"{msgid}.jpg"
    elif msg_type == "Voice": original_filename = f"{msgid}.ogg"

    safe_filename = sanitize_filename(original_filename)
    if not safe_filename.strip(): safe_filename = f"{msgid}.dat"
    file_path_to_save = task_folder_path / safe_filename

    # 3. START DOWNLOAD
    chat_for_status = status_message.chat.id if status_message else message.chat.id
    down_task = asyncio.create_task(downstatus(client, status_message, chat_for_status, index, total_count, header_text))
    file_path = None
    ph_path = None
    download_success = False

    # Forced to 2GB because Bots cannot upload files larger than 2GB via MTProto
    split_limit = 2000 * 1024 * 1024 

    try: 
        for attempt in range(3):
            if task_uuid and CANCEL_FLAGS.get(task_uuid): return False
            try:
                msg_fresh = await acc.get_messages(chatid, msgid)
                if msg_fresh.empty: return False
                
                file_size = 0
                if msg_fresh.document: file_size = msg_fresh.document.file_size
                elif msg_fresh.video: file_size = msg_fresh.video.file_size
                elif msg_fresh.audio: file_size = msg_fresh.audio.file_size

                if file_size > split_limit:
                    file_path = await acc.download_media(msg_fresh, file_name=str(file_path_to_save), progress=progress, progress_args=[status_message, "down", task_uuid])
                    
                    if down_task and not down_task.done(): down_task.cancel()
                    await status_message.edit_text(f"✂️ **Splitting large file ({_pretty_bytes(file_size)})...**")
                    parts = await split_file_python(file_path, chunk_size=1900*1024*1024)
                    
                    if f"{status_message.id}:up" in PROGRESS: del PROGRESS[f"{status_message.id}:up"]
                    up_task = asyncio.create_task(upstatus(client, status_message, chat_for_status, index, total_count, header_text))
                    caption = msg.caption.html if msg.caption else ""
                    
                    # Split Multi-Upload
                    async with USER_SEMAPHORES[user_id]:
                        async with SERVER_UPLOAD_LIMIT:
                            for part in parts:
                                if task_uuid and CANCEL_FLAGS.get(task_uuid): raise Exception("CANCELLED")
                                for dest in targets:
                                    dest_chat_id = dest['dest_id']
                                    dest_thread_id = dest.get('dest_thread')
                                    retry_part = 0
                                    while retry_part < 5: # Fight through network blips!
                                        try:
                                            await client.send_document(
                                                dest_chat_id, 
                                                str(part), 
                                                caption=caption, 
                                                parse_mode=enums.ParseMode.HTML, 
                                                reply_to_message_id=dest_thread_id,
                                                progress=progress,  # <--- ADDED
                                                progress_args=[status_message, "up", task_uuid] # <--- ADDED
                                            )
                                            break
                                        except FloodWait as e: 
                                            await asyncio.sleep(e.value + 5)
                                        except Exception as e: 
                                            if "CANCELLED" in str(e): raise e
                                            retry_part += 1
                                            await asyncio.sleep(3)
                                try: os.remove(part)
                                except: pass
                    
                    if up_task and not up_task.done(): up_task.cancel()
                    try: os.remove(file_path)
                    except: pass
                    return True 
                else:
                    try:
                        file_path = await asyncio.wait_for(
                            acc.download_media(msg_fresh, file_name=str(file_path_to_save), progress=progress, progress_args=[status_message, "down", task_uuid]),
                            timeout=1200
                        )
                    except asyncio.TimeoutError:
                        return False
                
                try:
                    thumb = None
                    if msg_fresh.document and msg_fresh.document.thumbs: thumb = msg_fresh.document.thumbs[0]
                    elif msg_fresh.video and msg_fresh.video.thumbs: thumb = msg_fresh.video.thumbs[0]
                    elif msg_fresh.audio and msg_fresh.audio.thumbs: thumb = msg_fresh.audio.thumbs[0]
                    if thumb: ph_path = await acc.download_media(thumb.file_id, file_name=str(task_folder_path / "thumb.jpg"))
                except: pass

                download_success = True
                break
            except FloodWait as e: 
                if e.value > 300: raise e
                await asyncio.sleep(e.value + 5)
            except Exception as e:
                if "CANCELLED" in str(e): return False
                await asyncio.sleep(5)

        if down_task and not down_task.done(): down_task.cancel()
        if not download_success: return False
        if task_uuid and CANCEL_FLAGS.get(task_uuid): return False

        if f"{status_message.id}:up" in PROGRESS: del PROGRESS[f"{status_message.id}:up"]
        up_task = asyncio.create_task(upstatus(client, status_message, chat_for_status, index, total_count, header_text))
        
        caption = msg.caption.html if msg.caption else ""
        
        # CHANGED: Hand the upload over to the Bot to bypass User Bandwidth limits!
        uploader = client 

        upload_success = False
        
        async def upload_to_dest(dest):
            dest_chat_id = dest['dest_id']
            dest_thread_id = dest.get('dest_thread')
            success_local = False
            async with SERVER_UPLOAD_LIMIT:
                async with USER_SEMAPHORES[user_id]:
                    retry_count = 0
                    while retry_count < 5: # Fight through network blips!
                        if task_uuid and CANCEL_FLAGS.get(task_uuid): break
                        try:
                            if "Document" == msg_type: await uploader.send_document(dest_chat_id, file_path, thumb=ph_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                            elif "Video" == msg_type: await uploader.send_video(dest_chat_id, file_path, duration=msg.video.duration, width=msg.video.width, height=msg.video.height, thumb=ph_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                            elif "Audio" == msg_type: await uploader.send_audio(dest_chat_id, file_path, thumb=ph_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                            elif "Photo" == msg_type: await uploader.send_photo(dest_chat_id, file_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id)
                            elif "Voice" == msg_type: await uploader.send_voice(dest_chat_id, file_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id, progress=progress, progress_args=[status_message,"up", task_uuid])
                            elif "Animation" == msg_type: await uploader.send_animation(dest_chat_id, file_path, caption=caption, parse_mode=enums.ParseMode.HTML, reply_to_message_id=dest_thread_id)
                            elif "Sticker" == msg_type: await uploader.send_sticker(dest_chat_id, file_path, reply_to_message_id=dest_thread_id)
                            success_local = True
                            break 
                        except FloodWait as e:
                            await asyncio.sleep(e.value + 5)
                        except Exception as e:
                            if "CANCELLED" in str(e): break
                            retry_count += 1
                            await asyncio.sleep(3)
            return success_local

        # Execute loop for Multi-Target
        for dest in targets:
            if task_uuid and CANCEL_FLAGS.get(task_uuid): break
            if await upload_to_dest(dest):
                upload_success = True
        
        if up_task and not up_task.done(): up_task.cancel()
        return upload_success

    finally:
        try: await asyncio.to_thread(shutil.rmtree, task_folder_path, ignore_errors=True)
        except: pass
        gc.collect()

# ==============================================================================
# --- Koyeb health check (optional) ---
# ==============================================================================
try:
    from aiohttp import web
except ImportError:
    web = None

async def _koyeb_health_handler(request):
    return web.Response(text="OK", status=200)

async def start_koyeb_health_check(host: str = "0.0.0.0", port: int | str = 8080):
    if web is None:
        print("aiohttp not installed; Koyeb health check not started.")
        return
    try:
        port = int(os.environ.get("PORT", str(port)))
    except Exception:
        port = 8080
    app_web = web.Application()
    app_web.router.add_get("/", _koyeb_health_handler)
    app_web.router.add_get("/health", _koyeb_health_handler)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Starting Koyeb health check server on port {port}...")

# ==============================================================================
# --- LIVE WATCHER ENGINE ---
# ==============================================================================

from pyrogram.handlers import MessageHandler

WATCHER_LOCKS = {} # Stores queues for live watchers

async def process_watcher_message(client, message):
    chat_id = message.chat.id
    topic_id = getattr(message, "message_thread_id", None)
    
    print(f"🔍 [DEBUG WATCHER] Caught a message! ID: {message.id} | Chat: {chat_id} | Topic: {topic_id}")
    
    # 1. Find Watcher in DB
    watcher = await db.get_watcher(chat_id, topic_id)
    if not watcher:
        watcher = await db.get_watcher(chat_id, None)
        
    if not watcher: 
        print(f"❌ [DEBUG WATCHER] Ignored: No DB entry for {chat_id}")
        return

    print(f"✅ [DEBUG WATCHER] DB Match Found for {chat_id}! Checking filters...")

    # 2. Extract Config & Validate Types
    targets = watcher.get('targets', [])
    if not targets and 'dest_id' in watcher:
        targets = [{"dest_id": watcher['dest_id'], "dest_thread": watcher.get('dest_thread'), "dest_title": watcher.get('dest_title')}]
        
    if not targets: 
        print("❌ [DEBUG WATCHER] Ignored: No targets configured.")
        return

    allowed_types = watcher.get('allowed_types', ["Video", "Document"])
    msg_type = get_message_type(message)
    
    print(f"⚙️ [DEBUG WATCHER] Message Type: {msg_type} | Allowed: {allowed_types}")
    
    if msg_type not in allowed_types: 
        print("❌ [DEBUG WATCHER] Ignored: Message type not in allowed list.")
        return

    delay = watcher.get('delay', 0)
    owner_id = watcher['user_id']
    is_restricted = watcher.get('is_restricted', False)

    print(f"⏳ [DEBUG WATCHER] Proceeding with routing. Delay: {delay}s | Restricted: {is_restricted}")

    # --- SEQUENTIAL QUEUE LOCK FOR THIS SOURCE ---
    lock_key = f"watcher_{chat_id}_{owner_id}"
    if lock_key not in WATCHER_LOCKS:
        WATCHER_LOCKS[lock_key] = asyncio.Lock()

    # The lock forces 100 simultaneous messages to wait in a single-file line!
    async with WATCHER_LOCKS[lock_key]:
        # 3. Wait Delay BETWEEN forwards
        if delay > 0:
            await asyncio.sleep(delay)

        is_content_protected = getattr(message, "has_protected_content", False) or getattr(message.chat, "has_protected_content", False)
        
        # --- MODE A: FAST COPY TO ALL (If Unrestricted) ---
        if not is_restricted and not is_content_protected:
            fallback_to_download = False
            
            # Use username if available so the Bot can access public channels it hasn't joined
            safe_source_id = message.chat.username if message.chat.username else chat_id
            
            for t in targets:
                success = False
                dest_id = t['dest_id']
                dest_thread = t.get('dest_thread')
                
                print(f"🚀 [DEBUG WATCHER] MODE A: Attempting Fast-Copy to {dest_id}")
                
                try: 
                    # OPTION 1: Try Bot First 
                    await app.copy_message(chat_id=dest_id, from_chat_id=safe_source_id, message_id=message.id, reply_to_message_id=dest_thread)
                    success = True
                    print("✅ [DEBUG WATCHER] Bot Fast-Copy SUCCESS!")
                except Exception as e1: 
                    print(f"⚠️ [DEBUG WATCHER] Bot Fast-Copy failed: {e1}. Trying Userbot Fallback...")
                    try:
                        # Refresh Userbot's blank memory so it doesn't get PeerIdInvalid
                        await client.get_chat(dest_id)
                    except Exception:
                        pass

                    try: 
                        # OPTION 2: Try User Fallback (Copy)
                        await client.copy_message(chat_id=dest_id, from_chat_id=chat_id, message_id=message.id, reply_to_message_id=dest_thread)
                        success = True
                        print("✅ [DEBUG WATCHER] Userbot Copy SUCCESS!")
                    except Exception as e2:
                        print(f"⚠️ [DEBUG WATCHER] Userbot Copy failed: {e2}. Trying Userbot Forward...")
                        try:
                            # OPTION 3: Try User Fallback (Forward)
                            await client.forward_messages(chat_id=dest_id, from_chat_id=chat_id, message_ids=message.id, message_thread_id=dest_thread)
                            success = True
                            print("✅ [DEBUG WATCHER] Userbot Forward SUCCESS!")
                        except Exception as e3:
                            # Send failure reason to LOG_CHANNEL so it never fails silently again
                            print(f"❌ [DEBUG WATCHER] ALL FAST-COPIES FAILED! Final Error: {e3}")
                            if LOG_CHANNEL:
                                try:
                                    log_chat = int(LOG_CHANNEL.split("/")[0]) if "/" in LOG_CHANNEL else int(LOG_CHANNEL)
                                    await app.send_message(log_chat, f"❌ **Fast-Copy Failed!**\nSource: `{chat_id}`\nDest: `{dest_id}`\nError: `{e3}`")
                                except: pass
                
                # If all failed, flag it to use Download mode!
                if not success:
                    print("🔄 [DEBUG WATCHER] Falling back to Mode B (Download/Upload)")
                    fallback_to_download = True

            # If fast copy worked perfectly, we are done!
            if not fallback_to_download:
                return 
                
        # --- MODE B: DOWNLOAD ONCE & UPLOAD TO ALL (Using Session) ---
        print("📥 [DEBUG WATCHER] MODE B: Entering Download/Upload Mode...")
        owner_client = USER_CLIENTS.get(owner_id)
        if not owner_client: 
            print("❌ [DEBUG WATCHER] Owner client not found in memory!")
            return 

        try:
            # Always notify the User so they aren't left in the dark
            dummy_status = await app.send_message(owner_id, f"⬇️ **Watcher:** Processing ID `{message.id}` (Download Mode)...")
            
            if LOG_CHANNEL:
                try:
                    if "/" in LOG_CHANNEL:
                        _parts = LOG_CHANNEL.split("/")
                        log_chat_id = int(_parts[0])
                        log_topic_id = int(_parts[1])
                    else:
                        log_chat_id = int(LOG_CHANNEL)
                        log_topic_id = None
                    await app.send_message(log_chat_id, f"⬇️ **Watcher:** Processing ID `{message.id}`...", reply_to_message_id=log_topic_id)
                except Exception:
                    pass

            await handle_private(
                client=app,
                acc=owner_client,
                message=message, 
                chatid=chat_id, 
                msgid=message.id, 
                index=1, 
                total_count=1, 
                status_message=dummy_status, 
                targets=targets,
                delay=0, 
                user_id=owner_id, 
                task_uuid=uuid.uuid4().hex,
                is_restricted=True,
                allowed_types=allowed_types
            )
            await dummy_status.delete()
            print("✅ [DEBUG WATCHER] Download/Upload Mode COMPLETE!")
        except Exception as e:
            print(f"❌ [DEBUG WATCHER] Watcher Mode B Fail: {e}")
            
async def user_watcher_handler(client, message):
    # This runs when User Session receives a message
    await process_watcher_message(client, message)

# ==============================================================================
# --- MAIN ENTRY POINT ---
# ==============================================================================

async def cleanup_startup():
    folder = Path("./downloads")
    if folder.exists():
        try:
            shutil.rmtree(folder)
            print("🧹 Startup: Cleared temporary downloads folder.")
        except Exception as e:
            print(f"⚠️ Could not clean downloads folder: {e}")
    folder.mkdir(parents=True, exist_ok=True)

async def main():
    global USER_CLIENTS
    
    # 1. Cleanup
    await cleanup_startup()
    asyncio.create_task(cleanup_watchdog())
    print("🛡️ Auto-Cleanup Watchdog Started")

    # 2. Start Bot
    await app.start()
    print("🤖 Bot Started")
    
    # --- AUTO-UPDATE COMMANDS (SCOPED) ---
    print("📝 Updating Bot Commands...")
    try:
        # A. Define the Public Commands (For Everyone)
        public_commands = [
            BotCommand("start", "⚡ Check Bot Is Working Or Not"),
            BotCommand("help", "🔎 Check How To Use Bot"),
            BotCommand("login", "📍 Login Your Telegram String Session"),
            BotCommand("logout", "🚨 Logout Your Session"),
            BotCommand("dl", "🦥 Reply to the link to forward"),
            BotCommand("watch", "👀 To live forward"),
            BotCommand("unwatch", "🗑 Stop watching a source"),
            BotCommand("watchers", "📋 List your active watchers"),
            BotCommand("cancel", "❌ Cancel Your Any Ongoing Task")
        ]

        # B. Define Admin Commands (Public + Extra)
        admin_commands = public_commands + [
            BotCommand("broadcast", "🗞 Broadcast Message"),
            BotCommand("botstats", "🔎 Check User Stats"),
            BotCommand("status", "🦥 Check System Status"),
            BotCommand("log", "📄 Fetch Bot Logs"),
            BotCommand("pixel", "✨ Bypass Pixeldrain Links")
        ]

        # C. Set Default Scope (Everyone sees public_commands)
        await app.set_bot_commands(public_commands, scope=BotCommandScopeDefault())

        # D. Set Admin Scope (Admins see EVERYTHING)
        # Combine ADMINS and SUDOS lists, remove duplicates
        all_admins = set(ADMINS + SUDOS)
        
        for admin_id in all_admins:
            try:
                await app.set_bot_commands(
                    admin_commands, 
                    scope=BotCommandScopeChat(chat_id=admin_id)
                )
            except Exception as e:
                print(f"⚠️ Could not set commands for Admin {admin_id}: {e}")
                
        print("✅ Commands Updated: Public vs Admin scopes set!")
        
    except Exception as e:
        print(f"⚠️ Failed to set commands: {e}")
    # ----------------------------
    
    # 3. Smart Load: Only load sessions for users with ACTIVE Watchers
    print("🔄 Loading Sessions for Active Watchers...")
    
    # Get all unique user_ids that have watchers in the DB
    active_watcher_users = set()
    cursor = await db.get_all_watchers()
    async for w in cursor:
        active_watcher_users.add(w['user_id'])

    for user_id in active_watcher_users:
        user_session = await db.get_session(user_id)
        if not user_session:
            continue
            
        try:
            # Check if already loaded
            if user_id in USER_CLIENTS: continue

            print(f"👤 Starting Watcher Session for: {user_id}")
            
            u_api = await db.get_api_id(user_id) or API_ID
            u_hash = await db.get_api_hash(user_id) or API_HASH
            
            user_client = Client(
                f"watcher_main_{user_id}", 
                session_string=user_session, 
                api_id=u_api, 
                api_hash=u_hash, 
                workers=4, 
                ipv6=False,
                no_updates=False
                # Removed in_memory=True so Telegram streams updates perfectly
            )
            
            await user_client.start()
            USER_CLIENTS[user_id] = user_client
            
            # Attach Handler without filters to ensure Pyrogram catches ALL channel updates!
            user_client.add_handler(MessageHandler(user_watcher_handler))
            
            print(f"🔄 Syncing channel updates for {user_id}...")
            
            # CRITICAL CHECK: Ensure userbot is actually an active participant in all watched sources
            async for w in db.db.watchers.find({'user_id': user_id}):
                target_source = w['source_id']
                try: 
                    # Use get_chat_member to actually verify physical membership!
                    await user_client.get_chat_member(target_source, "me")
                    print(f"📡 [DEBUG] Membership confirmed. Live stream bound to {target_source}")
                except Exception as e: 
                    print(f"🚨 [CRITICAL ERROR] Userbot is NOT a member of Source Channel {target_source}!! Telegram will ignore this channel.")
                    print(f"Reason: {e}")
                    print("⚠️ Fix: Manually join this channel on your userbot account!")

            print(f"✅ Active: {user_id}")
                
            # WAKE UP CALL 2: Force history pull on specific watched channels
            async for w in db.db.watchers.find({'user_id': user_id}):
                try: 
                    await user_client.get_chat(w['source_id'])
                    async for _ in user_client.get_chat_history(w['source_id'], limit=1): break
                    print(f"📡 [DEBUG] Stream bound to {w['source_id']}")
                except Exception as e: 
                    print(f"⚠️ [DEBUG] Stream bind failed for {w['source_id']}: {e}")

            print(f"✅ Active: {user_id}")
            
        except Exception as e:
            print(f"❌ Failed to load {user_id}: {e}")

    print(f"🔥 Total Live Listeners: {len(USER_CLIENTS)}")

    # 4. Health Check & Idle
    asyncio.create_task(start_koyeb_health_check())
    await idle()
    
    # 5. Stop
    await app.stop()
    for uid, client in USER_CLIENTS.items():
        try: await client.stop()
        except: pass
        
if __name__ == "__main__":
    app.run(main())
        
