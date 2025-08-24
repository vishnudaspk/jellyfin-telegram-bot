# bot.py
import os
import re
import shutil
import logging
import traceback
import aiohttp
import asyncio
import math
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import Optional, Tuple
from dotenv import load_dotenv


from telethon import TelegramClient, events, Button
from telethon.sessions import MemorySession
from telethon.errors import (
    MessageNotModifiedError, FloodWaitError,
    ChannelPrivateError, ChatAdminRequiredError, UsernameInvalidError,
    UsernameNotOccupiedError, MessageIdInvalidError, RpcCallFailError
)

load_dotenv()

# ====================== CONFIG ======================
API_ID = 23347751
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SESSION = os.getenv("SESSION")

JELLYFIN_PATH = os.getenv("JELLYFIN_PATH", "./movies")
TEMP_DIR = os.getenv("TEMP_DIR", "./temp")
JELLYFIN_URL = os.getenv("JELLYFIN_URL")
JELLYFIN_API_KEY = os.getenv("JELLYFIN_API_KEY")

# Allowed users (comma-separated)
ALLOWED_USERS = set(map(int, os.getenv("ALLOWED_USERS", "").split(","))) if os.getenv("ALLOWED_USERS") else set()

TEMP_DIR = os.getenv("TEMP_DIR", "./temp")
LOG_DIR = os.getenv("LOG_DIR", "./logs")

os.makedirs(JELLYFIN_PATH, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

MAX_LOG_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5

VALID_VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".m4v"}
VALID_SUB_EXT = {".srt", ".vtt", ".ass"}

# cleanup stale temp files older than N days on startup
TEMP_MAX_AGE_DAYS = 3

# =================== LOGGING SETUP ==================
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

logger = logging.getLogger("tg-jellyfin-bot")
logger.setLevel(logging.INFO)

# Console
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(ch)

# Rotating file
from logging.handlers import RotatingFileHandler
fh = RotatingFileHandler(
    os.path.join(LOG_DIR, "bot.log"),
    maxBytes=MAX_LOG_BYTES,
    backupCount=BACKUP_COUNT,
    encoding="utf-8"
)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)

# =================== TELETHON CLIENT =================
client = TelegramClient(MemorySession(), API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# =================== SMALL UTILITIES =================

def is_allowed(uid: int) -> bool:
    return not ALLOWED_USERS or uid in ALLOWED_USERS

def now_ts() -> float:
    return asyncio.get_event_loop().time()

def safe_title(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]', "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name

def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while os.path.exists(f"{base} ({i}){ext}"):
        i += 1
    return f"{base} ({i}){ext}"

def fmt_size(n: int) -> str:
    if n is None:
        return "?"
    units = ["B","KB","MB","GB","TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024.0; i += 1
    if f >= 100 or i == 0:
        return f"{int(f)} {units[i]}"
    return f"{f:.1f} {units[i]}"

def progress_bar_str(progress: float, width: int = 24) -> str:
    progress = max(0.0, min(1.0, progress))
    filled = int(round(progress * width))
    return "[" + "█"*filled + "░"*(width - filled) + f"] {int(progress*100):3d}%"

def eta_string(bytes_done: int, total: Optional[int], speed_bps: float) -> str:
    if not total or total <= 0 or speed_bps <= 1e-6:
        return "—"
    remaining = total - bytes_done
    secs = remaining / speed_bps
    if secs < 60:
        return f"{int(secs)}s"
    mins = secs/60
    if mins < 60:
        return f"{int(mins)}m"
    hrs = mins/60
    return f"{int(hrs)}h"

def ensure_user_temp(uid: int) -> str:
    p = os.path.join(TEMP_DIR, str(uid))
    os.makedirs(p, exist_ok=True)
    return p

def cleanup_stale_temp():
    cutoff = datetime.now() - timedelta(days=TEMP_MAX_AGE_DAYS)
    removed = 0
    for root, dirs, files in os.walk(TEMP_DIR):
        for name in files:
            fp = os.path.join(root, name)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(fp))
                if mtime < cutoff:
                    os.remove(fp)
                    removed += 1
            except Exception:
                pass
    if removed:
        logger.info(f"Temp cleanup: removed {removed} stale files older than {TEMP_MAX_AGE_DAYS} days.")

# =============== PROGRESS MESSAGE UPDATER ============
class ProgressUpdater:
    def __init__(self, message_obj, *,
                 min_percent_step: int = 1,
                 min_interval: float = 1.0,
                 prefix: str = ""):
        self.message = message_obj
        self.min_percent_step = min_percent_step
        self.min_interval = min_interval
        self.prefix = prefix
        self._last_percent = -1
        self._last_time = 0.0
        self._lock = asyncio.Lock()

    async def update(self, fraction: float, text: str):
        percent = int(round(fraction * 100))
        now = now_ts()
        async with self._lock:
            if percent == self._last_percent and (now - self._last_time) < self.min_interval:
                return
            if self._last_percent >= 0 and (percent - self._last_percent) < self.min_percent_step and (now - self._last_time) < self.min_interval:
                return
            self._last_percent = percent
            self._last_time = now
            await safe_edit_msg(self.message, text)

async def safe_edit_msg(message_obj, text: str, *, max_retries: int = 3):
    if not message_obj:
        return
    attempt = 0
    while attempt <= max_retries:
        try:
            if message_obj.raw_text == text:
                return
            await message_obj.edit(text)
            return
        except MessageNotModifiedError:
            return
        except FloodWaitError as e:
            wait_time = int(getattr(e, "seconds", 5))
            logger.warning(f"safe_edit_msg FloodWait - sleeping {wait_time}s")
            await asyncio.sleep(wait_time)
            attempt += 1
        except Exception:
            logger.error("safe_edit_msg failed:\n" + traceback.format_exc())
            return
    logger.error(f"safe_edit_msg: failed after {max_retries} retries")

# ================= DOWNLOAD MANAGER ==================
class DownloadManager:
    """
    Telegram download with resume support using client.download_file and offset.
    """
    def __init__(self, client: TelegramClient):
        self.client = client

    async def download_message_media(
        self,
        msg,
        dest_path: str,
        status_updater: ProgressUpdater,
        label: str = "Downloading"
    ) -> str:
        # decide temp and final
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        part_path = dest_path + ".part"

        # get (maybe) total file size from message
        total = getattr(getattr(msg, "file", None), "size", None)

        # resume position
        existing = 0
        if os.path.exists(part_path):
            existing = os.path.getsize(part_path)
            if total and existing > total:
                # corrupt; start over
                existing = 0
                os.remove(part_path)

        # speed averaging
        window = []
        window_secs = 5.0
        last_t = now_ts()
        last_b = existing

        async def prog_cb(current: int, total_inner: int):
            nonlocal window, last_t, last_b
            # current here is absolute bytes downloaded in this session; add existing for total-done
            done = existing + current
            t = now_ts()
            dt = t - last_t
            db = done - last_b
            if dt > 0:
                window.append(db / dt)
                # keep last ~5s samples
                while len(window) > 10:
                    window.pop(0)
            speed = sum(window) / len(window) if window else 0.0
            last_t = t
            last_b = done

            frac = (done / total) if total else 0.0
            bar = progress_bar_str(frac)
            text = f"⏳ {label} ({fmt_size(done)} / {fmt_size(total)}) – {int(frac*100) if total else 0}% {bar} • ~{eta_string(done, total, speed)} ETA"
            await status_updater.update(frac, text)

        try:
            # open file in append-binary
            fobj = open(part_path, "ab")
        except Exception:
            logger.error("download: cannot open part file\n" + traceback.format_exc())
            raise

        try:
            await self.client.download_file(
                msg.media,
                file=fobj,
                file_size=total,
                progress_callback=prog_cb,
                # offset=existing if existing > 0 else None
            )
        finally:
            fobj.close()

        # finalize
        os.replace(part_path, dest_path)
        await status_updater.update(1.0, f"✅ {label} complete ({fmt_size(total) if total else 'done'})")
        return dest_path

# =================== FILE COPIER (RESUME) =============
class FileCopier:
    """
    Resume-able copy using *.part; continues from existing part size.
    """
    def __init__(self, updater: ProgressUpdater):
        self.updater = updater

    async def copy_with_progress(self, src: str, dst: str, label: str = "Copying"):
        total = os.path.getsize(src)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        tmp = dst + ".part"

        # resume if tmp exists
        copied = 0
        if os.path.exists(tmp):
            copied = os.path.getsize(tmp)
            if copied > total:
                # corrupt
                os.remove(tmp)
                copied = 0

        # progress state
        window = []
        last_t = now_ts()
        last_b = copied

        async def emit():
            nonlocal window, last_t, last_b, copied
            t = now_ts()
            dt = t - last_t
            db = copied - last_b
            if dt > 0:
                window.append(db/dt)
                while len(window) > 10:
                    window.pop(0)
            spd = sum(window)/len(window) if window else 0.0
            frac = copied/total if total else 0.0
            bar = progress_bar_str(frac)
            text = f"📂 {label} ({fmt_size(copied)} / {fmt_size(total)}) – {int(frac*100)}% {bar} • ~{eta_string(copied, total, spd)} ETA"
            await self.updater.update(frac, text)
            last_t = t
            last_b = copied

        # open src and tmp
        with open(src, "rb") as r, open(tmp, "ab") as w:
            if copied:
                r.seek(copied)
            chunk = 4 * 1024 * 1024
            while True:
                data = r.read(chunk)
                if not data:
                    break
                w.write(data)
                copied += len(data)
                # flush occasionally to persist progress
                if copied % (16*chunk) == 0:
                    w.flush()
                await emit()

        os.replace(tmp, dst)
        await self.updater.update(1.0, f"✅ {label} complete ({fmt_size(total)})")

# ================== USER SESSION (STATE) ==============
class UserSession:
    def __init__(self, uid: int):
        self.uid = uid
        self.mode: Optional[str] = None            # "upload" | "forward"
        self.step: Optional[str] = None
        self.language: Optional[str] = None
        self.movie_name: Optional[str] = None
        self.movie_year: Optional[str] = None
        self.movie_path: Optional[str] = None      # downloaded file path
        self.subtitle_path: Optional[str] = None
        self.movie_link: Optional[str] = None      # if provided as link

    def reset(self):
        self.__init__(self.uid)

_sessions = {}  # uid -> UserSession

def get_session(uid: int) -> UserSession:
    s = _sessions.get(uid)
    if not s:
        s = UserSession(uid)
        _sessions[uid] = s
    return s

# =================== JELLYFIN REFRESH =================
async def jellyfin_refresh() -> bool:
    url = f"{JELLYFIN_URL.rstrip('/')}/Library/Refresh"
    headers = {"X-Emby-Token": JELLYFIN_API_KEY}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, timeout=30) as resp:
                if resp.status == 204:
                    logger.info("Jellyfin: library refresh triggered (204).")
                    return True
                logger.warning(f"Jellyfin refresh HTTP {resp.status}")
                return False
    except Exception:
        logger.exception("Jellyfin refresh error")
        return False

# =================== LINK RESOLUTION ==================
async def fetch_message_from_link(link: str):
    """
    Supports:
      - https://t.me/c/<raw_chat_id>/<msg_id>
      - https://t.me/<username>/<msg_id>
    Returns message or raises an error with reason.
    """
    try:
        link = link.split("?")[0].strip()
        u = urlparse(link)
        if u.netloc not in {"t.me", "telegram.me"}:
            raise ValueError("invalid_link")

        parts = u.path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("invalid_link")

        if parts[0] == "c" and len(parts) >= 3:
            raw = parts[1]
            msg_id = int(parts[2])
            peer = int(f"-100{raw}")
            msg = await client.get_messages(peer, ids=msg_id)
            if not msg:
                raise MessageIdInvalidError(request=None)
            return msg
        else:
            username = parts[0]
            msg_id = int(parts[1])
            msg = await client.get_messages(username, ids=msg_id)
            if not msg:
                raise MessageIdInvalidError(request=None)
            return msg

    except ChannelPrivateError:
        raise PermissionError("bot_not_in_channel_or_private_chat")
    except ChatAdminRequiredError:
        raise PermissionError("bot_lacks_rights")
    except UsernameInvalidError:
        raise ValueError("username_invalid")
    except UsernameNotOccupiedError:
        raise ValueError("username_not_found")
    except MessageIdInvalidError:
        raise ValueError("message_not_found")
    except ValueError as ve:
        if str(ve) in {"invalid_link", "username_invalid", "username_not_found"}:
            raise
        raise ValueError("invalid_link")
    except Exception:
        logger.error("fetch_message_from_link unexpected:\n" + traceback.format_exc())
        raise RuntimeError("unexpected_error")

# ===================== VALIDATION =====================
def validate_movie_path(path: str) -> Tuple[bool, str]:
    ext = (os.path.splitext(path)[1] or "").lower()
    if ext not in VALID_VIDEO_EXT:
        return False, f"Unsupported video type: {ext or '(none)'}.\nAllowed: {', '.join(sorted(VALID_VIDEO_EXT))}"
    return True, ""

def validate_subtitle_path(path: str) -> Tuple[bool, str]:
    ext = (os.path.splitext(path)[1] or "").lower()
    if ext not in VALID_SUB_EXT:
        return False, f"Unsupported subtitle type: {ext or '(none)'}.\nAllowed: {', '.join(sorted(VALID_SUB_EXT))}"
    return True, ""

# ======================= UX HELPERS ===================
def start_keyboard():
    return [
        [Button.inline("⬆️ Upload", b"input_upload")],
        [Button.inline("🔗 Link / Forward", b"input_forward")]
    ]

def language_keyboard():
    return [
        [Button.inline("English", b"lang_en")],
        [Button.inline("Malayalam", b"lang_ml")],
        [Button.inline("Tamil", b"lang_ta")],
        [Button.inline("Custom…", b"lang_custom")]
    ]

def yes_no_keyboard(yes=b"yes", no=b"no"):
    return [[Button.inline("✅ Yes", yes), Button.inline("❌ No", no)]]

def retry_cancel_keyboard(retry_data: bytes, cancel_data: bytes = b"cancel"):
    return [[Button.inline("🔁 Retry", retry_data), Button.inline("🚫 Cancel", cancel_data)]]

async def send_year_picker(event):
    years = [str(datetime.now().year - i) for i in range(6)]
    kb = [[Button.inline(y, f"year_{y}".encode())] for y in years]
    kb.append([Button.inline("Other…", b"year_other")])
    await event.reply("📅 Select release year:", buttons=kb)

async def send_summary(event, s: UserSession):
    title = s.movie_name or "<missing>"
    yr = s.movie_year or "<missing>"
    lang = (s.language or "<missing>").title()
    sub = "Yes" if s.subtitle_path else "No"
    summary = f"🎬 *Confirm Movie Info:*\n\nTitle: {title} ({yr})\nLanguage: {lang}\nSubtitle: {sub}"
    kb = [[Button.inline("✅ Confirm", b"confirm_yes"), Button.inline("🔄 Start Over", b"confirm_no")]]
    await event.reply(summary, parse_mode="md", buttons=kb)

# ===================== EVENT HANDLERS =================
@client.on(events.NewMessage(pattern="/start"))
async def on_start(event):
    uid = event.sender_id
    logger.info(f"[uid={uid}] /start")
    if not is_allowed(uid):
        await event.respond("🚫 You are not authorized to use this bot.")
        return
    get_session(uid).reset()
    await event.respond("📥 How would you like to provide the movie?", buttons=start_keyboard())

@client.on(events.CallbackQuery(pattern=b"input_"))
async def on_input_choice(event):
    uid = event.sender_id
    if not is_allowed(uid):
        await event.answer("🚫 Not authorized", alert=True); return
    s = get_session(uid)
    choice = event.data.decode().split("_", 1)[1]
    if choice == "upload":
        s.mode = "upload"; s.step = "waiting_media"
        await event.edit("📤 Send the movie file (video/document).")
    else:
        s.mode = "forward"; s.step = "waiting_link"
        await event.edit("🔗 Forward the message with media *or* paste the Telegram link (https://t.me/…)", parse_mode="md")
    logger.info(f"[uid={uid}] chose {choice}")

@client.on(events.CallbackQuery(pattern=b"lang_"))
async def on_lang(event):
    uid = event.sender_id
    if not is_allowed(uid): await event.answer("🚫 Not authorized", alert=True); return
    s = get_session(uid)
    if not s.step:
        await event.answer("Start with /start", alert=True); return
    chosen = event.data.decode().split("_",1)[1]
    if chosen == "custom":
        s.step = "custom_language"
        await event.edit("✍️ Type the custom language name:")
    else:
        mapping = {"en":"english","ml":"malayalam","ta":"tamil"}
        s.language = mapping.get(chosen, chosen)
        s.step = "movie_name"
        await event.edit("🎬 Enter the movie name:")
    logger.info(f"[uid={uid}] language -> {chosen}")

@client.on(events.CallbackQuery(pattern=b"year_"))
async def on_year(event):
    uid = event.sender_id
    if not is_allowed(uid): await event.answer("🚫 Not authorized", alert=True); return
    s = get_session(uid)
    if not s.step: await event.answer("Start with /start", alert=True); return
    y = event.data.decode().split("_",1)[1]
    if y == "other":
        s.step = "movie_year_other"
        await event.edit("✍️ Enter the four-digit release year (e.g. 2024):")
    else:
        s.movie_year = y; s.step = "subtitle_question"
        await event.edit("📝 Do you have a subtitle file?", buttons=yes_no_keyboard(b"subtitle_yes", b"subtitle_no"))
    logger.info(f"[uid={uid}] year -> {y}")

@client.on(events.CallbackQuery(pattern=b"subtitle_"))
async def on_subtitle_choice(event):
    uid = event.sender_id
    if not is_allowed(uid): await event.answer("🚫 Not authorized", alert=True); return
    s = get_session(uid)
    if not s.step: await event.answer("Start with /start", alert=True); return
    choice = event.data.decode().split("_",1)[1]
    if choice == "yes":
        s.step = "waiting_subtitle"
        await event.edit("📤 Send the subtitle file (.srt / .ass / .vtt):")
    else:
        await event.edit("✅ Noted. No subtitle. Preparing summary…")
        await send_summary(event, s)
    logger.info(f"[uid={uid}] subtitle -> {choice}")

@client.on(events.CallbackQuery(pattern=b"confirm_"))
async def on_confirm(event):
    uid = event.sender_id
    if not is_allowed(uid): await event.answer("🚫 Not authorized", alert=True); return
    s = get_session(uid)
    choice = event.data.decode().split("_",1)[1]
    if choice == "yes":
        status = await event.edit("📂 Finalizing and copying files…")
        await finalize_pipeline(uid, edit_msg=status)
    else:
        s.reset()
        await event.edit("🔄 Start over with /start")

@client.on(events.CallbackQuery(pattern=b"retry_"))
async def on_retry(event):
    """
    Generic retry router:
    - retry_download
    - retry_copy
    - retry_refresh
    """
    uid = event.sender_id
    if not is_allowed(uid): return
    s = get_session(uid)
    tag = event.data.decode().split("_",1)[1]
    if tag == "download":
        msg = await event.edit("🔁 Retrying download…")
        await retry_download(uid, msg)
    elif tag == "copy":
        msg = await event.edit("🔁 Retrying copy…")
        await retry_copy(uid, msg)
    elif tag == "refresh":
        msg = await event.edit("🔁 Retrying library refresh…")
        await retry_refresh(uid, msg)

@client.on(events.CallbackQuery(pattern=b"cancel"))
async def on_cancel(event):
    uid = event.sender_id
    s = get_session(uid)
    await event.edit("🛑 Cancelled. Use /start to begin again.")
    s.reset()

# ============== TEXT + MEDIA HANDLERS =================
@client.on(events.NewMessage)
async def on_text(event):
    uid = event.sender_id
    text = (event.raw_text or "").strip()
    if not is_allowed(uid): return
    s = get_session(uid)
    if not s.step:
        return

    # Forward mode: expecting link/forward
    if s.mode == "forward" and s.step == "waiting_link":
        # link
        if text.startswith("http") and "t.me" in text:
            try:
                msg_obj = await fetch_message_from_link(text)
            except PermissionError as pe:
                reason = str(pe)
                friendly = "⚠ I can’t access that message."
                if reason == "bot_not_in_channel_or_private_chat":
                    friendly += "\n• The bot is not a member of that private channel/chat."
                elif reason == "bot_lacks_rights":
                    friendly += "\n• The bot lacks permission in that chat."
                await event.reply(friendly, buttons=retry_cancel_keyboard(b"retry_download"))
                return
            except ValueError as ve:
                code = str(ve)
                friendly = "⚠ Invalid link."
                if code == "message_not_found":
                    friendly = "⚠ That message ID was not found."
                elif code == "username_not_found":
                    friendly = "⚠ That username/channel was not found."
                await event.reply(friendly)
                return
            except RuntimeError:
                logger.error(f"[uid={uid}] link fetch unexpected:\n{traceback.format_exc()}")
                await event.reply("⚠ Unexpected error while resolving the link.", buttons=retry_cancel_keyboard(b"retry_download"))
                return

            if msg_obj and getattr(msg_obj, "media", None):
                user_tmp = ensure_user_temp(uid)
                status_msg = await event.respond("⏳ Resolving media and preparing download…")
                s.step = "downloading_from_link"
                s.movie_link = text
                await do_download_message_media(uid, msg_obj, status_msg)
                return
            else:
                await event.reply("⚠ That message doesn’t seem to contain downloadable media.")
                return

        # forwarded media directly
        if event.media:
            user_tmp = ensure_user_temp(uid)
            status_msg = await event.respond("⏳ Preparing download…")
            s.step = "downloading_forwarded"
            await do_download_message_media(uid, event.message, status_msg)
            return

        await event.reply("⚠ Please forward a message with media or send a direct Telegram link (https://t.me/…)")
        return

    # Custom language text
    if s.step == "custom_language":
        s.language = text.lower(); s.step = "movie_name"
        await event.reply("🎬 Enter the movie name:")
        return

    # Movie name
    if s.step == "movie_name":
        s.movie_name = text; s.step = "movie_year"
        await send_year_picker(event)
        return

    # Year other
    if s.step == "movie_year_other":
        if not text.isdigit() or len(text) != 4:
            await event.reply("⚠ Enter a valid 4-digit year (e.g., 2024):")
            return
        s.movie_year = text; s.step = "subtitle_question"
        await event.reply("📝 Do you have a subtitle file?", buttons=yes_no_keyboard(b"subtitle_yes", b"subtitle_no"))
        return

    # Subtitle upload
    if s.step == "waiting_subtitle" and event.media:
        user_tmp = ensure_user_temp(uid)
        status_msg = await event.reply("⏳ Downloading subtitle…")
        try:
            downloaded = await client.download_media(event.media, file=user_tmp)
            ok, why = validate_subtitle_path(downloaded)
            if not ok:
                os.remove(downloaded)
                await safe_edit_msg(status_msg, f"⚠ {why}")
                return
            s.subtitle_path = downloaded
            await safe_edit_msg(status_msg, "✅ Subtitle downloaded.")
            await send_summary(event, s)
        except Exception:
            logger.error(f"[uid={uid}] subtitle download failed:\n{traceback.format_exc()}")
            await safe_edit_msg(status_msg, "⚠ Failed to download subtitle.",)
        return

# direct uploads (video/document)
@client.on(events.NewMessage(func=lambda e: e.document or e.video))
async def on_direct_upload(event):
    uid = event.sender_id
    if not is_allowed(uid): return
    s = get_session(uid)
    if not (s.mode == "upload" and s.step == "waiting_media"):
        return
    user_tmp = ensure_user_temp(uid)
    status_msg = await event.reply("⏳ Downloading movie…")
    s.step = "downloading_upload"
    await do_download_message_media(uid, event.message, status_msg)

# ================ DOWNLOAD / COPY STEPS ===============
async def do_download_message_media(uid: int, message_obj, status_msg):
    s = get_session(uid)
    user_tmp = ensure_user_temp(uid)
    # decide dest filename (may not have metadata yet)
    base = safe_title(s.movie_name or f"movie_{message_obj.id}")
    ext = os.path.splitext(getattr(message_obj, "file", type("", (), {"name": None})) .name or "")[1].lower()
    if not ext:
        # guess from mime
        mime = getattr(getattr(message_obj, "file", None), "mime_type", "") or ""
        ext = {
            "video/mp4": ".mp4",
            "video/x-matroska": ".mkv",
            "video/x-msvideo": ".avi",
            "video/quicktime": ".mov",
        }.get(mime, ".mp4")
    dest_path = os.path.join(user_tmp, base + ext)
    dest_path = unique_path(dest_path)

    updater = ProgressUpdater(status_msg, min_percent_step=1, min_interval=1.0)
    dm = DownloadManager(client)
    try:
        await dm.download_message_media(message_obj, dest_path, updater, label="Downloading")
    except Exception:
        logger.error(f"[uid={uid}] download failed:\n{traceback.format_exc()}")
        await safe_edit_msg(status_msg, "⚠ Download failed.",)
        await status_msg.edit(buttons=retry_cancel_keyboard(b"retry_download"))
        return

    # validate file type
    ok, why = validate_movie_path(dest_path)
    if not ok:
        try: os.remove(dest_path)
        except: pass
        await safe_edit_msg(status_msg, f"⚠ {why}")
        return

    s.movie_path = dest_path
    s.step = "choose_language"
    await safe_edit_msg(status_msg, "✅ Download complete. Please choose language:")
    await status_msg.respond("🌐 Select movie language:", buttons=language_keyboard())

async def retry_download(uid: int, status_msg):
    s = get_session(uid)
    # Try to re-download from stored link or last message path (not available).
    # If we have a link, refetch & redownload to same directory (resume will kick in).
    try:
        if s.movie_link:
            msg_obj = await fetch_message_from_link(s.movie_link)
        else:
            await safe_edit_msg(status_msg, "⚠ Nothing to retry. Send a link/forward again or /start.")
            return
    except Exception:
        await safe_edit_msg(status_msg, "⚠ Still cannot access that link.")
        return
    await do_download_message_media(uid, msg_obj, status_msg)

async def retry_copy(uid: int, status_msg):
    s = get_session(uid)
    if not s.movie_path or not (s.language and s.movie_name and s.movie_year):
        await safe_edit_msg(status_msg, "⚠ Missing information to retry copy. Use /start.")
        return
    await finalize_pipeline(uid, edit_msg=status_msg, retry_copy_only=True)

async def retry_refresh(uid: int, status_msg):
    ok = await jellyfin_refresh()
    if ok:
        await safe_edit_msg(status_msg, "✅ Library refresh triggered.")
    else:
        await safe_edit_msg(status_msg, "⚠ Library refresh failed. Try again later.")
        await status_msg.edit(buttons=retry_cancel_keyboard(b"retry_refresh"))

# ================ FINALIZE PIPELINE ===================
async def finalize_pipeline(uid: int, edit_msg, retry_copy_only: bool = False):
    s = get_session(uid)
    if not s:
        await safe_edit_msg(edit_msg, "⚠ Missing state; start over with /start"); return

    movie_path = s.movie_path
    language = s.language
    movie_name = s.movie_name
    year = s.movie_year
    subtitle_path = s.subtitle_path

    if not all([language, movie_name, year, movie_path]):
        await safe_edit_msg(edit_msg, "⚠ Missing info; start over with /start")
        s.reset()
        return

    safe_base = f"{safe_title(movie_name)} ({year})"
    target_lang_dir = os.path.join(JELLYFIN_PATH, language.lower())
    os.makedirs(target_lang_dir, exist_ok=True)

    # decide final paths
    ext = os.path.splitext(movie_path)[1] or ".mp4"
    if subtitle_path:
        movie_folder = os.path.join(target_lang_dir, safe_base)
        os.makedirs(movie_folder, exist_ok=True)
        final_movie_path = unique_path(os.path.join(movie_folder, safe_base + ext))
        final_sub_path = unique_path(os.path.join(movie_folder, safe_base + os.path.splitext(subtitle_path)[1]))
    else:
        final_movie_path = unique_path(os.path.join(target_lang_dir, safe_base + ext))

    # COPY MOVIE (resume-able)
    updater = ProgressUpdater(edit_msg, min_percent_step=1, min_interval=1.0)
    copier = FileCopier(updater)
    try:
        await copier.copy_with_progress(movie_path, final_movie_path, label="Copying movie")
    except Exception:
        logger.error(f"[uid={uid}] copy movie failed:\n{traceback.format_exc()}")
        await safe_edit_msg(edit_msg, "⚠ Failed to copy movie to library.")
        await edit_msg.edit(buttons=retry_cancel_keyboard(b"retry_copy"))
        if not retry_copy_only:
            return

    # COPY SUBTITLE (optional)
    if subtitle_path:
        try:
            await copier.copy_with_progress(subtitle_path, final_sub_path, label="Copying subtitle")
        except Exception:
            logger.error(f"[uid={uid}] copy subtitle failed:\n{traceback.format_exc()}")
            await safe_edit_msg(edit_msg, "⚠ Failed to copy subtitle to library (movie copied).")

    # cleanup temp originals (keep if retry_copy_only? Already copied.)
    try:
        if movie_path and os.path.exists(movie_path):
            os.remove(movie_path)
    except Exception:
        pass
    if subtitle_path:
        try: os.remove(subtitle_path)
        except Exception: pass

    # Trigger Jellyfin refresh
    await safe_edit_msg(edit_msg, "🔄 Triggering Jellyfin library refresh…")
    ok = await jellyfin_refresh()
    if ok:
        await safe_edit_msg(edit_msg, f"✅ Saved: {final_movie_path}\n🔄 Jellyfin refresh triggered.")
    else:
        await safe_edit_msg(edit_msg, f"✅ Saved: {final_movie_path}\n⚠ Jellyfin refresh failed.")
        await edit_msg.edit(buttons=retry_cancel_keyboard(b"retry_refresh"))

    s.reset()

# ====================== MAIN =========================
if __name__ == "__main__":
    logger.info("🚀 Telethon → Jellyfin Movie Bot starting…")
    cleanup_stale_temp()
    client.run_until_disconnected()