import os
import re
import time
import logging
import threading
import uuid
import queue
import yt_dlp
import imageio_ffmpeg
from urllib.parse import urlparse
from collections import defaultdict
from flask import Flask, jsonify
import telebot
from telebot import types

# ============================================================
# Configuration
# ============================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required!")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()
logging.info(f"FFmpeg path: {FFMPEG_PATH}")

# ============================================================
# Allowed domains
# ============================================================
ALLOWED_DOMAINS = {
    'youtube.com', 'youtu.be', 'm.youtube.com',
    'tiktok.com', 'vm.tiktok.com', 'vt.tiktok.com',
    'instagram.com', 'twitter.com', 'x.com', 't.co',
    'facebook.com', 'fb.watch', 'fb.com',
    'reddit.com', 'v.redd.it',
    'vimeo.com', 'dailymotion.com', 'dai.ly',
    'twitch.tv', 'soundcloud.com',
    'pinterest.com', 'pin.it',
    'snapchat.com', 'linkedin.com',
    'bilibili.com', 'weibo.com',
    'imgur.com', 'flickr.com',
    'tumblr.com', 'vk.com',
    'ok.ru', 'rutube.ru',
    'streamable.com', 'gfycat.com',
    'coub.com', 'likee.video',
    'threads.net', 'truthsocial.com',
}

def is_allowed_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        for allowed in ALLOWED_DOMAINS:
            if domain == allowed or domain.endswith('.' + allowed):
                return True
        return False
    except Exception:
        return False

# ============================================================
# Rate limiting
# ============================================================
user_requests = defaultdict(list)
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 5

def check_rate_limit(user_id: int):
    now = time.time()
    user_requests[user_id] = [t for t in user_requests[user_id] if now - t < RATE_LIMIT_WINDOW]
    if len(user_requests[user_id]) >= RATE_LIMIT_MAX:
        wait = int(RATE_LIMIT_WINDOW - (now - user_requests[user_id][0]))
        return False, wait
    user_requests[user_id].append(now)
    return True, 0

# ============================================================
# Queue and shared state
# ============================================================
download_queue = queue.Queue(maxsize=100)
user_pending = {}          # {user_id: url} for "More" button
progress_lock = threading.Lock()
last_progress_update = {}  # {msg_id: timestamp}

def download_worker():
    while True:
        try:
            task = download_queue.get(timeout=5)
            if task is None:
                break
            process_download(task)
            download_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker error: {e}", exc_info=True)

# ============================================================
# Helpers
# ============================================================
def format_duration(seconds) -> str:
    if not seconds:
        return "Unknown"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

def format_size(mb: float) -> str:
    if not mb:
        return "Unknown"
    if mb >= 1024:
        return f"{mb/1024:.2f} GB"
    return f"{mb:.1f} MB"

def make_blocks_bar(percent: float, length: int = 16) -> str:
    filled = int(length * max(0, min(100, percent)) / 100)
    return "█" * filled + "░" * (length - filled)

def progress_message_text(percent: float, quality: str, size_str: str) -> str:
    bar = make_blocks_bar(percent)
    return (
        f"Downloading\n\n"
        f"<code>{bar}</code> {percent:.0f}%\n\n"
        f"Quality: {quality}\n"
        f"Format: MP4\n"
        f"Size: {size_str}"
    )

def progress_hook_factory(chat_id: int, msg_id: int, quality_label: str):
    def hook(d):
        try:
            if d['status'] != 'downloading':
                return
            now = time.time()
            with progress_lock:
                last = last_progress_update.get(msg_id, 0)
                if now - last < 2:
                    return
                last_progress_update[msg_id] = now

            downloaded = d.get('downloaded_bytes', 0) or 0
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            percent = (downloaded / total * 100) if total > 0 else 0

            size_str = f"{format_size(downloaded / (1024*1024))}"
            if total > 0:
                size_str += f" / {format_size(total / (1024*1024))}"

            text = progress_message_text(percent, quality_label, size_str)
            try:
                bot.edit_message_text(text, chat_id, msg_id)
            except Exception:
                pass
        except Exception as e:
            logging.error(f"progress_hook error: {e}")
    return hook

# ============================================================
# yt-dlp options and functions
# ============================================================
def base_ydl_opts():
    return {
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        },
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'ignoreerrors': False,
        'no_color': True,
    }

def translate_error(err: str) -> str:
    if 'Private video' in err or 'This video is private' in err:
        return "This video is private."
    if 'not available' in err or 'This video is not available' in err:
        return "Video is not available or has been removed."
    if 'region' in err.lower() or 'geo' in err.lower():
        return "Video is geo-blocked in your region."
    if 'Login required' in err or 'Sign in' in err:
        return "This content requires login."
    if 'Unsupported URL' in err:
        return "URL is not supported."
    if 'HTTP Error 429' in err:
        return "Rate limit exceeded. Try again later."
    if 'HTTP Error 403' in err:
        return "Server rejected the request (403)."
    if 'HTTP Error 404' in err:
        return "Video not found (404)."
    if 'Unexpected response' in err:
        return "Target server rejected the request. Try again."
    if 'Unable to extract' in err:
        return "Unable to extract video data."
    if 'Video unavailable' in err:
        return "Video unavailable."
    if 'ffmpeg' in err.lower():
        return "Video processing error (ffmpeg)."
    clean = err.replace('ERROR:', '').strip()
    return f"Download failed: {clean[:200]}"

def get_media_info(url: str):
    """Fetch media metadata without downloading."""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'no_color': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if 'entries' in info:
                info = info['entries'][0]

            title = info.get('title', 'Unknown')
            duration = info.get('duration', 0)
            thumbnail = info.get('thumbnail')
            extractor = info.get('extractor_key') or info.get('extractor') or 'Unknown'

            formats = info.get('formats', [])
            heights = set()
            has_audio = False
            best_size = 0
            for f in formats:
                h = f.get('height')
                if h and f.get('vcodec') and f.get('vcodec') != 'none':
                    heights.add(h)
                if f.get('acodec') and f.get('acodec') != 'none':
                    has_audio = True
                fs = f.get('filesize') or f.get('filesize_approx')
                if fs and fs > best_size:
                    best_size = fs

            return {
                'title': title,
                'duration': duration,
                'thumbnail': thumbnail,
                'platform': extractor,
                'qualities': sorted(heights, reverse=True),
                'has_audio': has_audio,
                'size_mb': round(best_size / (1024*1024), 1) if best_size else 0,
            }
    except Exception as e:
        logging.error(f"get_media_info error: {e}")
        return None

def get_available_formats(url: str) -> list:
    ydl_opts = base_ydl_opts()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if 'entries' in info:
                info = info['entries'][0]
            formats = []
            for f in info.get('formats', []):
                if f.get('vcodec') == 'none':
                    continue
                height = f.get('height')
                if not height:
                    continue
                filesize = f.get('filesize') or f.get('filesize_approx') or 0
                size_mb = filesize / (1024 * 1024)
                formats.append({
                    'id': f.get('format_id', ''),
                    'height': height,
                    'size_mb': round(size_mb, 1),
                })
            seen = set()
            unique = []
            for f in sorted(formats, key=lambda x: x['height'], reverse=True):
                if f['height'] not in seen:
                    seen.add(f['height'])
                    unique.append(f)
            return unique[:6]
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []

def download_video(url, output_path, chat_id=None, msg_id=None, quality_label="Auto"):
    ydl_opts = base_ydl_opts()
    ydl_opts.update({
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if chat_id and msg_id:
        ydl_opts['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        return False, translate_error(str(e))
    except Exception as e:
        return False, f"Technical error: {type(e).__name__}"

def download_specific_format(url, output_path, format_id, chat_id=None, msg_id=None, quality_label="Custom"):
    ydl_opts = base_ydl_opts()
    ydl_opts.update({
        'format': f'{format_id}+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if chat_id and msg_id:
        ydl_opts['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        return False, translate_error(str(e))
    except Exception as e:
        return False, f"Technical error: {type(e).__name__}"

def download_audio(url, output_path):
    ydl_opts = base_ydl_opts()
    ydl_opts.update({
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': output_path,
    })
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        return False, translate_error(str(e))
    except Exception as e:
        return False, f"Technical error: {type(e).__name__}"

def download_gallery(url, output_dir):
    try:
        from gallery_dl import job
        os.makedirs(output_dir, exist_ok=True)
        config = {
            'base-directory': output_dir,
            'filename': '{num:03d}_{filename}.{extension}',
            'quiet': True,
        }
        j = job.DownloadJob(url, config=config)
        j.run()
        files = sorted([f for f in os.listdir(output_dir)
                        if os.path.isfile(os.path.join(output_dir, f))])
        return True, [os.path.join(output_dir, f) for f in files]
    except Exception as e:
        logging.error(f"gallery-dl error: {e}")
        return False, []

def cleanup_file(path):
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

def cleanup_dir(directory):
    import shutil
    if directory and os.path.exists(directory):
        try:
            shutil.rmtree(directory)
        except Exception:
            pass

def find_output_file(temp_path):
    if os.path.exists(temp_path):
        return temp_path
    base = os.path.basename(temp_path).rsplit('.', 1)[0]
    if os.path.exists('/tmp'):
        for f in os.listdir('/tmp'):
            if f.startswith(base):
                return f"/tmp/{f}"
    return None

# ============================================================
# Media info message
# ============================================================
def send_media_info(chat_id, info):
    qualities = info['qualities'][:5]
    qualities_str = " • ".join(f"{q}p" for q in qualities) if qualities else "Unknown"
    duration = format_duration(info['duration'])
    size_str = format_size(info['size_mb']) if info['size_mb'] else "Unknown"
    audio_str = "Available" if info['has_audio'] else "Not available"
    platform = info['platform']

    caption = (
        "MEDIA INFORMATION\n\n"
        f"Title\n{info['title'][:120]}\n\n"
        f"Platform\n{platform}\n\n"
        f"Duration\n{duration}\n\n"
        f"Available Quality\n{qualities_str}\n\n"
        f"Audio\n{audio_str}\n\n"
        f"Size\n{size_str}"
    )

    if info.get('thumbnail'):
        try:
            return bot.send_photo(chat_id, info['thumbnail'], caption=caption)
        except Exception as e:
            logging.warning(f"send_photo failed: {e}")
    return bot.send_message(chat_id, caption)

# ============================================================
# Process download
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"

    info_msg = None
    progress_msg = None

    try:
        # 1. Fetch info
        info = get_media_info(url)
        quality_label = "Auto (Best)"
        if info and info['qualities']:
            quality_label = f"{info['qualities'][0]}p"

        # 2. Send info message
        if info:
            try:
                info_msg = send_media_info(chat_id, info)
            except Exception as e:
                logging.warning(f"info message error: {e}")

        # 3. Send initial progress message (immediately)
        try:
            progress_msg = bot.send_message(
                chat_id,
                progress_message_text(0, quality_label, "0.0 MB")
            )
        except Exception as e:
            logging.warning(f"progress message error: {e}")

        # 4. Download with progress updates
        ok, err = download_video(
            url, temp,
            chat_id=chat_id,
            msg_id=progress_msg.message_id if progress_msg else None,
            quality_label=quality_label
        )

        if not ok:
            # Fallback to gallery-dl
            logging.info(f"yt-dlp failed ({err}), trying gallery-dl...")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                # Delete info/progress and send gallery
                for m in (info_msg, progress_msg):
                    if m:
                        try: bot.delete_message(chat_id, m.message_id)
                        except: pass
                send_gallery(chat_id, files)
                return
            err_text = f"Download failed.\n\n{err}"
            if progress_msg:
                try:
                    bot.edit_message_text(err_text, chat_id, progress_msg.message_id)
                except: pass
            return

        final = find_output_file(temp)
        if not final:
            if progress_msg:
                try:
                    bot.edit_message_text("File was not created.", chat_id, progress_msg.message_id)
                except: pass
            return

        size_mb = os.path.getsize(final) / (1024 * 1024)

        # 5. Delete info and progress messages
        for m in (info_msg, progress_msg):
            if m:
                try:
                    bot.delete_message(chat_id, m.message_id)
                except: pass

        if size_mb > 50:
            bot.send_message(
                chat_id,
                f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).\n"
                f"Use the options below to download a lower quality."
            )
            send_more_options(chat_id, user_id, url)
            cleanup_file(final)
            return

        # 6. Store URL for "More" button
        user_pending[user_id] = url

        # 7. Send video with "More" button only
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("More", callback_data="more"))
        with open(final, 'rb') as v:
            bot.send_video(chat_id, v, reply_markup=markup, timeout=180,
                           supports_streaming=True)

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        if progress_msg:
            try:
                bot.edit_message_text(
                    f"Technical error: {type(e).__name__}\n{str(e)[:200]}",
                    chat_id, progress_msg.message_id
                )
            except: pass
    finally:
        cleanup_file(temp)
        cleanup_dir(temp_dir)

def send_gallery(chat_id, files):
    try:
        for i, f in enumerate(files[:10]):
            with open(f, 'rb') as img:
                bot.send_photo(chat_id, img)
        if len(files) > 10:
            bot.send_message(chat_id, f"Sent 10 of {len(files)} images.")
    except Exception as e:
        logging.error(f"send_gallery error: {e}")

def send_more_options(chat_id, user_id, url):
    formats = get_available_formats(url)
    if not formats:
        bot.send_message(chat_id, "No format information available.")
        return
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        label = f"{f['height']}p"
        if f['size_mb']:
            label += f" - {f['size_mb']:.0f}MB"
        buttons.append(types.InlineKeyboardButton(
            label, callback_data=f"q|{f['id']}|{f['height']}p"
        ))
    buttons.append(types.InlineKeyboardButton("Audio Only (MP3)", callback_data="a"))
    markup.add(*buttons)
    bot.send_message(chat_id, "AVAILABLE OPTIONS", reply_markup=markup)

# ============================================================
# Handlers
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    text = (
        "<b>yt-dlp</b>\n"
        "Professional Media Downloading Service\n\n"
        "Built on the powerful yt-dlp engine, this service provides fast and reliable "
        "media extraction from a wide range of supported platforms.\n\n"
        "Download videos, audio, images, posts, and supported media in the available "
        "quality and formats.\n\n"
        "<b>FEATURES</b>\n"
        "• High-quality video and audio extraction\n"
        "• Multiple formats and quality options\n"
        "• Support for videos, posts, and media galleries\n"
        "• Direct file delivery\n"
        "• Fast and reliable processing\n"
        "• Broad platform compatibility\n\n"
        "<b>HOW TO USE</b>\n"
        "Send a supported media URL and the service will process it automatically."
    )
    bot.reply_to(message, text)

@bot.callback_query_handler(func=lambda call: call.data == 'more')
def handle_more(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    url = user_pending.get(user_id)

    bot.answer_callback_query(call.id)

    if not url:
        bot.send_message(chat_id, "Session expired. Please send the URL again.")
        return

    send_more_options(chat_id, user_id, url)

@bot.callback_query_handler(func=lambda call: call.data.startswith('q|'))
def handle_quality(call):
    try:
        parts = call.data.split('|', 2)
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Invalid data.")
            return
        fmt_id = parts[1]
        quality_label = parts[2]

        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = user_pending.get(user_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        temp = f"/tmp/q_{user_id}_{uuid.uuid4().hex[:8]}.mp4"

        progress_msg = bot.send_message(
            chat_id,
            progress_message_text(0, quality_label, "0.0 MB")
        )

        try:
            ok, err = download_specific_format(
                url, temp, fmt_id,
                chat_id=chat_id, msg_id=progress_msg.message_id,
                quality_label=quality_label
            )
            if not ok:
                try:
                    bot.edit_message_text(f"Download failed.\n\n{err}",
                                          chat_id, progress_msg.message_id)
                except: pass
                return

            final = find_output_file(temp)
            if not final:
                try:
                    bot.edit_message_text("File was not created.",
                                          chat_id, progress_msg.message_id)
                except: pass
                return

            size_mb = os.path.getsize(final) / (1024 * 1024)

            try: bot.delete_message(chat_id, progress_msg.message_id)
            except: pass

            if size_mb > 50:
                bot.send_message(chat_id,
                    f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).")
                return

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data="more"))

            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180,
                               supports_streaming=True)
        finally:
            cleanup_file(temp)
    except Exception as e:
        logging.error(f"handle_quality error: {e}", exc_info=True)

@bot.callback_query_handler(func=lambda call: call.data == 'a')
def handle_audio(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = user_pending.get(user_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        progress_msg = bot.send_message(chat_id, "Downloading audio...")
        temp = f"/tmp/a_{user_id}_{uuid.uuid4().hex[:8]}"

        try:
            ok, err = download_audio(url, temp)
            if not ok:
                try:
                    bot.edit_message_text(f"Download failed.\n\n{err}",
                                          chat_id, progress_msg.message_id)
                except: pass
                return

            final = find_output_file(temp + ".mp3") or find_output_file(temp)
            if not final:
                try:
                    bot.edit_message_text("File was not created.",
                                          chat_id, progress_msg.message_id)
                except: pass
                return

            try: bot.delete_message(chat_id, progress_msg.message_id)
            except: pass

            with open(final, 'rb') as a:
                bot.send_audio(chat_id, a, timeout=180)
        finally:
            cleanup_file(temp + ".mp3")
            cleanup_file(temp)
    except Exception as e:
        logging.error(f"handle_audio error: {e}", exc_info=True)

@bot.message_handler(func=lambda m: True)
def handle(message):
    logging.info(f"Got message: {message.text}")
    url = message.text.strip()

    if not re.match(r'https?://', url):
        bot.reply_to(message, "Please send a valid URL starting with http:// or https://")
        return

    if not is_allowed_url(url):
        bot.reply_to(message, "This domain is not supported.")
        return

    allowed, wait = check_rate_limit(message.from_user.id)
    if not allowed:
        bot.reply_to(message,
            f"Rate limit exceeded (5 links per minute).\nTry again in {wait} seconds.")
        return

    try:
        msg = bot.reply_to(message, "Received. Processing...")
        task = {
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'url': url,
            'msg_id': msg.message_id,
        }
        download_queue.put_nowait(task)
    except queue.Full:
        bot.reply_to(message, "Server busy. Try again shortly.")

# ============================================================
# Flask
# ============================================================
@app.route('/health')
def health():
    return jsonify({"status": "ok", "queue_size": download_queue.qsize()}), 200

@app.route('/')
def index():
    return "Bot is running!", 200

# ============================================================
# Run
# ============================================================
def run_bot():
    try:
        bot.remove_webhook()
        logging.info("Old webhook removed.")
    except Exception as e:
        logging.warning(f"remove_webhook: {e}")
    logging.info("=== Starting polling ===")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)

threading.Thread(target=download_worker, daemon=True).start()
threading.Thread(target=run_bot, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
