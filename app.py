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
user_pending = {}  # {user_id: {'url': str, 'ts': float, 'formats': list|None}}
USER_PENDING_TTL = 3600  # ساعة واحدة
_cleanup_counter = 0


def _cleanup_user_pending():
    now = time.time()
    stale = [uid for uid, data in user_pending.items()
             if now - data.get('ts', 0) > USER_PENDING_TTL]
    for uid in stale:
        user_pending.pop(uid, None)
    if stale:
        logging.info(f"Cleaned {len(stale)} stale entries from user_pending")


def _store_user_pending(user_id: int, url: str, formats=None):
    existing = user_pending.get(user_id, {})
    user_pending[user_id] = {
        'url': url,
        'ts': time.time(),
        'formats': formats if formats is not None else existing.get('formats'),
    }


def _get_user_pending(user_id: int):
    data = user_pending.get(user_id)
    if not data:
        return None
    if time.time() - data.get('ts', 0) > USER_PENDING_TTL:
        user_pending.pop(user_id, None)
        return None
    return data.get('url')


def _get_user_formats(user_id: int):
    data = user_pending.get(user_id)
    if not data:
        return None
    return data.get('formats')


def _background_fetch_formats(user_id: int, url: str):
    """جلب التنسيقات في الخلفية بعد إرسال الفيديو."""
    try:
        formats = get_available_formats(url)
        if formats and user_id in user_pending:
            user_pending[user_id]['formats'] = formats
            logging.info(f"Background formats cached for user {user_id}: {len(formats)} formats")
    except Exception as e:
        logging.warning(f"background fetch formats failed: {e}")


def download_worker():
    global _cleanup_counter
    while True:
        try:
            task = download_queue.get(timeout=5)
            if task is None:
                break
            # ✅ تنظيف كل 50 مهمة فقط لتسريع الأداء
            _cleanup_counter += 1
            if _cleanup_counter % 50 == 0:
                _cleanup_user_pending()
            process_download(task)
            download_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker error: {e}", exc_info=True)


# ============================================================
# Helpers
# ============================================================
def format_size(mb: float) -> str:
    if not mb:
        return "Unknown"
    if mb >= 1024:
        return f"{mb/1024:.2f} GB"
    return f"{mb:.1f} MB"


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
        # ✅ إصلاح يوتيوب: تجربة عملاء متعددين لتجاوز حظر مراكز البيانات
        'extractor_args': {
            'youtube': {
                'player_client': ['ios', 'android', 'tv_embedded', 'web_safari', 'mweb'],
            }
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
    if 'Failed to extract any player response' in err:
        return "YouTube blocked this request from the server. Try again later."
    if 'Video unavailable' in err:
        return "Video unavailable."
    if 'ffmpeg' in err.lower():
        return "Video processing error (ffmpeg)."
    clean = err.replace('ERROR:', '').strip()
    return f"Download failed: {clean[:200]}"


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


def download_video(url, output_path):
    """
    تنزيل بأعلى جودة مع دعم كامل لجميع المنصات.
    يستخدم عدة محاولات متتالية مع إعدادات مختلفة لتجنب الفشل.
    """
    original_url = url
    is_twitter = any(d in url for d in ['twitter.com', 'x.com'])
    is_tiktok = 'tiktok.com' in url
    is_youtube = 'youtube.com' in url or 'youtu.be' in url

    # تحويل روابط تويتر إلى fxtwitter (يعمل بشكل أفضل مع yt-dlp)
    if is_twitter:
        url = url.replace('twitter.com', 'fxtwitter.com').replace('x.com', 'fxtwitter.com')
        logging.info(f"Twitter URL converted: {url}")

    attempts = []

    # محاولة 1: الإعدادات المثالية
    opts1 = base_ydl_opts()
    opts1.update({
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if is_twitter:
        opts1['extractor_args'] = {'twitter': {'api': ['syndication']}}
    if is_tiktok:
        opts1['format'] = 'b'
    attempts.append(('optimal', url, opts1))

    # محاولة 2: تويتر legacy
    if is_twitter:
        opts2 = base_ydl_opts()
        opts2.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'extractor_args': {'twitter': {'api': ['legacy']}},
        })
        attempts.append(('twitter_legacy', original_url, opts2))

    # ✅ محاولة يوتيوب بديلة 1: tv_embedded (يعمل بدون po_token)
    if is_youtube:
        opts_yt1 = base_ydl_opts()
        opts_yt1.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'extractor_args': {
                'youtube': {
                    'player_client': ['tv_embedded', 'web_embedded'],
                    'player_skip': ['webpage', 'configs'],
                }
            },
        })
        attempts.append(('youtube_tv_embedded', original_url, opts_yt1))

    # ✅ محاولة يوتيوب بديلة 2: mweb (بدون po_token)
    if is_youtube:
        opts_yt2 = base_ydl_opts()
        opts_yt2.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'extractor_args': {
                'youtube': {
                    'player_client': ['mweb', 'web'],
                }
            },
        })
        attempts.append(('youtube_mweb', original_url, opts_yt2))

    # محاولة عامة: أفضل ملف مدمج
    opts3 = base_ydl_opts()
    opts3.update({
        'format': 'b',
        'outtmpl': output_path,
    })
    attempts.append(('best_merged', original_url, opts3))

    last_error = None
    for attempt_name, attempt_url, opts in attempts:
        try:
            logging.info(f"Trying download [{attempt_name}]: {attempt_url[:80]}")
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([attempt_url])
            logging.info(f"Success with [{attempt_name}]")
            return True, None
        except yt_dlp.utils.DownloadError as e:
            last_error = str(e)
            logging.warning(f"[{attempt_name}] failed: {last_error[:150]}")
            continue
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            logging.warning(f"[{attempt_name}] exception: {last_error[:150]}")
            continue

    return False, translate_error(last_error or "All attempts failed")


def download_specific_format(url, output_path, format_id, height=None):
    """تنزيل جودة محددة مع نظام محاولات متعدد."""
    original_url = url
    if any(d in url for d in ['twitter.com', 'x.com']):
        url = url.replace('twitter.com', 'fxtwitter.com').replace('x.com', 'fxtwitter.com')

    attempts = []

    # محاولة 1: الجودة المطلوبة + أفضل صوت
    opts1 = base_ydl_opts()
    opts1.update({
        'format': f'{format_id}+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    attempts.append(('specific+audio', url, opts1))

    # محاولة 2: الجودة المطلوبة فقط
    opts2 = base_ydl_opts()
    opts2.update({
        'format': format_id,
        'outtmpl': output_path,
    })
    attempts.append(('specific_only', original_url, opts2))

    # محاولة 3: أعلى جودة أقل من أو تساوي الارتفاع المطلوب
    if height and str(height).isdigit():
        opts3 = base_ydl_opts()
        opts3.update({
            'format': f'bv*[height<={height}]+ba/b[height<={height}]/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
        })
        attempts.append(('height_filter', original_url, opts3))

    last_error = None
    for attempt_name, attempt_url, opts in attempts:
        try:
            logging.info(f"Trying format download [{attempt_name}]")
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([attempt_url])
            return True, None
        except yt_dlp.utils.DownloadError as e:
            last_error = str(e)
            logging.warning(f"[{attempt_name}] failed: {last_error[:150]}")
            continue
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            continue

    return False, translate_error(last_error or "Format download failed")


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
    """تنزيل الصور باستخدام gallery-dl مع إعدادات محسّنة."""
    try:
        from gallery_dl import job
        os.makedirs(output_dir, exist_ok=True)

        config = {
            'base-directory': output_dir,
            'filename': '{num:03d}_{filename}.{extension}',
            'quiet': True,
            'sleep-request': 1.0,
            'retries': 3,
            'timeout': 30,
            'verify': True,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        j = job.DownloadJob(url, config=config)
        j.run()

        files = sorted([
            f for f in os.listdir(output_dir)
            if os.path.isfile(os.path.join(output_dir, f))
        ])

        if not files:
            logging.warning(f"gallery-dl: no files downloaded from {url}")
            return False, []

        logging.info(f"gallery-dl: downloaded {len(files)} files")
        return True, [os.path.join(output_dir, f) for f in files]

    except Exception as e:
        logging.error(f"gallery-dl error: {e}", exc_info=True)
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
# Process download
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"

    final = None

    try:
        ok, err = download_video(url, temp)

        if not ok:
            logging.info(f"yt-dlp failed ({err}), trying gallery-dl...")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                send_gallery(chat_id, files)
                return
            bot.send_message(chat_id, f"Download failed.\n\n{err}")
            return

        final = find_output_file(temp)
        if not final:
            bot.send_message(chat_id, "File was not created.")
            return

        size_mb = os.path.getsize(final) / (1024 * 1024)

        if size_mb > 50:
            bot.send_message(
                chat_id,
                f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).\n"
                f"Use the options below to download a lower quality."
            )
            _store_user_pending(user_id, url)
            send_more_options(chat_id, user_id, url)
            return

        _store_user_pending(user_id, url)

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("More", callback_data="more"))

        with open(final, 'rb') as v:
            bot.send_video(
                chat_id, v,
                reply_markup=markup,
                timeout=180,
                supports_streaming=True
            )

        # ✅ جلب التنسيقات في الخلفية لتسريع زر More
        threading.Thread(
            target=_background_fetch_formats,
            args=(user_id, url),
            daemon=True
        ).start()

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, f"Technical error: {type(e).__name__}")
        except:
            pass
    finally:
        cleanup_file(temp)
        if final and final != temp:
            cleanup_file(final)
        cleanup_dir(temp_dir)


def send_gallery(chat_id, files):
    """إرسال الصور مع دعم صيغ متعددة."""
    if not files:
        bot.send_message(chat_id, "No images found.")
        return

    image_exts = ('.jpg', '.jpeg', '.png', '.webp', '.gif')
    video_exts = ('.mp4', '.webm', '.mov', '.mkv')

    images = [f for f in files if f.lower().endswith(image_exts)]
    videos = [f for f in files if f.lower().endswith(video_exts)]

    sent_count = 0
    for f in images[:10]:
        try:
            with open(f, 'rb') as img:
                bot.send_photo(chat_id, img, timeout=120)
            sent_count += 1
        except Exception as e:
            logging.error(f"send_photo failed: {e}")

    for f in videos[:3]:
        try:
            with open(f, 'rb') as v:
                bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
        except Exception as e:
            logging.error(f"send_video from gallery failed: {e}")

    if len(images) > 10:
        bot.send_message(chat_id, f"Sent 10 of {len(images)} images.")

    if sent_count == 0 and not videos:
        bot.send_message(chat_id, "No viewable media found.")


def send_more_options(chat_id, user_id, url):
    # ✅ استخدام التنسيقات المخزنة مؤقتاً إذا متوفرة
    formats = _get_user_formats(user_id)
    if not formats:
        formats = get_available_formats(url)
        if formats and user_id in user_pending:
            user_pending[user_id]['formats'] = formats

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
            label, callback_data=f"q|{f['id']}|{f['height']}|{f['height']}p"
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
    PHOTO_FILE_ID = "AgACAgQAAxkBAAM7ar1x_-DZJG9VDd5NRPy2xPtsreoAAs4SaxsCGulR6CNW3Nej7kwBAAMCAAN5AAM9BA"

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("⚙ Settings", callback_data="dev"),
        types.InlineKeyboardButton("❓ Help", callback_data="dev"),
    )
    markup.add(
        types.InlineKeyboardButton("ℹ About", callback_data="dev"),
        types.InlineKeyboardButton("📊 Status", callback_data="dev"),
    )

    try:
        bot.send_photo(message.chat.id, PHOTO_FILE_ID, caption=text, reply_markup=markup)
    except Exception as e:
        logging.error(f"send_photo failed: {e}")
        bot.reply_to(message, text, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data == 'dev')
def handle_dev(call):
    bot.answer_callback_query(call.id, "Under development", show_alert=False)


@bot.callback_query_handler(func=lambda call: call.data == 'more')
def handle_more(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    url = _get_user_pending(user_id)

    bot.answer_callback_query(call.id)

    if not url:
        bot.send_message(chat_id, "Session expired. Please send the URL again.")
        return

    send_more_options(chat_id, user_id, url)


@bot.callback_query_handler(func=lambda call: call.data.startswith('q|'))
def handle_quality(call):
    try:
        parts = call.data.split('|', 3)
        if len(parts) < 4:
            bot.answer_callback_query(call.id, "Invalid data.")
            return
        fmt_id = parts[1]
        height = parts[2]

        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = _get_user_pending(user_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        temp = f"/tmp/q_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
        final = None

        try:
            ok, err = download_specific_format(url, temp, fmt_id, height=height)
            if not ok:
                bot.send_message(chat_id, f"Download failed.\n\n{err}")
                return

            final = find_output_file(temp)
            if not final:
                bot.send_message(chat_id, "File was not created.")
                return

            size_mb = os.path.getsize(final) / (1024 * 1024)

            if size_mb > 50:
                bot.send_message(
                    chat_id,
                    f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB)."
                )
                return

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data="more"))

            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180,
                               supports_streaming=True)
        finally:
            cleanup_file(temp)
            if final and final != temp:
                cleanup_file(final)
    except Exception as e:
        logging.error(f"handle_quality error: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data == 'a')
def handle_audio(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = _get_user_pending(user_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        temp = f"/tmp/a_{user_id}_{uuid.uuid4().hex[:8]}"
        final = None

        try:
            ok, err = download_audio(url, temp)
            if not ok:
                bot.send_message(chat_id, f"Download failed.\n\n{err}")
                return

            final = find_output_file(temp + ".mp3") or find_output_file(temp)
            if not final:
                bot.send_message(chat_id, "File was not created.")
                return

            with open(final, 'rb') as a:
                bot.send_audio(chat_id, a, timeout=180)
        finally:
            cleanup_file(temp + ".mp3")
            cleanup_file(temp)
            if final and final not in (temp, temp + ".mp3"):
                cleanup_file(final)
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
