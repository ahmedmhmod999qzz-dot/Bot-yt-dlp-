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
video_details_cache = {}   # {message_id: {'details': ..., 'minimal': ...}}
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

def escape_html(text: str) -> str:
    """تهريب رموز HTML الخاصة."""
    if not text:
        return ""
    return (str(text)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;'))

def clean_title(title: str) -> str:
    """إزالة الهاشتاجات والروابط من العنوان."""
    if not title:
        return "Media"
    title = re.sub(r'#\S+', '', title)
    title = re.sub(r'https?://\S+', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    if not title or len(title) < 3:
        return "Media"
    return title

def build_compact_caption(info):
    """بناء Caption مختصر ومرتب لتفاصيل الفيديو."""
    qualities = info['qualities'][:5]
    qualities_str = " • ".join(f"{q}p" for q in qualities) if qualities else "Unknown"
    duration = format_duration(info['duration'])
    size_str = format_size(info['size_mb']) if info['size_mb'] else "Unknown"
    audio_str = "Yes" if info['has_audio'] else "No"
    platform = escape_html(info['platform'])
    title = escape_html(clean_title(info['title'])[:80])

    return (
        f"<b>{title}</b>\n"
        f"{platform}  •  {duration}  •  {size_str}\n"
        f"Quality: {qualities_str}\n"
        f"Audio: {audio_str}"
    )

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
    """
    تنزيل بأعلى جودة مع دعم كامل لجميع المنصات.
    يستخدم 3 محاولات متتالية مع إعدادات مختلفة لتجنب الفشل.
    """
    original_url = url
    is_twitter = any(d in url for d in ['twitter.com', 'x.com'])
    is_tiktok = 'tiktok.com' in url
    is_instagram = 'instagram.com' in url

    # تحويل روابط تويتر إلى fxtwitter (يعمل بشكل أفضل مع yt-dlp)
    if is_twitter:
        url = url.replace('twitter.com', 'fxtwitter.com').replace('x.com', 'fxtwitter.com')
        logging.info(f"Twitter URL converted: {url}")

    # قائمة الإعدادات للتجربة المتتالية
    attempts = []

    # محاولة 1: الإعدادات المثالية (أفضل جودة مع دمج)
    opts1 = base_ydl_opts()
    opts1.update({
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if is_twitter:
        opts1['extractor_args'] = {'twitter': {'api': ['syndication']}}
    if is_tiktok:
        opts1['format'] = 'b'  # تيك توك يقدم ملفات مدمجة جاهزة
    if chat_id and msg_id:
        opts1['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    attempts.append(('optimal', url, opts1))

    # محاولة 2: الرابط الأصلي (بدون تحويل) مع إعدادات بديلة
    if is_twitter:
        opts2 = base_ydl_opts()
        opts2.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'extractor_args': {'twitter': {'api': ['legacy']}},
        })
        if chat_id and msg_id:
            opts2['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
        attempts.append(('twitter_legacy', original_url, opts2))

    # محاولة 3: أفضل ملف مدمج (يتجنب الدمج الذي قد يفشل)
    opts3 = base_ydl_opts()
    opts3.update({
        'format': 'b',
        'outtmpl': output_path,
    })
    if chat_id and msg_id:
        opts3['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    attempts.append(('best_merged', original_url, opts3))

    # محاولة 4: أسوأ جودة (كحل أخير)
    opts4 = base_ydl_opts()
    opts4.update({
        'format': 'w',
        'outtmpl': output_path,
    })
    if chat_id and msg_id:
        opts4['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    attempts.append(('worst', original_url, opts4))

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

    # فشلت كل المحاولات - جرب gallery-dl
    if is_twitter or is_instagram:
        logging.info("All yt-dlp attempts failed, trying gallery-dl...")
        try:
            from gallery_dl import job
            temp_dir = os.path.dirname(output_path) + "/gallery_fallback"
            os.makedirs(temp_dir, exist_ok=True)
            config = {
                'base-directory': temp_dir,
                'filename': '{num:03d}_{filename}.{extension}',
                'quiet': True,
            }
            j = job.DownloadJob(original_url, config=config)
            j.run()
            files = sorted([f for f in os.listdir(temp_dir)
                            if os.path.isfile(os.path.join(temp_dir, f))])
            import shutil
            for f in files:
                if f.endswith(('.mp4', '.webm', '.mov', '.mkv')):
                    shutil.move(os.path.join(temp_dir, f), output_path)
                    return True, None
        except Exception as ge:
            logging.error(f"gallery-dl fallback failed: {ge}")

    return False, translate_error(last_error or "All attempts failed")


def download_specific_format(url, output_path, format_id, chat_id=None, msg_id=None, quality_label="Custom"):
    """
    تنزيل جودة محددة مع نظام محاولات متعدد.
    """
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
    if chat_id and msg_id:
        opts1['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    attempts.append(('specific+audio', url, opts1))

    # محاولة 2: الجودة المطلوبة فقط (بدون دمج)
    opts2 = base_ydl_opts()
    opts2.update({
        'format': format_id,
        'outtmpl': output_path,
    })
    if chat_id and msg_id:
        opts2['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
    attempts.append(('specific_only', original_url, opts2))

    # محاولة 3: أعلى جودة متاحة أقل من أو تساوي المطلوب
    try:
        height = format_id.replace('p', '').strip()
        if height.isdigit():
            opts3 = base_ydl_opts()
            opts3.update({
                'format': f'bv*[height<={height}]+ba/b[height<={height}]/b',
                'merge_output_format': 'mp4',
                'outtmpl': output_path,
            })
            if chat_id and msg_id:
                opts3['progress_hooks'] = [progress_hook_factory(chat_id, msg_id, quality_label)]
            attempts.append(('height_filter', original_url, opts3))
    except Exception:
        pass

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

    info = None
    progress_msg = None

    try:
        info = get_media_info(url)
        quality_label = "Auto (Best)"
        if info and info['qualities']:
            quality_label = f"{info['qualities'][0]}p"

        try:
            progress_msg = bot.send_message(
                chat_id,
                progress_message_text(0, quality_label, "0.0 MB")
            )
        except Exception as e:
            logging.warning(f"progress message error: {e}")

        ok, err = download_video(
            url, temp,
            chat_id=chat_id,
            msg_id=progress_msg.message_id if progress_msg else None,
            quality_label=quality_label
        )

        if not ok:
            logging.info(f"yt-dlp failed ({err}), trying gallery-dl...")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                if progress_msg:
                    try: bot.delete_message(chat_id, progress_msg.message_id)
                    except: pass
                send_gallery(chat_id, files)
                return
            if progress_msg:
                try:
                    bot.edit_message_text(f"Download failed.\n\n{err}",
                                          chat_id, progress_msg.message_id)
                except: pass
            return

        final = find_output_file(temp)
        if not final:
            if progress_msg:
                try:
                    bot.edit_message_text("File was not created.",
                                          chat_id, progress_msg.message_id)
                except: pass
            return

        size_mb = os.path.getsize(final) / (1024 * 1024)

        if progress_msg:
            try: bot.delete_message(chat_id, progress_msg.message_id)
            except: pass

        if size_mb > 50:
            bot.send_message(
                chat_id,
                f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).\n"
                f"Use the options below to download a lower quality."
            )
            user_pending[user_id] = url
            send_more_options(chat_id, user_id, url)
            cleanup_file(final)
            return

        # بناء النصوص
        if info:
            details_text = build_compact_caption(info)
            minimal_text = f"<b>{escape_html(info['title'][:70])}</b>"
        else:
            details_text = ""
            minimal_text = ""

        user_pending[user_id] = url

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("Details", callback_data="toggle_details"),
            types.InlineKeyboardButton("More", callback_data="more"),
        )

        # محاولة 1: caption + أزرار
        sent = None
        try:
            with open(final, 'rb') as v:
                sent = bot.send_video(
                    chat_id, v,
                    caption=details_text,
                    reply_markup=markup,
                    timeout=180,
                    supports_streaming=True
                )
            logging.info(f"[OK] Video sent with caption+buttons, msg_id={sent.message_id}")
        except Exception as e1:
            logging.error(f"[FAIL] send with caption+buttons: {e1}")
            # محاولة 2: أزرار فقط بدون caption
            try:
                with open(final, 'rb') as v:
                    sent = bot.send_video(
                        chat_id, v,
                        reply_markup=markup,
                        timeout=180,
                        supports_streaming=True
                    )
                logging.info(f"[OK] Video sent with buttons only, msg_id={sent.message_id}")
                if details_text:
                    try:
                        bot.send_message(chat_id, details_text, parse_mode="HTML")
                    except Exception as se:
                        logging.warning(f"send details msg failed: {se}")
            except Exception as e2:
                logging.error(f"[FAIL] send with buttons only: {e2}")
                # محاولة 3: بدون أي شيء
                with open(final, 'rb') as v:
                    sent = bot.send_video(
                        chat_id, v,
                        timeout=180,
                        supports_streaming=True
                    )
                logging.info(f"[OK] Video sent without anything, msg_id={sent.message_id}")

        # تخزين التفاصيل للتبديل
        if sent:
            video_details_cache[sent.message_id] = {
                'details': details_text,
                'minimal': minimal_text,
            }

            # إخفاء تلقائي بعد 5 ثوانٍ
            threading.Timer(
                5.0, auto_hide_caption,
                args=[chat_id, sent.message_id, minimal_text]
            ).start()

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


def auto_hide_caption(chat_id, msg_id, minimal_text):
    """إخفاء التفاصيل مع الحفاظ على الأزرار."""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("Details", callback_data="toggle_details"),
        types.InlineKeyboardButton("More", callback_data="more"),
    )
    try:
        bot.edit_message_caption(
            caption=minimal_text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
        logging.info(f"Caption hidden for {msg_id}")
    except Exception as e:
        logging.warning(f"auto_hide_caption failed: {e}")

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

@bot.callback_query_handler(func=lambda call: call.data == 'toggle_details')
def handle_toggle_details(call):
    msg_id = call.message.message_id
    chat_id = call.message.chat.id
    cache = video_details_cache.get(msg_id)

    if not cache:
        bot.answer_callback_query(call.id, "Details not available")
        return

    current_caption = (call.message.caption or "").strip()
    details = (cache['details'] or "").strip()

    # احتفظ بالأزرار في كل تعديل
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("Details", callback_data="toggle_details"),
        types.InlineKeyboardButton("More", callback_data="more"),
    )

    target = cache['minimal'] if current_caption == details else cache['details']

    try:
        bot.edit_message_caption(
            caption=target,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
        bot.answer_callback_query(
            call.id,
            "Details hidden" if current_caption == details else "Details shown"
        )
    except Exception as e:
        logging.error(f"toggle failed: {e}")
        bot.answer_callback_query(call.id, "Failed")

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

@bot.message_handler(content_types=['photo'])
def get_photo_id(message):
    """مؤقت: استخراج file_id للصورة."""
    file_id = message.photo[-1].file_id
    bot.reply_to(message, f"<code>{file_id}</code>")
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
