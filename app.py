import os
import sys
import re
import time
import logging
import threading
import uuid
import queue
import shutil
import socket
import subprocess
import gc
from contextlib import ExitStack
import yt_dlp
import imageio_ffmpeg
from urllib.parse import urlparse, quote
from collections import defaultdict
from flask import Flask, jsonify
import telebot
from telebot import types

# محاولة استيراد PIL لتحويل الصور إلى JPEG إذا كانت مدعومة
try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# ============================================================
# Configuration
# ============================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required in environment variables!")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()
logging.info(f"FFmpeg path: {FFMPEG_PATH}")

socket.setdefaulttimeout(35)

MAX_SLIDESHOW_IMAGES = int(os.environ.get("MAX_SLIDESHOW_IMAGES", "20"))
PORT = int(os.environ.get("PORT", 5000))
# رابط موقع ريندر الخاص بك للحفاظ على الاستضافة نشطة (يوفره Render تلقائياً)
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')


# ============================================================
# Cookies Setup (يدعم النص الصريح أو base64)
# ============================================================
def _setup_cookies():
    raw = os.environ.get("YTDLP_COOKIES", "").strip()
    if not raw:
        return None
    try:
        content = raw
        if not raw.startswith('#'):
            try:
                import base64
                decoded = base64.b64decode(raw).decode('utf-8', errors='ignore')
                if decoded.strip().startswith('#'):
                    content = decoded
            except Exception:
                pass
        path = '/tmp/yt_cookies.txt'
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info("Cookies loaded successfully")
        return path
    except Exception as e:
        logging.warning(f"Cookie setup failed: {e}")
        return None


COOKIES_FILE = _setup_cookies()


# ============================================================
# Allowed Domains & Classification
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
    'kwai.com', 'snackvideo.com',
    'capcut.com', 'xiaohongshu.com', 'xhslink.com',
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


def classify_url(url: str) -> str:
    d = urlparse(url).netloc.lower()
    if 'youtube.com' in d or 'youtu.be' in d:
        return 'youtube'
    if 'tiktok.com' in d:
        return 'tiktok'
    if 'instagram.com' in d:
        return 'instagram'
    if 'twitter.com' in d or 'x.com' in d or 't.co' in d:
        return 'twitter'
    if 'facebook.com' in d or 'fb.watch' in d or 'fb.com' in d:
        return 'facebook'
    if 'pinterest.com' in d or 'pin.it' in d:
        return 'pinterest'
    return 'other'


# ============================================================
# Rate Limiting
# ============================================================
_state_lock = threading.Lock()
user_requests = defaultdict(list)
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 7


def check_rate_limit(user_id: int):
    now = time.time()
    with _state_lock:
        user_requests[user_id] = [t for t in user_requests[user_id] if now - t < RATE_LIMIT_WINDOW]
        if len(user_requests[user_id]) >= RATE_LIMIT_MAX:
            wait = int(RATE_LIMIT_WINDOW - (now - user_requests[user_id][0]))
            return False, wait
        user_requests[user_id].append(now)
        return True, 0


# ============================================================
# Queues & State
# ============================================================
download_queue = queue.Queue(maxsize=150)
user_pending = {}
USER_PENDING_TTL = 3600


def _cleanup_user_pending():
    now = time.time()
    with _state_lock:
        stale = [uid for uid, data in user_pending.items()
                 if now - data.get('ts', 0) > USER_PENDING_TTL]
        for uid in stale:
            data = user_pending.pop(uid, None)
            if data and data.get('assets'):
                cleanup_dir(data.get('assets', {}).get('dir'))


def _store_user_pending(user_id: int, url: str, formats=None, assets=None):
    with _state_lock:
        existing = user_pending.get(user_id, {})
        user_pending[user_id] = {
            'url': url,
            'ts': time.time(),
            'formats': formats if formats is not None else existing.get('formats'),
            'assets': assets if assets is not None else existing.get('assets'),
        }


def _get_pending(user_id: int):
    with _state_lock:
        data = user_pending.get(user_id)
        if not data:
            return None
        if time.time() - data.get('ts', 0) > USER_PENDING_TTL:
            assets = data.get('assets')
            if assets:
                cleanup_dir(assets.get('dir'))
            user_pending.pop(user_id, None)
            return None
        return data


def _get_user_pending(user_id: int):
    data = _get_pending(user_id)
    return data.get('url') if data else None


def _get_user_formats(user_id: int):
    data = _get_pending(user_id)
    return data.get('formats') if data else None


def _set_user_formats(user_id: int, formats):
    with _state_lock:
        if user_id in user_pending:
            user_pending[user_id]['formats'] = formats


def _background_fetch_formats(user_id: int, url: str):
    try:
        formats = get_available_formats(url)
        if formats:
            _set_user_formats(user_id, formats)
    except Exception as e:
        logging.warning(f"Background fetch formats error: {e}")


# ============================================================
# Worker & Background Cleanups
# ============================================================
def download_worker(worker_id: int):
    logging.info(f"Worker #{worker_id} started")
    while True:
        try:
            task = download_queue.get(timeout=5)
            if task is None:
                break
            try:
                process_download(task)
            except Exception as e:
                logging.error(f"Worker #{worker_id} task error: {e}", exc_info=True)
                try:
                    bot.send_message(task['chat_id'], f"⚠️ خطأ أثناء المعالجة: {type(e).__name__}")
                except Exception:
                    pass
            finally:
                download_queue.task_done()
                gc.collect()  # تحرير الذاكرة فوراً لمنع الـ OOM
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker #{worker_id} error: {e}", exc_info=True)


def _cleanup_loop():
    """تنظيف الملفات المؤقتة والذاكرة كل 10 دقائق"""
    while True:
        try:
            time.sleep(600)
            _cleanup_user_pending()
            _sweep_tmp()
            gc.collect()
        except Exception as e:
            logging.error(f"Cleanup loop error: {e}", exc_info=True)


def _sweep_tmp(max_age=3600):
    """حذف ملفات /tmp القديمة لمنع امتلاء مساحة التخزين المحدودة"""
    try:
        now = time.time()
        for name in os.listdir('/tmp'):
            if name == 'yt_cookies.txt':
                continue
            path = os.path.join('/tmp', name)
            try:
                if now - os.path.getmtime(path) > max_age:
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
            except OSError:
                pass
    except Exception as e:
        logging.warning(f"/tmp sweep error: {e}")


# ============================================================
# Keep-Alive Engine لمنع توقف Render المجاني
# ============================================================
def keep_alive_ping():
    """إرسال طلب دوري كل 8 دقائق لإبقاء سيرفر Render نشطاً طوال الشهر"""
    time.sleep(20)  # انتظار بدء السيرفر
    urls_to_ping = [f"http://127.0.0.1:{PORT}/health"]
    if RENDER_EXTERNAL_URL:
        clean_ext = RENDER_EXTERNAL_URL.rstrip('/')
        urls_to_ping.append(f"{clean_ext}/health")

    import urllib.request
    while True:
        for u in urls_to_ping:
            try:
                req = urllib.request.Request(u, headers={'User-Agent': 'KeepAlive/1.0'})
                with urllib.request.urlopen(req, timeout=15) as res:
                    if res.status == 200:
                        logging.info(f"Keep-alive ping success: {u}")
            except Exception as e:
                logging.debug(f"Keep-alive ping to {u}: {e}")
        time.sleep(480)  # كل 8 دقائق


# ============================================================
# Helpers & Media Processing
# ============================================================
def format_size(mb: float) -> str:
    if not mb:
        return "Unknown"
    if mb >= 1024:
        return f"{mb/1024:.2f} GB"
    return f"{mb:.1f} MB"


def cleanup_file(path):
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass


def cleanup_dir(directory):
    if directory and os.path.exists(directory):
        try:
            shutil.rmtree(directory, ignore_errors=True)
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


def sanitize_image_to_jpeg(img_path: str) -> str:
    """تحويل أي صورة (WebP أو PNG غير متوافق) إلى JPEG نقي لضمان قبولها في ألبومات تليجرام"""
    if not img_path or not os.path.exists(img_path):
        return img_path
    target_jpg = img_path.rsplit('.', 1)[0] + '_fixed.jpg'
    
    if HAS_PIL:
        try:
            with Image.open(img_path) as im:
                rgb_im = im.convert('RGB')
                rgb_im.save(target_jpg, 'JPEG', quality=95)
                cleanup_file(img_path)
                return target_jpg
        except Exception:
            pass

    # بديل باستخدام FFmpeg إذا لم تتوفر PIL
    cmd = [FFMPEG_PATH, '-y', '-i', img_path, '-q:v', '2', target_jpg]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=20)
        if r.returncode == 0 and os.path.exists(target_jpg) and os.path.getsize(target_jpg) > 0:
            cleanup_file(img_path)
            return target_jpg
    except Exception:
        pass

    return img_path


def ensure_mp3(audio_path: str) -> str:
    """تحويل أي ملف صوتي إجبارياً إلى MP3 حقيقي 128kbps لضمان تشغيله وقبوله"""
    if not audio_path or not os.path.exists(audio_path):
        return None
    if audio_path.lower().endswith('.mp3'):
        # التأكد من صحة الملف
        if os.path.getsize(audio_path) > 1000:
            return audio_path

    out_path = audio_path.rsplit('.', 1)[0] + '_final.mp3'
    cmd = [
        FFMPEG_PATH, '-y', '-i', audio_path,
        '-vn', '-c:a', 'libmp3lame', '-b:a', '128k',
        out_path
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=60)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            cleanup_file(audio_path)
            return out_path
    except Exception as e:
        logging.warning(f"ensure_mp3 failed: {e}")
    return audio_path


# ============================================================
# yt-dlp Options
# ============================================================
def base_ydl_opts():
    opts = {
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 5,
        'ffmpeg_location': FFMPEG_PATH,
        'format_sort': ['vcodec:h264', 'vcodec:h265', 'ext:mp4:m4a'],
        'http_headers': {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        },
        'extractor_args': {
            'youtube': {
                'player_client': ['android_vr', 'tv_embedded', 'web_embedded', 'ios', 'mweb'],
            }
        },
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'ignoreerrors': False,
        'no_color': True,
    }
    if COOKIES_FILE:
        opts['cookiefile'] = COOKIES_FILE
    return opts


def _yt_opts(fmt, output_path, clients=None, player_skip=None):
    opts = base_ydl_opts()
    opts.update({
        'format': fmt,
        'outtmpl': output_path,
        'merge_output_format': 'mp4',
    })
    if clients:
        ea = {'youtube': {'player_client': clients}}
        if player_skip:
            ea['youtube']['player_skip'] = player_skip
        opts['extractor_args'] = ea
    return opts


def translate_error(err: str) -> str:
    err = err or ""
    low = err.lower()
    if 'max_filesize' in low or 'larger than max-filesize' in low:
        return "حجم الملف يتجاوز حد تليجرام المسموح به (50 ميجابايت)."
    if 'login' in low or 'cookies' in low or 'sign in' in low:
        return "هذا المحتوى يتطلب تسجيل دخول أو مقيد بعمر. أضف ملف الكوكيز في YTDLP_COOKIES."
    if 'private' in low:
        return "هذا المحتوى خاص (Private) ولا يمكن تحميله."
    if 'not available' in low or 'removed' in low:
        return "المحتوى غير متاح أو تم حذفه من المنصة."
    if '429' in low:
        return "تم حظر الطلب مؤقتاً لكثرة التنزيلات. حاول بعد دقيقة."
    if '403' in low:
        return "الخادم رفض الاتصال (403). جاري تجربة مسار بديل."
    return f"فشل التحميل: {err[:120]}"


def get_available_formats(url: str) -> list:
    ydl_opts = base_ydl_opts()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                return []
            if 'entries' in info and info['entries']:
                info = info['entries'][0]
                if not info:
                    return []

            duration = info.get('duration') or 0
            formats = []

            for f in info.get('formats', []):
                if f.get('vcodec') == 'none':
                    continue
                height = f.get('height')
                if not height:
                    continue

                filesize = f.get('filesize') or f.get('filesize_approx')
                if not filesize and f.get('tbr') and duration:
                    filesize = (f['tbr'] * 1000 / 8) * duration

                size_mb = filesize / (1024 * 1024) if filesize else 0

                formats.append({
                    'id': f.get('format_id', ''),
                    'height': height,
                    'size_mb': round(size_mb, 1),
                    'ext': f.get('ext', 'mp4'),
                    'tbr': f.get('tbr') or 0,
                    'vcodec': f.get('vcodec', ''),
                })

            seen = {}
            for f in formats:
                h = f['height']
                vcodec = f.get('vcodec', '').lower()
                is_h264 = 'avc' in vcodec or 'h264' in vcodec
                if h not in seen:
                    seen[h] = f
                else:
                    if is_h264:
                        seen[h] = f

            unique = sorted(seen.values(), key=lambda x: x['height'], reverse=True)
            return unique[:6]
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []


# ============================================================
# HTTP Requests & Direct Downloads (curl_cffi)
# ============================================================
def api_get_json(api_url: str, timeout: int = 25):
    try:
        from curl_cffi import requests as cffi_requests
        r = cffi_requests.get(api_url, impersonate='chrome', timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def fetch_file(media_url: str, dest: str, referer: str = None) -> bool:
    headers = {'User-Agent': USER_AGENT}
    if referer:
        headers['Referer'] = referer

    try:
        from curl_cffi import requests as cffi_requests
        with cffi_requests.get(media_url, headers=headers, impersonate='chrome', timeout=35, stream=True) as r:
            if r.status_code != 200:
                return False
            cl = int(r.headers.get('Content-Length', 0))
            if cl > 50 * 1024 * 1024:
                return False
            with open(dest, 'wb') as f:
                for chunk in r.iter_content(chunk_size=128 * 1024):
                    f.write(chunk)
            return os.path.exists(dest) and os.path.getsize(dest) > 0
    except Exception:
        pass

    # احتياطي باستخدام urllib
    try:
        import urllib.request
        req = urllib.request.Request(media_url, headers=headers)
        with urllib.request.urlopen(req, timeout=35) as r:
            cl = int(r.headers.get('Content-Length', 0))
            if cl > 50 * 1024 * 1024:
                return False
            with open(dest, 'wb') as f:
                shutil.copyfileobj(r, f, length=128 * 1024)
        return os.path.exists(dest) and os.path.getsize(dest) > 0
    except Exception:
        return False


# ============================================================
# Media Bundle
# ============================================================
class MediaBundle:
    def __init__(self):
        self.images = []
        self.videos = []
        self.audio = None
        self.title = ''
        self.duration = 0

    def has_media(self) -> bool:
        return bool(self.images or self.videos)


# ============================================================
# Twitter / X Strategy
# ============================================================
def try_fxtwitter(url: str, workdir: str):
    m = re.search(r'(?:twitter\.com|x\.com)/[^/]+/status(?:es)?/(\d+)', url)
    if not m:
        return None
    tid = m.group(1)
    data = api_get_json(f'https://api.fxtwitter.com/status/{tid}')
    if not data or not isinstance(data, dict):
        return None
    tweet = data.get('tweet') or {}
    media = tweet.get('media') or {}
    photos = media.get('photos') or []
    videos = media.get('videos') or []

    if not photos and not videos:
        return None

    os.makedirs(workdir, exist_ok=True)
    bundle = MediaBundle()
    bundle.title = (tweet.get('text') or '')[:800]

    for i, p in enumerate(photos[:MAX_SLIDESHOW_IMAGES]):
        u = (p or {}).get('url')
        if u:
            dest = os.path.join(workdir, f'img_{i:03d}.jpg')
            if fetch_file(u, dest, referer=url):
                fixed = sanitize_image_to_jpeg(dest)
                bundle.images.append(fixed)

    for i, v in enumerate(videos[:2]):
        u = (v or {}).get('url')
        if u:
            dest = os.path.join(workdir, f'vid_{i:03d}.mp4')
            if fetch_file(u, dest, referer=url):
                bundle.videos.append(dest)

    return bundle if bundle.has_media() else None


# ============================================================
# TikTok Strategy (tikwm API)
# ============================================================
def try_tikwm(url: str, workdir: str):
    hosts = ("https://www.tikwm.com/api/", "https://tikwm.com/api/")
    data = None
    for host in hosts:
        data = api_get_json(f'{host}?url={quote(url, safe="")}', timeout=30)
        if data and data.get('code') == 0:
            break
    if not data or data.get('code') != 0:
        return None

    d = data.get('data') or {}
    os.makedirs(workdir, exist_ok=True)
    bundle = MediaBundle()
    bundle.title = (d.get('title') or '')[:800]
    bundle.duration = d.get('duration') or 0

    images = d.get('images') or []
    if images:
        # تحميل الصوت أولاً
        music = d.get('music')
        if music:
            adest = os.path.join(workdir, 'audio.mp3')
            if fetch_file(music, adest, referer=url):
                bundle.audio = ensure_mp3(adest)

        # تحميل الصور وتصحيحها لـ JPEG
        for i, u in enumerate(images[:MAX_SLIDESHOW_IMAGES]):
            if u:
                dest = os.path.join(workdir, f'img_{i:03d}.jpg')
                if fetch_file(u, dest, referer=url):
                    fixed = sanitize_image_to_jpeg(dest)
                    bundle.images.append(fixed)
    else:
        vu = d.get('hdplay') or d.get('play')
        if vu:
            vdest = os.path.join(workdir, 'video.mp4')
            if fetch_file(vu, vdest, referer=url):
                bundle.videos.append(vdest)

    return bundle if bundle.has_media() else None


# ============================================================
# Slideshow Extraction & Merging
# ============================================================
def extract_slideshow(url: str):
    try:
        opts = base_ydl_opts()
        opts.update({'retries': 3, 'ignore_no_formats_error': True})
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if not info:
            return None, "No info found"

        entries = [e for e in (info.get('entries') or []) if e] or [info]
        image_urls = []
        clip_urls = []
        audio_url = None
        duration = info.get('duration') or 0

        for entry in entries:
            for f in entry.get('formats') or []:
                ext = (f.get('ext') or '').lower()
                acodec = f.get('acodec')
                vcodec = f.get('vcodec')
                u = f.get('url')
                if not u:
                    continue
                if ext in ('jpg', 'jpeg', 'png', 'webp') or (vcodec == 'none' and acodec == 'none' and ext not in ('mp4', 'm4a', 'mp3')):
                    if u not in image_urls:
                        image_urls.append(u)
                elif acodec and acodec != 'none' and (not vcodec or vcodec == 'none'):
                    audio_url = u
                elif vcodec and vcodec != 'none' and ext == 'mp4':
                    if u not in clip_urls:
                        clip_urls.append(u)

        if not image_urls and not clip_urls:
            return None, "No photos or clips found"

        return {
            'images': image_urls[:MAX_SLIDESHOW_IMAGES],
            'clips': clip_urls[:2],
            'audio_url': audio_url,
            'duration': duration,
        }, None
    except Exception as e:
        return None, str(e)


def download_slideshow_assets(url: str, data: dict, workdir: str):
    os.makedirs(workdir, exist_ok=True)

    # 1. تنزيل الصوت أولاً
    audio_path = None
    if data.get('audio_url'):
        adest = os.path.join(workdir, 'audio.m4a')
        if fetch_file(data['audio_url'], adest, referer=url):
            audio_path = ensure_mp3(adest)

    # إذا لم يوجد صوت مباشر، استخراجه عبر yt-dlp
    if not audio_path:
        try:
            adest_base = os.path.join(workdir, 'audio')
            opts = base_ydl_opts()
            opts.update({
                'format': 'bestaudio/best',
                'outtmpl': adest_base + '.%(ext)s',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '128',
                }]
            })
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            for f in os.listdir(workdir):
                if f.startswith('audio') and f.endswith('.mp3'):
                    audio_path = os.path.join(workdir, f)
                    break
        except Exception:
            pass

    audio_path = ensure_mp3(audio_path)

    # 2. تنزيل الصور وتحويلها إلى JPEG
    image_paths = []
    for idx, u in enumerate(data.get('images') or []):
        dest = os.path.join(workdir, f'img_{idx:03d}.jpg')
        if fetch_file(u, dest, referer=url):
            fixed = sanitize_image_to_jpeg(dest)
            image_paths.append(fixed)

    # 3. تنزيل مقاطع الكاروسيل
    video_paths = []
    for idx, cu in enumerate(data.get('clips') or []):
        dest = os.path.join(workdir, f'clip_{idx:03d}.mp4')
        if fetch_file(cu, dest, referer=url):
            video_paths.append(dest)

    if not image_paths and not video_paths:
        return None, "فشل تنزيل الوسائط."

    return {
        'images': image_paths,
        'videos': video_paths,
        'audio': audio_path,
        'dir': workdir,
        'duration': data.get('duration')
    }, None


def merge_slideshow(image_paths: list, audio_path: str, out_path: str, total_duration=None) -> bool:
    n = len(image_paths)
    if not n or not audio_path or not os.path.exists(audio_path):
        return False

    per_image = max((total_duration or (n * 3.0)) / n, 1.5)
    cmd = [FFMPEG_PATH, '-y']
    for img in image_paths:
        cmd += ['-loop', '1', '-t', f'{per_image:.2f}', '-i', img]
    cmd += ['-i', audio_path]

    parts = []
    for i in range(n):
        parts.append(
            f'[{i}:v]scale=720:1280:force_original_aspect_ratio=decrease,'
            f'pad=720:1280:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=24,format=yuv420p[v{i}]'
        )
    parts.append(''.join(f'[v{i}]' for i in range(n)) + f'concat=n={n}:v=1:a=0[vout]')
    filter_complex = ';'.join(parts)

    cmd += [
        '-filter_complex', filter_complex,
        '-map', '[vout]', '-map', f'{n}:a',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '25',
        '-c:a', 'aac', '-b:a', '96k',
        '-shortest', '-movflags', '+faststart',
        out_path
    ]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=300)
        return r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception as e:
        logging.error(f"merge_slideshow error: {e}")
        return False


# ============================================================
# Video & Audio Download Methods
# ============================================================
def download_video(url, output_path, platform='other'):
    attempts = []

    if platform == 'youtube':
        attempts = [
            ('yt_android_vr', _yt_opts('bv*[height<=1080]+ba/b[height<=1080]/b', output_path, clients=['android_vr'])),
            ('yt_web_embedded', _yt_opts('bv*[height<=720]+ba/b', output_path, clients=['web_embedded'], player_skip=['webpage', 'configs'])),
            ('yt_tv_embedded', _yt_opts('bv*+ba/b', output_path, clients=['tv_embedded'])),
            ('yt_simple_best', _yt_opts('b[ext=mp4]/b', output_path)),
        ]
        if COOKIES_FILE:
            attempts.insert(0, ('yt_cookies', _yt_opts('bv*+ba/b', output_path, clients=['web', 'mweb'])))
    elif platform == 'twitter':
        o = _yt_opts('bv*+ba/b', output_path)
        o['extractor_args'] = {'twitter': {'api': ['syndication', 'legacy']}}
        attempts = [('twitter_synd', o), ('twitter_best', _yt_opts('b', output_path))]
    elif platform == 'tiktok':
        attempts = [('tiktok_best', _yt_opts('b', output_path)), ('tiktok_gen', _yt_opts('bv*+ba/b', output_path))]
    else:
        attempts = [
            ('optimal', _yt_opts('bv*[height<=1080]+ba/b[height<=1080]/b', output_path)),
            ('best_direct', _yt_opts('b', output_path)),
        ]

    last_err = None
    for name, opts in attempts:
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            target = find_output_file(output_path)
            if target and os.path.getsize(target) > 0:
                return True, None
        except Exception as e:
            last_err = str(e)
            cleanup_file(output_path)

    return False, translate_error(last_err or "All download attempts failed")


def download_audio_file(url, output_path):
    opts = base_ydl_opts()
    opts.update({
        'format': 'bestaudio/best',
        'outtmpl': output_path + '.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '128',
        }],
    })
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        target = find_output_file(output_path + '.mp3')
        if target:
            return True, target
    except Exception as e:
        return False, str(e)
    return False, "Audio file not created"


def download_gallery(url, output_dir):
    """تنزيل صور المنصات باستخدام gallery-dl كخطة بديلة"""
    try:
        os.makedirs(output_dir, exist_ok=True)
        cmd = [sys.executable, '-m', 'gallery_dl', '--directory', output_dir, '--user-agent', USER_AGENT]
        if COOKIES_FILE:
            cmd.extend(['--cookies', COOKIES_FILE])
        cmd.append(url)

        subprocess.run(cmd, capture_output=True, timeout=180)
        files = []
        for root, _, names in os.walk(output_dir):
            for n in names:
                p = os.path.join(root, n)
                if os.path.isfile(p) and os.path.getsize(p) > 0:
                    files.append(p)
        files.sort()
        return bool(files), files[:MAX_SLIDESHOW_IMAGES]
    except Exception as e:
        logging.error(f"gallery-dl error: {e}")
        return False, []


# ============================================================
# Send Media Functions (ضمان إرسال الصوت أولاً ثم الألبوم كاملاً)
# ============================================================
def _mkvid_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🎬 تحويل لـ فيديو (صور + صوت)", callback_data="mkvid"))
    return markup


def send_photo_post(chat_id, assets):
    """إرسال الصوت أولاً بشكل مؤكد ثم تجميع الصور في ألبومات متكاملة"""
    raw_images = assets.get('images', [])
    # تصفية وتصحيح الصور لـ JPEG لضمان قبول تليجرام لـ send_media_group
    images = []
    for p in raw_images:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            fixed_p = sanitize_image_to_jpeg(p)
            images.append(fixed_p)

    audio = assets.get('audio')
    has_audio = bool(audio and os.path.exists(audio) and os.path.getsize(audio) > 0)

    # 1. إرسال ملف الصوت أولاً قبل كل شيء!
    if has_audio:
        try:
            with open(audio, 'rb') as a:
                bot.send_audio(
                    chat_id, a,
                    caption="🎵 الصوت الأصلي للمنشور",
                    reply_markup=_mkvid_markup() if len(images) >= 1 else None,
                    timeout=120
                )
        except Exception as e:
            logging.error(f"Send audio failed: {e}")

    # 2. إرسال الصور: إذا كانت صورة واحدة ترسل مفردة، وإذا كانت أكثر ترسل كألبوم (Media Group)
    if len(images) == 1:
        try:
            with open(images[0], 'rb') as f:
                bot.send_photo(
                    chat_id, f,
                    caption="📷 الصورة",
                    reply_markup=_mkvid_markup() if has_audio else None,
                    timeout=120
                )
        except Exception as e:
            logging.error(f"Send single photo failed: {e}")
    elif len(images) > 1:
        # تقسيم الصور لمجموعات ألبومات (كل ألبوم بحد أقصى 10 صور حسب قيود تليجرام)
        for chunk_start in range(0, len(images), 10):
            chunk = images[chunk_start:chunk_start + 10]

            if len(chunk) == 1:
                try:
                    with open(chunk[0], 'rb') as f:
                        bot.send_photo(chat_id, f, timeout=90)
                except Exception:
                    pass
                continue

            with ExitStack() as stack:
                media_list = []
                for i, img_path in enumerate(chunk):
                    try:
                        f = stack.enter_context(open(img_path, 'rb'))
                        caption = '📷 ألبوم الصور' if (chunk_start == 0 and i == 0) else ''
                        media_list.append(types.InputMediaPhoto(f, caption=caption))
                    except Exception as e:
                        logging.warning(f"Error opening image {img_path}: {e}")

                if len(media_list) >= 2:
                    try:
                        bot.send_media_group(chat_id, media_list, timeout=180)
                    except Exception as e:
                        logging.error(f"send_media_group failed: {e}, falling back to individual photos")
                        for m in media_list:
                            try:
                                m.media.seek(0)
                                bot.send_photo(chat_id, m.media, timeout=60)
                            except Exception:
                                pass


def send_bundle(chat_id, user_id, url, bundle: MediaBundle, workdir: str):
    sent = False

    # إرسال الفيديوهات
    for vp in bundle.videos[:2]:
        if not os.path.exists(vp):
            continue
        size_mb = os.path.getsize(vp) / (1024 * 1024)
        if size_mb > 50:
            bot.send_message(chat_id, f"حجم الفيديو ({format_size(size_mb)}) يتجاوز حد تليجرام (50MB).")
            continue
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("خيارات الجودة ⚙️", callback_data="more"))
            with open(vp, 'rb') as v:
                bot.send_video(
                    chat_id, v,
                    caption=bundle.title[:900] if bundle.title else None,
                    reply_markup=markup,
                    timeout=180,
                    supports_streaming=True
                )
            sent = True
        except Exception as e:
            logging.error(f"send_video error: {e}")

    # إرسال الصور والصوت معاً
    if bundle.images:
        assets = {
            'images': bundle.images,
            'videos': [],
            'audio': bundle.audio,
            'dir': workdir,
            'duration': bundle.duration
        }
        _store_user_pending(user_id, url, assets=assets)
        send_photo_post(chat_id, assets)
        sent = True
    elif bundle.audio and not bundle.videos:
        try:
            with open(bundle.audio, 'rb') as a:
                bot.send_audio(chat_id, a, timeout=120)
            sent = True
        except Exception as e:
            logging.error(f"send_audio error: {e}")

    return sent


def send_gallery(chat_id, files):
    """إرسال الصور المستخرجة عبر gallery-dl كألبومات منظمة بدلاً من إرسالها متتالية"""
    if not files:
        bot.send_message(chat_id, "لم يتم العثور على وسائط.")
        return

    images = [sanitize_image_to_jpeg(f) for f in files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    videos = [f for f in files if f.lower().endswith(('.mp4', '.mov', '.mkv'))]

    # إرسال الفيديوهات
    for v in videos[:2]:
        try:
            if os.path.getsize(v) <= 50 * 1024 * 1024:
                with open(v, 'rb') as vf:
                    bot.send_video(chat_id, vf, timeout=120)
        except Exception:
            pass

    # إرسال الصور كألبومات منظمة
    if len(images) == 1:
        try:
            with open(images[0], 'rb') as f:
                bot.send_photo(chat_id, f, timeout=100)
        except Exception:
            pass
    elif len(images) > 1:
        for chunk_start in range(0, len(images), 10):
            chunk = images[chunk_start:chunk_start + 10]
            with ExitStack() as stack:
                media_list = []
                for p in chunk:
                    try:
                        f = stack.enter_context(open(p, 'rb'))
                        media_list.append(types.InputMediaPhoto(f))
                    except Exception:
                        pass
                if len(media_list) >= 2:
                    try:
                        bot.send_media_group(chat_id, media_list, timeout=180)
                    except Exception:
                        for m in media_list:
                            try:
                                m.media.seek(0)
                                bot.send_photo(chat_id, m.media, timeout=60)
                            except Exception:
                                pass


# ============================================================
# Main Processing Pipeline
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']
    platform = classify_url(url)

    uid = uuid.uuid4().hex[:8]
    temp_video = f"/tmp/v_{user_id}_{uid}.mp4"
    workdir = f"/tmp/media_{user_id}_{uid}"

    try:
        # 1. منصة X/Twitter
        if platform == 'twitter':
            bundle = try_fxtwitter(url, workdir)
            if bundle and send_bundle(chat_id, user_id, url, bundle, workdir):
                return

        # 2. منصة TikTok
        if platform == 'tiktok':
            bundle = try_tikwm(url, workdir)
            if bundle and send_bundle(chat_id, user_id, url, bundle, workdir):
                return

        # 3. التحميل القياسي للفيديو (yt-dlp)
        ok, err = download_video(url, temp_video, platform)
        if ok:
            final_file = find_output_file(temp_video)
            if final_file and os.path.exists(final_file) and os.path.getsize(final_file) > 0:
                size_mb = os.path.getsize(final_file) / (1024 * 1024)
                if size_mb > 50:
                    bot.send_message(
                        chat_id,
                        f"حجم الفيديو ({format_size(size_mb)}) أكبر من حد 50MB المسموح به في تليجرام."
                    )
                    return

                _store_user_pending(user_id, url)
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("خيارات الجودة ⚙️", callback_data="more"))

                with open(final_file, 'rb') as v:
                    bot.send_video(
                        chat_id, v,
                        reply_markup=markup,
                        timeout=180,
                        supports_streaming=True
                    )
                threading.Thread(target=_background_fetch_formats, args=(user_id, url), daemon=True).start()
                return

        # 4. هل المنشور ألبوم صور وكاروسيل؟
        data, ss_err = extract_slideshow(url)
        if data:
            assets, a_err = download_slideshow_assets(url, data, workdir)
            if assets:
                _store_user_pending(user_id, url, assets=assets)
                for cv in assets.get('videos', []):
                    try:
                        if os.path.getsize(cv) <= 50 * 1024 * 1024:
                            with open(cv, 'rb') as v:
                                bot.send_video(chat_id, v, timeout=120)
                    except Exception:
                        pass
                send_photo_post(chat_id, assets)
                return

        # 5. خطة الطوارئ البديلة (gallery-dl)
        ok2, files = download_gallery(url, workdir)
        if ok2 and files:
            _store_user_pending(user_id, url)
            send_gallery(chat_id, files)
            return

        # فشل التحميل
        hint = ""
        if platform == 'instagram' and not COOKIES_FILE:
            hint = "\n\n💡 ملاحظة: إنستجرام يحظر سيرفرات الاستضافة السحابية. يرجى إضافة كوكيز إنستجرام في متغير YTDLP_COOKIES."
        bot.send_message(chat_id, f"عذراً، تعذر تنزيل هذا الرابط.\n\n{translate_error(err)}{hint}")

    except Exception as e:
        logging.error(f"process_download critical error: {e}", exc_info=True)
        bot.send_message(chat_id, f"⚠️ حدث خطأ تقني غير متوقع: {type(e).__name__}")
    finally:
        cleanup_file(temp_video)
        gc.collect()


# ============================================================
# Bot Handlers & Callbacks
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    text = (
        "<b>مرحباً بك في بوت تحميل الميديا الشامل 🚀</b>\n\n"
        "أرسل أي رابط من منصات التواصل الاجتماعي وسأقوم بتحميله فوراً بأعلى جودة:\n\n"
        "• <b>المنصات المدعومة:</b> TikTok, Instagram, YouTube, X/Twitter, Facebook, Pinterest, Reddit وغيرها.\n"
        "• <b>منشورات الصور:</b> يتم إرسال الصوت أولاً ثم الصور كألبوم موحد.\n"
        "• <b>زر التحويل:</b> إمكانية دمج صور المنشور مع الصوت في فيديو واحد بنقرة زر."
    )
    bot.reply_to(message, text)


@bot.callback_query_handler(func=lambda call: call.data == 'more')
def handle_more(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    url = _get_user_pending(user_id)
    bot.answer_callback_query(call.id)

    if not url:
        bot.send_message(chat_id, "انتهت صلاحية الجلسة، أعد إرسال الرابط مجدداً.")
        return

    formats = _get_user_formats(user_id) or get_available_formats(url)
    if not formats:
        bot.send_message(chat_id, "لا توجد خيارات دقة متوفرة لهذا المقطع.")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        label = f"{f['height']}p"
        if f.get('size_mb'):
            label += f" ({f['size_mb']}MB)"
        buttons.append(types.InlineKeyboardButton(label, callback_data=f"q|{f['id']}|{f['height']}"))
    buttons.append(types.InlineKeyboardButton("تحميل الصوت فقط (MP3) 🎵", callback_data="audio_only"))
    markup.add(*buttons)
    bot.send_message(chat_id, "اختر الجودة المطلوبة:", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data == 'audio_only')
def handle_audio_only(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    url = _get_user_pending(user_id)
    bot.answer_callback_query(call.id, "جاري استخراج الصوت...")

    if not url:
        bot.send_message(chat_id, "انتهت الجلسة.")
        return

    temp = f"/tmp/aud_{user_id}_{uuid.uuid4().hex[:8]}"
    ok, target = download_audio_file(url, temp)
    if ok and target and os.path.exists(target):
        try:
            with open(target, 'rb') as f:
                bot.send_audio(chat_id, f, timeout=120)
        finally:
            cleanup_file(target)
    else:
        bot.send_message(chat_id, "تعذر استخراج ملف الصوت.")


@bot.callback_query_handler(func=lambda call: call.data == 'mkvid')
def handle_mkvid(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id, "جاري تجميع الفيديو...")

    data = _get_pending(user_id)
    if not data or not data.get('assets'):
        bot.send_message(chat_id, "انتهت الجلسة أو تم حذف الملفات المؤقتة. أرسل الرابط مرة أخرى.")
        return

    assets = data['assets']
    images = [p for p in assets.get('images', []) if os.path.exists(p)]
    audio = assets.get('audio')

    if not images or not audio or not os.path.exists(audio):
        bot.send_message(chat_id, "لا تتوفر صور كافية أو ملف صوت لدمجهما.")
        return

    out_vid = os.path.join(assets['dir'], 'merged.mp4')
    if merge_slideshow(images, audio, out_vid, total_duration=assets.get('duration')):
        try:
            with open(out_vid, 'rb') as vf:
                bot.send_video(chat_id, vf, caption="🎬 تم تجميع الفيديو بنجاح!", timeout=180)
        finally:
            cleanup_file(out_vid)
    else:
        bot.send_message(chat_id, "فشلت عملية دمج الفيديو.")


@bot.message_handler(func=lambda m: True)
def handle_message(message):
    text = (message.text or '').strip()
    m = re.search(r'https?://[^\s<>"\']+', text)
    if not m:
        bot.reply_to(message, "يرجى إرسال رابط صالح يبدأ بـ http:// أو https://")
        return

    url = m.group(0).rstrip('.,)]}')
    if not is_allowed_url(url):
        bot.reply_to(message, "هذا الموقع غير مدعوم حالياً.")
        return

    allowed, wait = check_rate_limit(message.from_user.id)
    if not allowed:
        bot.reply_to(message, f"يرجى الانتظار {wait} ثانية قبل إرسال رابط جديد لتفادي الضغط.")
        return

    try:
        msg = bot.reply_to(message, "⏳ جاري الفحص والتحميل بأقصى سرعة...")
        task = {
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'url': url,
            'msg_id': msg.message_id,
        }
        download_queue.put_nowait(task)
    except queue.Full:
        bot.reply_to(message, "السيرفر مشغول حالياً بعدة طلبات، يرجى المحاولة بعد لحظات.")


# ============================================================
# Flask Endpoints
# ============================================================
@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "queue_size": download_queue.qsize(),
        "time": time.time()
    }), 200


@app.route('/')
def index():
    return "All-in-One Media Downloader Bot is Running 24/7!", 200


# ============================================================
# Main Entry Point
# ============================================================
def run_bot():
    try:
        bot.remove_webhook()
    except Exception:
        pass
    logging.info("Bot polling started...")
    bot.infinity_polling(timeout=35, long_polling_timeout=35)


if __name__ == '__main__':
    # تشغيل عمال التنزيل
    NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
    for i in range(NUM_WORKERS):
        threading.Thread(target=download_worker, args=(i + 1,), daemon=True).start()

    # تشغيل البوت ومهام الصيانة
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=_cleanup_loop, daemon=True).start()
    threading.Thread(target=keep_alive_ping, daemon=True).start()

    # تشغيل Flask على البورت المحدد
    app.run(host="0.0.0.0", port=PORT)