import os
import re
import time
import logging
import threading
import uuid
import queue
import shutil
import socket
import subprocess
import requests
import yt_dlp
import imageio_ffmpeg
from urllib.parse import urlparse, quote
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

socket.setdefaulttimeout(30)

MAX_SLIDESHOW_IMAGES = int(os.environ.get("MAX_SLIDESHOW_IMAGES", "20"))

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

# ============================================================
# Cookies
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
        with open(path, 'w') as f:
            f.write(content)
        logging.info("Cookies file loaded from YTDLP_COOKIES")
        return path
    except Exception as e:
        logging.warning(f"cookie setup failed: {e}")
        return None

COOKIES_FILE = _setup_cookies()

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
    if 'youtube.com' in d or 'youtu.be' in d: return 'youtube'
    if 'tiktok.com' in d: return 'tiktok'
    if 'instagram.com' in d: return 'instagram'
    if 'twitter.com' in d or 'x.com' in d or 't.co' in d: return 'twitter'
    if 'facebook.com' in d or 'fb.watch' in d or 'fb.com' in d: return 'facebook'
    return 'other'

# ============================================================
# Rate limiting
# ============================================================
_state_lock = threading.Lock()
user_requests = defaultdict(list)
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 5

def _cleanup_user_requests():
    now = time.time()
    with _state_lock:
        stale = [uid for uid, times in user_requests.items() if not times or (now - max(times)) > RATE_LIMIT_WINDOW]
        for uid in stale: user_requests.pop(uid, None)

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
# Queue and shared state
# ============================================================
download_queue = queue.Queue(maxsize=100)
task_pending = {}
TASK_PENDING_TTL = 3600
user_locks = defaultdict(threading.Lock)

def _cleanup_task_pending():
    now = time.time()
    with _state_lock:
        stale = [tid for tid, data in task_pending.items() if now - data.get('ts', 0) > TASK_PENDING_TTL]
        for tid in stale:
            data = task_pending.pop(tid, None)
            if data: cleanup_dir(data.get('assets', {}).get('dir'))

def _store_task_pending(task_id: str, user_id: int, url: str, formats=None, assets=None):
    with _state_lock:
        existing = task_pending.get(task_id, {})
        task_pending[task_id] = {
            'user_id': user_id, 'url': url, 'ts': time.time(),
            'formats': formats if formats is not None else existing.get('formats'),
            'assets': assets if assets is not None else existing.get('assets'),
        }

def _clear_task_assets(task_id: str):
    with _state_lock:
        if task_id in task_pending: task_pending[task_id]['assets'] = None

def _get_task_pending(task_id: str):
    with _state_lock:
        data = task_pending.get(task_id)
        if not data: return None
        if time.time() - data.get('ts', 0) > TASK_PENDING_TTL:
            assets = data.get('assets')
            if assets: cleanup_dir(assets.get('dir'))
            task_pending.pop(task_id, None)
            return None
        return data

def _get_task_url(task_id: str):
    data = _get_task_pending(task_id)
    return data.get('url') if data else None

def _get_task_formats(task_id: str):
    data = _get_task_pending(task_id)
    return data.get('formats') if data else None

def _set_task_formats(task_id: str, formats):
    with _state_lock:
        if task_id in task_pending: task_pending[task_id]['formats'] = formats

def _background_fetch_formats(task_id: str, url: str):
    try:
        formats = get_available_formats(url)
        if formats:
            _set_task_formats(task_id, formats)
            logging.info(f"Background formats cached for task {task_id}: {len(formats)} formats")
    except Exception as e:
        logging.warning(f"background fetch formats failed: {e}")

def download_worker(worker_id: int):
    logging.info(f"Worker #{worker_id} started")
    while True:
        try:
            task = download_queue.get(timeout=5)
            if task is None: break
            try:
                process_download(task)
            except Exception as e:
                logging.error(f"Worker #{worker_id} task error: {e}", exc_info=True)
                try: bot.send_message(task['chat_id'], f"Technical error while processing: {type(e).__name__}")
                except: pass
            finally:
                download_queue.task_done()
        except queue.Empty: continue
        except Exception as e: logging.error(f"Worker #{worker_id} error: {e}", exc_info=True)

def _cleanup_loop():
    while True:
        try:
            time.sleep(600)
            _cleanup_task_pending()
            _cleanup_user_requests()
            _sweep_tmp()
            with _state_lock:
                for uid in list(user_locks.keys()):
                    lock = user_locks[uid]
                    if lock.acquire(blocking=False):
                        del user_locks[uid]
                        lock.release()
        except Exception as e:
            logging.error(f"cleanup loop error: {e}", exc_info=True)

def _sweep_tmp(max_age=7200):
    try:
        now = time.time()
        removed = 0
        for name in os.listdir('/tmp'):
            if not name.startswith('task_'): continue
            path = os.path.join('/tmp', name)
            try:
                if now - os.path.getmtime(path) > max_age:
                    if os.path.isdir(path): shutil.rmtree(path, ignore_errors=True)
                    removed += 1
            except OSError: pass
        if removed: logging.info(f"/tmp sweep: removed {removed} stale task directories")
    except Exception as e:
        logging.warning(f"/tmp sweep failed: {e}")

# ============================================================
# Helpers
# ============================================================
def format_size(mb: float) -> str:
    if not mb: return "Unknown"
    if mb >= 1024: return f"{mb/1024:.2f} GB"
    return f"{mb:.1f} MB"

def cleanup_file(path):
    if path and os.path.exists(path):
        try: os.remove(path)
        except: pass

def cleanup_dir(directory):
    if directory and os.path.exists(directory):
        try: shutil.rmtree(directory)
        except: pass

def find_output_file(base_path):
    if os.path.exists(base_path) and os.path.isfile(base_path): return base_path
    dir_name = os.path.dirname(base_path)
    base_name = os.path.basename(base_path)
    if os.path.exists(dir_name):
        for f in os.listdir(dir_name):
            if f.startswith(base_name): return os.path.join(dir_name, f)
    return None

def send_chat_action(chat_id, action):
    try: bot.send_chat_action(chat_id, action)
    except: pass

def update_status(chat_id, msg_id, text):
    if not msg_id: return
    try: bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
    except: pass

def ensure_h264(video_path, task_dir):
    """إصلاح مشكلة الشاشة السوداء في المعرض عبر إجبار تحويل الفيديو لـ H.264 والصوت لـ AAC"""
    try:
        cmd = [FFMPEG_PATH, '-hide_banner', '-i', video_path]
        r = subprocess.run(cmd, capture_output=True, text=True)
        output = r.stderr.lower()
        
        is_h264 = 'video: h264' in output or 'video: avc' in output
        is_yuv420p = 'yuv420p' in output
        is_aac = 'audio: aac' in output
        
        if is_h264 and is_yuv420p and is_aac:
            return video_path
            
        logging.info(f"Converting video to H.264/AAC for Gallery compatibility: {video_path}")
        out_path = os.path.join(task_dir, f"fixed_{uuid.uuid4().hex[:6]}.mp4")
        
        conv_cmd = [
            FFMPEG_PATH, '-y', '-i', video_path,
            '-c:v', 'libx264', '-preset', 'superfast', '-crf', '26',
            '-profile:v', 'main', '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            out_path
        ]
        subprocess.run(conv_cmd, capture_output=True, timeout=300)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            return out_path
    except Exception as e:
        logging.error(f"ensure_h264 error: {e}")
    return video_path

# ============================================================
# yt-dlp options
# ============================================================
FORMAT_PREF = "bv*[vcodec~='^avc|^h264']+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b"

def base_ydl_opts():
    opts = {
        'noplaylist': True,
        'retries': 3, # تقليل المحاولات لمنع التعليق الطويل
        'fragment_retries': 3,
        'socket_timeout': 15, # إجبار الفشل السريع إذا كان الخادم لا يستجيب
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        'max_filesize': 55000000,
        'http_headers': {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
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
    if COOKIES_FILE: opts['cookiefile'] = COOKIES_FILE
    return opts

def _yt_opts(fmt, output_path, clients=None, player_skip=None):
    opts = base_ydl_opts()
    opts.update({'format': fmt, 'outtmpl': output_path, 'merge_output_format': 'mp4'})
    if clients:
        ea = {'youtube': {'player_client': clients}}
        if player_skip: ea['youtube']['player_skip'] = player_skip
        opts['extractor_args'] = ea
    return opts

def translate_error(err: str) -> str:
    err = err or ""
    low = err.lower()
    if 'max-filesize' in low or 'file is larger than' in low: return "File is too large"
    if 'may not be comfortable' in err or 'Log in for access' in err:
        return "This post is restricted by the platform and requires a logged-in account."
    if 'private' in low: return "This content is private."
    if 'not available' in low or 'has been removed' in low: return "Content is not available or has been removed."
    if 'region' in low or 'geo' in low: return "Video is geo-blocked in your region."
    if 'login required' in low or 'sign in' in low or 'rate-limit' in low:
        return "This platform requires login from server IPs. The bot owner can fix this by adding account cookies."
    if 'Unsupported URL' in err: return "URL is not supported."
    if 'HTTP Error 429' in err: return "Rate limit exceeded. Try again later."
    if 'no video' in low or 'no video formats' in low: return "No video found in this post (it may be photos only)."
    clean = err.replace('ERROR:', '').strip()
    return f"Download failed: {clean[:200]}"

def get_available_formats(url: str) -> list:
    ydl_opts = base_ydl_opts()
    ydl_opts.pop('max_filesize', None)
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info: return []
            if 'entries' in info and info['entries']: info = info['entries'][0]
            if not info: return []

            duration = info.get('duration') or 0
            formats = []
            for f in info.get('formats', []):
                if f.get('vcodec') == 'none': continue
                height = f.get('height')
                if not height: continue
                filesize = f.get('filesize') or f.get('filesize_approx')
                if not filesize and f.get('tbr') and duration: filesize = (f['tbr'] * 1000 / 8) * duration
                size_mb = filesize / (1024 * 1024) if filesize else 0
                formats.append({
                    'id': f.get('format_id', ''), 'height': height,
                    'size_mb': round(size_mb, 1), 'ext': f.get('ext', 'mp4'),
                    'fps': f.get('fps'), 'tbr': f.get('tbr') or 0,
                })
            seen = {}
            for f in formats:
                h = f['height']
                if h not in seen or f['tbr'] > seen[h]['tbr']: seen[h] = f
            unique = sorted(seen.values(), key=lambda x: x['height'], reverse=True)
            return unique[:8]
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []

# ============================================================
# HTTP helpers
# ============================================================
def api_get_json(api_url: str, timeout: int = 30):
    try:
        from curl_cffi import requests as cffi_requests
        r = cffi_requests.get(api_url, impersonate='chrome', timeout=timeout)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return None

def fetch_file(media_url: str, dest: str, referer: str = None) -> bool:
    try:
        from curl_cffi import requests as cffi_requests
        headers = {'User-Agent': USER_AGENT}
        if referer: headers['Referer'] = referer
        for attempt in range(3):
            try:
                with cffi_requests.get(media_url, headers=headers, impersonate='chrome', timeout=40, stream=True) as r:
                    if r.status_code == 200:
                        with open(dest, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=256 * 1024): f.write(chunk)
                        return os.path.getsize(dest) > 0
            except Exception: time.sleep(1.5 * (attempt + 1))
        return False
    except ImportError:
        import urllib.request
        try:
            req = urllib.request.Request(media_url, headers={'User-Agent': USER_AGENT, **({'Referer': referer} if referer else {})})
            with urllib.request.urlopen(req, timeout=40) as r, open(dest, 'wb') as f:
                shutil.copyfileobj(r, f, length=256 * 1024)
            return os.path.getsize(dest) > 0
        except Exception: return False

# ============================================================
# Media bundle
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
# Cobalt API (السلاح السري لتخطي الحظر)
# ============================================================
def try_cobalt_api(url: str, workdir: str):
    """محرك Cobalt: سريع جداً ويدعم تيك توك، انستجرام، تويتر، يوتيوب"""
    try:
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        payload = {"url": url, "vQuality": "1080", "isAudioOnly": False, "isNoAudio": False}
        r = requests.post("https://api.cobalt.tools/api/json", json=payload, headers=headers, timeout=15)
        if r.status_code != 200: return None
        
        data = r.json()
        status = data.get('status')
        os.makedirs(workdir, exist_ok=True)
        bundle = MediaBundle()

        if status == 'redirect' or status == 'stream':
            v_url = data.get('url')
            if v_url:
                dest = os.path.join(workdir, 'video.mp4')
                if fetch_file(v_url, dest): bundle.videos.append(dest)
        elif status == 'picker':
            for i, item in enumerate(data.get('picker', [])[:MAX_SLIDESHOW_IMAGES]):
                if item.get('type') == 'photo':
                    dest = os.path.join(workdir, f'img_{i:03d}.jpg')
                    if fetch_file(item.get('url'), dest): bundle.images.append(dest)
            audio_url = data.get('audio')
            if audio_url:
                adest = os.path.join(workdir, 'audio.mp3')
                if fetch_file(audio_url, adest): bundle.audio = adest

        return bundle if bundle.has_media() else None
    except Exception as e:
        logging.warning(f"Cobalt API failed: {e}")
        return None

# ============================================================
# Twitter Strategy
# ============================================================
def _tweet_id(url: str):
    m = re.search(r'(?:twitter\.com|x\.com)/[^/]+/status(?:es)?/(\d+)', url)
    return m.group(1) if m else None

def try_fxtwitter(url: str, workdir: str):
    tid = _tweet_id(url)
    if not tid: return None
    data = api_get_json(f'https://api.fxtwitter.com/status/{tid}')
    if not data or not isinstance(data, dict): return None
    tweet = data.get('tweet') or {}
    media = tweet.get('media') or {}
    photos = media.get('photos') or []
    videos = media.get('videos') or []
    if not photos and not videos: return None

    os.makedirs(workdir, exist_ok=True)
    bundle = MediaBundle()
    bundle.title = (tweet.get('text') or '')[:900]

    for i, p in enumerate(photos[:MAX_SLIDESHOW_IMAGES]):
        u = (p or {}).get('url')
        if u:
            dest = os.path.join(workdir, f'img_{i:03d}.jpg')
            if fetch_file(u, dest, referer=url): bundle.images.append(dest)

    for i, v in enumerate(videos[:3]):
        u = (v or {}).get('url')
        if u:
            dest = os.path.join(workdir, f'vid_{i:03d}.mp4')
            if fetch_file(u, dest, referer=url): bundle.videos.append(dest)

    if bundle.has_media(): return bundle
    return None

# ============================================================
# TikTok Strategy
# ============================================================
TIKWM_HOSTS = ("https://www.tikwm.com/api/", "https://tikwm.com/api/")

def _tikwm_api(url: str):
    for host in TIKWM_HOSTS:
        try:
            r = requests.post(host, data={'url': url, 'hd': '1'}, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data and data.get('code') == 0: return data
        except Exception: pass
        try:
            from curl_cffi import requests as cffi_requests
            r = cffi_requests.post(host, data={'url': url, 'hd': '1'}, impersonate='chrome', timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data and data.get('code') == 0: return data
        except Exception: pass
    return None

def try_tikwm(url: str, workdir: str):
    data = _tikwm_api(url)
    if not data: return None
    d = data.get('data') or {}

    os.makedirs(workdir, exist_ok=True)
    bundle = MediaBundle()
    bundle.title = (d.get('title') or '')[:900]
    bundle.duration = d.get('duration') or 0

    images = d.get('images') or []
    if images:
        for i, u in enumerate(images[:MAX_SLIDESHOW_IMAGES]):
            if not u: continue
            dest = os.path.join(workdir, f'img_{i:03d}.jpg')
            if fetch_file(u, dest, referer=url): bundle.images.append(dest)
        music = d.get('music')
        if music:
            adest = os.path.join(workdir, 'audio.mp3')
            if fetch_file(music, adest, referer=url): bundle.audio = adest
    else:
        vu = d.get('hdplay') or d.get('play')
        if vu:
            vdest = os.path.join(workdir, 'video.mp4')
            if fetch_file(vu, vdest, referer=url): bundle.videos.append(vdest)

    if bundle.has_media(): return bundle
    return None

# ============================================================
# Slideshow Extraction
# ============================================================
IMAGE_EXTS = ('jpg', 'jpeg', 'png', 'webp')

def _pick_clip_url(entry):
    best = None
    for f in entry.get('formats') or []:
        if not f or not f.get('url'): continue
        if f.get('vcodec') in (None, 'none'): continue
        if (f.get('ext') or '').lower() != 'mp4': continue
        h = f.get('height') or 0
        if best is None or h > (best.get('height') or 0):
            if h <= 1080: best = f
            elif best is None: best = f
    if best: return best['url']
    u = entry.get('url')
    if u and (entry.get('ext') or '').lower() == 'mp4': return u
    return None

def _collect_slideshow_data(info):
    if not info: return None
    entries = [e for e in (info.get('entries') or []) if e]
    if not entries: entries = [info]

    image_urls, clip_urls = [], []
    audio_fmt = None
    duration = info.get('duration') or 0

    for entry in entries:
        if entry.get('duration') and not duration: duration = entry['duration']
        entry_has_video = False
        for f in entry.get('formats') or []:
            if not f or not f.get('url'): continue
            ext = (f.get('ext') or '').lower()
            vcodec, acodec = f.get('vcodec'), f.get('acodec')
            if ext in IMAGE_EXTS or (vcodec == 'none' and acodec == 'none' and ext not in ('mp4', 'webm', 'm4a', 'mp3', 'aac', 'mov')):
                if f['url'] not in image_urls: image_urls.append(f['url'])
            elif (acodec and acodec != 'none') and (not vcodec or vcodec == 'none'):
                abr = f.get('abr') or f.get('tbr') or 0
                if audio_fmt is None or abr > (audio_fmt.get('abr') or audio_fmt.get('tbr') or 0): audio_fmt = f
            elif vcodec and vcodec != 'none':
                entry_has_video = True

        direct_url = entry.get('url')
        direct_ext = (entry.get('ext') or '').lower()
        if direct_url and direct_ext in IMAGE_EXTS and direct_url not in image_urls:
            image_urls.append(direct_url)
        elif entry_has_video:
            cu = _pick_clip_url(entry)
            if cu and cu not in clip_urls: clip_urls.append(cu)

    if not image_urls and not clip_urls: return None
    return {
        'images': image_urls[:MAX_SLIDESHOW_IMAGES],
        'clips': clip_urls[:3],
        'audio_url': audio_fmt.get('url') if audio_fmt else None,
        'duration': duration,
    }

def extract_slideshow(url: str):
    errors = []
    for extra in ({'ignore_no_formats_error': True}, {}):
        try:
            opts = base_ydl_opts()
            opts['retries'] = 2
            opts['fragment_retries'] = 2
            opts.update(extra)
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            data = _collect_slideshow_data(info)
            if data: return data, None
        except Exception as e:
            errors.append(str(e)[:200])
    return None, errors[-1] if errors else "No photo content detected"

_encoders_cache = {}
def _has_encoder(name: str) -> bool:
    if FFMPEG_PATH not in _encoders_cache:
        try:
            r = subprocess.run([FFMPEG_PATH, '-hide_banner', '-encoders'], capture_output=True, text=True, timeout=60)
            _encoders_cache[FFMPEG_PATH] = r.stdout
        except Exception: _encoders_cache[FFMPEG_PATH] = ''
    return name in _encoders_cache[FFMPEG_PATH]

def merge_slideshow(image_paths: list, audio_path: str, out_path: str, total_duration=None) -> bool:
    n = len(image_paths)
    if not n or not audio_path or not os.path.exists(audio_path): return False
        
    if total_duration and total_duration > 0:
        total_duration = min(total_duration, 60.0)
        per_image = max(total_duration / n, 1.0)
    else:
        per_image = min(3.0, 60.0 / n)

    encoder = 'libx264' if _has_encoder('libx264') else 'mpeg4'
    cmd = [FFMPEG_PATH, '-y']
    for img in image_paths: cmd += ['-loop', '1', '-t', f'{per_image:.3f}', '-i', img]
    cmd += ['-i', audio_path]

    if n == 1:
        filter_complex = '[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[vout]'
        cmd += ['-filter_complex', filter_complex, '-map', '[vout]', '-map', '1:a']
    else:
        parts = []
        for i in range(n):
            parts.append(f'[{i}:v]scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[v{i}]')
        parts.append(''.join(f'[v{i}]' for i in range(n)) + f'concat=n={n}:v=1:a=0[vout]')
        cmd += ['-filter_complex', ';'.join(parts), '-map', '[vout]', '-map', f'{n}:a']

    cmd += ['-c:v', encoder, '-preset', 'superfast', '-crf', '24', '-c:a', 'aac', '-b:a', '128k', '-shortest', '-movflags', '+faststart', out_path]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception as e:
        logging.error(f"merge_slideshow error: {e}")
        return False

def download_slideshow_assets(url: str, data: dict, workdir: str):
    os.makedirs(workdir, exist_ok=True)
    image_paths, video_paths = [], []
    for idx, img_url in enumerate(data.get('images') or []):
        dest = os.path.join(workdir, f'img_{idx:03d}.jpg')
        if fetch_file(img_url, dest, referer=url): image_paths.append(dest)
    for idx, clip_url in enumerate(data.get('clips') or []):
        dest = os.path.join(workdir, f'clip_{idx:03d}.mp4')
        if fetch_file(clip_url, dest, referer=url): video_paths.append(dest)

    if not image_paths and not video_paths: return None, "Could not download any of the post media."

    audio_path = None
    if data.get('audio_url'):
        audio_dest = os.path.join(workdir, 'audio.m4a')
        if fetch_file(data['audio_url'], audio_dest, referer=url): audio_path = audio_dest

    if not audio_path and image_paths:
        try:
            audio_dest = os.path.join(workdir, 'audio')
            opts = base_ydl_opts()
            opts['retries'] = 3
            opts.update({'format': 'bestaudio/best', 'outtmpl': audio_dest})
            with yt_dlp.YoutubeDL(opts) as ydl: ydl.download([url])
            audio_path = find_output_file(audio_dest)
        except Exception as e:
            logging.warning(f"slideshow audio via yt-dlp failed: {str(e)[:120]}")

    return {'images': image_paths, 'videos': video_paths, 'audio': audio_path, 'dir': workdir, 'duration': data.get('duration')}, None

# ============================================================
# Video Download
# ============================================================
def _ydl_try(opts, url):
    with yt_dlp.YoutubeDL(opts) as ydl: ydl.download([url])

def download_video(url, output_path, platform='other'):
    attempts = []
    if platform == 'youtube':
        attempts = [
            ('yt_default_clients', _yt_opts(FORMAT_PREF, output_path)),
            ('yt_web_embedded', _yt_opts(FORMAT_PREF, output_path, clients=['web_embedded'], player_skip=['webpage', 'configs'])),
            ('yt_android_vr', _yt_opts(FORMAT_PREF, output_path, clients=['android_vr'])),
            ('yt_tv_embedded', _yt_opts(FORMAT_PREF, output_path, clients=['tv_embedded'], player_skip=['webpage', 'configs'])),
            ('yt_mweb_ios', _yt_opts(FORMAT_PREF, output_path, clients=['mweb', 'ios'])),
            ('yt_simple_best', _yt_opts('b[ext=mp4]/b', output_path)),
        ]
        if COOKIES_FILE: attempts.insert(1, ('yt_web_cookies', _yt_opts(FORMAT_PREF, output_path, clients=['web', 'mweb'])))
    elif platform == 'twitter':
        o = _yt_opts(FORMAT_PREF, output_path)
        o['extractor_args'] = {'twitter': {'api': ['syndication']}}
        o2 = _yt_opts(FORMAT_PREF, output_path)
        o2['extractor_args'] = {'twitter': {'api': ['legacy']}}
        attempts = [('twitter_syndication', o), ('twitter_legacy', o2), ('twitter_graphql', _yt_opts(FORMAT_PREF, output_path)), ('twitter_best', _yt_opts('b[ext=mp4]/b', output_path))]
    elif platform == 'tiktok':
        attempts = [('tiktok_best', _yt_opts('b[ext=mp4]/b', output_path)), ('tiktok_generic', _yt_opts(FORMAT_PREF, output_path))]
    else:
        attempts = [('optimal', _yt_opts(FORMAT_PREF, output_path)), ('best_merged', _yt_opts('b[ext=mp4]/b', output_path))]

    last_error = None
    for name, opts in attempts:
        try:
            logging.info(f"Trying download [{name}]: {url[:80]}")
            _ydl_try(opts, url)
            if find_output_file(output_path): return True, None
            last_error = "file not created"
        except yt_dlp.utils.DownloadError as e:
            last_error = str(e)
            if 'max-filesize' in last_error.lower() or 'file is larger than' in last_error.lower(): return False, "File is too large"
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
        cleanup_partial(output_path)
    return False, translate_error(last_error or "All attempts failed")

def cleanup_partial(output_path):
    dir_name = os.path.dirname(output_path)
    if os.path.exists(dir_name):
        for f in os.listdir(dir_name):
            p = os.path.join(dir_name, f)
            if os.path.isfile(p): os.remove(p)

def download_specific_format(url, output_path, format_id, height=None):
    platform = classify_url(url)
    attempts = []
    opts1 = _yt_opts(f'{format_id}+ba[ext=m4a]/{format_id}+ba/b', output_path)
    attempts.append(('specific+audio', opts1))
    opts2 = base_ydl_opts()
    opts2.update({'format': format_id, 'outtmpl': output_path})
    attempts.append(('specific_only', opts2))
    if height and str(height).isdigit():
        opts3 = _yt_opts(f'bv*[height<={height}]+ba[ext=m4a]/bv*[height<={height}]+ba/b[height<={height}]/b', output_path)
        attempts.append(('height_filter', opts3))
    if platform == 'youtube':
        opts4 = _yt_opts(f'bv*[height<={height}]+ba[ext=m4a]/bv*[height<={height}]+ba/b[height<={height}]/b' if height else 'b[ext=mp4]/b', output_path, clients=['android_vr', 'web_embedded'])
        attempts.append(('yt_alt_clients', opts4))

    last_error = None
    for attempt_name, opts in attempts:
        opts.pop('max_filesize', None)
        try:
            logging.info(f"Trying format download [{attempt_name}]")
            _ydl_try(opts, url)
            if find_output_file(output_path): return True, None
        except Exception as e:
            last_error = str(e)
        cleanup_partial(output_path)
    return False, translate_error(last_error or "Format download failed")

def download_audio(url, output_path):
    platform = classify_url(url)
    attempts = []
    o1 = base_ydl_opts()
    o1.update({'format': 'bestaudio/best', 'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}], 'outtmpl': output_path})
    attempts.append(('audio_default', o1))
    if platform == 'youtube':
        o2 = base_ydl_opts()
        o2.update({'format': 'bestaudio/best', 'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}], 'outtmpl': output_path, 'extractor_args': {'youtube': {'player_client': ['android_vr', 'web_embedded']}}})
        attempts.append(('audio_alt_clients', o2))

    last_error = None
    for name, opts in attempts:
        try:
            _ydl_try(opts, url)
            if find_output_file(output_path + '.mp3') or find_output_file(output_path): return True, None
        except Exception as e:
            last_error = str(e)
        cleanup_partial(output_path)
    return False, translate_error(last_error or "Audio download failed")

# ============================================================
# gallery-dl
# ============================================================
def download_gallery(url, output_dir):
    try:
        from gallery_dl import job, config as gdconfig
        os.makedirs(output_dir, exist_ok=True)
        gdconfig.set(('extractor',), 'base-directory', output_dir)
        gdconfig.set(('extractor',), 'sleep-request', 1.0)
        gdconfig.set(('extractor',), 'user-agent', USER_AGENT)
        if COOKIES_FILE: gdconfig.set(('extractor',), 'cookies', COOKIES_FILE)
        gdconfig.set(('downloader',), 'retries', 3)
        gdconfig.set(('downloader',), 'timeout', 30.0)

        j = job.DownloadJob(url)
        j.run()

        files = []
        for root, _dirs, names in os.walk(output_dir):
            for n in names:
                p = os.path.join(root, n)
                if os.path.isfile(p) and os.path.getsize(p) > 0: files.append(p)
        files.sort()
        if not files: return False, []
        return True, files[:MAX_SLIDESHOW_IMAGES]
    except Exception as e:
        logging.error(f"gallery-dl error: {e}", exc_info=True)
        return False, []

# ============================================================
# Sending Media
# ============================================================
def _mkvid_markup(task_id: str):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🎬 Make Video (images + audio)", callback_data=f"mkvid|{task_id}"))
    return markup

def send_photo_post(chat_id, assets, task_id):
    images = [p for p in assets.get('images', []) if os.path.exists(p)]
    audio = assets.get('audio')
    has_audio = bool(audio and os.path.exists(audio))

    # 1. إرسال الصوت أولاً
    if has_audio:
        send_chat_action(chat_id, 'upload_voice')
        try:
            markup = _mkvid_markup(task_id) if len(images) > 1 else None
            with open(audio, 'rb') as a:
                bot.send_audio(chat_id, a, caption="🎵 Audio", reply_markup=markup, timeout=180)
        except Exception as e:
            logging.error(f"send audio failed: {e}")

    # 2. إرسال الصور (بإدارة ذاكرة آمنة)
    if len(images) == 1:
        send_chat_action(chat_id, 'upload_photo')
        try:
            markup = _mkvid_markup(task_id) if has_audio else None
            with open(images[0], 'rb') as f:
                bot.send_photo(chat_id, f, caption="📷 Photo post", reply_markup=markup, timeout=120)
        except Exception as e:
            logging.error(f"send_photo failed: {e}")
            try:
                with open(images[0], 'rb') as f: bot.send_document(chat_id, f, timeout=120)
            except: pass
    elif images:
        for chunk_start in range(0, len(images), 10):
            chunk = images[chunk_start:chunk_start + 10]
            media = []
            open_files = []
            try:
                for i, p in enumerate(chunk):
                    f = open(p, 'rb')
                    open_files.append(f)
                    caption = '📷 Photo post' if (chunk_start == 0 and i == 0) else ''
                    media.append(types.InputMediaPhoto(f, caption=caption))
                
                if media:
                    send_chat_action(chat_id, 'upload_photo')
                    bot.send_media_group(chat_id, media, timeout=180)
            except Exception as e:
                logging.error(f"send_media_group failed: {e}")
                for p in chunk:
                    try:
                        with open(p, 'rb') as f: bot.send_photo(chat_id, f, timeout=120)
                    except: pass
            finally:
                for f in open_files: f.close()

def send_bundle(chat_id, user_id, url, bundle: MediaBundle, workdir: str, task_id: str):
    sent = False
    for vp in bundle.videos[:3]:
        if not os.path.exists(vp): continue
        size_mb = os.path.getsize(vp) / (1024 * 1024)
        if size_mb > 50:
            bot.send_message(chat_id, f"Video size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).\nUse the options below to pick a lower quality.")
            continue
            
        vp = ensure_h264(vp, workdir)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("More", callback_data=f"more|{task_id}"))
        send_chat_action(chat_id, 'upload_video')
        try:
            with open(vp, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True, caption=bundle.title[:1000] if bundle.title else None)
            sent = True
        except Exception as e:
            logging.error(f"send_bundle video failed: {e}")
            try:
                with open(vp, 'rb') as v: bot.send_document(chat_id, v, timeout=180)
                sent = True
            except: pass

    if bundle.images:
        assets = {'images': bundle.images, 'videos': [], 'audio': bundle.audio, 'dir': workdir, 'duration': bundle.duration}
        _store_task_pending(task_id, user_id, url, assets=assets)
        send_photo_post(chat_id, assets, task_id)
        sent = True
    elif bundle.audio and not bundle.videos:
        send_chat_action(chat_id, 'upload_voice')
        try:
            with open(bundle.audio, 'rb') as a: bot.send_audio(chat_id, a, timeout=180)
            sent = True
        except: pass
    return sent

def send_gallery(chat_id, files):
    if not files:
        bot.send_message(chat_id, "No images found.")
        return

    image_exts = ('.jpg', '.jpeg', '.png', '.webp', '.gif')
    video_exts = ('.mp4', '.webm', '.mov', '.mkv')

    images = [f for f in files if f.lower().endswith(image_exts)][:MAX_SLIDESHOW_IMAGES]
    videos = [f for f in files if f.lower().endswith(video_exts)][:3]

    sent_count = 0
    if len(images) == 1:
        try:
            size_mb = os.path.getsize(images[0]) / (1024 * 1024)
            send_chat_action(chat_id, 'upload_photo')
            with open(images[0], 'rb') as img:
                if size_mb > 10: bot.send_document(chat_id, img, timeout=120)
                else: bot.send_photo(chat_id, img, timeout=120)
            sent_count += 1
        except Exception as e: logging.error(f"send_photo failed: {e}")
    elif images:
        for chunk_start in range(0, len(images), 10):
            chunk = images[chunk_start:chunk_start + 10]
            media = []
            open_files = []
            try:
                for p in chunk:
                    size_mb = os.path.getsize(p) / (1024 * 1024)
                    if size_mb <= 10:
                        f = open(p, 'rb')
                        open_files.append(f)
                        media.append(types.InputMediaPhoto(f))
                if media:
                    send_chat_action(chat_id, 'upload_photo')
                    bot.send_media_group(chat_id, media, timeout=180)
                    sent_count += len(media)
            except Exception as e:
                logging.error(f"send_media_group gallery failed: {e}")
                for p in chunk:
                    try:
                        with open(p, 'rb') as f: bot.send_photo(chat_id, f, timeout=120)
                    except: pass
            finally:
                for f in open_files: f.close()

    for f in videos:
        try:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            if size_mb > 50:
                bot.send_message(chat_id, f"A video clip ({format_size(size_mb)}) exceeds 50 MB, skipped.")
                continue
            f = ensure_h264(f, os.path.dirname(f))
            send_chat_action(chat_id, 'upload_video')
            with open(f, 'rb') as v: bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
        except Exception as e: logging.error(f"send_video from gallery failed: {e}")

    if sent_count == 0 and not videos: bot.send_message(chat_id, "No viewable media found.")

def send_more_options(chat_id, user_id, url, task_id):
    formats = _get_task_formats(task_id)
    if not formats:
        send_chat_action(chat_id, 'typing')
        formats = get_available_formats(url)
        if formats: _set_task_formats(task_id, formats)

    if not formats:
        bot.send_message(chat_id, "No format information available.")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        label = f"{f['height']}p"
        if f['size_mb']: label += f" - {f['size_mb']:.0f}MB"
        buttons.append(types.InlineKeyboardButton(label, callback_data=f"q|{task_id}|{f['id']}|{f['height']}"))
    buttons.append(types.InlineKeyboardButton("Audio Only (MP3)", callback_data=f"a|{task_id}"))
    markup.add(*buttons)
    bot.send_message(chat_id, "AVAILABLE OPTIONS", reply_markup=markup)

# ============================================================
# process_download
# ============================================================
def process_download(task: dict):
    user_id = task['user_id']
    with user_locks[user_id]:
        _process_download_internal(task)

def _process_download_internal(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']
    status_msg_id = task.get('msg_id')
    task_id = task.get('task_id')
    platform = classify_url(url)

    task_dir = f"/tmp/task_{task_id}"
    os.makedirs(task_dir, exist_ok=True)
    temp = os.path.join(task_dir, "video")
    final = None

    try:
        send_chat_action(chat_id, 'typing')
        update_status(chat_id, status_msg_id, "🔍 Analyzing link and extracting media...")

        # Layer 1: Cobalt API (الأسرع والأقوى لتخطي الحظر)
        bundle = try_cobalt_api(url, task_dir)
        if bundle:
            _store_task_pending(task_id, user_id, url)
            update_status(chat_id, status_msg_id, "✅ Media found, uploading...")
            if send_bundle(chat_id, user_id, url, bundle, task_dir, task_id):
                threading.Thread(target=_background_fetch_formats, args=(task_id, url), daemon=True).start()
                try: bot.delete_message(chat_id, status_msg_id)
                except: pass
                return

        # Layer 2: Platform specific APIs
        if platform == 'twitter':
            bundle = try_fxtwitter(url, task_dir)
            if bundle:
                _store_task_pending(task_id, user_id, url)
                update_status(chat_id, status_msg_id, "✅ Media found, uploading...")
                if send_bundle(chat_id, user_id, url, bundle, task_dir, task_id):
                    threading.Thread(target=_background_fetch_formats, args=(task_id, url), daemon=True).start()
                    try: bot.delete_message(chat_id, status_msg_id)
                    except: pass
                    return

        elif platform == 'tiktok':
            bundle = try_tikwm(url, task_dir)
            if bundle:
                _store_task_pending(task_id, user_id, url)
                update_status(chat_id, status_msg_id, "✅ Media found, uploading...")
                if send_bundle(chat_id, user_id, url, bundle, task_dir, task_id):
                    threading.Thread(target=_background_fetch_formats, args=(task_id, url), daemon=True).start()
                    try: bot.delete_message(chat_id, status_msg_id)
                    except: pass
                    return

        elif platform == 'instagram':
            ok2, files = download_gallery(url, task_dir)
            if ok2 and files:
                _store_task_pending(task_id, user_id, url)
                update_status(chat_id, status_msg_id, "✅ Media extracted, uploading...")
                send_gallery(chat_id, files)
                try: bot.delete_message(chat_id, status_msg_id)
                except: pass
                return

        # Layer 3: yt-dlp
        update_status(chat_id, status_msg_id, "⏳ Downloading video (Primary Method)...")
        send_chat_action(chat_id, 'typing')
        ok, err = download_video(url, temp, platform)
        
        if not ok and err == "File is too large":
            update_status(chat_id, status_msg_id, "⚠️ File is too large. Fetching quality options...")
            _store_task_pending(task_id, user_id, url)
            send_more_options(chat_id, user_id, url, task_id)
            try: bot.delete_message(chat_id, status_msg_id)
            except: pass
            return

        if ok:
            final = find_output_file(temp)
            if not final or os.path.getsize(final) == 0:
                update_status(chat_id, status_msg_id, "❌ Downloaded file is empty. The platform may have blocked the request.")
                return

            size_mb = os.path.getsize(final) / (1024 * 1024)
            if size_mb > 50:
                update_status(chat_id, status_msg_id, "⚠️ File is too large. Fetching quality options...")
                _store_task_pending(task_id, user_id, url)
                send_more_options(chat_id, user_id, url, task_id)
                try: bot.delete_message(chat_id, status_msg_id)
                except: pass
                return
                
            final = ensure_h264(final, task_dir)
            _store_task_pending(task_id, user_id, url)
            _clear_task_assets(task_id)

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data=f"more|{task_id}"))

            update_status(chat_id, status_msg_id, "✅ Download complete, uploading video...")
            send_chat_action(chat_id, 'upload_video')
            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True)

            threading.Thread(target=_background_fetch_formats, args=(task_id, url), daemon=True).start()
            try: bot.delete_message(chat_id, status_msg_id)
            except: pass
            return

        # Layer 4: Slideshow/Photos via yt-dlp
        update_status(chat_id, status_msg_id, "⚠️ Video download failed, checking for photo post...")
        send_chat_action(chat_id, 'typing')
        data, ss_err = extract_slideshow(url)

        if data:
            update_status(chat_id, status_msg_id, "⏳ Downloading photo post assets...")
            assets, a_err = download_slideshow_assets(url, data, task_dir)
            if assets:
                _store_task_pending(task_id, user_id, url, assets=assets)
                update_status(chat_id, status_msg_id, "✅ Assets downloaded, uploading...")
                for cv in assets.get('videos', []):
                    try:
                        size_mb = os.path.getsize(cv) / (1024 * 1024)
                        if size_mb <= 50:
                            cv = ensure_h264(cv, task_dir)
                            send_chat_action(chat_id, 'upload_video')
                            with open(cv, 'rb') as v: bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
                    except Exception as e: logging.error(f"send carousel clip failed: {e}")
                send_photo_post(chat_id, assets, task_id)
                try: bot.delete_message(chat_id, status_msg_id)
                except: pass
                return
            logging.warning(f"slideshow asset download failed: {a_err}")

        # Layer 5: gallery-dl (Fallback if not instagram)
        if platform != 'instagram':
            update_status(chat_id, status_msg_id, "⚠️ Standard methods failed, trying fallback extractor (gallery-dl)...")
            send_chat_action(chat_id, 'typing')
            ok2, files = download_gallery(url, task_dir)
            if ok2 and files:
                _store_task_pending(task_id, user_id, url)
                update_status(chat_id, status_msg_id, "✅ Media extracted, uploading...")
                send_gallery(chat_id, files)
                try: bot.delete_message(chat_id, status_msg_id)
                except: pass
                return

        # Final Failure
        hint = ""
        if platform == 'instagram' and not COOKIES_FILE:
            hint = ("\n\nTip: Instagram often blocks server IPs. The bot owner can enable it "
                    "by adding Instagram cookies (YTDLP_COOKIES).")
        update_status(chat_id, status_msg_id, f"❌ Download failed.\n\n{err}{hint}")

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        update_status(chat_id, status_msg_id, f"❌ Technical error: {type(e).__name__}")
    finally:
        pending = _get_task_pending(task_id)
        if not (pending and pending.get('assets')):
            shutil.rmtree(task_dir, ignore_errors=True)

# =====
# ============================================================
# Handlers (Adapted for the new task_id system)
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    text = (
        "<b>yt-dlp</b>\n"
        "Professional Media Downloading Service\n\n"
        "Built on a multi-layer download engine with automatic fallbacks for "
        "maximum reliability across platforms.\n\n"
        "<b>SUPPORTED CONTENT</b>\n"
        "• Videos in the best available quality\n"
        "• Photo posts &amp; carousels (TikTok, Instagram, X) — delivered as albums\n"
        "• Slideshow audio delivered separately, with one-tap video merge\n"
        "• Audio extraction (MP3)\n\n"
        "<b>PLATFORMS</b>\n"
        "YouTube, TikTok, Instagram, X/Twitter, Facebook, Reddit, Pinterest, "
        "Vimeo, Twitch, SoundCloud, and many more.\n\n"
        "<b>HOW TO USE</b>\n"
        "Send any supported media URL and the service will handle the rest."
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

@bot.callback_query_handler(func=lambda call: call.data.startswith('more|'))
def handle_more(call):
    parts = call.data.split('|')
    task_id = parts[1] if len(parts) > 1 else None
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    url = _get_task_url(task_id)

    bot.answer_callback_query(call.id)

    if not url:
        bot.send_message(chat_id, "Session expired. Please send the URL again.")
        return

    send_more_options(chat_id, user_id, url, task_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('mkvid|'))
def handle_mkvid(call):
    parts = call.data.split('|')
    task_id = parts[1] if len(parts) > 1 else None
    user_id = call.from_user.id
    chat_id = call.message.chat.id

    bot.answer_callback_query(call.id, "Making video...")

    try:
        data = _get_task_pending(task_id)
        if not data or not data.get('assets'):
            bot.send_message(chat_id, "Session expired. Please send the link again.")
            return

        assets = data['assets']
        url = data['url']

        images = [p for p in assets.get('images', []) if os.path.exists(p)]
        audio = assets.get('audio')
        audio = audio if audio and os.path.exists(audio) else None

        if not images or not audio:
            bot.send_message(chat_id, "Media files are missing or expired from the server. Send the link again.")
            return

        out_path = os.path.join(assets['dir'], f'merged_{uuid.uuid4().hex[:6]}.mp4')
        if not merge_slideshow(images, audio, out_path, total_duration=assets.get('duration')):
            bot.send_message(chat_id, "Video processing failed. Please try again.")
            return

        size_mb = os.path.getsize(out_path) / (1024 * 1024)
        if size_mb > 50:
            bot.send_message(chat_id, f"Merged video ({format_size(size_mb)}) exceeds Telegram's 50 MB limit.")
            return

        try:
            with open(out_path, 'rb') as v:
                bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
        except Exception as e:
            logging.error(f"send merged video failed: {e}")
            bot.send_message(chat_id, "Couldn't send the video. Please try again.")

    except Exception as e:
        logging.error(f"handle_mkvid error: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, f"Technical error: {type(e).__name__}")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('q|'))
def handle_quality(call):
    try:
        parts = call.data.split('|')
        if len(parts) < 4:
            bot.answer_callback_query(call.id, "Invalid data.")
            return
        
        task_id = parts[1]
        fmt_id = parts[2]
        height = parts[3]

        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = _get_task_url(task_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        temp = f"/tmp/q_{task_id}.mp4"
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
                bot.send_message(chat_id, f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).")
                return

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data=f"more|{task_id}"))

            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True)
        finally:
            cleanup_file(temp)
            if final and final != temp:
                cleanup_file(final)
    except Exception as e:
        logging.error(f"handle_quality error: {e}", exc_info=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('a|'))
def handle_audio(call):
    try:
        parts = call.data.split('|')
        task_id = parts[1] if len(parts) > 1 else None
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        url = _get_task_url(task_id)

        bot.answer_callback_query(call.id, "Processing...")

        if not url:
            bot.send_message(chat_id, "Session expired.")
            return

        temp = f"/tmp/a_{task_id}"
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
    text = (message.text or '').strip()

    m = re.search(r'https?://[^\s<>"\']+', text)
    if not m:
        bot.reply_to(message, "Please send a valid URL starting with http:// or https://")
        return
    url = m.group(0).rstrip('.,)]}')

    if not is_allowed_url(url):
        bot.reply_to(message, "This domain is not supported.")
        return

    allowed, wait = check_rate_limit(message.from_user.id)
    if not allowed:
        bot.reply_to(message, f"Rate limit exceeded (5 links per minute).\nTry again in {wait} seconds.")
        return

    try:
        msg = bot.reply_to(message, "Received. Processing...")
        task = {
            'task_id': uuid.uuid4().hex,
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'url': url,
            'msg_id': msg.message_id,
        }
        download_queue.put_nowait(task)
    except queue.Full:
        bot.reply_to(message, "Server busy. Try again shortly.")

# ============================================================
# Flask & Run
# ============================================================
@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "queue_size": download_queue.qsize(),
    }), 200

@app.route('/')
def index():
    return "Bot is running!", 200

def run_bot():
    try:
        bot.remove_webhook()
        logging.info("Old webhook removed.")
    except Exception as e:
        logging.warning(f"remove_webhook: {e}")
    logging.info("=== Starting polling ===")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)

NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
for i in range(NUM_WORKERS):
    threading.Thread(target=download_worker, args=(i + 1,), daemon=True).start()

threading.Thread(target=run_bot, daemon=True).start()
threading.Thread(target=_cleanup_loop, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
