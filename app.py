--- START OF FILE app.py ---

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
from contextlib import ExitStack
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
        stale = [
            uid for uid, times in user_requests.items()
            if not times or (now - max(times)) > RATE_LIMIT_WINDOW
        ]
        for uid in stale:
            user_requests.pop(uid, None)


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
user_pending = {}
USER_PENDING_TTL = 3600


def _cleanup_user_pending():
    now = time.time()
    with _state_lock:
        stale = [uid for uid, data in user_pending.items()
                 if now - data.get('ts', 0) > USER_PENDING_TTL]
        for uid in stale:
            data = user_pending.pop(uid, None)
            if data:
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


def _clear_user_assets(user_id: int):
    with _state_lock:
        if user_id in user_pending:
            user_pending[user_id]['assets'] = None


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
        logging.warning(f"background fetch formats failed: {e}")


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
                    bot.send_message(task['chat_id'], f"Technical error: {type(e).__name__}")
                except Exception:
                    pass
            finally:
                download_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker #{worker_id} error: {e}", exc_info=True)


def _cleanup_loop():
    while True:
        try:
            time.sleep(600)
            _cleanup_user_pending()
            _cleanup_user_requests()
            _sweep_tmp()
        except Exception as e:
            logging.error(f"cleanup loop error: {e}", exc_info=True)


def _sweep_tmp(max_age=7200):
    """حذف جميع الملفات المؤقتة القديمة لمنع امتلاء القرص."""
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
        logging.warning(f"/tmp sweep failed: {e}")


# ============================================================
# Helpers
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


def ensure_mp3(audio_path):
    """تحويل الصوت إجبارياً إلى MP3 حقيقي لضمان قبوله في تليجرام."""
    if not audio_path or not os.path.exists(audio_path):
        return None
    if audio_path.lower().endswith('.mp3'):
        return audio_path
    
    out_path = audio_path.rsplit('.', 1)[0] + '_conv.mp3'
    cmd = [FFMPEG_PATH, '-y', '-i', audio_path, '-c:a', 'libmp3lame', '-q:a', '2', out_path]
    try:
        subprocess.run(cmd, capture_output=True, timeout=60)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            cleanup_file(audio_path)
            return out_path
    except Exception as e:
        logging.warning(f"ensure_mp3 failed: {e}")
    return audio_path


# ============================================================
# yt-dlp options
# ============================================================
def base_ydl_opts():
    opts = {
        'noplaylist': True,
        'retries': 8,
        'fragment_retries': 8,
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        'format_sort': ['vcodec:h264', 'vcodec:h265', 'ext:mp4:m4a'],
        'max_filesize': 50 * 1024 * 1024, # رفض الملفات الأكبر من 50 ميجا قبل التحميل
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
    if 'file is larger than max-filesize' in low or 'max_filesize' in low:
        return "File size exceeds Telegram limit (50 MB)."
    if 'may not be comfortable' in err or 'Log in for access' in err:
        return "This post is restricted by the platform and requires cookies."
    if 'private' in low:
        return "This content is private."
    if 'not available' in low or 'has been removed' in low:
        return "Content is not available or has been removed."
    if 'login required' in low or 'sign in' in low or 'rate-limit' in low:
        return "Platform requires login. Bot owner needs to add cookies."
    if 'Unsupported URL' in err:
        return "URL is not supported."
    if 'no video' in low or 'no video formats' in low:
        return "No video found in this post."
    clean = err.replace('ERROR:', '').strip()
    return f"Download failed: {clean[:200]}"


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
                elif not filesize and f.get('vbr') and duration:
                    filesize = ((f['vbr'] + 128) * 1000 / 8) * duration

                size_mb = filesize / (1024 * 1024) if filesize else 0

                formats.append({
                    'id': f.get('format_id', ''),
                    'height': height,
                    'size_mb': round(size_mb, 1),
                    'ext': f.get('ext', 'mp4'),
                    'fps': f.get('fps'),
                    'tbr': f.get('tbr') or 0,
                    'vcodec': f.get('vcodec', ''),
                })

            seen = {}
            for f in formats:
                h = f['height']
                vcodec = f.get('vcodec', '').lower()
                is_avc = 'avc' in vcodec or 'h264' in vcodec or 'h265' in vcodec or 'hevc' in vcodec
                
                if h not in seen:
                    seen[h] = f
                else:
                    seen_vcodec = seen[h].get('vcodec', '').lower()
                    seen_is_avc = 'avc' in seen_vcodec or 'h264' in seen_vcodec or 'h265' in seen_vcodec or 'hevc' in seen_vcodec
                    
                    if is_avc and not seen_is_avc:
                        seen[h] = f
                    elif is_avc == seen_is_avc and f['tbr'] > seen[h]['tbr']:
                        seen[h] = f

            unique = sorted(seen.values(), key=lambda x: x['height'], reverse=True)
            return unique[:8]
    except Exception:
        return []


# ============================================================
# HTTP helpers
# ============================================================
def api_get_json(api_url: str, timeout: int = 30):
    try:
        from curl_cffi import requests as cffi_requests
        r = cffi_requests.get(api_url, impersonate='chrome', timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def fetch_file(media_url: str, dest: str, referer: str = None) -> bool:
    """تحميل ملف مع فحص الحجم قبل التحميل لمنع استهلاك الباقة."""
    try:
        from curl_cffi import requests as cffi_requests
        headers = {'User-Agent': USER_AGENT}
        if referer:
            headers['Referer'] = referer
        for attempt in range(3):
            try:
                with cffi_requests.get(media_url, headers=headers, impersonate='chrome', timeout=40, stream=True) as r:
                    if r.status_code != 200:
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    
                    # فحص الحجم قبل التحميل
                    content_length = int(r.headers.get('Content-Length', 0))
                    if content_length > 50 * 1024 * 1024:
                        logging.warning("fetch_file: File exceeds 50MB limit.")
                        return False

                    with open(dest, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=256 * 1024):
                            f.write(chunk)
                    return os.path.getsize(dest) > 0
            except Exception:
                time.sleep(1.5 * (attempt + 1))
        return False
    except ImportError:
        import urllib.request
        try:
            req = urllib.request.Request(media_url, headers={
                'User-Agent': USER_AGENT,
                **({'Referer': referer} if referer else {}),
            })
            with urllib.request.urlopen(req, timeout=40) as r:
                content_length = int(r.headers.get('Content-Length', 0))
                if content_length > 50 * 1024 * 1024:
                    return False
                with open(dest, 'wb') as f:
                    shutil.copyfileobj(r, f, length=256 * 1024)
            return os.path.getsize(dest) > 0
        except Exception:
            return False


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
# Strategies
# ============================================================
def _tweet_id(url: str):
    m = re.search(r'(?:twitter\.com|x\.com)/[^/]+/status(?:es)?/(\d+)', url)
    return m.group(1) if m else None


def try_fxtwitter(url: str, workdir: str):
    tid = _tweet_id(url)
    if not tid:
        return None
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
    bundle.title = (tweet.get('text') or '')[:900]

    for i, p in enumerate(photos[:MAX_SLIDESHOW_IMAGES]):
        u = (p or {}).get('url')
        if not u: continue
        dest = os.path.join(workdir, f'img_{i:03d}.jpg')
        if fetch_file(u, dest, referer=url):
            bundle.images.append(dest)

    for i, v in enumerate(videos[:3]):
        u = (v or {}).get('url')
        if not u: continue
        dest = os.path.join(workdir, f'vid_{i:03d}.mp4')
        if fetch_file(u, dest, referer=url):
            bundle.videos.append(dest)

    if bundle.has_media():
        return bundle
    return None


TIKWM_HOSTS = ("https://www.tikwm.com/api/", "https://tikwm.com/api/")

def _tikwm_api(url: str):
    for host in TIKWM_HOSTS:
        data = api_get_json(f'{host}?url={quote(url, safe="")}', timeout=40)
        if data and data.get('code') == 0:
            return data
        try:
            from curl_cffi import requests as cffi_requests
            r = cffi_requests.post(host, data={'url': url, 'hd': '1'}, impersonate='chrome', timeout=40)
            if r.status_code == 200:
                data = r.json()
                if data and data.get('code') == 0:
                    return data
        except Exception:
            pass
    return None


def try_tikwm(url: str, workdir: str):
    data = _tikwm_api(url)
    if not data:
        return None
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
            if fetch_file(u, dest, referer=url):
                bundle.images.append(dest)
        music = d.get('music')
        if music:
            adest = os.path.join(workdir, 'audio.mp3')
            if fetch_file(music, adest, referer=url):
                bundle.audio = ensure_mp3(adest)
    else:
        vu = d.get('hdplay') or d.get('play')
        if vu:
            vdest = os.path.join(workdir, 'video.mp4')
            if fetch_file(vu, vdest, referer=url):
                bundle.videos.append(vdest)

    if bundle.has_media():
        return bundle
    return None


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
        if entry.get('duration') and not duration:
            duration = entry['duration']
        entry_has_video = False
        for f in entry.get('formats') or []:
            if not f or not f.get('url'): continue
            ext = (f.get('ext') or '').lower()
            vcodec, acodec = f.get('vcodec'), f.get('acodec')
            if ext in IMAGE_EXTS or (vcodec == 'none' and acodec == 'none' and ext not in ('mp4', 'webm', 'm4a', 'mp3', 'aac', 'mov')):
                if f['url'] not in image_urls: image_urls.append(f['url'])
            elif (acodec and acodec != 'none') and (not vcodec or vcodec == 'none'):
                abr = f.get('abr') or f.get('tbr') or 0
                if audio_fmt is None or abr > (audio_fmt.get('abr') or audio_fmt.get('tbr') or 0):
                    audio_fmt = f
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
        except Exception:
            _encoders_cache[FFMPEG_PATH] = ''
    return name in _encoders_cache[FFMPEG_PATH]


def merge_slideshow(image_paths: list, audio_path: str, out_path: str, total_duration=None) -> bool:
    """دمج صور + صوت في فيديو MP4 واحد بدقة 720p لمنع استهلاك الذاكرة."""
    n = len(image_paths)
    if not n or not audio_path or not os.path.exists(audio_path):
        return False
    per_image = max(total_duration / n, 1.0) if total_duration and total_duration > 0 else 3.0
    encoder = 'libx264' if _has_encoder('libx264') else 'mpeg4'

    cmd = [FFMPEG_PATH, '-y']
    for img in image_paths:
        cmd += ['-loop', '1', '-t', f'{per_image:.3f}', '-i', img]
    cmd += ['-i', audio_path]

    parts = []
    for i in range(n):
        # تم تقليل الدقة إلى 720p لمنع OOM
        parts.append(
            f'[{i}:v]scale=720:1280:force_original_aspect_ratio=decrease,'
            f'pad=720:1280:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[v{i}]'
        )
    parts.append(''.join(f'[v{i}]' for i in range(n)) + f'concat=n={n}:v=1:a=0[vout]')
    
    cmd += [
        '-filter_complex', ';'.join(parts),
        '-map', '[vout]', '-map', f'{n}:a',
        '-c:v', encoder, '-preset', 'ultrafast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-shortest', '-movflags', '+faststart',
        out_path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception:
        return False


def download_slideshow_assets(url: str, data: dict, workdir: str):
    os.makedirs(workdir, exist_ok=True)
    image_paths = []
    for idx, img_url in enumerate(data.get('images') or []):
        dest = os.path.join(workdir, f'img_{idx:03d}.jpg')
        if fetch_file(img_url, dest, referer=url) and os.path.getsize(dest) > 0:
            image_paths.append(dest)

    video_paths = []
    for idx, clip_url in enumerate(data.get('clips') or []):
        dest = os.path.join(workdir, f'clip_{idx:03d}.mp4')
        if fetch_file(clip_url, dest, referer=url) and os.path.getsize(dest) > 0:
            video_paths.append(dest)

    if not image_paths and not video_paths:
        return None, "Could not download any of the post media."

    audio_path = None
    if data.get('audio_url'):
        audio_dest = os.path.join(workdir, 'audio.m4a')
        if fetch_file(data['audio_url'], audio_dest, referer=url) and os.path.getsize(audio_dest) > 0:
            audio_path = audio_dest

    if not audio_path and image_paths:
        try:
            audio_dest = os.path.join(workdir, 'audio')
            opts = base_ydl_opts()
            opts['retries'] = 3
            opts.update({
                'format': 'bestaudio/best',
                'outtmpl': audio_dest,
            })
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            for f in os.listdir(workdir):
                if f.startswith('audio'):
                    p = os.path.join(workdir, f)
                    if os.path.getsize(p) > 0:
                        audio_path = p
                        break
        except Exception:
            pass

    # إجبار تحويل الصوت إلى MP3 حقيقي
    audio_path = ensure_mp3(audio_path)

    return {'images': image_paths, 'videos': video_paths, 'audio': audio_path,
            'dir': workdir, 'duration': data.get('duration')}, None


def _ydl_try(opts, url):
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])


def download_video(url, output_path, platform='other'):
    attempts = []
    if platform == 'youtube':
        attempts = [
            ('yt_default_clients', _yt_opts('bv*+ba/b', output_path)),
            ('yt_web_embedded', _yt_opts('bv*+ba/b', output_path, clients=['web_embedded'], player_skip=['webpage', 'configs'])),
            ('yt_android_vr', _yt_opts('bv*+ba/b', output_path, clients=['android_vr'])),
            ('yt_tv_embedded', _yt_opts('bv*+ba/b', output_path, clients=['tv_embedded'], player_skip=['webpage', 'configs'])),
            ('yt_mweb_ios', _yt_opts('bv*+ba/b', output_path, clients=['mweb', 'ios'])),
            ('yt_simple_best', _yt_opts('b[ext=mp4]/b', output_path)),
        ]
        if COOKIES_FILE:
            attempts.insert(1, ('yt_web_cookies', _yt_opts('bv*+ba/b', output_path, clients=['web', 'mweb'])))
    elif platform == 'twitter':
        o = _yt_opts('bv*+ba/b', output_path)
        o['extractor_args'] = {'twitter': {'api': ['syndication']}}
        o2 = _yt_opts('bv*+ba/b', output_path)
        o2['extractor_args'] = {'twitter': {'api': ['legacy']}}
        attempts = [
            ('twitter_syndication', o),
            ('twitter_legacy', o2),
            ('twitter_graphql', _yt_opts('bv*+ba/b', output_path)),
            ('twitter_best', _yt_opts('b', output_path)),
        ]
    elif platform == 'tiktok':
        attempts = [
            ('tiktok_best', _yt_opts('b', output_path)),
            ('tiktok_generic', _yt_opts('bv*+ba/b', output_path)),
        ]
    else:
        attempts = [
            ('optimal', _yt_opts('bv*+ba/b', output_path)),
            ('best_merged', _yt_opts('b', output_path)),
        ]

    last_error = None
    for name, opts in attempts:
        try:
            _ydl_try(opts, url)
            if find_output_file(output_path):
                return True, None
            last_error = "file not created"
        except yt_dlp.utils.DownloadError as e:
            last_error = str(e)
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
        cleanup_partial(output_path)

    return False, translate_error(last_error or "All attempts failed")


def cleanup_partial(output_path):
    base = os.path.basename(output_path).rsplit('.', 1)[0]
    try:
        for f in os.listdir('/tmp'):
            if f.startswith(base):
                p = os.path.join('/tmp', f)
                if os.path.isfile(p):
                    os.remove(p)
    except Exception:
        pass


def download_specific_format(url, output_path, format_id, height=None):
    platform = classify_url(url)
    attempts = []
    opts1 = _yt_opts(f'{format_id}+ba/b', output_path)
    attempts.append(('specific+audio', opts1))
    opts2 = base_ydl_opts()
    opts2.update({'format': format_id, 'outtmpl': output_path})
    attempts.append(('specific_only', opts2))
    if height and str(height).isdigit():
        opts3 = _yt_opts(f'bv*[height<={height}]+ba/b[height<={height}]/b', output_path)
        attempts.append(('height_filter', opts3))
    if platform == 'youtube':
        opts4 = _yt_opts(f'bv*[height<={height}]+ba/b[height<={height}]/b' if height else 'b', output_path, clients=['android_vr', 'web_embedded'])
        attempts.append(('yt_alt_clients', opts4))

    last_error = None
    for attempt_name, opts in attempts:
        try:
            _ydl_try(opts, url)
            if find_output_file(output_path):
                return True, None
        except yt_dlp.utils.DownloadError as e:
            last_error = str(e)
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
        cleanup_partial(output_path)
    return False, translate_error(last_error or "Format download failed")


def download_audio(url, output_path):
    platform = classify_url(url)
    attempts = []
    o1 = base_ydl_opts()
    o1.update({
        'format': 'bestaudio/best',
        'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}],
        'outtmpl': output_path,
    })
    attempts.append(('audio_default', o1))
    if platform == 'youtube':
        o2 = base_ydl_opts()
        o2.update({
            'format': 'bestaudio/best',
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}],
            'outtmpl': output_path,
            'extractor_args': {'youtube': {'player_client': ['android_vr', 'web_embedded']}},
        })
        attempts.append(('audio_alt_clients', o2))

    last_error = None
    for name, opts in attempts:
        try:
            _ydl_try(opts, url)
            if find_output_file(output_path + '.mp3') or find_output_file(output_path):
                return True, None
        except Exception as e:
            last_error = str(e)
        cleanup_partial(output_path)
    return False, translate_error(last_error or "Audio download failed")


def download_gallery(url, output_dir):
    """تنزيل الصور باستخدام gallery-dl كعملية خارجية لمنع تداخل المستخدمين."""
    try:
        os.makedirs(output_dir, exist_ok=True)
        cmd = [sys.executable, '-m', 'gallery_dl', '--directory', output_dir, '--sleep-request', '1.0', '--user-agent', USER_AGENT]
        if COOKIES_FILE:
            cmd.extend(['--cookies', COOKIES_FILE])
        cmd.append(url)
        
        subprocess.run(cmd, capture_output=True, timeout=300)

        files = []
        for root, _dirs, names in os.walk(output_dir):
            for n in names:
                p = os.path.join(root, n)
                if os.path.isfile(p) and os.path.getsize(p) > 0:
                    files.append(p)
        files.sort()

        if not files:
            return False, []
        return True, files[:MAX_SLIDESHOW_IMAGES]
    except Exception as e:
        logging.error(f"gallery-dl error: {e}")
        return False, []


# ============================================================
# إرسال الوسائط
# ============================================================
def _mkvid_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🎬 Make Video (images + audio)", callback_data="mkvid"))
    return markup


def send_photo_post(chat_id, assets):
    """إرسال منشور صور باستخدام ExitStack لضمان عدم تسريب الذاكرة."""
    images = [p for p in assets.get('images', []) if os.path.exists(p) and os.path.getsize(p) > 0]
    audio = assets.get('audio')
    has_audio = bool(audio and os.path.exists(audio) and os.path.getsize(audio) > 0)

    if has_audio:
        try:
            with open(audio, 'rb') as a:
                bot.send_audio(chat_id, a, caption="🎵 Audio",
                               reply_markup=_mkvid_markup() if len(images) >= 1 else None,
                               timeout=180)
        except Exception as e:
            logging.error(f"send audio failed: {e}")

    if len(images) == 1:
        try:
            with open(images[0], 'rb') as f:
                bot.send_photo(chat_id, f, caption="📷 Photo post",
                               reply_markup=_mkvid_markup() if has_audio else None,
                               timeout=120)
        except Exception as e:
            logging.error(f"send_photo failed: {e}")
    elif images:
        for chunk_start in range(0, len(images), 10):
            chunk = images[chunk_start:chunk_start + 10]
            
            if len(chunk) == 1:
                try:
                    with open(chunk[0], 'rb') as f:
                        bot.send_photo(chat_id, f, caption="📷 Photo post", timeout=120)
                except:
                    pass
                continue

            # استخدام ExitStack لفتح الملفات بأمان وإغلاقها تلقائياً
            with ExitStack() as stack:
                media = []
                for i, p in enumerate(chunk):
                    try:
                        f = stack.enter_context(open(p, 'rb'))
                        caption = '📷 Photo post' if (chunk_start == 0 and i == 0) else ''
                        media.append(types.InputMediaPhoto(f, caption=caption))
                    except Exception as e:
                        logging.error(f"read album image failed: {e}")
                
                if len(media) >= 2:
                    try:
                        bot.send_media_group(chat_id, media, timeout=180)
                    except Exception as e:
                        logging.error(f"send_media_group failed: {e}")
                        # الخطة البديلة: إرسال كصور متتالية
                        for m in media:
                            try:
                                m.media.seek(0)
                                bot.send_photo(chat_id, m.media, timeout=120)
                            except:
                                pass


def send_bundle(chat_id, user_id, url, bundle: MediaBundle, workdir: str):
    sent = False
    for vp in bundle.videos[:3]:
        if not os.path.exists(vp): continue
        size_mb = os.path.getsize(vp) / (1024 * 1024)
        if size_mb > 50:
            bot.send_message(chat_id, f"Video size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).")
            continue
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("More", callback_data="more"))
        try:
            with open(vp, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True, caption=bundle.title[:1000] if bundle.title else None)
            sent = True
        except Exception:
            try:
                with open(vp, 'rb') as v:
                    bot.send_document(chat_id, v, timeout=180)
                sent = True
            except Exception:
                pass

    if bundle.images:
        assets = {'images': bundle.images, 'videos': [], 'audio': bundle.audio, 'dir': workdir, 'duration': bundle.duration}
        _store_user_pending(user_id, url, assets=assets)
        send_photo_post(chat_id, assets)
        sent = True
    elif bundle.audio and not bundle.videos:
        try:
            with open(bundle.audio, 'rb') as a:
                bot.send_audio(chat_id, a, timeout=180)
            sent = True
        except Exception:
            pass
    return sent


def send_gallery(chat_id, files):
    if not files:
        bot.send_message(chat_id, "No images found.")
        return
    image_exts = ('.jpg', '.jpeg', '.png', '.webp', '.gif')
    video_exts = ('.mp4', '.webm', '.mov', '.mkv')
    images = [f for f in files if f.lower().endswith(image_exts)]
    videos = [f for f in files if f.lower().endswith(video_exts)]

    sent_count = 0
    for f in images[:MAX_SLIDESHOW_IMAGES]:
        try:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            with open(f, 'rb') as img:
                if size_mb > 10: bot.send_document(chat_id, img, timeout=120)
                else: bot.send_photo(chat_id, img, timeout=120)
            sent_count += 1
        except Exception:
            pass

    for f in videos[:3]:
        try:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            if size_mb > 50: continue
            with open(f, 'rb') as v:
                bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
        except Exception:
            pass

    if sent_count == 0 and not videos:
        bot.send_message(chat_id, "No viewable media found.")


def send_more_options(chat_id, user_id, url):
    formats = _get_user_formats(user_id)
    if not formats:
        formats = get_available_formats(url)
        if formats:
            _set_user_formats(user_id, formats)

    if not formats:
        bot.send_message(chat_id, "No format information available.")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        label = f"{f['height']}p"
        if f['size_mb']: label += f" - {f['size_mb']:.0f}MB"
        buttons.append(types.InlineKeyboardButton(label, callback_data=f"q|{f['id']}|{f['height']}|{f['height']}p"))
    buttons.append(types.InlineKeyboardButton("Audio Only (MP3)", callback_data="a"))
    markup.add(*buttons)
    bot.send_message(chat_id, "AVAILABLE OPTIONS", reply_markup=markup)


# ============================================================
# process_download
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']
    platform = classify_url(url)

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"
    ss_dir = f"/tmp/ss_{user_id}_{uuid.uuid4().hex[:8]}"

    final = None

    try:
        if platform == 'twitter':
            bundle = try_fxtwitter(url, ss_dir)
            if bundle:
                _store_user_pending(user_id, url)
                if send_bundle(chat_id, user_id, url, bundle, ss_dir):
                    threading.Thread(target=_background_fetch_formats, args=(user_id, url), daemon=True).start()
                    return

        ok, err = download_video(url, temp, platform)

        if ok:
            final = find_output_file(temp)
            if not final or os.path.getsize(final) == 0:
                bot.send_message(chat_id, "Downloaded file is empty or not created.")
                return

            size_mb = os.path.getsize(final) / (1024 * 1024)
            if size_mb > 50:
                bot.send_message(chat_id, f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).\nUse the options below to download a lower quality.")
                _store_user_pending(user_id, url)
                send_more_options(chat_id, user_id, url)
                return

            _store_user_pending(user_id, url)
            _clear_user_assets(user_id)

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data="more"))

            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True)

            threading.Thread(target=_background_fetch_formats, args=(user_id, url), daemon=True).start()
            return

        if platform == 'tiktok':
            bundle = try_tikwm(url, ss_dir)
            if bundle:
                _store_user_pending(user_id, url)
                if send_bundle(chat_id, user_id, url, bundle, ss_dir):
                    threading.Thread(target=_background_fetch_formats, args=(user_id, url), daemon=True).start()
                    return

        data, ss_err = extract_slideshow(url)
        if data:
            assets, a_err = download_slideshow_assets(url, data, ss_dir)
            if assets:
                _store_user_pending(user_id, url, assets=assets)
                for cv in assets.get('videos', []):
                    try:
                        if os.path.getsize(cv) / (1024 * 1024) <= 50:
                            with open(cv, 'rb') as v:
                                bot.send_video(chat_id, v, timeout=180, supports_streaming=True)
                    except Exception:
                        pass
                send_photo_post(chat_id, assets)
                return

        ok2, files = download_gallery(url, temp_dir)
        if ok2 and files:
            _store_user_pending(user_id, url)
            send_gallery(chat_id, files)
            return

        hint = "\n\nTip: Instagram often blocks server IPs. Add cookies (YTDLP_COOKIES)." if platform == 'instagram' and not COOKIES_FILE else ""
        bot.send_message(chat_id, f"Download failed.\n\n{err}{hint}")

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, f"Technical error: {type(e).__name__}")
        except Exception:
            pass
    finally:
        cleanup_file(temp)
        if final and final != temp:
            cleanup_file(final)
        cleanup_dir(temp_dir)


# ============================================================
# Handlers
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
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
        "<b>HOW TO USE</b>\n"
        "Send any supported media URL and the service will handle the rest."
    )
    PHOTO_FILE_ID = "AgACAgQAAxkBAAM7ar1x_-DZJG9VDd5NRPy2xPtsreoAAs4SaxsCGulR6CNW3Nej7kwBAAMCAAN5AAM9BA"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("⚙ Settings", callback_data="dev"), types.InlineKeyboardButton("❓ Help", callback_data="dev"))
    markup.add(types.InlineKeyboardButton("ℹ About", callback_data="dev"), types.InlineKeyboardButton("📊 Status", callback_data="dev"))
    try:
        bot.send_photo(message.chat.id, PHOTO_FILE_ID, caption=text, reply_markup=markup)
    except Exception:
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


@bot.callback_query_handler(func=lambda call: call.data == 'mkvid')
def handle_mkvid(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id, "Making video...")

    try:
        data = _get_pending(user_id)
        if not data or not data.get('assets'):
            bot.send_message(chat_id, "Session expired. Please send the link again.")
            return

        assets = data['assets']
        url = data['url']
        images = [p for p in assets.get('images', []) if os.path.exists(p)]
        audio = assets.get('audio')
        audio = audio if audio and os.path.exists(audio) else None

        if not images or not audio:
            platform = classify_url(url)
            new_assets = None
            if platform == 'tiktok':
                bundle = try_tikwm(url, assets.get('dir') or f"/tmp/ss_{user_id}_{uuid.uuid4().hex[:8]}")
                if bundle and bundle.images:
                    new_assets = {'images': bundle.images, 'videos': [], 'audio': bundle.audio, 'dir': assets.get('dir'), 'duration': bundle.duration}
            if not new_assets:
                ss_data, ss_err = extract_slideshow(url)
                if not ss_data:
                    bot.send_message(chat_id, f"Couldn't fetch the post again.\n\n{translate_error(ss_err)}")
                    return
                new_assets, a_err = download_slideshow_assets(url, ss_data, assets.get('dir') or f"/tmp/ss_{user_id}_{uuid.uuid4().hex[:8]}")
                if not new_assets:
                    bot.send_message(chat_id, f"Couldn't re-download the media.\n\n{a_err or 'unknown'}")
                    return
            assets = new_assets
            _store_user_pending(user_id, url, assets=assets)
            images = assets['images']
            audio = assets.get('audio')

        if not audio:
            bot.send_message(chat_id, "This post has no audio to merge into a video.")
            return

        out_path = os.path.join(assets['dir'], 'merged.mp4')
        if not merge_slideshow(images, audio, out_path, total_duration=assets.get('duration')):
            bot.send_message(chat_id, "Video processing failed. Please try again.")
            return

        size_mb = os.path.getsize(out_path) / (1024 * 1024)
        if size_mb > 50:
            bot.send_message(chat_id, f"Merged video ({format_size(size_mb)}) exceeds Telegram's 50 MB limit.")
            return

        with open(out_path, 'rb') as v:
            bot.send_video(chat_id, v, timeout=180, supports_streaming=True)

    except Exception as e:
        logging.error(f"handle_mkvid error: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, f"Technical error: {type(e).__name__}")
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith('q|'))
def handle_quality(call):
    try:
        parts = call.data.split('|', 3)
        if len(parts) < 4:
            bot.answer_callback_query(call.id, "Invalid data.")
            return
        fmt_id, height = parts[1], parts[2]
        user_id, chat_id = call.from_user.id, call.message.chat.id
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
                bot.send_message(chat_id, f"File size ({format_size(size_mb)}) exceeds Telegram limit (50 MB).")
                return

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("More", callback_data="more"))
            with open(final, 'rb') as v:
                bot.send_video(chat_id, v, reply_markup=markup, timeout=180, supports_streaming=True)
        finally:
            cleanup_file(temp)
            if final and final != temp:
                cleanup_file(final)
    except Exception as e:
        logging.error(f"handle_quality error: {e}")


@bot.callback_query_handler(func=lambda call: call.data == 'a')
def handle_audio(call):
    try:
        user_id, chat_id = call.from_user.id, call.message.chat.id
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
        logging.error(f"handle_audio error: {e}")


@bot.message_handler(func=lambda m: True)
def handle(message):
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
        task = {'chat_id': message.chat.id, 'user_id': message.from_user.id, 'url': url, 'msg_id': msg.message_id}
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
    except Exception:
        pass
    bot.infinity_polling(timeout=30, long_polling_timeout=30)


NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
for i in range(NUM_WORKERS):
    threading.Thread(target=download_worker, args=(i + 1,), daemon=True).start()

threading.Thread(target=run_bot, daemon=True).start()
threading.Thread(target=_cleanup_loop, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)