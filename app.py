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

socket.setdefaulttimeout(30)

MAX_SLIDESHOW_IMAGES = int(os.environ.get("MAX_SLIDESHOW_IMAGES", "15"))


# ============================================================
# Cookies (اختياري — لتجاوز قيود "المحتوى الحساس" في TikTok/Instagram/YouTube)
# ضع محتوى ملف الكوكيز (Netscape format) في متغير البيئة YTDLP_COOKIES
# (نص عادي أو base64)
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
# Rate limiting (محمي بقفل)
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
    if stale:
        logging.info(f"Cleaned {len(stale)} stale rate-limit entries")


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
user_pending = {}  # {user_id: {'url', 'ts', 'formats', 'assets': {'images': [...], 'audio': path, 'dir': dir}}}
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
            logging.info(f"Background formats cached for user {user_id}: {len(formats)} formats")
    except Exception as e:
        logging.warning(f"background fetch formats failed: {e}")


def download_worker(worker_id: int):
    logging.info(f"Worker #{worker_id} started")
    while True:
        try:
            task = download_queue.get(timeout=5)
            if task is None:
                break
            logging.info(f"Worker #{worker_id} processing: {task.get('url', '')[:80]}")
            try:
                process_download(task)
            except Exception as e:
                logging.error(f"Worker #{worker_id} task error: {e}", exc_info=True)
                try:
                    bot.send_message(task['chat_id'],
                                     f"Technical error while processing: {type(e).__name__}")
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
    """حذف ملفات /tmp القديمة (بادئات البوت) — قرص Render المجاني صغير."""
    try:
        now = time.time()
        removed = 0
        for name in os.listdir('/tmp'):
            if not re.match(r'^(v|g|q|a|ss)_\d+_[0-9a-f]{8}', name):
                continue
            path = os.path.join('/tmp', name)
            try:
                if now - os.path.getmtime(path) > max_age:
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                    removed += 1
            except OSError:
                pass
        if removed:
            logging.info(f"/tmp sweep: removed {removed} stale files")
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


# ============================================================
# yt-dlp options and functions
# ============================================================
def base_ydl_opts():
    opts = {
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        },
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
    if COOKIES_FILE:
        opts['cookiefile'] = COOKIES_FILE
    return opts


def translate_error(err: str) -> str:
    err = err or ""
    if 'may not be comfortable' in err or 'Log in for access' in err:
        return ("This post is restricted by the platform (age/sensitive content) and requires "
                "a logged-in account. The bot owner can unlock it by adding cookies "
                "(YTDLP_COOKIES environment variable).")
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
    if 'no video' in err.lower() or 'There is no video' in err or 'No video formats' in err:
        return "No video found in this post (it may be photos only)."
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
                })

            seen = {}
            for f in formats:
                h = f['height']
                if h not in seen or f['tbr'] > seen[h]['tbr']:
                    seen[h] = f

            unique = sorted(seen.values(), key=lambda x: x['height'], reverse=True)
            logging.info(f"Found {len(unique)} unique formats: {[(f['height'], f['size_mb']) for f in unique]}")
            return unique[:8]
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []


# ============================================================
# منشورات الصور: اكتشاف + تحميل الصور والصوت منفصلين
# ============================================================
IMAGE_EXTS = ('jpg', 'jpeg', 'png', 'webp')


def _collect_slideshow_data(info):
    """جمع روابط الصور وأفضل صيغة صوت من معلومات yt-dlp."""
    if not info:
        return None

    entries = list(info.get('entries') or [])
    if not entries:
        entries = [info]

    image_urls = []
    audio_fmt = None
    duration = info.get('duration') or 0

    for entry in entries:
        if not entry:
            continue
        if entry.get('duration') and not duration:
            duration = entry['duration']
        for f in entry.get('formats', []):
            if not f or not f.get('url'):
                continue
            ext = (f.get('ext') or '').lower()
            vcodec = f.get('vcodec')
            acodec = f.get('acodec')
            if ext in IMAGE_EXTS or (vcodec == 'none' and acodec == 'none' and ext not in ('mp4', 'webm', 'm4a', 'mp3', 'aac')):
                if f['url'] not in image_urls:
                    image_urls.append(f['url'])
            elif (acodec and acodec != 'none') and (not vcodec or vcodec == 'none'):
                abr = f.get('abr') or f.get('tbr') or 0
                if audio_fmt is None or abr > (audio_fmt.get('abr') or audio_fmt.get('tbr') or 0):
                    audio_fmt = f
        direct_url = entry.get('url')
        direct_ext = (entry.get('ext') or '').lower()
        if direct_url and direct_ext in IMAGE_EXTS and direct_url not in image_urls:
            image_urls.append(direct_url)

    if not image_urls:
        return None
    return {
        'images': image_urls[:MAX_SLIDESHOW_IMAGES],
        'audio_url': audio_fmt.get('url') if audio_fmt else None,
        'duration': duration,
    }


def extract_slideshow(url: str):
    """فحص المنشور: هل هو صور؟ يرجع (data, None) أو (None, error_message)."""
    errors = []
    for extra in ({}, {'ignore_no_formats_error': True}):
        try:
            opts = base_ydl_opts()
            # استخراج خفيف: محاولات قليلة حتى لا نستهلك رام/وقت عند فشل الفيديو
            opts['retries'] = 2
            opts['fragment_retries'] = 2
            opts.update(extra)
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            data = _collect_slideshow_data(info)
            if data:
                return data, None
            return None, "The post has no downloadable images."
        except Exception as e:
            errors.append(str(e)[:200])
    return None, errors[-1] if errors else "Unknown extraction error"


def fetch_file(media_url: str, dest: str, referer: str = None) -> bool:
    """تحميل ملف مباشر عبر curl_cffi (تمويه متصفح حقيقي)."""
    try:
        from curl_cffi import requests as cffi_requests
        headers = {}
        if referer:
            headers['Referer'] = referer
        for attempt in range(3):
            try:
                with cffi_requests.get(
                    media_url,
                    headers=headers,
                    impersonate='chrome',
                    timeout=40,
                    stream=True,
                ) as r:
                    if r.status_code != 200:
                        logging.warning(f"fetch_file HTTP {r.status_code} for {media_url[:80]}")
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    with open(dest, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=256 * 1024):
                            f.write(chunk)
                    return os.path.getsize(dest) > 0
            except Exception as e:
                logging.warning(f"fetch_file attempt {attempt + 1} failed: {str(e)[:100]}")
                time.sleep(1.5 * (attempt + 1))
        return False
    except ImportError:
        import urllib.request
        try:
            req = urllib.request.Request(media_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                **({'Referer': referer} if referer else {}),
            })
            with urllib.request.urlopen(req, timeout=40) as r, open(dest, 'wb') as f:
                shutil.copyfileobj(r, f, length=256 * 1024)
            return os.path.getsize(dest) > 0
        except Exception as e:
            logging.warning(f"fetch_file (urllib) failed: {str(e)[:100]}")
            return False


_encoders_cache = {}


def _has_encoder(name: str) -> bool:
    if FFMPEG_PATH not in _encoders_cache:
        try:
            r = subprocess.run([FFMPEG_PATH, '-hide_banner', '-encoders'],
                               capture_output=True, text=True, timeout=60)
            _encoders_cache[FFMPEG_PATH] = r.stdout
        except Exception:
            _encoders_cache[FFMPEG_PATH] = ''
    return name in _encoders_cache[FFMPEG_PATH]


def merge_slideshow(image_paths: list, audio_path: str, out_path: str, total_duration=None) -> bool:
    """دمج صور + صوت في فيديو MP4 واحد (ترميع خفيف لـ 512MB RAM)."""
    n = len(image_paths)
    if not n or not audio_path or not os.path.exists(audio_path):
        return False
    if total_duration and total_duration > 0:
        per_image = max(total_duration / n, 1.0)
    else:
        per_image = 3.0

    encoder = 'libx264' if _has_encoder('libx264') else 'mpeg4'

    cmd = [FFMPEG_PATH, '-y']
    for img in image_paths:
        cmd += ['-loop', '1', '-t', f'{per_image:.3f}', '-i', img]
    cmd += ['-i', audio_path]

    parts = []
    for i in range(n):
        parts.append(
            f'[{i}:v]scale=1080:1920:force_original_aspect_ratio=decrease,'
            f'pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,format=yuv420p[v{i}]'
        )
    parts.append(''.join(f'[v{i}]' for i in range(n)) + f'concat=n={n}:v=1:a=0[vout]')
    filter_complex = ';'.join(parts)

    cmd += [
        '-filter_complex', filter_complex,
        '-map', '[vout]', '-map', f'{n}:a',
        '-c:v', encoder, '-preset', 'ultrafast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-shortest', '-movflags', '+faststart',
        out_path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if r.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            logging.error(f"merge_slideshow ffmpeg failed: {r.stderr[-400:]}")
            return False
        return True
    except Exception as e:
        logging.error(f"merge_slideshow error: {e}")
        return False


def download_slideshow_assets(url: str, data: dict, workdir: str):
    """تحميل صور وصوت منشور صور إلى القرص.

    يرجع (assets_dict, None) أو (None, error_message)
    assets_dict = {'images': [paths], 'audio': path|None, 'dir': workdir}
    """
    os.makedirs(workdir, exist_ok=True)

    image_paths = []
    failed = 0
    for idx, img_url in enumerate(data['images']):
        dest = os.path.join(workdir, f'img_{idx:03d}.jpg')
        if fetch_file(img_url, dest, referer=url):
            image_paths.append(dest)
        else:
            failed += 1
            logging.warning(f"slideshow image {idx} download failed")

    if not image_paths:
        return None, "Could not download any of the post images."

    audio_path = None
    if data.get('audio_url'):
        audio_dest = os.path.join(workdir, 'audio.m4a')
        if fetch_file(data['audio_url'], audio_dest, referer=url):
            audio_path = audio_dest

    if not audio_path:
        # احتياطي: الصوت عبر yt-dlp (يتعامل مع الروابط الموقّعة)
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
            audio_path = find_output_file(audio_dest) or find_output_file(audio_dest + '.m4a')
        except Exception as e:
            logging.warning(f"slideshow audio via yt-dlp failed: {str(e)[:120]}")

    return {'images': image_paths, 'audio': audio_path, 'dir': workdir,
            'duration': data.get('duration')}, None


def download_video(url, output_path):
    original_url = url
    is_twitter = any(d in url for d in ['twitter.com', 'x.com'])
    is_tiktok = 'tiktok.com' in url
    is_youtube = 'youtube.com' in url or 'youtu.be' in url

    if is_twitter:
        url = url.replace('twitter.com', 'fxtwitter.com').replace('x.com', 'fxtwitter.com')
        logging.info(f"Twitter URL converted: {url}")

    attempts = []

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

    if is_twitter:
        opts2 = base_ydl_opts()
        opts2.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'extractor_args': {'twitter': {'api': ['legacy']}},
        })
        attempts.append(('twitter_legacy', original_url, opts2))

    if is_twitter:
        opts3 = base_ydl_opts()
        opts3.update({
            'format': 'bv*+ba/b',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
        })
        attempts.append(('twitter_graphql', original_url, opts3))

    if is_twitter:
        opts4 = base_ydl_opts()
        opts4.update({
            'format': 'best',
            'outtmpl': output_path,
        })
        attempts.append(('twitter_best', original_url, opts4))

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

    opts7 = base_ydl_opts()
    opts7.update({
        'format': 'b',
        'outtmpl': output_path,
    })
    attempts.append(('best_merged', original_url, opts7))

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
    original_url = url
    if any(d in url for d in ['twitter.com', 'x.com']):
        url = url.replace('twitter.com', 'fxtwitter.com').replace('x.com', 'fxtwitter.com')

    attempts = []

    opts1 = base_ydl_opts()
    opts1.update({
        'format': f'{format_id}+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    attempts.append(('specific+audio', url, opts1))

    opts2 = base_ydl_opts()
    opts2.update({
        'format': format_id,
        'outtmpl': output_path,
    })
    attempts.append(('specific_only', original_url, opts2))

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


# ============================================================
# gallery-dl (مُصلح: التهيئة الصحيحة عبر config.set)
# ============================================================
_gallery_configured = False


def _configure_gallery_dl(output_dir):
    global _gallery_configured
    from gallery_dl import config as gdconfig
    if not _gallery_configured:
        try:
            gdconfig.set(('extractor',), 'base-directory', output_dir)
            gdconfig.set(('extractor',), 'filename', '{num:03d}_{filename}.{extension}')
            gdconfig.set(('extractor',), 'sleep-request', 1.0)
            gdconfig.set(('extractor',), 'user-agent',
                         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
            gdconfig.set(('downloader',), 'retries', 3)
            gdconfig.set(('downloader',), 'timeout', 30.0)
            _gallery_configured = True
        except Exception as e:
            logging.warning(f"gallery-dl config failed: {e}")


def download_gallery(url, output_dir):
    """تنزيل الصور باستخدام gallery-dl."""
    try:
        from gallery_dl import job
        os.makedirs(output_dir, exist_ok=True)
        _configure_gallery_dl(output_dir)

        j = job.DownloadJob(url)
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


# ============================================================
# إرسال منشورات الصور (صور منفصلة + صوت منفصل)
# ============================================================
def send_photo_post(chat_id, assets):
    """إرسال منشور صور: الصور صوراً والصوت صوتاً. زر دمج فقط لصورة واحدة."""
    images = assets.get('images', [])
    audio = assets.get('audio')
    has_audio = bool(audio and os.path.exists(audio))

    if len(images) == 1:
        markup = None
        if has_audio:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🎬 Make Video", callback_data="mkvid"))
        try:
            with open(images[0], 'rb') as f:
                if markup:
                    bot.send_photo(chat_id, f, caption="📷 Photo post",
                                   reply_markup=markup, timeout=120)
                else:
                    bot.send_photo(chat_id, f, caption="📷 Photo post", timeout=120)
        except Exception as e:
            logging.error(f"send_photo failed: {e}")
            try:
                with open(images[0], 'rb') as f:
                    bot.send_document(chat_id, f, timeout=120)
            except Exception as e2:
                logging.error(f"send_document fallback failed: {e2}")

    else:
        media = []
        for i, p in enumerate(images[:10]):
            try:
                with open(p, 'rb') as f:
                    media.append(types.InputMediaPhoto(
                        f.read(), caption='📷 Photo post' if i == 0 else ''))
            except Exception as e:
                logging.error(f"read album image failed: {e}")
        if media:
            try:
                bot.send_media_group(chat_id, media, timeout=180)
            except Exception as e:
                logging.error(f"send_media_group failed: {e}")
                for p in images[:10]:
                    try:
                        with open(p, 'rb') as f:
                            bot.send_photo(chat_id, f, timeout=120)
                    except Exception:
                        pass
        if len(images) > 10:
            bot.send_message(chat_id, f"Sent 10 of {len(images)} images.")

    if has_audio:
        try:
            with open(audio, 'rb') as a:
                bot.send_audio(chat_id, a, caption="🎵 Audio", timeout=180)
        except Exception as e:
            logging.error(f"send audio failed: {e}")


# ============================================================
# process_download
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"
    ss_dir = f"/tmp/ss_{user_id}_{uuid.uuid4().hex[:8]}"

    final = None

    try:
        # 1) المحاولة الأساسية: فيديو (المسار القديم الذي كان يعمل)
        ok, err = download_video(url, temp)

        if not ok:
            # 2) هل المنشور صور؟ (استخراج خفيف بمحاولات قليلة)
            logging.info("video download failed, checking if this is a photo post...")
            data, ss_err = extract_slideshow(url)

            if data:
                assets, a_err = download_slideshow_assets(url, data, ss_dir)
                if assets:
                    _store_user_pending(user_id, url, assets=assets)
                    send_photo_post(chat_id, assets)
                    return
                else:
                    bot.send_message(
                        chat_id,
                        "This post contains photos, but I couldn't download them.\n\n"
                        f"Reason: {a_err or 'unknown'}"
                    )
                    return

            # 3) ليس منشور صور مؤكد → gallery-dl
            logging.info(f"not a detectable photo post ({ss_err}), trying gallery-dl...")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                send_gallery(chat_id, files)
                return

            # 4) فشل نهائي — أخبر المستخدم بالسبب بوضوح
            bot.send_message(chat_id, f"Download failed.\n\n{err}")
            return

        final = find_output_file(temp)
        if not final:
            bot.send_message(chat_id, "Download seemed to succeed but the file was not created.")
            return

        if os.path.getsize(final) == 0:
            bot.send_message(chat_id, "Downloaded file is empty. The platform may have blocked the request.")
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

        threading.Thread(
            target=_background_fetch_formats,
            args=(user_id, url),
            daemon=True
        ).start()

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
        # ملاحظة: لا نحذف ss_dir هنا — ملفاته محفوظة لزر "Make Video"
        # وتُنظَّف عبر TTL (ساعة) أو كنس /tmp (ساعتان)


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
            size_mb = os.path.getsize(f) / (1024 * 1024)
            with open(f, 'rb') as img:
                if size_mb > 10:
                    bot.send_document(chat_id, img, timeout=120)
                else:
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
        "• Photo posts: images and audio delivered separately\n"
        "• Single-photo posts can be merged into a video with one tap\n"
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


@bot.callback_query_handler(func=lambda call: call.data == 'mkvid')
def handle_mkvid(call):
    """دمج صورة + صوت منشور صور في فيديو (زر Make Video)."""
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
            # الملفات لم تعد موجودة (تنظيف/إعادة تشغيل) — أعد تحميلها
            ss_data, ss_err = extract_slideshow(url)
            if not ss_data:
                bot.send_message(chat_id,
                                 f"Couldn't fetch the post again.\n\n{translate_error(ss_err)}")
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
            bot.send_message(chat_id,
                             f"Merged video ({format_size(size_mb)}) exceeds Telegram's 50 MB limit.")
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
        except Exception:
            pass


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
    return jsonify({
        "status": "ok",
        "queue_size": download_queue.qsize(),
    }), 200


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


# 2 عمال فقط: ترميز/استخراج متعدد على 512MB يسبب OOM مع 3
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
for i in range(NUM_WORKERS):
    threading.Thread(target=download_worker, args=(i + 1,), daemon=True).start()

threading.Thread(target=run_bot, daemon=True).start()
threading.Thread(target=_cleanup_loop, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
