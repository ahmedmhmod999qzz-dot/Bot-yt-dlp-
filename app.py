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
# الإعدادات
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
# النطاقات المسموح بها
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
# Rate Limiting
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
# الطابور
# ============================================================
download_queue = queue.Queue(maxsize=100)

# تتبع التقدم لكل مستخدم: {user_id: {'msg_id':, 'chat_id':, 'last_update':, 'text':}}
progress_tracker = {}
progress_lock = threading.Lock()

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
# شريط التقدم
# ============================================================
def make_progress_bar(percent: float, length: int = 15) -> str:
    filled = int(length * percent / 100)
    return "━" * filled + "─" * (length - filled)

def format_bytes(b) -> str:
    if not b:
        return "?"
    b = float(b)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}TB"

def progress_hook_factory(user_id: int, chat_id: int, msg_id: int):
    """يرجع دالة progress_hook مخصصة لهذا التنزيل."""
    def hook(d):
        try:
            if d['status'] == 'downloading':
                now = time.time()
                with progress_lock:
                    info = progress_tracker.get(user_id, {})
                    last = info.get('last_update', 0)
                    if now - last < 3:  # تحديث كل 3 ثواني فقط
                        return
                    progress_tracker[user_id] = {'last_update': now}

                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                speed = d.get('speed') or 0
                eta = d.get('eta') or 0
                percent = (downloaded / total * 100) if total > 0 else 0

                bar = make_progress_bar(percent)
                text = (
                    f"جاري التنزيل...\n\n"
                    f"<code>{bar}</code> {percent:.1f}%\n"
                    f"الحجم: {format_bytes(downloaded)} / {format_bytes(total)}\n"
                    f"السرعة: {format_bytes(speed)}/s\n"
                    f"الوقت المتبقي: {eta} ثانية"
                )

                try:
                    bot.edit_message_text(text, chat_id, msg_id)
                except Exception:
                    pass  # تجاهل أخطاء تعديل الرسالة

            elif d['status'] == 'finished':
                try:
                    bot.edit_message_text(
                        "اكتمل التنزيل. جاري معالجة الملف...",
                        chat_id, msg_id
                    )
                except Exception:
                    pass
        except Exception as e:
            logging.error(f"progress_hook error: {e}")
    return hook

# ============================================================
# دوال التنزيل
# ============================================================
def base_ydl_opts():
    """الخيارات المشتركة بين كل التنزيلات."""
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

def download_video(url: str, output_path: str, user_id: int = None,
                   chat_id: int = None, msg_id: int = None) -> tuple[bool, str]:
    """تنزيل الفيديو بأعلى جودة."""
    ydl_opts = base_ydl_opts()
    ydl_opts.update({
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if user_id and chat_id and msg_id:
        ydl_opts['progress_hooks'] = [progress_hook_factory(user_id, chat_id, msg_id)]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        err = str(e)
        logging.error(f"DownloadError: {err}")
        return False, translate_error(err)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        logging.error(f"Download exception: {err}", exc_info=True)
        return False, f"خطأ تقني: {type(e).__name__} - {str(e)[:200]}"

def translate_error(err: str) -> str:
    """ترجمة أخطاء yt-dlp إلى رسائل عربية واضحة."""
    if 'Private video' in err or 'This video is private' in err:
        return "الفيديو خاص ولا يمكن تنزيله."
    if 'not available' in err or 'This video is not available' in err:
        return "الفيديو غير متاح أو تم حذفه."
    if 'region' in err.lower() or 'geo' in err.lower():
        return "الفيديو محجوب جغرافياً في منطقتك."
    if 'Login required' in err or 'Sign in' in err:
        return "هذا المحتوى يتطلب تسجيل دخول."
    if 'Unsupported URL' in err:
        return "الرابط غير مدعوم من yt-dlp."
    if 'HTTP Error 429' in err:
        return "تم تجاوز حد الطلبات. حاول بعد دقائق."
    if 'HTTP Error 403' in err:
        return "الموقع رفض الطلب (403). قد يحتاج الأمر تحديث yt-dlp."
    if 'HTTP Error 404' in err:
        return "الفيديو غير موجود (404)."
    if 'Unexpected response' in err:
        return "الموقع المستهدف رفض الطلب. تأكد من الرابط أو حاول مجدداً."
    if 'Unable to extract' in err:
        return "تعذر استخراج بيانات الفيديو. قد يكون الرابط غير صحيح."
    if 'Video unavailable' in err:
        return "الفيديو غير متاح."
    if 'ffmpeg' in err.lower():
        return "خطأ في معالجة الفيديو (ffmpeg)."
    # إذا لم نتعرف على الخطأ، أرجع نصاً مختصراً منه
    clean = err.replace('ERROR:', '').strip()
    return f"فشل التنزيل: {clean[:200]}"

def get_available_formats(url: str) -> list:
    ydl_opts = base_ydl_opts()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = []
            for f in info.get('formats', []):
                if f.get('vcodec') == 'none':
                    continue
                height = f.get('height')
                if not height:
                    continue
                filesize = f.get('filesize') or f.get('filesize_approx') or 0
                size_mb = filesize / (1024 * 1024)
                fmt_id = f.get('format_id', '')
                formats.append({
                    'id': fmt_id,
                    'height': height,
                    'size_mb': round(size_mb, 1),
                    'label': f"{height}p ({size_mb:.0f}MB)" if size_mb > 0 else f"{height}p",
                })
            seen = set()
            unique = []
            for f in sorted(formats, key=lambda x: x['height'], reverse=True):
                if f['height'] not in seen:
                    seen.add(f['height'])
                    unique.append(f)
            return unique[:5]
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []

def download_specific_format(url, output_path, format_id, user_id=None, chat_id=None, msg_id=None):
    ydl_opts = base_ydl_opts()
    ydl_opts.update({
        'format': f'{format_id}+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
    })
    if user_id and chat_id and msg_id:
        ydl_opts['progress_hooks'] = [progress_hook_factory(user_id, chat_id, msg_id)]
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        return False, translate_error(str(e))
    except Exception as e:
        return False, f"خطأ: {type(e).__name__}"

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
        return False, f"خطأ: {type(e).__name__}"

def download_gallery(url: str, output_dir: str):
    try:
        import gallery_dl
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
# معالجة الطلب
# ============================================================
def process_download(task: dict):
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']
    msg_id = task['msg_id']

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"

    try:
        ok, err = download_video(url, temp, user_id, chat_id, msg_id)
        if not ok:
            logging.info(f"yt-dlp failed ({err}), trying gallery-dl...")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                send_gallery(chat_id, user_id, url, files, msg_id)
                return
            bot.edit_message_text(f"فشل التنزيل.\n\n{err}", chat_id, msg_id)
            return

        final = find_output_file(temp)
        if not final:
            bot.edit_message_text("لم يتم إنشاء الملف.", chat_id, msg_id)
            return

        size_mb = os.path.getsize(final) / (1024 * 1024)

        if size_mb > 50:
            bot.edit_message_text(
                f"حجم الملف {size_mb:.1f} MB أكبر من حد تيليجرام (50 MB).\n"
                f"سيتم عرض خيارات جودة أقل.",
                chat_id, msg_id
            )
            send_quality_buttons(chat_id, url)
            cleanup_file(final)
            return

        with open(final, 'rb') as v:
            bot.send_video(
                chat_id, v,
                caption=f"تم التنزيل بنجاح.\nالحجم: {size_mb:.1f} MB",
                timeout=180,
                supports_streaming=True,
            )

        send_quality_buttons(chat_id, url)
        bot.delete_message(chat_id, msg_id)

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        try:
            bot.edit_message_text(
                f"خطأ تقني: {type(e).__name__}\n{str(e)[:250]}",
                chat_id, msg_id
            )
        except:
            pass
    finally:
        cleanup_file(temp)
        cleanup_dir(temp_dir)

def send_gallery(chat_id, user_id, url, files, msg_id):
    try:
        bot.edit_message_text(f"تم العثور على {len(files)} صورة. جاري الرفع...",
                              chat_id, msg_id)
        for i, f in enumerate(files[:10]):
            with open(f, 'rb') as img:
                bot.send_photo(chat_id, img, caption=f"صورة {i+1}/{min(len(files),10)}")
        if len(files) > 10:
            bot.send_message(chat_id, f"تم إرسال أول 10 صور من أصل {len(files)}.")
        bot.delete_message(chat_id, msg_id)
    except Exception as e:
        logging.error(f"send_gallery error: {e}")
        bot.send_message(chat_id, "فشل إرسال الصور.")

def send_quality_buttons(chat_id, url):
    formats = get_available_formats(url)
    if not formats:
        return
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        cb_data = f"q|{f['id']}|{f['label']}|{url}"
        if len(cb_data) <= 64:
            buttons.append(types.InlineKeyboardButton(f['label'], callback_data=cb_data))
    buttons.append(types.InlineKeyboardButton("MP3 (صوت فقط)", callback_data=f"a|{url}"))
    if not buttons:
        return
    markup.add(*buttons)
    try:
        bot.send_message(
            chat_id,
            "اختر جودة أخرى أو استخرج الصوت:",
            reply_markup=markup
        )
    except Exception as e:
        logging.error(f"send_quality_buttons error: {e}")

# ============================================================
# معالجات الأوامر
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    text = (
        "<b>yt-dlp — أداة تنزيل الوسائط</b>\n\n"
        "نزّل الفيديوهات والصوتيات من مختلف المنصات بسرعة وبالجودة المتاحة، باستخدام رابط مباشر فقط.\n\n"
        "يدعم البوت تنزيل المحتوى من منصات متعددة، بما في ذلك:\n\n"
        "يوتيوب، تيك توك، إنستغرام، فيسبوك، إكس، ريديت، بينتريست، وغيرها.\n\n"
        "<b>المزايا:</b>\n"
        "- تنزيل الفيديو بأعلى جودة متاحة.\n"
        "- استخراج الصوت من الفيديو.\n"
        "- اختيار صيغة وجودة التنزيل.\n"
        "- دعم روابط الفيديوهات والمنشورات والمعارض.\n"
        "- دعم تنزيل الصور من المنصات التي تتيح ذلك.\n"
        "- معالجة الروابط بسرعة وسهولة.\n"
        "- واجهة بسيطة دون خطوات معقدة.\n\n"
        "أرسل رابط المحتوى الآن، وسيتولى <b>yt-dlp</b> تنزيله لك."
    )
    bot.reply_to(message, text)

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    try:
        data = call.data
        bot.answer_callback_query(call.id, "جاري المعالجة...")

        if data.startswith('q|'):
            parts = data.split('|', 3)
            if len(parts) < 4:
                return
            _, fmt_id, label, url = parts
            chat_id = call.message.chat.id
            user_id = call.from_user.id

            msg = bot.send_message(chat_id, f"جاري تنزيل الجودة {label}...")
            temp = f"/tmp/q_{user_id}_{uuid.uuid4().hex[:8]}.mp4"

            try:
                ok, err = download_specific_format(
                    url, temp, fmt_id, user_id, chat_id, msg.message_id
                )
                if not ok:
                    bot.edit_message_text(f"فشل التنزيل.\n\n{err}",
                                          chat_id, msg.message_id)
                    return

                final = find_output_file(temp)
                if not final:
                    bot.edit_message_text("لم يتم إنشاء الملف.",
                                          chat_id, msg.message_id)
                    return

                size_mb = os.path.getsize(final) / (1024 * 1024)
                if size_mb > 50:
                    bot.edit_message_text(
                        f"الحجم {size_mb:.1f} MB أكبر من حد تيليجرام.",
                        chat_id, msg.message_id)
                    return

                with open(final, 'rb') as v:
                    bot.send_video(chat_id, v,
                                   caption=f"تم التنزيل ({size_mb:.1f} MB)",
                                   timeout=180, supports_streaming=True)
                bot.delete_message(chat_id, msg.message_id)
            finally:
                cleanup_file(temp)

        elif data.startswith('a|'):
            url = data[2:]
            chat_id = call.message.chat.id
            user_id = call.from_user.id

            msg = bot.send_message(chat_id, "جاري استخراج الصوت...")
            temp = f"/tmp/a_{user_id}_{uuid.uuid4().hex[:8]}"

            try:
                ok, err = download_audio(url, temp)
                if not ok:
                    bot.edit_message_text(f"فشل.\n\n{err}", chat_id, msg.message_id)
                    return

                final = find_output_file(temp + ".mp3") or find_output_file(temp)
                if not final:
                    bot.edit_message_text("لم يتم إنشاء الملف.",
                                          chat_id, msg.message_id)
                    return

                with open(final, 'rb') as a:
                    bot.send_audio(chat_id, a,
                                   caption="تم استخراج الصوت بنجاح.",
                                   timeout=180)
                bot.delete_message(chat_id, msg.message_id)
            finally:
                cleanup_file(temp + ".mp3")
                cleanup_file(temp)

    except Exception as e:
        logging.error(f"callback error: {e}", exc_info=True)

@bot.message_handler(func=lambda m: True)
def handle(message):
    logging.info(f"Got message: {message.text}")
    url = message.text.strip()

    if not re.match(r'https?://', url):
        bot.reply_to(message, "الرجاء إرسال رابط صحيح يبدأ بـ http:// أو https://")
        return

    if not is_allowed_url(url):
        bot.reply_to(message, "هذا النطاق غير مدعوم من البوت.")
        return

    allowed, wait = check_rate_limit(message.from_user.id)
    if not allowed:
        bot.reply_to(message, f"تجاوزت الحد المسموح (5 روابط في الدقيقة).\nحاول بعد {wait} ثانية.")
        return

    try:
        msg = bot.reply_to(message, "تم استلام الرابط. جاري المعالجة...")
        task = {
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'url': url,
            'msg_id': msg.message_id,
        }
        download_queue.put_nowait(task)
    except queue.Full:
        bot.reply_to(message, "الخادم مشغول حالياً. حاول بعد قليل.")

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
# التشغيل
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
