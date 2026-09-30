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
# قائمة النطاقات المسموح بها (الحماية من الروابط الضارة)
# ============================================================
ALLOWED_DOMAINS = {
    'youtube.com', 'youtu.be', 'm.youtube.com',
    'tiktok.com', 'vm.tiktok.com', 'vt.tiktok.com',
    'instagram.com', 'www.instagram.com',
    'twitter.com', 'x.com', 't.co',
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
    'triller.co', 'clout.com',
    'threads.net', 'truthsocial.com',
}

def is_allowed_url(url: str) -> bool:
    """التحقق من أن الرابط ينتمي إلى نطاق مسموح به."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        # تحقق مطابق أو نطاق فرعي
        for allowed in ALLOWED_DOMAINS:
            if domain == allowed or domain.endswith('.' + allowed):
                return True
        return False
    except Exception:
        return False

# ============================================================
# الحد من المعدل (Rate Limiting) - 5 روابط في الدقيقة
# ============================================================
user_requests = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # ثانية
RATE_LIMIT_MAX = 5      # أقصى عدد طلبات

def check_rate_limit(user_id: int) -> tuple[bool, int]:
    """يرجع (مسموح, الثواني المتبقية)."""
    now = time.time()
    user_requests[user_id] = [t for t in user_requests[user_id] if now - t < RATE_LIMIT_WINDOW]
    if len(user_requests[user_id]) >= RATE_LIMIT_MAX:
        wait = int(RATE_LIMIT_WINDOW - (now - user_requests[user_id][0]))
        return False, wait
    user_requests[user_id].append(now)
    return True, 0

# ============================================================
# طابور التنزيلات (Queue) - معالجة متسلسلة
# ============================================================
download_queue = queue.Queue(maxsize=100)  # حد أقصى 100 طلب في الانتظار
active_downloads = {}  # {user_id: {'message_id': ..., 'url': ...}}

def download_worker():
    """عامل واحد لمعالجة التنزيلات بالتسلسل."""
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
# دوال التنزيل
# ============================================================
def download_video(url: str, output_path: str) -> tuple[bool, str]:
    """تنزيل الفيديو بأعلى جودة باستخدام yt-dlp Python API."""
    ydl_opts = {
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        'impersonate': 'chrome-131',
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        },
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'force_ipv4': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        err_str = str(e)
        if 'Private video' in err_str:
            return False, "الفيديو خاص ولا يمكن تنزيله."
        if 'not available' in err_str or 'region' in err_str.lower():
            return False, "الفيديو غير متاح في منطقتك أو تم حذفه."
        if 'Login required' in err_str:
            return False, "هذا المحتوى يتطلب تسجيل دخول."
        if 'Unsupported URL' in err_str:
            return False, "الرابط غير مدعوم."
        if 'HTTP Error 429' in err_str:
            return False, "تم تجاوز حد الطلبات. حاول بعد دقائق."
        if 'Unexpected response' in err_str:
            return False, "الموقع المستهدف رفض الطلب. حاول مجدداً."
        return False, "فشل التنزيل. تحقق من الرابط وحاول مجدداً."
    except Exception:
        return False, "حدث خطأ غير متوقع أثناء التنزيل."

def get_available_formats(url: str) -> list:
    """جلب التنسيقات المتاحة مع الحجم التقريبي."""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'impersonate': 'chrome-131',
        'force_ipv4': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = []
            for f in info.get('formats', []):
                if f.get('vcodec') == 'none':
                    continue  # تجاهل الصوت فقط
                height = f.get('height')
                if not height:
                    continue
                filesize = f.get('filesize') or f.get('filesize_approx') or 0
                size_mb = filesize / (1024 * 1024)
                fmt_id = f.get('format_id', '')
                ext = f.get('ext', 'mp4')
                formats.append({
                    'id': fmt_id,
                    'height': height,
                    'ext': ext,
                    'size_mb': round(size_mb, 1),
                    'label': f"{height}p ({size_mb:.0f}MB)" if size_mb > 0 else f"{height}p",
                })
            # إزالة التكرار حسب الدقة
            seen = set()
            unique = []
            for f in sorted(formats, key=lambda x: x['height'], reverse=True):
                if f['height'] not in seen:
                    seen.add(f['height'])
                    unique.append(f)
            return unique[:5]  # أعلى 5 جودات
    except Exception as e:
        logging.error(f"get_available_formats error: {e}")
        return []

def download_specific_format(url: str, output_path: str, format_id: str) -> tuple[bool, str]:
    """تنزيل جودة محددة."""
    ydl_opts = {
        'format': f'{format_id}+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'ffmpeg_location': FFMPEG_PATH,
        'impersonate': 'chrome-131',
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'force_ipv4': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except Exception as e:
        return False, str(e)[:200]

def download_audio(url: str, output_path: str) -> tuple[bool, str]:
    """تنزيل الصوت فقط بصيغة MP3."""
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': output_path,
        'noplaylist': True,
        'ffmpeg_location': FFMPEG_PATH,
        'impersonate': 'chrome-131',
        'quiet': True,
        'no_warnings': True,
        'force_ipv4': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except Exception as e:
        return False, str(e)[:200]

def download_gallery(url: str, output_dir: str) -> tuple[bool, list]:
    """تنزيل معرض صور باستخدام gallery-dl."""
    import gallery_dl
    from gallery_dl import job
    try:
        os.makedirs(output_dir, exist_ok=True)
        config = {
            'base-directory': output_dir,
            'filename': '{num:03d}_{filename}.{extension}',
            'quiet': True,
        }
        j = job.DownloadJob(url, config=config)
        j.run()
        files = sorted(os.listdir(output_dir))
        return True, [os.path.join(output_dir, f) for f in files]
    except Exception as e:
        logging.error(f"gallery-dl error: {e}")
        return False, []

def cleanup_file(path: str):
    """حذف ملف مؤقت بأمان."""
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception as e:
            logging.warning(f"Could not delete {path}: {e}")

def cleanup_dir(directory: str):
    """حذف مجلد مؤقت بالكامل."""
    import shutil
    if directory and os.path.exists(directory):
        try:
            shutil.rmtree(directory)
        except Exception as e:
            logging.warning(f"Could not delete {directory}: {e}")

def find_output_file(temp_path: str) -> str | None:
    """البحث عن الملف الناتج بعد التنزيل."""
    if os.path.exists(temp_path):
        return temp_path
    base = os.path.basename(temp_path).rsplit('.', 1)[0]
    for f in os.listdir('/tmp'):
        if f.startswith(base):
            return f"/tmp/{f}"
    return None

# ============================================================
# معالجة الطلب (في الطابور)
# ============================================================
def process_download(task: dict):
    """معالجة تنزيل واحد من الطابور."""
    chat_id = task['chat_id']
    user_id = task['user_id']
    url = task['url']
    msg_id = task['msg_id']

    temp = f"/tmp/v_{user_id}_{uuid.uuid4().hex[:8]}.mp4"
    temp_dir = f"/tmp/g_{user_id}_{uuid.uuid4().hex[:8]}"

    try:
        # محاولة yt-dlp أولاً
        ok, err = download_video(url, temp)
        if not ok:
            # محاولة gallery-dl للصور
            logging.info(f"yt-dlp failed, trying gallery-dl for {url}")
            ok2, files = download_gallery(url, temp_dir)
            if ok2 and files:
                send_gallery(chat_id, user_id, url, files, msg_id)
                return
            bot.edit_message_text(f"فشل التنزيل: {err}", chat_id, msg_id)
            return

        final = find_output_file(temp)
        if not final:
            bot.edit_message_text("لم يتم إنشاء الملف.", chat_id, msg_id)
            return

        size_mb = os.path.getsize(final) / (1024 * 1024)

        # إرسال الفيديو مع أزرار الجودة
        with open(final, 'rb') as v:
            sent = bot.send_video(
                chat_id, v,
                caption=f"تم التنزيل بنجاح.\nالحجم: {size_mb:.1f} MB",
                timeout=180,
                supports_streaming=True,
                reply_markup=None  # سيتم إرسال الأزرار في رسالة منفصلة
            )

        # إرسال أزرار الجودة بعد التنزيل
        send_quality_buttons(chat_id, url, msg_id)

        bot.delete_message(chat_id, msg_id)

    except Exception as e:
        logging.error(f"process_download error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"حدث خطأ: {str(e)[:200]}", chat_id, msg_id)
        except:
            pass
    finally:
        cleanup_file(temp)
        cleanup_dir(temp_dir)

def send_gallery(chat_id: int, user_id: int, url: str, files: list, msg_id: int):
    """إرسال معرض صور."""
    try:
        bot.edit_message_text(f"تم العثور على {len(files)} صورة. جاري الرفع...", chat_id, msg_id)
        for i, f in enumerate(files[:10]):  # حد أقصى 10 صور
            with open(f, 'rb') as img:
                bot.send_photo(chat_id, img, caption=f"صورة {i+1}/{min(len(files),10)}")
        if len(files) > 10:
            bot.send_message(chat_id, f"تم إرسال أول 10 صور من أصل {len(files)}.")
    except Exception as e:
        logging.error(f"send_gallery error: {e}")
        bot.send_message(chat_id, "فشل إرسال الصور.")

def send_quality_buttons(chat_id: int, url: str, msg_id: int):
    """إرسال أزرار اختيار الجودة بعد التنزيل."""
    formats = get_available_formats(url)
    if not formats:
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for f in formats:
        cb_data = f"q|{url}|{f['id']}|{f['label']}"
        if len(cb_data) <= 64:  # حد تيليجرام 64 بايت
            buttons.append(types.InlineKeyboardButton(f['label'], callback_data=cb_data))

    if not buttons:
        return

    # إضافة زر الصوت
    buttons.append(types.InlineKeyboardButton("MP3 (صوت فقط)", callback_data=f"a|{url}"))

    markup.add(*buttons)
    bot.send_message(
        chat_id,
        "اختر جودة أخرى من الخيارات المتاحة:",
        reply_markup=markup
    )

# ============================================================
# معالجات أوامر البوت
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    bot.reply_to(message,
        "<b>بوت التنزيل الاحترافي</b>\n\n"
        "أرسل رابط فيديو من أي منصة مدعومة، وسيتم تنزيله بأعلى جودة متاحة.\n\n"
        "<b>المنصات المدعومة:</b>\n"
        "يوتيوب، تيك توك، إنستغرام، تويتر، فيسبوك، ريديت، وغيرها.\n\n"
        "<b>ملاحظات:</b>\n"
        "- الحد الأقصى لحجم الملف: 50 ميجابايت\n"
        "- يمكنك اختيار جودة أخرى بعد التنزيل\n"
        "- يدعم معارض الصور من إنستغرام وبينتريست\n\n"
        "<b>ملاحظة:</b> الحد الأقصى 5 روابط في الدقيقة لكل مستخدم."
    )

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """معالجة أزرار الجودة والصوت."""
    try:
        data = call.data
        bot.answer_callback_query(call.id, "جاري المعالجة...")

        if data.startswith('q|'):
            # q|url|format_id|label
            parts = data.split('|', 3)
            if len(parts) < 4:
                return
            _, url, fmt_id, label = parts
            chat_id = call.message.chat.id
            user_id = call.from_user.id

            msg = bot.send_message(chat_id, f"جاري تنزيل الجودة {label}...")
            temp = f"/tmp/q_{user_id}_{uuid.uuid4().hex[:8]}.mp4"

            try:
                ok, err = download_specific_format(url, temp, fmt_id)
                if not ok:
                    bot.edit_message_text(f"فشل التنزيل: {err}", chat_id, msg.message_id)
                    return

                final = find_output_file(temp)
                if not final:
                    bot.edit_message_text("لم يتم إنشاء الملف.", chat_id, msg.message_id)
                    return

                size_mb = os.path.getsize(final) / (1024 * 1024)
                if size_mb > 50:
                    bot.edit_message_text(
                        f"الحجم {size_mb:.1f} MB أكبر من حد تيليجرام (50 MB).",
                        chat_id, msg.message_id)
                    return

                with open(final, 'rb') as v:
                    bot.send_video(chat_id, v, caption=f"تم التنزيل ({size_mb:.1f} MB)",
                                   timeout=180, supports_streaming=True)
                bot.delete_message(chat_id, msg.message_id)
            finally:
                cleanup_file(temp)

        elif data.startswith('a|'):
            # a|url
            url = data[2:]
            chat_id = call.message.chat.id
            user_id = call.from_user.id

            msg = bot.send_message(chat_id, "جاري استخراج الصوت...")
            temp = f"/tmp/a_{user_id}_{uuid.uuid4().hex[:8]}"

            try:
                ok, err = download_audio(url, temp)
                if not ok:
                    bot.edit_message_text(f"فشل: {err}", chat_id, msg.message_id)
                    return

                final = find_output_file(temp + ".mp3") or find_output_file(temp)
                if not final:
                    bot.edit_message_text("لم يتم إنشاء الملف.", chat_id, msg.message_id)
                    return

                with open(final, 'rb') as a:
                    bot.send_audio(chat_id, a, caption="تم استخراج الصوت بنجاح.",
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

    # 1. التحقق من الرابط
    if not re.match(r'https?://', url):
        bot.reply_to(message, "الرجاء إرسال رابط صحيح يبدأ بـ http:// أو https://")
        return

    # 2. التحقق من النطاق المسموح
    if not is_allowed_url(url):
        bot.reply_to(message,
            "هذا النطاق غير مدعوم.\n"
            "المنصات المدعومة: يوتيوب، تيك توك، إنستغرام، تويتر، فيسبوك، ريديت، وغيرها.")
        return

    # 3. التحقق من الحد الأقصى للمعدل
    allowed, wait = check_rate_limit(message.from_user.id)
    if not allowed:
        bot.reply_to(message, f"تجاوزت الحد المسموح (5 روابط في الدقيقة).\nحاول بعد {wait} ثانية.")
        return

    # 4. إضافة إلى الطابور
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
# نقاط نهاية Flask
# ============================================================
@app.route('/health')
def health():
    return jsonify({"status": "ok", "queue_size": download_queue.qsize()}), 200

@app.route('/')
def index():
    return "Bot is running!", 200

# ============================================================
# تشغيل البوت
# ============================================================
def run_bot():
    try:
        bot.remove_webhook()
        logging.info("Old webhook removed.")
    except Exception as e:
        logging.warning(f"remove_webhook: {e}")

    logging.info("=== Starting polling ===")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)

# تشغيل عامل التنزيل
threading.Thread(target=download_worker, daemon=True).start()
# تشغيل البوت
threading.Thread(target=run_bot, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
