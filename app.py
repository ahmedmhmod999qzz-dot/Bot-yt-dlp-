import os
import re
import logging
import threading
import uuid
import yt_dlp
import imageio_ffmpeg
from flask import Flask, jsonify
import telebot

# --- الإعدادات ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required!")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()
logging.info(f"FFmpeg path: {FFMPEG_PATH}")


def download_video(url, output_path):
    """
    تنزيل الفيديو بأعلى جودة.
    يستخدم تقليد المتصفح (impersonate) لتجاوز حماية المواقع.
    """
    ydl_opts = {
        # التنسيق: أفضل فيديو + أفضل صوت، أو أفضل ملف مدمج
        'format': 'bv*+ba/b',
        'merge_output_format': 'mp4',
        'outtmpl': output_path,
        'noplaylist': True,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 4,
        'ffmpeg_location': FFMPEG_PATH,
        # ✅ المفتاح الأساسي: تقليد متصفح Chrome لتجاوز حماية TikTok
        'impersonate': 'chrome-131',
        # ✅ خيارات إضافية لتجاوز الحظر
        'extractor_args': {
            'tiktok': {
                'api_hostname': ['api22-normal-c-useast2a.tiktokv.com'],
                'app_info': ['7355728856979392262'],
            }
        },
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.tiktok.com/',
        },
        # تجاهل الأخطاء البسيطة
        'ignoreerrors': False,
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except yt_dlp.utils.DownloadError as e:
        # تحويل رسائل الخطأ لتكون مفهومة للمستخدم
        err_str = str(e)
        if 'Private video' in err_str or 'private' in err_str.lower():
            return False, "الفيديو خاص ولا يمكن تنزيله."
        if 'not available' in err_str or 'region' in err_str.lower():
            return False, "الفيديو غير متاح في منطقتك أو تم حذفه."
        if 'Login required' in err_str or 'login' in err_str.lower():
            return False, "هذا المحتوى يتطلب تسجيل دخول."
        if 'Unsupported URL' in err_str:
            return False, "الرابط غير مدعوم."
        return False, err_str[:300]
    except Exception as e:
        return False, str(e)[:300]


@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    bot.reply_to(message, 
        "👋 <b>مرحباً بك!</b>\n\n"
        "أرسل لي رابط فيديو من يوتيوب، تيك توك، إنستغرام، أو أي منصة مدعومة.\n\n"
        "⚠️ الحد الأقصى لحجم الفيديو: 50MB")


@bot.message_handler(func=lambda m: True)
def handle(message):
    logging.info(f"Got message: {message.text}")
    url = message.text.strip()
    
    if not re.match(r'https?://', url):
        bot.reply_to(message, "❌ أرسل رابطاً صحيحاً.")
        return

    msg = bot.reply_to(message, "⏳ جاري التنزيل...")
    temp = f"/tmp/v_{message.chat.id}_{uuid.uuid4().hex[:8]}.mp4"

    try:
        ok, err = download_video(url, temp)
        if not ok:
            bot.edit_message_text(f"❌ فشل:\n<code>{err}</code>",
                                  message.chat.id, msg.message_id)
            return

        # البحث عن الملف الناتج (yt-dlp قد يغيّر الامتداد)
        if not os.path.exists(temp):
            base_name = os.path.basename(temp).rsplit('.', 1)[0]
            found = None
            for f in os.listdir('/tmp'):
                if f.startswith(base_name):
                    found = f"/tmp/{f}"
                    break
            if found:
                temp = found
            else:
                bot.edit_message_text("❌ لم يتم إنشاء الملف.",
                                      message.chat.id, msg.message_id)
                return

        size_mb = os.path.getsize(temp) / (1024 * 1024)
        if size_mb > 50:
            bot.edit_message_text(
                f"⚠️ الحجم {size_mb:.1f}MB أكبر من 50MB.",
                message.chat.id, msg.message_id)
            return

        bot.edit_message_text(f"📤 جاري الرفع ({size_mb:.1f}MB)...",
                              message.chat.id, msg.message_id)
        with open(temp, 'rb') as v:
            bot.send_video(message.chat.id, v, timeout=180,
                           caption=f"✅ تم التنزيل ({size_mb:.1f}MB)",
                           supports_streaming=True)
        bot.delete_message(message.chat.id, msg.message_id)

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"❌ خطأ: {str(e)[:200]}",
                                  message.chat.id, msg.message_id)
        except:
            pass
    finally:
        if os.path.exists(temp):
            try:
                os.remove(temp)
            except:
                pass


@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200


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


logging.info("=== Bot thread starting ===")
threading.Thread(target=run_bot, daemon=True).start()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
