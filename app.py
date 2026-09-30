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

# مسار ffmpeg المرفق مع imageio-ffmpeg
FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()
logging.info(f"FFmpeg path: {FFMPEG_PATH}")


def download_video(url, output_path):
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
        'quiet': True,
        'no_warnings': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True, None
    except Exception as e:
        return False, str(e)[:500]


@bot.message_handler(commands=['start', 'help'])
def welcome(message):
    logging.info(f"Got /start from {message.chat.id}")
    bot.reply_to(message, "👋 أرسل لي رابط فيديو وسأنزّله لك.\n⚠️ الحد الأقصى: 50MB")


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

        # البحث عن الملف الناتج (yt-dlp قد يضيف امتداداً مختلفاً)
        if not os.path.exists(temp):
            # ابحث عن أي ملف يبدأ بنفس الاسم
            base = temp.rsplit('.', 1)[0]
            found = None
            for f in os.listdir('/tmp'):
                if f.startswith(os.path.basename(base)):
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
                           caption=f"✅ تم ({size_mb:.1f}MB)")
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
