import os
import re
import logging
import subprocess
import threading
import uuid
from flask import Flask, jsonify
import telebot

# --- الإعدادات ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required!")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def download_video(url, output_path):
    command = [
        "yt-dlp", "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        "--no-playlist", "--retries", "10", "-N", "4",
        "-o", output_path, url
    ]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True,
            timeout=600, stdin=subprocess.DEVNULL
        )
        if result.returncode != 0:
            return False, (result.stderr or "Unknown error")[:500]
        return True, None
    except subprocess.TimeoutExpired:
        return False, "انتهت المهلة"
    except Exception as e:
        return False, str(e)


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

        if not os.path.exists(temp):
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


# ✅ تشغيل البوت فوراً عند استيراد الملف (مهم لـ gunicorn)
logging.info("=== Bot thread starting (module level) ===")
_bot_thread = threading.Thread(target=run_bot, daemon=True)
_bot_thread.start()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
