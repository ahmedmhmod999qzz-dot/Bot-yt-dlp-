import os
import re
import logging
import subprocess
import threading
import uuid
from flask import Flask, request, jsonify
import telebot
from telebot.types import Update

# --- الإعدادات ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- دوال مساعدة ---
def get_video_info(url):
    """جلب معلومات الفيديو (العنوان، المدة) باستخدام yt-dlp --dump-json."""
    command = ["yt-dlp", "--dump-json", "--no-playlist", url]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL)
        if result.returncode == 0:
            import json
            info = json.loads(result.stdout)
            return {
                'title': info.get('title', 'فيديو'),
                'duration': info.get('duration', 0),
                'uploader': info.get('uploader', 'غير معروف')
            }
    except Exception as e:
        logging.error(f"Error getting video info: {e}")
    return None

def download_video(url, output_path):
    """
    تنزيل الفيديو بأفضل جودة مع دمج الصوت والفيديو.
    يستخدم تنسيقات متعددة لضمان التوافق.
    """
    # خيارات yt-dlp القوية:
    # -f: أفضل فيديو (bv*) + أفضل صوت (ba)، أو أفضل ملف مدمج (b)
    # --merge-output-format: دمج المخرجات في ملف mp4
    # --no-playlist: تجاهل قوائم التشغيل لتنزيل فيديو واحد فقط
    # --retries: إعادة المحاولة عند فشل الشبكة
    # -N: تنزيل أجزاء الفيديو بشكل متزامن لتسريع العملية
    command = [
        "yt-dlp",
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        "--no-playlist",
        "--retries", "10",
        "--fragment-retries", "10",
        "-N", "4",  # تنزيل 4 أجزاء بشكل متزامن (تسريع كبير)
        "-o", output_path,
        url
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=600,  # 10 دقائق كحد أقصى
            stdin=subprocess.DEVNULL
        )
        if result.returncode != 0:
            error_msg = result.stderr[:500] if result.stderr else "خطأ غير معروف"
            logging.error(f"yt-dlp error: {error_msg}")
            return False, error_msg
        return True, None
    except subprocess.TimeoutExpired:
        return False, "انتهت مهلة التنزيل (10 دقائق). الفيديو طويل جداً."
    except Exception as e:
        return False, str(e)

def cleanup_file(path):
    """حذف ملف مؤقت بأمان."""
    if path and os.path.exists(path):
        try:
            os.remove(path)
            logging.info(f"Cleaned up: {path}")
        except Exception as e:
            logging.warning(f"Could not delete {path}: {e}")

# --- معالجات أوامر البوت ---
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = (
        "👋 <b>مرحباً بك في بوت التنزيل القوي!</b>\n\n"
        "أرسل لي رابط فيديو من أي موقع يدعمه yt-dlp، وسأقوم بتنزيله لك بأفضل جودة ممكنة.\n\n"
        "⚠️ <b>ملاحظة مهمة:</b> الحد الأقصى لحجم الفيديو الذي يمكن إرساله عبر Telegram هو <b>50 ميجابايت</b>.\n"
        "الفيديوهات الأكبر من ذلك سيتم إعلامك بها."
    )
    bot.reply_to(message, welcome_text)

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    url = message.text.strip()
    
    # تحقق بسيط من صحة الرابط
    if not re.match(r'https?://', url):
        bot.reply_to(message, "❌ الرجاء إرسال رابط صحيح يبدأ بـ http:// أو https://")
        return
    
    # إرسال رسالة "جاري المعالجة"
    msg = bot.reply_to(message, "⏳ جاري تحليل الرابط...")
    
    # جلب معلومات الفيديو أولاً
    info = get_video_info(url)
    if info:
        title = info['title']
        duration = info['duration']
        uploader = info['uploader']
        duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "غير معروف"
        bot.edit_message_text(
            f"📹 <b>{title}</b>\n"
            f"👤 {uploader}\n"
            f"⏱️ المدة: {duration_str}\n\n"
            f"⏳ جاري التنزيل بأفضل جودة...",
            chat_id=message.chat.id,
            message_id=msg.message_id
        )
    else:
        bot.edit_message_text("⏳ جاري التنزيل...", chat_id=message.chat.id, message_id=msg.message_id)
    
    # مسار مؤقت فريد لكل تنزيل
    temp_video = f"/tmp/video_{message.chat.id}_{uuid.uuid4().hex[:8]}.mp4"
    
    try:
        # 1. تنزيل الفيديو
        success, error = download_video(url, temp_video)
        if not success:
            bot.edit_message_text(
                f"❌ <b>فشل التنزيل:</b>\n<code>{error}</code>",
                chat_id=message.chat.id,
                message_id=msg.message_id
            )
            return
        
        # 2. التحقق من وجود الملف
        if not os.path.exists(temp_video):
            bot.edit_message_text("❌ لم يتم العثور على الملف بعد التنزيل.", chat_id=message.chat.id, message_id=msg.message_id)
            return
        
        # 3. التحقق من حجم الملف
        file_size = os.path.getsize(temp_video)
        file_size_mb = file_size / (1024 * 1024)
        
        # 4. إرسال الفيديو (أو التعامل مع الحجم الكبير)
        if file_size_mb <= 50:
            bot.edit_message_text(
                f"📤 جاري رفع الفيديو ({file_size_mb:.1f} ميجابايت)...",
                chat_id=message.chat.id,
                message_id=msg.message_id
            )
            with open(temp_video, 'rb') as video:
                bot.send_video(
                    message.chat.id,
                    video,
                    caption=f"✅ تم التنزيل بنجاح ({file_size_mb:.1f} ميجابايت)",
                    timeout=180,
                    supports_streaming=True
                )
            bot.delete_message(message.chat.id, msg.message_id)
        else:
            # الفيديو أكبر من 50 ميجابايت
            bot.edit_message_text(
                f"⚠️ <b>الفيديو كبير جداً!</b>\n"
                f"الحجم: {file_size_mb:.1f} ميجابايت (الحد الأقصى: 50 ميجابايت).\n\n"
                f"💡 <b>الحلول المقترحة:</b>\n"
                f"1. جرب رابطاً لجودة أقل (إذا كان متاحاً).\n"
                f"2. استخدم خدمة رفع خارجية (سيتم إضافتها في تحديثات قادمة).",
                chat_id=message.chat.id,
                message_id=msg.message_id
            )
    
    except Exception as e:
        logging.error(f"Error handling message: {e}", exc_info=True)
        try:
            bot.edit_message_text(
                f"❌ <b>حدث خطأ غير متوقع:</b>\n<code>{str(e)[:200]}</code>",
                chat_id=message.chat.id,
                message_id=msg.message_id
            )
        except:
            pass
    finally:
        # تنظيف الملفات المؤقتة دائماً
        cleanup_file(temp_video)

# --- نقاط نهاية Flask (Webhook) ---
@app.route(f'/{BOT_TOKEN}', methods=['POST'])
def webhook():
    """استقبال التحديثات من Telegram."""
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    else:
        return '', 403

@app.route('/health', methods=['GET'])
def health():
    """نقطة نهاية للتحقق من صحة الخدمة (تُستخدم بواسطة cron-job للحفاظ على التشغيل)."""
    return jsonify({"status": "ok", "service": "video-downloader-bot"}), 200

@app.route('/', methods=['GET'])
def index():
    """الصفحة الرئيسية."""
    return "🤖 Bot is running!", 200

def set_webhook():
    """تعيين Webhook مع Telegram عند بدء التشغيل."""
    if RENDER_URL:
        webhook_url = f"{RENDER_URL}/{BOT_TOKEN}"
        try:
            bot.remove_webhook()
            bot.set_webhook(url=webhook_url)
            logging.info(f"Webhook set to: {webhook_url}")
        except Exception as e:
            logging.error(f"Failed to set webhook: {e}")
    else:
        logging.warning("RENDER_EXTERNAL_URL not set. Webhook not configured.")

# --- التشغيل ---
if __name__ == '__main__':
    # تعيين Webhook في خيط منفصل لتجنب تعليق بدء التشغيل
    threading.Thread(target=set_webhook, daemon=True).start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
