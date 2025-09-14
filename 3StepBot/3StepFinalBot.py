import asyncio, os, logging, datetime, gspread
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.bot import DefaultBotProperties
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.utils.keyboard import InlineKeyboardBuilder

from google.oauth2.service_account import Credentials

BOT_TOKEN = 'BOT_TOKEN'
dp = Dispatcher()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
CHAT_ID = -1111111111111
THREAD_1_ID = 2
THREAD_2_ID = 3
GSHEET_ID = 'GSHEET_ID'
APPLICATIONS_PATH = r"C:\Users\Администратор\Desktop\bots\3StepBot\Applications"

OWNER_ID = 11111111111

SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(r"C:\Users\Администратор\Desktop\bots\3StepBot\3step_credentials.json",scopes=SCOPES)

gc = gspread.authorize(creds)
sh = gc.open_by_key(GSHEET_ID)
worksheet1 = sh.get_worksheet(0)
worksheet2 = sh.get_worksheet(1)

logging.basicConfig(level=logging.INFO)

pending_applications = {}

def get_media_files(cell: str):
    if not cell: return [], []
    files = [f.strip() for f in cell.splitlines() if f.strip()]
    photos, videos = [], []
    for f in files:
        ext = f.lower().split('.')[-1]
        if ext in ("jpg", "jpeg", "png", "bmp", "webp"): photos.append(f)
        elif ext in ("mp4", "mov", "mkv", "avi"): videos.append(f)
    return photos, videos

def is_application_row(row): return len(row) > 0 and row[0].strip().lower() == "выполнено"

def compose_message_ws1(row):
    return f"""Заявка {row[1]} от {row[2]}

ФИО водителя: {row[3]}
Предыдущий парк: {row[4]}
Текущий парк: {row[5]}
Общий стаж: {row[6]}
Текущий стаж: {row[7]}
Общий долг: {row[8]}
Ранние акции: {row[9]}
Желаемый класс авто: {row[10]}
Желаемая арендная ставка: {row[11]}
Фото ВУ: Приложено выше"""

def compose_message_ws2(row): 
    return f"""Заявка {row[1]} от {row[2]}

ФИО водителя: {row[3]}
Номер ТС: {row[4]}
Ссылка на рассрочку: {row[5]}
Описание ситуации: {row[6]}
Изначальная сумма рассрочки: {row[7]}
Желаемая сумма скидки: {row[8]}
Примечание для спец ситуаций: {row[9]}
Фото/видео повреждений: Приложены выше"""

def build_full_message(sheet_idx, row):
    if sheet_idx == 0:
        base_msg = compose_message_ws1(row)
        comment_col = 15
    else:
        base_msg = compose_message_ws2(row)
        comment_col = 13
    if len(row) > comment_col and row[comment_col].strip(): base_msg += f"\n\nКомментарий от ОККР: {row[comment_col].strip()}"
    return base_msg

def review_keyboard(app_key):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да", callback_data=f"approve_{app_key}")
    builder.button(text="❌ Нет", callback_data=f"reject_{app_key}")
    return builder.as_markup()

def result_keyboard(text="Одобрено"):
    builder = InlineKeyboardBuilder()
    builder.button(text=text, callback_data="noop", disabled=True)
    return builder.as_markup()

async def send_review_application(sheet_idx, idx, row, media_files, app_key):
    if sheet_idx == 0:
        thread_id = THREAD_1_ID
        msg_text = build_full_message(sheet_idx, row)
    elif sheet_idx == 1:
        thread_id = THREAD_2_ID
        msg_text = build_full_message(sheet_idx, row)
    else: return
    photos, videos = media_files
    media = []
    failed_files = []
    for i, file in enumerate(photos):
        path = os.path.join(APPLICATIONS_PATH, file)
        if os.path.exists(path):
            if len(media) == 0: media.append(InputMediaPhoto(media=FSInputFile(path), caption=msg_text))
            else: media.append(InputMediaPhoto(media=FSInputFile(path)))
        else: failed_files.append(file)
    for i, file in enumerate(videos):
        path = os.path.join(APPLICATIONS_PATH, file)
        if os.path.exists(path):
            if len(media) == 0: media.append(InputMediaVideo(media=FSInputFile(path), caption=msg_text))
            else: media.append(InputMediaVideo(media=FSInputFile(path)))
        else: failed_files.append(file)
    if len(media) > 0:
        messages = await bot.send_media_group(
            chat_id=CHAT_ID,
            message_thread_id=thread_id,
            media=media[:10])
        app_msg = messages[0]
    else:
        app_msg = await bot.send_message(
            chat_id=CHAT_ID,
            message_thread_id=thread_id,
            text=msg_text)
    followup_text = "Сотрудник ОККР выполнил операцию, вы согласны с этим решением?"
    follow_msg = await bot.send_message(
        chat_id=CHAT_ID,
        message_thread_id=thread_id,
        text=followup_text,
        reply_markup=review_keyboard(app_key)
    )
    pending_applications[app_key] = {
        "sheet_idx": sheet_idx,
        "row_idx": idx + 1,
        "msg_id": app_msg.message_id,
        "follow_msg_id": follow_msg.message_id,
        "thread_id": thread_id,
        "created_at": datetime.datetime.now(),
    }
                         
async def process_new_applications():
    wsheets = [(worksheet1, 0), (worksheet2, 1)]
    for ws, sheet_idx in wsheets:
        rows = ws.get_all_values()
        for idx, row in enumerate(rows):
            if not is_application_row(row): continue
            app_key = f"{sheet_idx}_{idx}"
            if app_key not in pending_applications:
                media_files = ([], [])
                if sheet_idx == 0 and len(row) >= 13: media_files = get_media_files(row[12])
                elif sheet_idx == 1 and len(row) >= 11: media_files = get_media_files(row[10])
                await send_review_application(sheet_idx, idx, row, media_files, app_key)

async def application_polling_task():
    global worksheet1, worksheet2
    while True:
        try:
            worksheet1 = sh.get_worksheet(0)
            worksheet2 = sh.get_worksheet(1)
            await process_new_applications()
        except Exception as ex: logging.exception(ex)
        await asyncio.sleep(10)

@dp.callback_query(lambda c: c.data.startswith("approve_"))
async def approve_callback(callback: types.CallbackQuery):
    app_key = callback.data.split("_", 1)[1]
    info = pending_applications.get(app_key)
    username = callback.from_user.username
    if callback.from_user.id != OWNER_ID:
        await callback.answer("У вас нет прав на это действие!", show_alert=True)
        return
    info["active"] = False
    sheet_idx = info["sheet_idx"]
    row_idx = info["row_idx"]
    ws = worksheet1 if sheet_idx == 0 else worksheet2
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    username = callback.from_user.username or callback.from_user.full_name or str(callback.from_user.id)
    result_status = "Одобрено"

    ws.update([[result_status]], f"A{row_idx}")
    if sheet_idx == 0: ws.update([[f"@{username} {now_str}"]], f"O{row_idx}")
    else:  ws.update([[f"@{username} {now_str}"]], f"M{row_idx}")

    await bot.edit_message_reply_markup(
        chat_id=CHAT_ID,
        message_id=info["follow_msg_id"],
        reply_markup=result_keyboard("✅Одобрено✅"))

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_callback(callback: types.CallbackQuery):
    app_key = callback.data.split("_", 1)[1]
    info = pending_applications.get(app_key)
    username = callback.from_user.username
    if callback.from_user.id != OWNER_ID:
        await callback.answer("У вас нет прав на это действие!", show_alert=True)
        return
    info["active"] = False
    sheet_idx = info["sheet_idx"]
    row_idx = info["row_idx"]
    ws = worksheet1 if sheet_idx == 0 else worksheet2
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    username = callback.from_user.username or callback.from_user.full_name or str(callback.from_user.id)
    result_status = "Отказано"

    ws.update([[result_status]], f"A{row_idx}")
    if sheet_idx == 0: ws.update([[f"@{username} {now_str}"]], f"O{row_idx}")
    else: ws.update([[f"@{username} {now_str}"]], f"M{row_idx}")

    await bot.edit_message_reply_markup(
        chat_id=CHAT_ID,
        message_id=info["follow_msg_id"],
        reply_markup=result_keyboard("❌Отказано❌"))

async def main():
    asyncio.create_task(application_polling_task())
    await dp.start_polling(bot)

if __name__ == '__main__': asyncio.run(main())