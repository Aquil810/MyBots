import asyncio, os, logging, datetime, gspread
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.bot import DefaultBotProperties
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.utils.keyboard import InlineKeyboardBuilder
from google.oauth2.service_account import Credentials

BOT_TOKEN = 'BOT_TOKEN'
dp = Dispatcher()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
CHAT_ID = -111111111111111
THREAD_1_ID = 2
THREAD_2_ID = 3
GSHEET_ID = 'GSHEET_ID'
APPLICATIONS_PATH = r"C:\Users\Администратор\Desktop\bots\3StepBot\Applications"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(r"C:\Users\Администратор\Desktop\bots\3StepBot\3step_credentials.json", scopes=SCOPES)

gc = gspread.authorize(creds)
sh = gc.open_by_key(GSHEET_ID)
worksheet1 = sh.get_worksheet(0)
worksheet2 = sh.get_worksheet(1)

logging.basicConfig(level=logging.INFO)

pending_applications = {}
reminder_interval = 30

def get_media_files(cell: str):
    if not cell: return [], []
    files = [f.strip() for f in cell.splitlines() if f.strip()]
    photos, videos = [], []
    for f in files:
        ext = f.lower().split('.')[-1]
        if ext in ("jpg", "jpeg", "png", "bmp", "webp"): photos.append(f)
        elif ext in ("mp4", "mov", "mkv", "avi"): videos.append(f)
    return photos, videos

def is_application_row(row):
    return len(row) > 0 and row[0].strip().lower() in ("создано", "отказано")

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
        done_col = 13
        comment_col = 15
    else:
        base_msg = compose_message_ws2(row)
        done_col = 11
        comment_col = 13
    done_value = row[done_col] if len(row) > done_col else ""
    if done_value: base_msg += f"\n\nВыполнено: {done_value}"
    else: base_msg += f"\n\nВыполнено: -"
    if len(row) > comment_col and row[comment_col].strip(): base_msg += f"\nКомментарий: {row[comment_col].strip()}"
    return base_msg

def inline_keyboard(not_executed=True, app_index=None, hidden=False):
    builder = InlineKeyboardBuilder()
    if hidden: builder.button(text="🥷 Скрыто", callback_data="noop")
    elif not_executed and app_index is not None: builder.button(text="❗️❌🛑Не выполнено🛑❌❗️", callback_data=f'exec_{app_index}')
    else: builder.button(text="✅Выполнено✅", callback_data="noop")
    return builder.as_markup()

def inline_cancel_keyboard(cancelled=False, app_index=None, hidden=False):
    builder = InlineKeyboardBuilder()
    if hidden: builder.button(text="🥷 Скрыто", callback_data="noop")
    elif not cancelled and app_index is not None: builder.button(text="❗️❌🛑Не отменено🛑❌❗️", callback_data=f'cancel_{app_index}')
    else: builder.button(text="✅Отменено✅", callback_data="noop")
    return builder.as_markup()

async def send_application(sheet_idx, idx, row, media_files, app_key):
    if sheet_idx == 0:
        thread_id = THREAD_1_ID
        msg_text = build_full_message(sheet_idx, row)
        followup_text = "Чтобы проверить заявку по акции нужно сделать то-то то-то"
    elif sheet_idx == 1:
        thread_id = THREAD_2_ID
        msg_text = build_full_message(sheet_idx, row)
        followup_text = "Чтобы проверить заявку по повреждениям нужно сделать та-та та-та"
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
    follow_msg = await bot.send_message(
        chat_id=CHAT_ID,
        message_thread_id=thread_id,
        text=followup_text,
        reply_markup=inline_keyboard(True, app_key, hidden=False))
    pending_applications[app_key] = {
        "sheet_idx": sheet_idx,
        "row_idx": idx + 1,
        "msg_id": app_msg.message_id,
        "follow_msg_id": follow_msg.message_id,
        "thread_id": thread_id,
        "created_at": datetime.datetime.now(),
        "remind_time": datetime.datetime.now() + datetime.timedelta(seconds=reminder_interval),
        "active": True,
        "notified_times": 0,
        "hidden": False}

async def send_declined_application(sheet_idx, idx, row, media_files, app_key):
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
    followup_text = "Просьба провести отмену операции"
    follow_msg = await bot.send_message(
        chat_id=CHAT_ID,
        message_thread_id=thread_id,
        text=followup_text,
        reply_markup=inline_cancel_keyboard(cancelled=False, app_index=app_key, hidden=False))
    pending_applications[app_key] = {
        "sheet_idx": sheet_idx,
        "row_idx": idx + 1,
        "msg_id": app_msg.message_id,
        "follow_msg_id": follow_msg.message_id,
        "thread_id": thread_id,
        "created_at": datetime.datetime.now(),
        "remind_time": datetime.datetime.now() + datetime.timedelta(seconds=reminder_interval),
        "active": True,
        "notified_times": 0,
        "hidden": False,
        "declined": True
    }
                         
async def process_new_applications():
    wsheets = [(worksheet1, 0), (worksheet2, 1)]
    for ws, sheet_idx in wsheets:
        rows = ws.get_all_values()
        for idx, row in enumerate(rows):
            if not is_application_row(row): continue
            app_key = f"{sheet_idx}_{idx}"
            if app_key not in pending_applications:
                status = row[0].strip().lower()
            if status == "создано":
                ws.update([["Выполняется"]], f"A{idx+1}")
                media_files = ([], [])
                if sheet_idx == 0 and len(row) >= 13: media_files = get_media_files(row[12])
                elif sheet_idx == 1 and len(row) >= 11: media_files = get_media_files(row[10])
                await send_application(sheet_idx, idx, row, media_files, app_key)
            elif status == "отказано":
                media_files = ([], [])
                if sheet_idx == 0 and len(row) >= 13: media_files = get_media_files(row[12])
                elif sheet_idx == 1 and len(row) >= 11: media_files = get_media_files(row[10])
                await send_declined_application(sheet_idx, idx, row, media_files, app_key)

async def application_polling_task():
    global worksheet1, worksheet2
    while True:
        try:
            worksheet1 = sh.get_worksheet(0)
            worksheet2 = sh.get_worksheet(1)
            await process_new_applications()
        except Exception as ex: logging.exception(ex)
        await asyncio.sleep(10)
        
async def reminder_task():
    while True:
        try:
            now = datetime.datetime.now()
            for app_key, info in list(pending_applications.items()):
                if not info.get("active") or info.get("hidden"): continue
                remind_time = info.get("remind_time", now)
                if now >= remind_time:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        message_thread_id=info.get("thread_id"),
                        text="Заявка всё ещё не выполнена!",
                        reply_to_message_id=info.get("msg_id"))
                    info["remind_time"] = now + datetime.timedelta(seconds=reminder_interval)
                    info["notified_times"] += 1
        except Exception as ex: logging.exception("Reminder error: %s", ex)
        await asyncio.sleep(5)

@dp.message(F.text.regexp(r'^/(скрыть|открыть)$'))
async def on_toggle_hidden_command(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Эта команда работает только в ответ (reply) на сообщение заявки.")
        return
    reply_message_id = message.reply_to_message.message_id
    target_key = None
    for app_key, info in pending_applications.items():
        if info.get("follow_msg_id") == reply_message_id or info.get("msg_id") == reply_message_id:
            target_key = app_key
            break
    if not target_key:
        await message.reply("Не могу найти заявку по этому сообщению.")
        return
    info = pending_applications[target_key]
    cmd = message.text.strip().lower()
    if cmd == "/скрыть":
        if info.get("hidden", False):
            await message.reply("Заявка уже скрыта.")
            return
        info["hidden"] = True
        await bot.edit_message_reply_markup(
            chat_id=CHAT_ID,
            message_id=info["follow_msg_id"],
            reply_markup=inline_keyboard(not_executed=True, app_index=target_key, hidden=True))
        await message.reply("Заявка скрыта. Напоминания остановлены.", reply=False)
    elif cmd == "/открыть":
        if not info.get("hidden", False):
            await message.reply("Заявка и так не скрыта.")
            return
        info["hidden"] = False
        info["active"] = True
        info["remind_time"] = datetime.datetime.now() + datetime.timedelta(seconds=reminder_interval)
        await bot.edit_message_reply_markup(
            chat_id=CHAT_ID,
            message_id=info["follow_msg_id"],
            reply_markup=inline_keyboard(not_executed=True, app_index=target_key, hidden=False))
        await message.reply("Заявка снова открыта.", reply=False)

@dp.callback_query(lambda c: c.data.startswith("exec_"))
async def on_mark_not_done(callback: types.CallbackQuery):
    app_key = callback.data.split("_", 1)[1]
    info = pending_applications.get(app_key)
    if not info or not info["active"]:
        await callback.answer("Заявка не активна.", show_alert=True)
        return
    info["active"] = False
    sheet_idx = info["sheet_idx"]
    row_idx = info["row_idx"]
    ws = worksheet1 if sheet_idx == 0 else worksheet2
    username = callback.from_user.username or callback.from_user.full_name or str(callback.from_user.id)
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    mention_field = f"@{username} {now_str}"
    row = ws.row_values(row_idx)
    ws.update([["Выполнено"]], f"A{row_idx}")
    if sheet_idx == 0:
        if len(row) < 14: row += [""] * (14 - len(row))
        ws.update([[mention_field]], f"N{row_idx}")
    else:
        if len(row) < 12:row += [""] * (12 - len(row))
        ws.update([[mention_field]], f"L{row_idx}")
    await bot.edit_message_reply_markup(
        chat_id=CHAT_ID,
        message_id=info["follow_msg_id"],
        reply_markup=inline_keyboard(not_executed=False),)
    
@dp.callback_query(lambda c: c.data.startswith("cancel_"))
async def on_cancel_action(callback: types.CallbackQuery):
    app_key = callback.data.split("_", 1)[1]
    info = pending_applications.get(app_key)
    if not info or not info["active"]:
        await callback.answer("Заявка не активна.", show_alert=True)
        return
    info["active"] = False
    sheet_idx = info["sheet_idx"]
    row_idx = info["row_idx"]
    ws = worksheet1 if sheet_idx == 0 else worksheet2
    username = callback.from_user.username or callback.from_user.full_name or str(callback.from_user.id)
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    mention_field = f"@{username} {now_str}"
    ws.update([["Отменено"]], f"A{row_idx}")
    if sheet_idx == 0:
        if len(ws.row_values(row_idx)) < 17: ws.add_cols(17 - len(ws.row_values(row_idx)))
        ws.update([[mention_field]], f"Q{row_idx}")
    else:
        if len(ws.row_values(row_idx)) < 15: ws.add_cols(15 - len(ws.row_values(row_idx)))
        ws.update([[mention_field]], f"O{row_idx}")
    await bot.edit_message_reply_markup(
        chat_id=CHAT_ID,
        message_id=info["follow_msg_id"],
        reply_markup=inline_cancel_keyboard(cancelled=True),)
    await callback.answer("Отмена операции проведена.", show_alert=True)

@dp.message()
async def on_comment_reply(message: types.Message):
    if not message.reply_to_message: return
    reply_message_id = message.reply_to_message.message_id
    target_key = None
    for app_key, info in pending_applications.items():
        if info.get("follow_msg_id") == reply_message_id or info.get("msg_id") == reply_message_id:
            target_key = app_key
            break
    if not target_key: return
    info = pending_applications[target_key]
    if not info["active"]:
        await message.reply("Эта заявка уже завершена и недоступна для комментирования.")
        return
    if info.get("declined"):
        await message.reply("Нельзя добавлять комментарии к отклонённой/отменённой заявке.")
        return
    sheet_idx = info["sheet_idx"]
    row_idx = info["row_idx"]
    ws = worksheet1 if sheet_idx == 0 else worksheet2
    row = ws.row_values(row_idx)
    comment = message.text.strip()
    if not comment: return
    if sheet_idx == 0:
        if len(row) < 16: row += [""] * (16 - len(row))
        col_letter = 'P'
    else:
        if len(row) < 14: row += [""] * (14 - len(row))
        col_letter = 'N'
    ws.update([[comment]], f"{col_letter}{row_idx}")
    info["active"] = False
    ws.update([["Выполнено"]], f"A{row_idx}")
    username = message.from_user.username or message.from_user.full_name or str(message.from_user.id)
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    mention_field = f"@{username} {now_str}"
    if sheet_idx == 0:
        if len(row) < 14: row += [""] * (14 - len(row))
        ws.update([[mention_field]], f"N{row_idx}")
    else:
        if len(row) < 12: row += [""] * (12 - len(row))
        ws.update([[mention_field]], f"L{row_idx}")
    await bot.edit_message_reply_markup(
        chat_id=CHAT_ID,
        message_id=info["follow_msg_id"],
        reply_markup=inline_keyboard(not_executed=False),)
    await message.reply("Комментарий добавлен, заявка автоматически выполнена!", reply=False)

async def main():
    asyncio.create_task(application_polling_task())
    asyncio.create_task(reminder_task())
    await dp.start_polling(bot)

if __name__ == '__main__': asyncio.run(main())