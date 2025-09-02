import asyncio, logging, gspread, os, re
from datetime import datetime, time, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.utils.keyboard import InlineKeyboardBuilder
from google.oauth2.service_account import Credentials

BOT_TOKEN = "bot_token"
GOOGLE_SHEET_ID = "GS_ID"
SERVICE_ACCOUNT = r'credentials.json'
MEDIA_DIR = r'Applications'

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

CHECK_INTERVAL = 30  # время проверки в секундах
DAILY_REMIND_TIMES = ["12:00", "18:00"]  # список строк в формате HH:MM

USERID_BY_LEADER = {
    "Руководитель1": 1111,
    "Руководитель2": 2222,
    "Руководитель3": 3333,
    "Руководитель4": 4444,
    "Руководитель5": 5555,
    "Руководитель6": 6666,
    "Руководитель7": 7777,
    "Руководитель8": 8888,
    "Руководитель9": 9999,
    "Руководитель10": 10101010,
    "Руководитель11": 11111111,
    "Руководитель12": 12121212,
    "Руководитель13": 13131313,
    "Руководитель14": 14141414,
    "Руководитель15": 15151515,
    "Руководитель16": 16161616,
}

def safe_sheet_get_all_values():
    try:
        return sheet.get_all_values()
    except Exception as e:
        logging.exception(f"Ошибка гугл-таблицы при получении значений: {e}")
        return []

def safe_sheet_update_cell(row, col, value):
    try:
        sheet.update_cell(row, col, value)
    except Exception as e:
        logging.exception(f"Ошибка гугл-таблицы при update_cell({row}, {col}): {e}")

def safe_fetch_sheet_line(idx):
    all_rows = safe_sheet_get_all_values()
    if 0 < idx <= len(all_rows):
        return all_rows[idx-1]
    return None

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(GOOGLE_SHEET_ID).sheet1

finished_requests = set()
awaiting_fine = {}
pending_feedback = {}
user_reminders = {}

def get_media_files(links):
    if not links:
        return []
    files = []
    for part in links.replace('\r', '').replace(',', '\n').split('\n'):
        fname = part.strip()
        if fname:
            fpath = os.path.join(MEDIA_DIR, fname)
            if os.path.exists(fpath):
                files.append(fpath)
    return files

def fetch_open_requests():
    """ {user_id: [row_index, ...]} """
    all_rows = safe_sheet_get_all_values()
    leader_to_requests = {}
    for idx, row in enumerate(all_rows[1:], start=2):
        status = row[0].strip()
        if status != "Заявка рассматривается":
            continue
        leader_name = row[8].strip() if len(row) > 8 else ""
        user_id = USERID_BY_LEADER.get(leader_name)
        if user_id:
            leader_to_requests.setdefault(user_id, []).append(idx)
    return leader_to_requests

def sync_pending_feedback():
    leader_to_rows = fetch_open_requests()
    users = list(pending_feedback)
    for user_id in users:
        if user_id not in leader_to_rows:
            pending_feedback.pop(user_id, None)
        else:
            real_rows = leader_to_rows[user_id]
            old_rows = pending_feedback[user_id].get('row_indexes', [])
            pending_feedback[user_id]['row_indexes'] = [
                i for i in old_rows if i in real_rows
            ]
            if pending_feedback[user_id].get('selected') and pending_feedback[user_id]['selected'] not in real_rows:
                pending_feedback[user_id]['selected'] = None
    for user_id, rows in leader_to_rows.items():
        pf = pending_feedback.setdefault(user_id, {'row_indexes': [], 'selected': None, 'feedbacks': {}})
        for r in rows:
            if r not in pf['row_indexes']:
                pf['row_indexes'].append(r)
        if not pf['row_indexes']:
            pf['selected'] = None

async def send_full_request(user_id, row_idx, table_row):
    leader_name = table_row[8].strip() if len(table_row) > 8 else ""
    leader_user_id = USERID_BY_LEADER.get(leader_name)
    if leader_user_id == user_id:
        await notify_leader_from_row(table_row, row_idx, reminder=False)

def parse_remind_times(times_list):
    result = []
    for s in times_list:
        if isinstance(s, str):
            m = re.fullmatch(r'(\d{1,2}):(\d{2})', s.strip())
            if not m:
                raise ValueError(f"Некорректный формат времени: {s}")
            hour, minute = int(m.group(1)), int(m.group(2))
            result.append((hour, minute))
        elif isinstance(s, (tuple, list)):
            result.append((int(s[0]), int(s[1])))
        else:
            raise ValueError(f"Неподдерживаемый тип времени: {s}")
    return result

REMIND_SLOTS = parse_remind_times(DAILY_REMIND_TIMES)

def parse_time_interval(val):
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        pattern = r"(?:(d+)s*d)?s*(?:(d+)s*h)?s*(?:(d+)s*m)?s*(?:(d+)s*s)?"
        m = re.fullmatch(pattern, val.strip(), re.I)
        if not m:
            raise ValueError("Некорректный формат времени: '{}'".format(val))
        days, hours, mins, secs = (int(x) if x else 0 for x in m.groups())
        return days*86400 + hours*3600 + mins*60 + secs
    raise TypeError("CHECK_INTERVAL должен быть int/float или строка")

def next_remind_slot(now):
    today = now.date()
    slots = [datetime.combine(today, time(h, m)) for h, m in REMIND_SLOTS]
    future_slots = [dt for dt in slots if dt > now]
    if future_slots:
        return future_slots[0]
    slots_next_day = [datetime.combine(today + timedelta(days=1), time(h, m)) for h, m in REMIND_SLOTS]
    return slots_next_day[0]

def last_passed_remind_slot(now):
    today = now.date()
    slots = [datetime.combine(today, time(h, m)) for h, m in REMIND_SLOTS]
    past_slots = [dt for dt in slots if dt <= now]
    if past_slots:
        return past_slots[-1]
    yesterday = today - timedelta(days=1)
    slots_yesterday = [datetime.combine(yesterday, time(h, m)) for h, m in REMIND_SLOTS]
    return slots_yesterday[-1]

async def poll_google_table():
    global user_reminders
    while True:
        try:
            all_rows = safe_sheet_get_all_values()
            for idx, row in enumerate(all_rows[1:], start=2):
                status = row[0].strip()
                leader_name = row[8].strip() if len(row) > 8 else ""
                user_id = USERID_BY_LEADER.get(leader_name)
                if not user_id:
                    continue
                if status not in ("Ответ получен", "Заявка рассматривается"):
                    try:
                        safe_sheet_update_cell(idx, 1, "Заявка рассматривается")
                    except Exception as e:
                        logging.exception(f"Не удалось обновить статус заявки: строка {idx}: {e}")
                    row = safe_fetch_sheet_line(idx)
                    if row:
                        await notify_leader_from_row(row, idx, reminder=False)

            open_requests = {}
            for idx, row in enumerate(all_rows[1:], start=2):
                status = row[0].strip()
                leader_name = row[8].strip() if len(row) > 8 else ""
                user_id = USERID_BY_LEADER.get(leader_name)
                if not user_id:
                    continue
                if status == "Заявка рассматривается":
                    open_requests.setdefault(user_id, []).append(idx)

            now = datetime.now()
            for user_id, rows in open_requests.items():
                if not rows:
                    if user_id in user_reminders:
                        del user_reminders[user_id]
                    continue
                ur = user_reminders.setdefault(user_id, {})
                ur['pending_rows'] = rows.copy()
                last_remind_time = ur.get('last_remind_time')
                slot = last_passed_remind_slot(now)                
                if last_remind_time is None or last_remind_time < slot:
                    if len(rows) == 1:
                        idx = rows[0]
                        row = all_rows[idx - 1]
                        await notify_leader_from_row(row, idx, reminder=True)
                    else:
                        await show_requests_buttons(user_id, requests=rows, force=True)
                    ur['last_remind_time'] = slot

            close_ids = [uid for uid in user_reminders if uid not in open_requests or not open_requests[uid]]
            for uid in close_ids:
                del user_reminders[uid]

            sync_pending_feedback()
        except Exception as e:
            logging.exception(f"Ошибка при опросе гугл-таблицы: {e}")

        await asyncio.sleep(CHECK_INTERVAL)

async def notify_leader_from_row(row, row_index, reminder=False):
    leader_name = row[8].strip() if len(row) > 8 else ""
    user_id = USERID_BY_LEADER.get(leader_name)
    if not user_id:
        logging.warning(f"Нет контакта TG для: {leader_name!r}")
        return

    park_name = row[3].strip() if len(row) > 3 else ""
    department = row[4].strip() if len(row) > 4 else ""
    violation_type = row[5].strip() if len(row) > 5 else ""
    violation_text = row[6].strip() if len(row) > 6 else ""

    text = (
        f"Уважаемый {leader_name}!\n"
        f"В парке {park_name} в отделе {department} было обнаружено нарушение\n"
        f"Подробное описание ситуации от ОККР:\n\n\n"
        f"{violation_text}\n\n\n"
        f"Пожалуйста, прокомментируйте данное нарушение и укажите какие меры были приняты."
    )
    media_paths = get_media_files(row[7] if len(row) > 7 else "")
    await send_notification(user_id, leader_name, text, media_paths, row_index, reminder)

async def send_notification(user_id, _name, text, media_paths, row_index, reminder=False):
    pf = pending_feedback.setdefault(user_id, {'row_indexes': [], 'selected': None, 'feedbacks': {}})
    first_request = not pf['row_indexes']
    if row_index not in pf['row_indexes']:
        pf['row_indexes'].append(row_index)
        pf['selected'] = None

    if not reminder:
        if media_paths:
            medias = []
            for idx, media in enumerate(media_paths):
                caption = text if idx == 0 else None
                if media.lower().endswith(('.jpg', '.jpeg', '.png')):
                    medias.append(InputMediaPhoto(media=FSInputFile(media), caption=caption))
                elif media.lower().endswith('.mp4'):
                    medias.append(InputMediaVideo(media=FSInputFile(media), caption=caption))
            if len(medias) == 1:
                if medias[0].type == "photo":
                    await bot.send_photo(user_id, FSInputFile(media_paths[0]), caption=text)
                else:
                    await bot.send_video(user_id, FSInputFile(media_paths[0]), caption=text)
            else:
                await bot.send_media_group(user_id, medias)
        else:
            await bot.send_message(user_id, text)
        if len(pf['row_indexes']) > 1 and pf['selected'] is None:
            await asyncio.sleep(0.2)
            await show_requests_buttons(user_id, requests=pf['row_indexes'], force=False)

async def show_requests_buttons(user_id, requests=None, force=False):
    pf = pending_feedback.get(user_id, {})
    selected = pf.get('selected')
    queue = requests if requests is not None else pf.get('row_indexes', [])
    queue = [i for i in queue if i is not None]

    if force and len(queue) == 1:
        idx = queue[0]
        pf['selected'] = idx
        return

    if not queue or ((len(queue) <= 1 or selected) and not force):
        return
    builder = InlineKeyboardBuilder()
    all_rows = safe_sheet_get_all_values()
    for idx in queue:
        if 0 < idx <= len(all_rows):
            row = all_rows[idx - 1]
            descr = row[6].strip() if len(row) > 6 else ""
            short_descr = (descr[:100] + "...") if len(descr) > 100 else descr
            builder.button(text=short_descr, callback_data=f"feedback_{idx}")
    kb = builder.as_markup(resize_keyboard=True, one_time_keyboard=True)
    kb.inline_keyboard = [[btn] for btn in kb.inline_keyboard[0]]
    text_msg = "У вас остались замечания без ответа. Выберите, по какому из них дать комментарий:"
    await bot.send_message(user_id, text_msg, reply_markup=kb)

@dp.message(Command("myrequests"))
async def show_my_requests(message: types.Message):
    sync_pending_feedback()
    user_id = message.from_user.id
    requests = fetch_open_requests().get(user_id, [])
    if not requests:
        await message.answer("Нет новых замечаний.")
        return
    await show_requests_buttons(user_id, requests)

@dp.callback_query(F.data.startswith("feedback_"))
async def process_choose_feedback(callback: types.CallbackQuery):
    sync_pending_feedback()
    user_id = callback.from_user.id
    idx = int(callback.data.replace("feedback_", ""))
    if user_id not in pending_feedback or idx not in pending_feedback[user_id]['row_indexes']:
        await callback.answer("Замечание не найдено", show_alert=True)
        return
    if user_id in awaiting_fine:
        await callback.answer("Сначала укажите сумму штрафа для предыдущего замечания!", show_alert=True)
        return
    pending_feedback[user_id]['selected'] = idx
    await callback.answer()
    all_rows = safe_sheet_get_all_values()
    if 0 < idx <= len(all_rows):
        await send_full_request(user_id, idx, all_rows[idx-1])
    await callback.message.answer("Пожалуйста, напишите комментарий к замечанию.")

@dp.message(F.text)
async def process_feedback(message: types.Message):
    sync_pending_feedback()
    user_id = message.from_user.id
    feedback_data = pending_feedback.get(user_id)
    if user_id in awaiting_fine:
        fine_text = message.text.strip()
        if not fine_text.isdigit():
            await message.reply("Пожалуйста, введите сумму штрафа (только число)")
            return
        row_index = awaiting_fine[user_id]
        comment = feedback_data['feedbacks'][row_index]
        try:
            now = datetime.now().strftime('%d.%m.%Y')
            safe_sheet_update_cell(row_index, 10, now)
            safe_sheet_update_cell(row_index, 11, comment)
            safe_sheet_update_cell(row_index, 12, fine_text)
            safe_sheet_update_cell(row_index, 1, "Ответ получен")
            pending_feedback[user_id]['row_indexes'].remove(row_index)
            del pending_feedback[user_id]['feedbacks'][row_index]
            del awaiting_fine[user_id]
            if pending_feedback[user_id]['selected'] == row_index:
                pending_feedback[user_id]['selected'] = None
            if pending_feedback[user_id]['row_indexes']:
                await message.reply("Ответ принят. \nОстались неотвеченные замечания.")
                if len(pending_feedback[user_id]['row_indexes']) == 1:
                    await show_requests_buttons(user_id, requests=pending_feedback[user_id]['row_indexes'], force=True)
                else:
                    await show_requests_buttons(user_id)
            else:
                await message.reply("Спасибо за ответ! Все замечания обработаны.")
                pending_feedback.pop(user_id, None)
        except Exception as e:
            logging.exception(f"Ошибка записи штрафа: {e}")
            await message.reply("Ошибка при сохранении данных.")
        return
    if feedback_data and feedback_data['row_indexes']:
        idx = feedback_data.get('selected')
        if not idx:
            if len(feedback_data['row_indexes']) > 1:
                await message.reply("Пожалуйста, выберите одно из замечаний в списке выше (нажмите на нужную кнопку).")
            else:
                feedback_data['selected'] = feedback_data['row_indexes'][0]
                idx = feedback_data['selected']
        if not idx:
            return
        pending_feedback[user_id].setdefault('feedbacks', {})[idx] = message.text.strip()
        awaiting_fine[user_id] = idx
        await message.reply("Комментарий принят.\nТеперь укажите сумму штрафа, если штрафа не было введите просто 0")
        return
    await message.reply("У вас нет ожидающих замечаний.")

async def main():
    loop = asyncio.get_running_loop()
    poller = loop.create_task(poll_google_table())
    await dp.start_polling(bot)
    poller.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")