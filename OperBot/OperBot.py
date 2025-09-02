import logging, asyncio, gspread, os
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InputMediaPhoto, InputMediaVideo
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from datetime import datetime
from uuid import uuid4
from google.oauth2.service_account import Credentials

BOT_TOKEN = "BotToken"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

HEAD1_ID = 111111111
HEAD2_ID = 222222222

WORKERS = [
    {"id": 111111111, "fio": "Исполнитель1", "username": "aql810"},
    {"id": 222222222, "fio": "Исполнитель2", "username": "ivanivan"},
    {"id": 333333333, "fio": "Исполнитель3", "username": "petyapetya"},
]

WORKERS_GROUP_ID = -100111111111
WORKERS_THREAD_ID = 1

REMINDER_INTERVAL = 1800

ATTACHMENTS_DIR = r"Applications"
os.makedirs(ATTACHMENTS_DIR, exist_ok=True)

GOOGLE_SHEET_ID = "GS_ID"
GOOGLE_SHEET_WORKSHEET = "GS_WS_NAME"

GOOGLE_CREDS_FILE = r"credentials.json"

def get_worksheet():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    worksheet = sh.worksheet(GOOGLE_SHEET_WORKSHEET)
    return worksheet

class AppForm(StatesGroup):
    q = State() 
    attachments = State()

active_requests = {}
request_reply_mapping = {}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUESTION = "Опишите ситуацию водителя как можно подробнее:"

def format_dt(ts: datetime) -> str:
    return ts.strftime('%d.%m.%Y %H:%M:%S')

def get_workers_keyboard(request_id):
    btns = []
    for worker in WORKERS:
        btns.append([
            InlineKeyboardButton(
                text=worker["fio"],
                callback_data=f"assign|{request_id}|{worker['id']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def get_assigned_info_keyboard(worker_fio):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Исполнитель назначен:", callback_data="noop")],
            [InlineKeyboardButton(text=worker_fio, callback_data="noop")],
        ]
    )

def get_worker_username(worker_id):
    for w in WORKERS:
        if w["id"] == worker_id:
            return w.get("username")
    return None

def get_add_button():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Добавить")]],
        resize_keyboard=True,
        one_time_keyboard=False
    )

async def get_request_text(data, assigned_worker_fio=None, worker_username=None):
    text = f"Заявка:\n{data['q']}"
    if assigned_worker_fio:
        if worker_username:
            text += f"\n\nНазначен исполнитель: {assigned_worker_fio} (@{worker_username})"
        else:
            text += f"\n\nНазначен исполнитель: {assigned_worker_fio}"
    return text

async def save_tg_file(bot, file_id, save_dir, dt_prefix):
    file = await bot.get_file(file_id)
    suffix = ""

    if hasattr(file, "file_path"):
        _, ext = os.path.splitext(file.file_path)
        suffix = ext if ext else ""

    base_name = f"file_{dt_prefix}"

    i = 0
    while True:
        if i == 0:
            fname = f"{base_name}{suffix}"
        else:
            fname = f"{base_name}_{i}{suffix}"
        full_path = os.path.join(save_dir, fname)
        if not os.path.exists(full_path):
            break
        i += 1

    file_obj = await bot.download_file(file.file_path)
    with open(full_path, "wb") as out_f:
        out_f.write(file_obj.read())
    return fname

@dp.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    await msg.answer(
        "Здравствуйте!\nДля создания заявки используйте кнопку 'Добавить'.",
        reply_markup=get_add_button()
    )

@dp.message(Command("apply"))
@dp.message(F.text.lower() == "добавить")
async def apply_start(msg: Message, state: FSMContext):
    await state.set_state(AppForm.q)
    await state.update_data(request_id=str(uuid4()))
    await msg.answer(QUESTION)

@dp.message(AppForm.q)
async def answer_q(msg: Message, state: FSMContext):
    await state.update_data(q=msg.text, attachments=[], attachments_names=[])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить", callback_data="skip_attach")]
    ])
    await msg.answer(
        "Если хотите, добавьте фото/видео/аудио. Можете отправить несколько файлов. Для завершения или пропуска этого этапа — нажмите кнопку ниже ⬇️",
        reply_markup=kb
    )
    await state.set_state(AppForm.attachments)

@dp.message(AppForm.attachments, F.photo | F.video | F.audio | F.document)
async def process_attachment(msg: Message, state: FSMContext):
    data = await state.get_data()
    attachments = data.get("attachments", [])
    attachments_names = data.get("attachments_names", [])
    ts = datetime.now()
    dt_prefix = ts.strftime("%d.%m.%Y_%H.%M.%S_%f")
    bot_instance = bot

    file_id = None
    file_name = None
    true_file_name = None
    content_type = msg.content_type

    if msg.photo:
        file_id = msg.photo[-1].file_id
        content_type = "photo"
    elif msg.video:
        file_id = msg.video.file_id
        content_type = "video"
    elif msg.audio:
        file_id = msg.audio.file_id
        content_type = "audio"
    elif msg.document:
        file_id = msg.document.file_id
        file_name = msg.document.file_name
        content_type = "document"

    if file_id:
        if content_type == "document" and file_name:
            save_name = f"{dt_prefix}_{file_name}"
            full_path = os.path.join(ATTACHMENTS_DIR, save_name)
        else:
            ext = ""
            if msg.photo:
                ext = ".jpg"
            elif msg.video:
                ext = ".mp4"
            elif msg.audio:
                ext = ".mp3"
            save_name = f"{content_type}_{dt_prefix}{ext}"
            full_path = os.path.join(ATTACHMENTS_DIR, save_name)

        file = await bot_instance.get_file(file_id)
        file_bytes = await bot_instance.download_file(file.file_path)
        with open(full_path, "wb") as out_f:
            out_f.write(file_bytes.read())

        true_file_name = file_name if file_name else save_name
        attachments_names.append(true_file_name)

        attachments.append({'file_id': file_id,
                            'type': content_type,
                            'file_name': save_name})

    await state.update_data(attachments=attachments, attachments_names=attachments_names)
    await msg.answer(
        "Файл добавлен. Можно отправить ещё, либо нажмите «Продолжить», чтобы завершить.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Продолжить", callback_data="skip_attach")]]
        )
    )

async def send_request_with_attachments(
    bot, user_id, text, attachments, reply_markup=None, group_chat_id=None, thread_id=None
):
    chat_id = group_chat_id if group_chat_id else user_id

    photos = [a for a in attachments if a['type'] == 'photo']
    videos = [a for a in attachments if a['type'] == 'video']
    audios = [a for a in attachments if a['type'] == 'audio']
    documents = [a for a in attachments if a['type'] == 'document']

    sent_msg_id = None

    if photos or videos:
        if photos:
            first = photos[0]
            msg = await bot.send_photo(
                chat_id, first['file_id'],
                caption=text, reply_markup=reply_markup,
                message_thread_id=thread_id if thread_id else None,
            )
            sent_msg_id = msg.message_id
            media = []
            for att in photos[1:]:
                media.append(InputMediaPhoto(media=att['file_id']))
            for att in videos:
                media.append(InputMediaVideo(media=att['file_id']))
            if media:
                await bot.send_media_group(
                    chat_id, media,
                    message_thread_id=thread_id if thread_id else None,
                )
        else:
            first = videos[0]
            msg = await bot.send_video(
                chat_id, first['file_id'],
                caption=text, reply_markup=reply_markup,
                message_thread_id=thread_id if thread_id else None,
            )
            sent_msg_id = msg.message_id
            media = []
            for att in videos[1:]:
                media.append(InputMediaVideo(media=att['file_id']))
            if media:
                await bot.send_media_group(
                    chat_id, media,
                    message_thread_id=thread_id if thread_id else None,
                )
        for att in audios:
            await bot.send_audio(
                chat_id, att['file_id'],
                message_thread_id=thread_id if thread_id else None,
            )
        for att in documents:
            await bot.send_document(
                chat_id, att['file_id'],
                caption=att.get('file_name', ''),
                message_thread_id=thread_id if thread_id else None,
            )
        return sent_msg_id

    elif audios and not (photos or videos):
        msg = await bot.send_audio(
            chat_id, audios[0]['file_id'],
            caption=text, reply_markup=reply_markup,
            message_thread_id=thread_id if thread_id else None,
        )
        sent_msg_id = msg.message_id
        for att in audios[1:]:
            await bot.send_audio(
                chat_id, att['file_id'],
                message_thread_id=thread_id if thread_id else None,
            )
        for att in documents:
            await bot.send_document(
                chat_id, att['file_id'],
                caption=att.get('file_name', ''),
                message_thread_id=thread_id if thread_id else None,
            )
        return sent_msg_id

    elif documents and not (photos or videos or audios):
        msg = await bot.send_document(
            chat_id, documents[0]['file_id'],
            caption=f"{text}\n\n{documents[0].get('file_name', '')}" if text else documents[0].get('file_name', ''),
            reply_markup=reply_markup,
            message_thread_id=thread_id if thread_id else None,
        )
        sent_msg_id = msg.message_id
        for att in documents[1:]:
            await bot.send_document(
                chat_id, att['file_id'],
                caption=att.get('file_name', ''),
                message_thread_id=thread_id if thread_id else None,
            )
        return sent_msg_id

    else:
        msg = await bot.send_message(
            chat_id, text, reply_markup=reply_markup,
            message_thread_id=thread_id if thread_id else None,
        )
        sent_msg_id = msg.message_id
        return sent_msg_id

@dp.callback_query(F.data == "skip_attach")
async def finish_attachments(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    request_id = data.get('request_id') or str(uuid4())
    username = callback.from_user.username if callback.from_user.username else callback.from_user.full_name
    created_at = datetime.now()

    active_requests[request_id] = {
        "data": data,
        "author_id": callback.from_user.id,
        "author_uname": username,
        "created_at": created_at,
        "status": "pending_head",
        "assigned_worker_id": None,
        "assigned_head_id": None,
        "assigned_worker_fio": None,
        "assigned_worker_uname": None,
        "group_msg_id": None,
        "reminder_task": None,
        "reply_user_id": None,
        "dt_assigned": None,
        "dt_worker_reply": None,
        "worker_comment": None,
        "attachments": data.get('attachments', [])
    }
    kb = get_workers_keyboard(request_id)
    text = f"Поступила новая заявка от @{username}:\n{data['q']}\n\nВыберите, кому из сотрудников поручить выполнение:"
    await callback.message.answer("Спасибо! Ваша заявка отправлена руководству.",
                                 reply_markup=get_add_button())

    head_message_ids = {}
    for head_id in (HEAD1_ID, HEAD2_ID):
        msg_id = await send_request_with_attachments(
            bot, head_id, text, data.get('attachments', []), reply_markup=kb)
        head_message_ids[head_id] = msg_id

    active_requests[request_id]['head_message_ids'] = head_message_ids

    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("assign"))
async def assign_worker(callback: CallbackQuery):
    _, request_id, worker_id_str = callback.data.split("|")
    worker_id = int(worker_id_str)
    user_id = callback.from_user.id

    req = active_requests.get(request_id)
    if not req or req['status'] != "pending_head":
        await callback.answer("Заявка уже обработана или устарела.", show_alert=True)
        return
    if user_id not in (HEAD1_ID, HEAD2_ID):
        await callback.answer("Вы не руководитель!", show_alert=True)
        return
    if req["assigned_head_id"] and req["assigned_head_id"] != user_id:
        await callback.answer("Другой руководитель уже назначил ответственного.", show_alert=True)
        return

    worker = next(w for w in WORKERS if w["id"] == worker_id)
    worker_fio = worker["fio"]
    worker_uname = worker.get("username", "")

    req["assigned_worker_id"] = worker_id
    req["assigned_head_id"] = user_id
    req["status"] = "pending_worker"
    req["assigned_worker_fio"] = worker_fio
    req["assigned_worker_uname"] = worker_uname
    req["dt_assigned"] = datetime.now()

    kb_done = get_assigned_info_keyboard(worker_fio)
    head_message_ids = req.get('head_message_ids', {})
    for head_id, msg_id in head_message_ids.items():
        try:
            await bot.edit_message_reply_markup(
                chat_id=head_id,
                message_id=msg_id,
                reply_markup=kb_done
            )
        except Exception:
            pass

    req_text = await get_request_text(
        req["data"],
        assigned_worker_fio=worker_fio, worker_username=worker_uname or None
    )

    group_msg_id = await send_request_with_attachments(
        bot, None, req_text, req["data"].get("attachments", []),
        group_chat_id=WORKERS_GROUP_ID,
        thread_id=WORKERS_THREAD_ID
    )
    req["group_msg_id"] = group_msg_id
    request_reply_mapping[group_msg_id] = request_id

    if req.get("reminder_task"):
        try:
            req['reminder_task'].cancel()
        except Exception:
            pass
    reminder_task = asyncio.create_task(reminder_worker(request_id))
    req["reminder_task"] = reminder_task

async def reminder_worker(request_id):
    try:
        while True:
            await asyncio.sleep(REMINDER_INTERVAL)
            req = active_requests.get(request_id)
            if req is None or req["status"] != "pending_worker":
                break
            worker_fio = req.get("assigned_worker_fio", "")
            worker_username = req.get("assigned_worker_uname", "")
            assigned_line = f"{worker_fio}" + (f" (@{worker_username})" if worker_username else "")
            group_msg_id = req.get("group_msg_id")
            if group_msg_id:
                await bot.send_message(
                    WORKERS_GROUP_ID,
                    f"{assigned_line}, заявка всё ещё не разобрана, пожалуйста отправьте ответ на данную заявку.",
                    reply_to_message_id=group_msg_id,
                    message_thread_id=WORKERS_THREAD_ID
                )
    except asyncio.CancelledError:
        pass

@dp.message(F.reply_to_message)
async def reply_to_task(msg: Message):
    group_msg_id = msg.reply_to_message.message_id
    if group_msg_id not in request_reply_mapping:
        return
    request_id = request_reply_mapping[group_msg_id]
    req = active_requests.get(request_id)
    if not req or req["status"] != "pending_worker":
        return
    responsible_worker_id = req.get('assigned_worker_id')
    if not responsible_worker_id:
        return
    if msg.from_user.id != responsible_worker_id:
        return

    req["status"] = "done"
    req["reply_user_id"] = msg.from_user.id
    req["dt_worker_reply"] = datetime.now()
    req["worker_comment"] = msg.text
    if req.get("reminder_task"):
        req["reminder_task"].cancel()

    worker_username = req.get('assigned_worker_uname')
    assigned_line = f"{req['assigned_worker_fio']}" + (f" (@{worker_username})" if worker_username else "")
    await bot.send_message(
        WORKERS_GROUP_ID,
        f"✅ {assigned_line} ответил(а) на заявку!",
        reply_to_message_id=req['group_msg_id'],
        message_thread_id=WORKERS_THREAD_ID
    )
    await save_to_gsheet(req)

async def save_to_gsheet(req):
    try:
        worksheet = get_worksheet()
        names = req["data"].get('attachments_names', [])
        files_col = "\n".join(names)
        worksheet.append_row([
            '@' + str(req['author_uname']),
            format_dt(req['created_at']),
            req['data']['q'],
            files_col,
            format_dt(req['dt_assigned']) if req.get('dt_assigned') else '',
            req.get('assigned_worker_fio', ''),
            format_dt(req['dt_worker_reply']) if req.get('dt_worker_reply') else '',
            req.get('worker_comment', '')
        ])
    except Exception as e:
        logger.exception(f"Ошибка отправки в Google Таблицу: {e}")

async def reminder_worker(request_id):
    req = active_requests.get(request_id)
    if not req or not req.get("assigned_worker_id"):
        return
    worker_username = req.get("assigned_worker_uname")
    assigned_line = f"{req['assigned_worker_fio']}" + (f" (@{worker_username})" if worker_username else "")
    try:
        while True:
            await asyncio.sleep(REMINDER_INTERVAL)
            req = active_requests.get(request_id)
            if req is None or req["status"] != "pending_worker":
                break
            group_msg_id = req.get("group_msg_id")
            if group_msg_id:
                await bot.send_message(
                    WORKERS_GROUP_ID,
                    f"⚠️ {assigned_line} напоминаю вам! Заявка всё ещё не обработана. Пожалуйста, ответьте!",
                    reply_to_message_id=group_msg_id,
                    message_thread_id=WORKERS_THREAD_ID
                )
    except asyncio.CancelledError:
        pass

@dp.errors()
async def global_error_handler(event: types.Update, exception: Exception):
    logger.exception(f"Произошла ошибка: {exception}")
    return True

if __name__ == "__main__":
    dp.run_polling(bot)