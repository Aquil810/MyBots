import datetime, asyncio, logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from google.oauth2.service_account import Credentials
from gspread_asyncio import AsyncioGspreadClientManager
from aiogram.client.bot import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, ErrorEvent

BOT_TOKEN = 'Bot_Token'
SPREADSHEET_ID = 'GS_ID'

dp = Dispatcher()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
logging.basicConfig(level=logging.INFO)

EXECUTOR_IDS = [111111111, 2222222222]
OWNER_ID = 333333333

def get_creds():
    return Credentials.from_service_account_file(
        r"credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

REQUEST_TYPES = {
    "action": "Акция (Условия аренды)",
    "damage": "Повреждения скидки"
}
user_chosen_type = {}

QUESTIONS = [
    "1. Введите ваше ФИО",
    "2. Введите ФИО водителя",
    "3. Введите номер авто водителя",
    "4. Введите стаж работы водителя (только число)",
    "5. Введите общий баланс аренды водителя (только число)",
    "6. Введите общий долг водителя (только число)",
    "7. Введите ссылку на аккаунт водителя в Оде",
    "8. Опишите ситуацию водителя",
]

FIELDS = [
    "ФИО автора",
    "ФИО водителя",
    "Номер авто водителя",
    "Стаж работы водителя",
    "Общий баланс аренды",
    "Общий долг водителя",
    "Ссылка на аккаунт водителя",
    "Описание ситуации"
]

agcm = AsyncioGspreadClientManager(get_creds)
async def get_sheet_by_type(type_key: str):
    agc = await agcm.authorize()
    ss = await agc.open_by_key(SPREADSHEET_ID)
    sheet_name = REQUEST_TYPES[type_key]
    worksheet = await ss.worksheet(sheet_name)
    return worksheet

user_states = {}
user_answers = {}
user_request_msgs = dict()

def is_number(text):
    try:
        float(text.replace(',', '.'))
        return True
    except Exception:
        return False

def plural_days(n):
    n = int(n)
    if n % 10 == 1 and n % 100 != 11:
        return "день"
    elif 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14):
        return "дня"
    else:
        return "дней"
    
def get_type_select_kb():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=REQUEST_TYPES["action"], callback_data="req_type_action"),
                InlineKeyboardButton(text=REQUEST_TYPES["damage"], callback_data="req_type_damage"),
            ]
        ]
    )
    return kb

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

def get_main_kb(user_id):
    if user_id in EXECUTOR_IDS or user_id == OWNER_ID:
        return ReplyKeyboardRemove()
    return main_kb

def build_request_text(username, now, answers, req_type_text):
    fio_avtor = answers[0]
    fio_vod = answers[1]
    car_number = answers[2]
    stazh = str(int(float(answers[3].replace(",", "."))))
    balance = str(int(float(answers[4].replace(",", "."))))
    dolg = str(int(float(answers[5].replace(",", "."))))
    oda_link = answers[6]
    opisanie = answers[7]
    stazh_str = f"Стаж: {stazh} {plural_days(stazh)}"
    balance_str = f"Баланс: {balance} ₽"
    dolg_str = f"Долг: {dolg} ₽"
    return (
        f'Заявка {now} от {fio_avtor} (@{username})\n{req_type_text}\n'
        f"{fio_vod}\n"
        f"{car_number}\n"
        f"{stazh_str}\n"
        f"{balance_str}\n"
        f"{dolg_str}\n"
        f"{oda_link}\n"
        f"{opisanie}"
    )

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    await msg.answer(
        "Привет! Чтобы подать заявку, нажмите кнопку 'Добавить' ниже.",
        reply_markup=get_main_kb(msg.from_user.id)
    )

@dp.message(F.text == "Добавить")
async def add_request(msg: types.Message):
    if msg.from_user.id in EXECUTOR_IDS or msg.from_user.id == OWNER_ID:
        await msg.answer("У вас нет права добавлять заявки.", reply_markup=ReplyKeyboardRemove())
        return
    user_states.pop(msg.from_user.id, None)
    user_answers.pop(msg.from_user.id, None)
    user_chosen_type.pop(msg.from_user.id, None)
    await msg.answer(
        "Какую заявку вы хотите сделать?", 
        reply_markup=get_type_select_kb()
    )

def get_selected_type_kb(type_key):
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ " + REQUEST_TYPES[type_key],
                callback_data="selected_type"
            )
        ]]
    )

@dp.callback_query(F.data.regexp(r"^req_type_(action|damage)$"))
async def process_type_choose(call: types.CallbackQuery):
    type_key = call.data[len('req_type_'):]
    user_id = call.from_user.id
    user_chosen_type[user_id] = type_key
    await call.message.edit_reply_markup(reply_markup=get_selected_type_kb(type_key))
    user_states[user_id] = 0
    user_answers[user_id] = []
    await call.message.answer(QUESTIONS[0], reply_markup=get_main_kb(user_id))
    await call.answer()

@dp.callback_query(F.data.regexp(r"^add_req_by_button_([0-9]+)$"))
async def add_request_by_button(call: types.CallbackQuery):
    author_id = int(call.data.split("_")[-1])
    if call.from_user.id != author_id:
        await call.answer("Эта кнопка доступна только автору заявки.", show_alert=True)
        return
    if author_id in EXECUTOR_IDS or author_id == OWNER_ID:
        await call.answer("Исполнитель и собственник не могут добавлять заявку.", show_alert=True)
        return
    user_states[author_id] = 0
    user_answers[author_id] = []
    await call.message.answer(QUESTIONS[0], reply_markup=ReplyKeyboardRemove())
    await call.answer()

@dp.message(F.text)
async def process_questions(msg: types.Message):
    user_id = msg.from_user.id
    if user_id not in user_states:
        return

    qn = user_states[user_id]
    answer = msg.text.strip()
    if qn in [3, 4, 5]:
        if not is_number(answer):
            await msg.answer("Пожалуйста, введите только число (можно через точку или запятую).")
            return

    user_answers[user_id].append(answer)
    qn += 1

    if qn < len(QUESTIONS):
        user_states[user_id] = qn
        await msg.answer(QUESTIONS[qn])
    else:
        try:
            req_type_key = user_chosen_type.get(user_id)
            if not req_type_key:
                await msg.answer("Ошибка: не выбран тип заявки. Начните заново.")
                return
            req_type_text = REQUEST_TYPES[req_type_key]
            worksheet = await get_sheet_by_type(req_type_key)
            all_recs = await worksheet.get_all_values()
            row_number = len(all_recs) + 1

            username = msg.from_user.username or f"id{msg.from_user.id}"
            now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
            await worksheet.append_row([username, now, *user_answers[user_id]])

            text = build_request_text(username, now, user_answers[user_id], req_type_text)

            kb_exec = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="❗️Не выполнено", callback_data=f"done_{req_type_key}_{row_number}")]]
            )

            sent_msgs = []
            for exec_id in EXECUTOR_IDS:
                m = await bot.send_message(chat_id=exec_id, text=text, reply_markup=kb_exec)
                sent_msgs.append(m)

            sent = await msg.answer(
                f"Ваша заявка отправлена исполнителям:\n\n{text}",
                reply_markup=get_main_kb(user_id)
            )
            user_data = {
                "msg_id": sent.message_id,
                "answers": list(user_answers[user_id]),
                "username": username,
                "now": now,
                "type_key": req_type_key,
                "type_text": req_type_text
            }
            user_request_msgs.setdefault(user_id, dict())[row_number] = user_data

            for exec_id, exec_msg in zip(EXECUTOR_IDS, sent_msgs):
                exec_data = user_data.copy()
                exec_data["msg_id"] = exec_msg.message_id
                user_request_msgs.setdefault(exec_id, dict())[row_number] = exec_data

            await worksheet.update(f"A{row_number}", [["Выполняется"]])
        finally:
            user_states.pop(user_id, None)
            user_answers.pop(user_id, None)
            user_chosen_type.pop(user_id, None)

@dp.callback_query(lambda c: c.data and c.data.startswith("done_"))
async def executor_done(call: types.CallbackQuery):
    _, type_key, row_number = call.data.split("_")
    row_number = int(row_number)
    worksheet = await get_sheet_by_type(type_key)
    try:
        status_cell = await worksheet.acell(f"A{row_number}")
    except Exception:
        await call.answer("Ошибка доступа к таблице.", show_alert=True)
        return

    if status_cell.value == "Рассматривается":
        await call.answer("Эта заявка уже отмечена как выполненная.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
            text="✅Выполнено", 
            callback_data=f"done_{type_key}_{row_number}"
        )]])
    for exec_id in EXECUTOR_IDS:
        exec_entry = user_request_msgs.get(exec_id, {}).get(row_number)
        if exec_entry:
            try:
                await bot.edit_message_reply_markup(chat_id=exec_id, message_id=exec_entry['msg_id'], reply_markup=kb)
            except Exception:
                pass

    await worksheet.update(f"A{row_number}", [["Рассматривается"]])
    now_exec = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    await worksheet.update(f"K{row_number}", [[now_exec]])
    all_vals = await worksheet.get_all_values()
    header = all_vals[0]
    row_vals = all_vals[row_number - 1]
    field_map = dict(zip(header, row_vals))
    username = field_map.get("username") or field_map.get("Пользователь") or "неизвестно"

    user_id = None
    request_data = None
    for uid, rows in user_request_msgs.items():
        if row_number in rows and uid not in EXECUTOR_IDS and uid != OWNER_ID:
            user_id = uid
            request_data = rows[row_number]
            break

    if not user_id or not request_data:
        await call.answer("Не найден автор заявки.", show_alert=True)
        return

    msg_id = request_data['msg_id']
    await bot.send_message(
        chat_id=user_id,
        text="Сотрудник ОККР выполнил операцию, если собственник не согласится, будет проведена отмена",
        reply_to_message_id=msg_id,
        reply_markup=get_main_kb(user_id)
    )

    now = request_data['now']
    answers = request_data['answers']
    username = request_data['username']

    text = build_request_text(
        username,
        now,
        answers,
        request_data['type_text']
    ) + "\n\nОперация выполнена, согласны ли вы с этим решением?"
    kb_owner = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=f"owneryes_{type_key}_{row_number}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"ownerno_{type_key}_{row_number}"),
            ]
        ]
    )
    m = await bot.send_message(chat_id=OWNER_ID, text=text, reply_markup=kb_owner)
    owner_data = {
        "msg_id": m.message_id,
        "answers": answers,
        "username": username,
        "now": now,
        "type_text": request_data["type_text"],
        "type_key": request_data["type_key"]
    }
    user_request_msgs.setdefault(OWNER_ID, dict())[row_number] = owner_data
    await call.answer("Отправлено собственнику.")

@dp.callback_query(lambda c: c.data and c.data.startswith("owneryes_"))
async def owner_accept(call: types.CallbackQuery):
    _, type_key, row_number = call.data.split("_")
    row_number = int(row_number)
    worksheet = await get_sheet_by_type(type_key)
    await worksheet.update(f"A{row_number}", [["Одобрено"]])
    now_owner = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    await worksheet.update(f"L{row_number}", [[now_owner]])

    approved_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅Одобрено", callback_data="none")]]
    )
    try:
        msg_data = user_request_msgs.get(OWNER_ID, {}).get(row_number)
        if msg_data:
            await bot.edit_message_reply_markup(chat_id=OWNER_ID, message_id=msg_data["msg_id"], reply_markup=approved_kb)
    except Exception:
        pass

    author_id = None
    author_msg_id = None
    for uid, rows in user_request_msgs.items():
        if row_number in rows and uid not in (OWNER_ID, *EXECUTOR_IDS):
            author_id = uid
            author_msg_id = rows[row_number]["msg_id"]
            break

    if author_id:
        await bot.send_message(
            chat_id=author_id,
            text="Собственник одобрил данную операцию",
            reply_to_message_id=author_msg_id,
            reply_markup=get_main_kb(author_id)
        )

    for uid in list(user_request_msgs.keys()):
        if row_number in user_request_msgs[uid]:
            user_request_msgs[uid].pop(row_number)
        if f"cancel_{row_number}" in user_request_msgs[uid]:
            user_request_msgs[uid].pop(f"cancel_{row_number}")
        if not user_request_msgs[uid]:
            user_request_msgs.pop(uid)
    await call.answer("Заявка одобрена.")

@dp.callback_query(lambda c: c.data and c.data.startswith("ownerno_"))
async def owner_decline(call: types.CallbackQuery):
    _, type_key, row_number = call.data.split("_")
    row_number = int(row_number)
    worksheet = await get_sheet_by_type(type_key)
    now_owner = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    await worksheet.update(f"L{row_number}", [[now_owner]])

    declined_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌Отказано", callback_data="none")]]
    )
    try:
        msg_data = user_request_msgs.get(OWNER_ID, {}).get(row_number)
        if msg_data:
            await bot.edit_message_reply_markup(chat_id=OWNER_ID, message_id=msg_data["msg_id"], reply_markup=declined_kb)
    except Exception:
        pass

    for exec_id in EXECUTOR_IDS:
        exec_data = user_request_msgs.get(exec_id, {}).get(row_number)
        kb_cancel = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❗️Не отменено", callback_data=f"notcanceled_{type_key}_{row_number}")]
            ]
        )
        if exec_data:
            try:
                m = await bot.send_message(
                    chat_id=exec_id,
                    text="Собственник отказал данной заявке, пожалуйста проведите отмену",
                    reply_to_message_id=exec_data["msg_id"],
                    reply_markup=kb_cancel
                )
                user_request_msgs[exec_id][f"cancel_{row_number}"] = m.message_id
            except Exception:
                pass
    await call.answer("Ожидание отмены исполнителем.")

@dp.callback_query(lambda c: c.data and c.data.startswith("notcanceled_"))
async def executor_cancel(call: types.CallbackQuery):
    _, type_key, row_number = call.data.split("_")
    row_number = int(row_number)
    worksheet = await get_sheet_by_type(type_key)
    await worksheet.update(f"A{row_number}", [["Отказано"]])
    kb_done = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅Отменено", callback_data=f"notcanceled_{type_key}_{row_number}")]
        ]
    )
                                                    
    for exec_id in EXECUTOR_IDS:
        cancel_msg_id = user_request_msgs.get(exec_id, {}).get(f"cancel_{row_number}")
        if cancel_msg_id:
            try:
                await bot.edit_message_reply_markup(chat_id=exec_id, message_id=cancel_msg_id, reply_markup=kb_done)
            except Exception:
                pass
    
    owner_msg_data = user_request_msgs.get(OWNER_ID, {}).get(row_number)
    if owner_msg_data:
        try:
            await bot.edit_message_reply_markup(
                chat_id=OWNER_ID,
                message_id=owner_msg_data["msg_id"],
                reply_markup=kb_done
            )
        except Exception:
            pass

    author_id = None
    author_msg_id = None
    for uid, rows in user_request_msgs.items():
        if row_number in rows and uid not in (*EXECUTOR_IDS, OWNER_ID):
            author_id = uid
            author_msg_id = rows[row_number]["msg_id"]
            break
    if author_id:
        await bot.send_message(
            chat_id=author_id,
            text="Собственник отказал данной операции, операция уже отменена",
            reply_to_message_id=author_msg_id,
            reply_markup=get_main_kb(author_id)
        )

    for uid in list(user_request_msgs.keys()):
        if row_number in user_request_msgs[uid]:
            user_request_msgs[uid].pop(row_number)
        if f"cancel_{row_number}" in user_request_msgs[uid]:
            user_request_msgs[uid].pop(f"cancel_{row_number}")
        if not user_request_msgs[uid]:
            user_request_msgs.pop(uid)
    await call.answer("Отмена подтверждена.")

@dp.errors()
async def err_handler(event: ErrorEvent):
    logging.exception(event.exception)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())