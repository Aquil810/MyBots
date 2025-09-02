import os, asyncio, gspread
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.media_group import MediaGroupBuilder
from google.oauth2.service_account import Credentials

TOKEN = 'BotToken'
GSPREAD_JSON = r'credentials.json'
SHEET_ID = 'GS_ID'
SHEET_NAME = 'Нарушения'
PHOTO_SAVE_FOLDER = r'Applications'

bot = Bot(
    token=TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

os.makedirs(PHOTO_SAVE_FOLDER, exist_ok=True)

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']
creds = Credentials.from_service_account_file(GSPREAD_JSON, scopes=scope)
worksheet = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)

questions = [
    {"q": "Выберите сотрудника, выявившего нарушения:", "answers": [
            "Сотрудник1","Сотрудник2","Сотрудник3","Сотрудник4",]},
    {"q": "Выберите подразделение, где обнаружено нарушение:","answers": [
            "Парк1","Парк2","Парк3","Парк4","Парк5","Парк6","Парк7","Парк8","Парк9","Парк10","Парк11","Парк12","Парк13","Парк14","Парк15"]},
    {"q": "Выберите отдел, где обнаружено нарушение","answers": [
            "Отдел1","Отдел2","Отдел3","Отдел4","Отдел5","Отдел6"]},
    {"q": "Выберите тип выявленного нарушения","answers": [
            "Тип1","Тип2","Тип3","Тип4","Тип5","Тип6","Тип7","Тип8"]}]
Q5 = "Приведите, пожалуйста, описание нарушения, пишите кратко и ясно, не забудьте обязательно указать дату нарушения и виновника:"
Q6 = "Прикрепите фото/видео-доказательство или нажмите 'Пропустить'."

TRIGGERS = {"добавить нарушение", "новое нарушение", "внести нарушение", "начать", "старт"}

class Form(StatesGroup):
    q1 = State()
    q2 = State()
    q3 = State()
    q4 = State()
    q5 = State()
    q6 = State()
    confirm = State()

def kb_answers(idx):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=ans)] for ans in questions[idx]["answers"]],
        resize_keyboard=True,
        one_time_keyboard=True)

def main_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Добавить нарушение")]], resize_keyboard=True)

def confirm_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Да"), KeyboardButton(text="Нет")]],
        resize_keyboard=True, one_time_keyboard=True)

def skip_media_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Пропустить")]],
        resize_keyboard=True, one_time_keyboard=True)

def generate_media_filename(file_type, dt=None, idx=None):
    if dt is None:
        dt = datetime.now()
    idx_str = f"_{idx}" if idx is not None else ""
    return f"{file_type}_{dt.strftime('%d.%m.%Y_%H.%M.%S')}{idx_str}.{'jpg' if file_type == 'photo' else 'mp4'}"

def determine_manager(park, dept):
    parks_tver = {'Парк9','Парк10', 'Парк11', 'Парк12'}
    parks_volgograd = {'Парк13', 'Парк14', 'Парк15'}
    if park in parks_tver:
        return 'Руководитель14'
    if park in parks_volgograd:
        return 'Руководитель15'
    dept_managers = {
        'Отдел2': 'Руководитель9',
        'Отдел3': 'Руководитель10',
        'Отдел4': 'Руководитель11',
        'Отдел5': 'Руководитель12',
        'Отдел6': 'Руководитель13'
    }
    if dept in dept_managers:
        return dept_managers[dept]
    park_managers = {
        'Парк1': 'Руководитель1',
        'Парк2': 'Руководитель2',
        'Парк3': 'Руководитель3',
        'Парк4': 'Руководитель4',
        'Парк5': 'Руководитель5',
        'Парк6': 'Руководитель6',
        'Парк7': 'Руководитель7',
        'Парк8': 'Руководитель8'
    }
    if park in park_managers:
        return park_managers[park]
    return "Не определён"

@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Этот бот предназначен для автоматической фиксации нарушений.\n"
        "Для фиксации нарушения нажмите кнопку <b>Добавить нарушение</b>.",
        reply_markup=main_menu_kb())

@dp.message(lambda m: m.text and m.text.lower().strip() in TRIGGERS)
async def begin(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(questions[0]['q'], reply_markup=kb_answers(0))
    await state.set_state(Form.q1)

def handle_q(idx, next_state):
    async def inner(message: types.Message, state: FSMContext):
        if message.text not in questions[idx]['answers']:
            return await message.answer("Пожалуйста, выберите вариант кнопкой ниже.", reply_markup=kb_answers(idx))
        await state.update_data(**{f'q{idx+1}': message.text})
        if idx + 1 < len(questions):
            await message.answer(questions[idx + 1]['q'], reply_markup=kb_answers(idx + 1))
            await state.set_state(next_state)
        else:
            await message.answer(Q5, reply_markup=ReplyKeyboardRemove())
            await state.set_state(Form.q5)
    return inner

@dp.message(Form.q1)
async def handle_q1(message: types.Message, state: FSMContext):
    await handle_q(0, Form.q2)(message, state)

@dp.message(Form.q2)
async def handle_q2(message: types.Message, state: FSMContext):
    await handle_q(1, Form.q3)(message, state)

@dp.message(Form.q3)
async def handle_q3(message: types.Message, state: FSMContext):
    await handle_q(2, Form.q4)(message, state)

@dp.message(Form.q4)
async def handle_q4(message: types.Message, state: FSMContext):
    await handle_q(3, Form.q5)(message, state)


@dp.message(Form.q5)
async def handle_q5(message: types.Message, state: FSMContext):
    await state.update_data(q5=message.text)
    await message.answer(Q6, reply_markup=skip_media_kb())
    await state.set_state(Form.q6)

async def save_file(file, file_type, idx=None):
    now = datetime.now()
    filename = generate_media_filename(file_type, now, idx)
    path = os.path.join(PHOTO_SAVE_FOLDER, filename)
    file_info = await bot.get_file(file.file_id)
    await bot.download_file(file_info.file_path, destination=path)
    return path, filename

@dp.message(Form.q6, F.media_group_id)
async def handle_album(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    media_group_id = message.media_group_id
    key = f"{chat_id}:{media_group_id}"
    data = await state.get_data()
    abuf = data.get('media_album_buffer', {})
    abuf.setdefault(key, []).append(message)
    await state.update_data(media_album_buffer=abuf)

    await asyncio.sleep(2.5)
    abuf = (await state.get_data()).get('media_album_buffer', {})
    messages = abuf.get(key, [])
    if len(messages) > 1:
        files = data.get('files', [])
        now = datetime.now()
        for idx, m in enumerate(messages):
            if m.photo:
                file = m.photo[-1]
                path, filename = await save_file(file, 'photo', idx)
                files.append({'file_path': path, 'file_type': 'photo', 'filename': filename})
            elif m.video:
                file = m.video
                path, filename = await save_file(file, 'video', idx)
                files.append({'file_path': path, 'file_type': 'video', 'filename': filename})
        await state.update_data(files=files)
        abuf.pop(key, None)
        await state.update_data(media_album_buffer=abuf)
        await message.answer("Фото/видео из альбома добавлены! Можете отправить ещё или нажмите Пропустить.", reply_markup=skip_media_kb())

@dp.message(Form.q6, F.photo)
async def get_photo(message: types.Message, state: FSMContext):
    file = message.photo[-1]
    path, filename = await save_file(file, 'photo')
    data = await state.get_data()
    files = data.get('files', [])
    files.append({'file_path': path, 'file_type': 'photo', 'filename': filename})
    await state.update_data(files=files)
    await message.answer(
        "Фото добавлено! Можете отправить ещё или нажмите кнопку Пропустить.",
        reply_markup=skip_media_kb()
    )

@dp.message(Form.q6, F.video)
async def get_video(message: types.Message, state: FSMContext):
    file = message.video
    path, filename = await save_file(file, 'video')
    data = await state.get_data()
    files = data.get('files', [])
    files.append({'file_path': path, 'file_type': 'video', 'filename': filename})
    await state.update_data(files=files)
    await message.answer(
        "Видео добавлено! Можете отправить ещё или нажмите кнопку Пропустить.",
        reply_markup=skip_media_kb()
    )

@dp.message(Form.q6, lambda m: m.text and m.text.lower() == "пропустить")
async def skip_media(message: types.Message, state: FSMContext):
    await confirm_state(message, state)

@dp.message(Form.q6)
async def ask_media(message: types.Message, state: FSMContext):
    await message.answer(
        "Пожалуйста, отправьте фото/видео, либо нажмите кнопку Пропустить.",
        reply_markup=skip_media_kb()
    )

async def confirm_state(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])
    text = (
        f"<b>Сотрудник:</b> {data.get('q1')}\n"
        f"<b>Подразделение:</b> {data.get('q2')}\n"
        f"<b>Отдел:</b> {data.get('q3')}\n"
        f"<b>Тип:</b> {data.get('q4')}\n"
        f"<b>Описание:\n</b> {data.get('q5')}\n"
        f"<b>Фото/видео:</b> {'Нет приложений' if not files else 'Приложены выше'}\n\n"
        "Вы подтверждаете внесённую информацию?"
    )

    if files:
        media_group = []
        for f in files:
            input_file = FSInputFile(f['file_path'])
            if f['file_type'] == 'photo':
                media_group.append(InputMediaPhoto(media=input_file))
            else:
                media_group.append(InputMediaVideo(media=input_file))
        for i in range(0, len(media_group), 10):
            chunk = media_group[i:i + 10]
            await message.answer_media_group(chunk)
    await message.answer(
        text,
        reply_markup=confirm_kb(),
        parse_mode="HTML"
    )
    await state.set_state(Form.confirm)

@dp.message(Form.confirm, lambda m: m.text and m.text.lower() == "нет")
async def cancel_form(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Операция отменена.", reply_markup=main_menu_kb())

@dp.message(Form.confirm, lambda m: m.text and m.text.lower() == "да")
async def save_to_gsheet(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])
    file_names = '\n'.join(os.path.basename(f['file_path']) for f in files) if files else "Нет приложений"

    park = data.get('q2','')
    dept = data.get('q3','')
    manager = determine_manager(park, dept)

    new_row = [
        "Заявка создана", datetime.now().strftime("%d.%m.%Y"),
        data.get('q1',''), park, dept, data.get('q4',''), data.get('q5',''), file_names, manager
    ]
    worksheet.append_row(new_row, value_input_option="RAW")
    await message.answer("Нарушение добавлено!", reply_markup=main_menu_kb())
    await state.clear()

@dp.message()
async def fallback(message: types.Message, state: FSMContext):
    st = await state.get_state()
    if not st:
        await message.answer("Для начала нажмите 'Добавить нарушение'.", reply_markup=main_menu_kb())
    else:
        await message.answer("Пожалуйста, ответьте на текущий вопрос.")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())