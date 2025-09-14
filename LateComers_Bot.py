import re, sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Message
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

BOT_TOKEN = "Bot_Token"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

def init_db():
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS late_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        date TEXT NOT NULL,
        minutes_late INTEGER NOT NULL
    )""")
    conn.commit()
    conn.close()

def cleanup_old():
    cutoff = (datetime.now() - timedelta(days=62)).date().isoformat()
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM late_notes WHERE date < ?", (cutoff,))
    conn.commit()
    conn.close()

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Внести опоздание")],
            [KeyboardButton(text="Статистика опозданий")],
            [KeyboardButton(text="Удалить опоздание")],
        ],
        resize_keyboard=True,
    )
    
class Form(StatesGroup):
    waiting_for_late_note = State()
    statistic_wait_name = State()

class DeleteNote(StatesGroup):
    waiting_for_name = State()
    waiting_for_date = State()

def normalize_name(name: str) -> str:
    name = re.sub(r'\s+', ' ', name.strip())
    name = name.lower()
    name = name.title()
    return name

def parse_time(text):
    """Возвращает минуты задержки, если найдено (целое число), иначе None"""
    text = text.lower()
    minutes = 0

    hours = re.search(r'(\d+)\s*(?:час|ч)\w*', text)
    mins = re.search(r'(\d+)\s*(?:минут|м)\w*', text)
    if hours:
        minutes += int(hours.group(1)) * 60
    if mins:
        minutes += int(mins.group(1))
    if not (hours or mins):
        try:
            minutes = int(text.strip())
        except Exception:
            return None
    return minutes if minutes > 0 else None

def parse_late_note(text: str):
    if text.count('\n') != 2:
        return None, None, None
    name, date_str, time_str = text.split('\n')
    name = normalize_name(name)
    date_str = date_str.strip()
    try:
        if '.' in date_str:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        elif '-' in date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            return None, None, None
    except Exception:
        return None, None, None
    minutes = parse_time(time_str.strip())
    if minutes is None:
        return None, None, None
    return name, date_obj.isoformat(), minutes

def delete_late_note_by_id(row_id):
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM late_notes WHERE id=?", (row_id,))
    conn.commit()
    conn.close()

@dp.startup()
async def on_start(*args, **kwargs):
    print('Бот запущен!')

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Привет! Я бот учёта опозданий сотрудников. Я умею:\n"
        "• Принимать записи об опозданиях (кнопка Внести опоздание)\n"
        "• Показывать сводную статистику (кнопка Статистика опозданий)\n"
        "• Удалять добавленные опоздания (кнопка Удалить опоздание)\n",
        reply_markup=main_kb()
    )

@dp.message(F.text.in_(["Внести опоздание", "внести опоздание", "Опоздание", "опоздание"]))
async def ask_late_note(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите информацию по шаблону:\n"
        "Фамилия Имя сотрудника\nДата опоздания (например, 2025-03-20 или 20.03.2025)\nВремя опоздания (например, 2 часа 7 минут или 3 минуты)",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.waiting_for_late_note)

@dp.message(Form.waiting_for_late_note)
async def process_late_note(message: types.Message, state: FSMContext):
    name, date_str, minutes = parse_late_note(message.text)
    if None in (name, date_str, minutes):
        await message.answer(
            "⚠️ Формат не распознан. Попробуйте ещё раз по шаблону:\n"
            "Фамилия Имя сотрудника\nДата опоздания (например, 20.03.2025)\nВремя опоздания (например, 2 часа 7 минут или 3 минуты)"
        )
        return
    cleanup_old()
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO late_notes (name, date, minutes_late) VALUES (?, ?, ?)",
        (name, date_str, minutes)
    )
    conn.commit()
    conn.close()
    await message.answer("Опоздание успешно внесено!", reply_markup=main_kb())
    await state.clear()

@dp.message(F.text.in_(["Статистика опозданий", "статистика опозданий", "Стата", "стата"]))
async def ask_stat_person(message: types.Message, state: FSMContext):
    cleanup_old()
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM late_notes")
    persons = [row[0] for row in cursor.fetchall()]
    conn.close()
    if not persons:
        await message.answer("Пока нет данных об опозданиях.", reply_markup=main_kb())
        return
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=p)] for p in persons],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Выберите сотрудника:", reply_markup=kb)
    await state.set_state(Form.statistic_wait_name)

@dp.message(Form.statistic_wait_name)
async def get_stat_for_name(message: types.Message, state: FSMContext):
    name = normalize_name(message.text)
    cleanup_old()
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT date, minutes_late FROM late_notes WHERE name=? ORDER BY date",
        (name,)
    )
    notes = cursor.fetchall()
    conn.close()
    if not notes:
        await message.answer("Нет данных об опозданиях у этого сотрудника.", reply_markup=main_kb())
        await state.clear()
        return
    total = sum([x[1] for x in notes])
    msg = f"Статистика опозданий для: {name} (за 2 месяца)\n\n"
    for d, m in notes:
        try:
            formatted_date = datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            formatted_date = d
        msg += f"• {formatted_date}: {m} мин.\n"
    hours, minutes = divmod(total, 60)
    total_str = f"{hours} часов {minutes} минут" if hours > 0 else f"{minutes} мин."
    msg += f"\nВсего опозданий: {len(notes)}\nСуммарно: {total_str}"
    await message.answer(msg, reply_markup=main_kb())
    await state.clear()

@dp.message(lambda m: m.text.lower() in ["удалить опоздание"])
async def ask_delete_name(message: types.Message, state: FSMContext):
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM late_notes")
    persons = [row[0] for row in cursor.fetchall()]
    conn.close()
    if not persons:
        await message.answer("Нет данных для удаления.", reply_markup=main_kb())
        return
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=p)] for p in persons],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await state.set_state(DeleteNote.waiting_for_name)
    await message.answer("Для кого удалить опоздание?", reply_markup=kb)

@dp.message(DeleteNote.waiting_for_name)
async def ask_delete_date(message: types.Message, state: FSMContext):
    name = normalize_name(message.text)
    conn = sqlite3.connect("late_notes.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, date, minutes_late FROM late_notes WHERE name=? ORDER BY date", (name,))
    results = cursor.fetchall()
    conn.close()
    if not results:
        await message.answer("У этого сотрудника нет опозданий.", reply_markup=main_kb())
        await state.clear()
        return

    date_btn_to_id = {}
    kb = []
    for row in results:
        date_h = datetime.strptime(row[1], '%Y-%m-%d').strftime('%d.%m.%Y')
        btn_text = f"{date_h} ({row[2]} мин.)"
        date_btn_to_id[btn_text] = row[0]
        kb.append([KeyboardButton(text=btn_text)])
    await state.update_data(date_btn_to_id=date_btn_to_id)
    await state.set_state(DeleteNote.waiting_for_date)
    await message.answer("Выберите дату опоздания для удаления:", reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=True))

@dp.message(DeleteNote.waiting_for_date)
async def confirm_delete(message: types.Message, state: FSMContext):
    data = await state.get_data()
    date_btn_to_id = data.get("date_btn_to_id", {})
    btn_text = message.text.strip()
    row_id = date_btn_to_id.get(btn_text)
    if not row_id:
        await message.answer("Ошибка. Не удалось найти запись для удаления.", reply_markup=main_kb())
        await state.clear()
        return
    delete_late_note_by_id(row_id)
    await message.answer("Опоздание удалено!", reply_markup=main_kb())
    await state.clear()
        
@dp.message(~F.via_bot)
async def fallback(message: types.Message):
    await message.answer(
        'Используйте кнопки ниже или команды:\n'
        '• "Внести опоздание"\n'
        '• "Статистика опозданий"\n'
        '• "Удалить опоздание"',
        reply_markup=main_kb()
    )

if __name__ == "__main__":
    import asyncio
    init_db()
    cleanup_old()
    asyncio.run(dp.start_polling(bot))


