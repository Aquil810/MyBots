import os, datetime, asyncio, logging, uuid
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command, Filter
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ErrorEvent, FSInputFile, InputMediaPhoto, InputMediaVideo
from google.oauth2.service_account import Credentials
from gspread_asyncio import AsyncioGspreadClientManager
from collections import defaultdict

BOT_TOKEN = 'BOT_TOKEN'
dp = Dispatcher()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
logging.basicConfig(level=logging.INFO)
PHOTO_SAVE_DIR = os.path.join(os.path.dirname(__file__), "Applications")
os.makedirs(PHOTO_SAVE_DIR, exist_ok=True)

SPREADSHEET_ID = 'ID гугл-таблицы'
def get_creds(): return Credentials.from_service_account_file(r"C:\Users\Администратор\Desktop\bots\3StepBot\3step_credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
agcm = AsyncioGspreadClientManager(get_creds)
async def get_sheet_by_type(type_key: str):
    agc = await agcm.authorize()
    ws = await (await agc.open_by_key(SPREADSHEET_ID)).worksheet(FORMS_CONFIG[type_key]['title'])
    return ws
async def append_row_with_notify(ws, row, chat_id):
    try: await ws.append_row(row)
    except Exception as e:
        await bot.send_message(chat_id, f"⚠️ Ошибка при записи в Google-таблицу: {e}")
        logging.exception(e)

park_options = ["Парк1", "Парк2", "Парк3", "Парк4"]
FORMS_CONFIG = {
    'action': {
        'title': "Акция",
        'fields': [{"field": "fio_vod", "question": "Введите ФИО водителя", "label": "ФИО водителя", "type": "text"},
            {"field": "prevpark", "question": "Выберите предыдущий парк, где работал водитель", "label": "Предыдущий парк", "type": "inline", "options": park_options},
            {"field": "nowpark", "question": "Выберите текущий парк, куда устраивается водитель", "label": "Текущий парк", "type": "inline", "options": park_options},
            {"field": "stazh", "question": "Введите общий стаж работы (только число дней)", "label": "Общий стаж", "type": "text"},
            {"field": "nowstazh", "question": "Введите текущий стаж работы (только число дней)", "label": "Текущий стаж", "type": "text"},
            {"field": "dolg", "question": "Введите общий долг (только число)", "label": "Общий долг", "type": "text"},
            {"field": "actions", "question": "Напишите были ли у водителя акции ранее?", "label": "Ранние акции", "type": "text"},
            {"field": "autoclass", "question": "Выберите желаемый класс авто", "label": "Желаемый класс авто", "type": "inline", "options": ["Эконом", "Комфорт", "Комфорт+", "Бизнес"]},
            {"field": "price","question": "Введите желаемую арендную ставку (только число)", "label": "Желаемая арендная ставка", "type": "text"},
            {"field": "photo", "question": "Прикрепите фото ВУ (только одно)", "label": "Фото ВУ", "type": "photo"},],
        'order': ["fio_vod", "prevpark", "nowpark", "stazh", "nowstazh", "dolg", "actions", "autoclass", "price", "photo"],
        'preambule': "Для отправления заявки по акции нужно поочерёдно отправить ответы на эти вопросы о водителе:\n\n"},
    'damage': {
        'title': "Скидка",
        'fields': [{"field": "fio_vod", "question": "Введите ФИО водителя", "label": "ФИО водителя", "type": "text"},
            {"field": "ts_number", "question": "Введите номер ТС", "label": "Номер ТС", "type": "text"},
            {"field": "installment_link", "question": "Введите ссылку на рассрочку", "label": "Ссылка на рассрочку", "type": "text", "skippable": True},
            {"field": "situation", "question": "Введите описание ситуации", "label": "Описание ситуации", "type": "text", "skippable": True},
            {"field": "init_amount", "question": "Введите изначальную сумму рассрочки (только число)", "label": "Изначальная сумма рассрочки", "type": "text", "skippable": True},
            {"field": "desired_amount", "question": "Введите желаемую сумму скидки (только число)", "label": "Желаемая сумма скидки", "type": "text", "skippable": True},
            {"field": "media", "question": "Прикрепите фото/видео повреждений", "label": "Фото/видео повреждений", "type": "media"},],
        'order': ["fio_vod", "ts_number", "installment_link", "situation", "init_amount", "desired_amount", "note", "media"],
        'preambule': "Для отправления заявки по повреждениям нужно поочерёдно отправить ответы на эти вопросы о водителе:\n\n"}}

for k, conf in FORMS_CONFIG.items(): conf['fields_by_name'] = {f['field']: f for f in conf['fields']}

def get_form_conf(form_type):
    conf = FORMS_CONFIG[form_type]
    return {"FIELDS": conf["fields"], "FIELDS_BY_NAME": conf["fields_by_name"], "row_gen": lambda dtstr, username, fields: build_row(dtstr, username, fields, conf["order"], conf["fields_by_name"], special_fields={"media"} if form_type == "damage" else None), "value_present": build_value, "shutter_text": lambda *a, **k: get_shutter_text(form_type, *a, **k), "title": conf["title"], "preambule": conf.get("preambule", "")}

user_info_msgs, user_form_states, user_chosen_type = {}, {}, {}
damage_media_group_cache, media_group_batches = defaultdict(list), defaultdict(list)
media_group_to_user = dict()

def is_number_with_sign(x):
    x = x.replace(',', '.').replace(' ', '').replace('₽', '')
    for suffix in ('дней', 'дня', 'день'): 
        if x.endswith(suffix): x = x[:-len(suffix)]
    try:
        float(x)
        return True
    except Exception: return False

def build_value(field, value, fields_by_name, silent=True):
    value = value.strip() if isinstance(value, str) else value
    scheme = fields_by_name.get(field, {})
    label = scheme.get("label", "")
    ftype = scheme.get("type", "")
    skippable = scheme.get("skippable", False)
    numbers = ("stazh", "nowstazh", "dolg", "price", "init_amount", "desired_amount")
    plural = lambda n: "день" if abs(n)%10==1 and abs(n)%100!=11 else ("дня" if 2<=abs(n)%10<=4 and not 12<=abs(n)%100<=14 else "дней")
    if (not value or value == label) and ftype != "media": return value, None
    if field in numbers:
        if not is_number_with_sign(str(value)): return (None, "Введите только число (можно через точку/запятую)") if not silent else (value, None)
        n = float(str(value).replace(',', '.').replace('₽', '').replace(' ', '').replace('дней', '').replace('дня', '').replace('день',''))
        if field in {"stazh", "nowstazh"}:
            n = int(n)
            return f"{n} {plural(n)}", None
        return f"{n:g} ₽", None
    if skippable and value == "Пропущено": return "Пропущено", None
    return value, None

def get_shutter_text(form_type, user_id, user_fields, username, mode='starting', dtstr=None):
    conf = get_form_conf(form_type)
    F, FN = conf["FIELDS"], conf["FIELDS_BY_NAME"]
    lines = [{'starting': conf["preambule"],'processing': f"Заявка от {username}\n{conf['title']}\n\n",'ending': f"Заявка {dtstr} от {username}\n{conf['title']}\n\n"}.get(mode, f"Заявка от {username}\n{conf['title']}\n\n")]
    for item in F:
        label, field = item["label"], item["field"]
        raw = user_fields.get(field, "")
        pretty = build_value(field, raw, FN)[0]
        if form_type == "action":
            if field == "photo":
                if pretty and pretty != label and pretty != 'Пропущено': lines.append(f"{label}: Приложено выше")
                elif pretty == "Пропущено": lines.append(f"{label}: Пропущено")
                else: lines.append(label)
            else: lines.append(f"{label}: {pretty}" if pretty and pretty != label else label)
        elif form_type == "damage":
            if field == "media":
                state = user_form_states.get(user_id, {})
                if user_fields.get('media') == 'Пропущено': lines.append(f"{label}: Пропущено")
                else:
                    cnt = len(state.get("media_files", []))
                    if cnt:
                        plural = lambda n: "файл" if n % 10 == 1 and n % 100 != 11 else ("файла" if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14 else "файлов")
                        lines.append(f"{label}: {cnt} {plural(cnt)}")
                    else: lines.append(label)
            else:
                if pretty and pretty != label and pretty != "Пропущено": lines.append(f"{label}: {pretty}")
                elif pretty == "Пропущено": lines.append(f"{label}: Пропущено")
                else: lines.append(label)
    return "\n".join(lines)

def build_row(dtstr, username, fields, order, fields_by, special_fields=None):
    def get(key): return build_value(key, fields.get(key, ""), fields_by)[0]
    row = ["Создано", dtstr, username]
    for key in order:
        if special_fields and key in special_fields:
            val = fields.get(key, "")
            if isinstance(val, list): cell = "\n".join(os.path.basename(f) for f in val)
            else:
                items = [
                    x.strip() for x in val.split(";")
                    if x.strip() and not any(substr in x for substr in (fields_by.get(key, {}).get('label') or '', "Фото","Видео"))]
                if val == "Пропущено": cell = "Пропущено"
                else: cell = "\n".join(items)
            row.append(cell)
        else: row.append(get(key))
    return row

def get_next_field(state): step = state.get("step", 0); f = get_form_conf(state["form_type"])["FIELDS"]; return f[step] if step < len(f) else None
def get_user_form_type(user_id): return user_chosen_type.get(user_id)
def get_form_fields(user_id): return user_form_states[user_id]["fields"]
def get_user_form_state(user_id): return user_form_states.get(user_id)
def build_inline_kbd(options, prefix, selected=None): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=("✅ " if selected == opt else "")+opt, callback_data=f"{prefix}:{opt}")] for opt in options])
def is_skippable(fs): return fs.get("skippable", False)
def get_skip_kbd(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Пропустить", callback_data='damage_skip')]])
def get_confirm_kbd(form_type): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Завершить", callback_data=f"{form_type}_finish"),InlineKeyboardButton(text="❌ Удалить", callback_data=f"{form_type}_delete"),]])
def get_main_kb(_): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Добавить")]], resize_keyboard=True)
def get_type_select_kb():
    t = [("action", FORMS_CONFIG["action"]["title"]),("damage", FORMS_CONFIG["damage"]["title"])]
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label, callback_data=f"req_type_{key}") for key, label in t]])
def get_author_username(x): u = getattr(x, "from_user", None); return f"@{u.username}" if u and u.username else (str(u.id) if u else "unknown")
async def try_delete(chat_id, msg_id):
    try: await bot.delete_message(chat_id, msg_id)
    except: pass

async def wipe_messages(user_id, chat_id, clear_only=False):
    keys = ("user_info_msgs", "questions_msgs", "answers_msgs", "error_msgs")
    state = user_form_states.get(user_id)
    for key in keys:
        ids = user_info_msgs.get(user_id, []) if key == "user_info_msgs" else (state or {}).get(key, [])
        if not clear_only and ids: 
            for mid in (ids if isinstance(ids, list) else [ids]): await try_delete(chat_id, mid)
        if key == "user_info_msgs": user_info_msgs[user_id] = []
        elif state: state[key] = [] if isinstance(ids, list) else None

async def remove_shutter_msgs(user_id, chat_id, keep_damage_media=False):
    state = user_form_states.get(user_id)
    if not state: return
    for arr in ["shutter_id", "shutter_media_msgs", "questions_msgs", "answers_msgs", "error_msgs"]:
        v = state.get(arr)
        if v is not None and not (keep_damage_media and arr in ("shutter_id", "shutter_media_msgs")):
            for mid in (v if isinstance(v, list) else [v]): await try_delete(chat_id, mid)
            state[arr]=[] if isinstance(v, list) else None    

async def send_or_edit_shutter(user_id, chat_id, conf, state, dtstr=None, mode='processing'):
    txt = conf["shutter_text"](user_id, state["fields"], state.get("req_username"), mode, dtstr)
    sid = state.get("shutter_id")
    if sid:
        try: await bot.edit_message_text(chat_id=chat_id, message_id=sid, text=txt)
        except: m = await bot.send_message(chat_id, txt); state["shutter_id"] = m.message_id
    else: state["shutter_id"] = (await bot.send_message(chat_id, txt)).message_id

async def clear_old_shutters_and_confirmations(user_id, chat_id, state):
    for arr in ["shutter_id", "shutter_media_msgs", "questions_msgs", "answers_msgs", "error_msgs"]:
        v = state.get(arr); 
        if v: [await try_delete(chat_id, mid) for mid in (v if isinstance(v, list) else [v])]
        state[arr]=[] if isinstance(v, list) else None

async def show_media_group(chat_id, files, caption, finish_text, kb, state, store_id_field="shutter_media_msgs", only_media_group=False):
    for mid in (state.get(store_id_field) or []): await try_delete(chat_id, mid)
    state[store_id_field]=[]
    media = []
    for i, path in enumerate(files):
        fn = path.lower()
        m = None
        if fn.endswith(('.jpg', '.jpeg', '.png')): m = InputMediaPhoto(media=FSInputFile(path), caption=caption if i == 0 else None)
        elif fn.endswith('.mp4'): m = InputMediaVideo(media=FSInputFile(path), caption=caption if i == 0 else None)
        if m: media.append(m)
    sent=[]
    if media:
        try: sent = [m.message_id for m in await bot.send_media_group(chat_id, media)]
        except Exception: pass
    else: sent = [(await bot.send_message(chat_id, caption or "Ошибка при формировании медиагруппы.", reply_markup=kb)).message_id]
    if not only_media_group and finish_text: sent.append((await bot.send_message(chat_id, finish_text, reply_markup=kb)).message_id)
    if store_id_field: state[store_id_field]=sent

@dp.errors()
async def err_handler(event: ErrorEvent): logging.exception(event.exception)
@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    m = await msg.answer("Привет! Чтобы подать заявку, нажмите кнопку 'Добавить' ниже.", reply_markup=get_main_kb(msg.from_user.id))
    user_info_msgs[msg.from_user.id]=[m.message_id]

@dp.message(F.text=="Добавить")
async def add_request(msg: types.Message):
    await wipe_messages(msg.from_user.id, msg.chat.id)
    await try_delete(msg.chat.id, msg.message_id)
    user_form_states.pop(msg.from_user.id, None)
    user_chosen_type.pop(msg.from_user.id, None)
    menu = await msg.answer("Какую заявку вы хотите сделать?", reply_markup=get_type_select_kb())
    user_info_msgs[msg.from_user.id] = [menu.message_id]

@dp.callback_query(lambda c: c.data and c.data.startswith("req_type_"))
async def cb_choose_type(call: types.CallbackQuery):
    t = call.data.split("_")[2]; u, c = call.from_user.id, call.message.chat.id
    username = get_author_username(call)
    user_chosen_type[u] = t
    await wipe_messages(u, c)
    conf = get_form_conf(t)
    init_vals = {f["field"]: f["label"] for f in conf["FIELDS"]}
    state = {"form_type": t, "step": 0, "fields": init_vals.copy(), "shutter_id": None, "questions_msgs": [], "answers_msgs": [], "error_msgs": [], "req_username": username}
    user_form_states[u]=state
    shutter_msg = await call.message.answer(conf["shutter_text"](u, init_vals, username, mode='starting'))
    state["shutter_id"]=shutter_msg.message_id
    user_info_msgs[u]=[]
    await show_next_question(u, c)
    await call.answer()

async def show_next_question(user_id, chat_id):
    state = user_form_states.get(user_id)
    if not state: return
    conf = get_form_conf(state["form_type"])
    step = state.get("step", 0)
    F = conf["FIELDS"]
    if step >= len(F):
        await finish_form(user_id, chat_id)
        return
    fs = F[step]
    markup = None
    if fs["type"] == "inline": markup = build_inline_kbd(fs["options"], fs["field"], selected=state["fields"].get(fs["field"]))
    elif fs["type"] == "text" and is_skippable(fs): markup = get_skip_kbd()
    elif state["form_type"] == "damage" and fs["type"] == "media":
        tp = state["form_type"]
        if state.get("media_files"): markup = get_confirm_kbd(tp)
        else: markup = None
    elif fs["type"] == "media": markup = get_skip_kbd()
    m = await bot.send_message(chat_id, fs["question"], reply_markup=markup)
    state["questions_msgs"].append(m.message_id)

@dp.message(lambda m: user_chosen_type.get(m.from_user.id) and user_form_states.get(m.from_user.id) and get_next_field(user_form_states[m.from_user.id]) and get_next_field(user_form_states[m.from_user.id])["type"]=="text")
async def process_text_answer(msg: types.Message):
    uid, cid = msg.from_user.id, msg.chat.id; state = user_form_states[uid]; conf=get_form_conf(state["form_type"]); fs = get_next_field(state); val, err = build_value(fs["field"], msg.text.strip(), conf["FIELDS_BY_NAME"],silent=False)
    await wipe_messages(uid, cid)
    if err:
        await try_delete(cid, msg.message_id)
        warn = await bot.send_message(cid, err, reply_markup=get_skip_kbd() if is_skippable(fs) else None)
        state["error_msgs"]=[warn.message_id]
        return
    state["fields"][fs["field"]]=msg.text.strip()
    state["answers_msgs"].append(msg.message_id)
    await send_or_edit_shutter(uid, cid, conf, state)
    state["step"]+=1
    await show_next_question(uid, cid)

for f in ["prevpark", "nowpark", "autoclass"]:
    @dp.callback_query(lambda c, field=f: c.data and c.data.startswith(f"{field}:"))
    async def cb_generic(call: types.CallbackQuery, field=f):
        uid, cid = call.from_user.id, call.message.chat.id; state = user_form_states[uid]
        state["fields"][field]=call.data.split(":",1)[1]
        await wipe_messages(uid, cid)
        await send_or_edit_shutter(uid, cid, get_form_conf(state["form_type"]), state)
        state["step"]+=1
        await show_next_question(uid, cid)
        await call.answer()

@dp.message(lambda msg: user_chosen_type.get(msg.from_user.id) and user_form_states.get(msg.from_user.id) and get_next_field(user_form_states[msg.from_user.id]) and get_next_field(user_form_states[msg.from_user.id])["type"] in {"photo", "media"} and (msg.photo or getattr(msg, "video", None)))
async def media_handler(msg: types.Message):
    uid, cid = msg.from_user.id, msg.chat.id
    state = user_form_states[uid]
    fs = get_next_field(state); tp=state["form_type"]
    async def save_file(fid, ext, prefix, idx=None):
        suf = datetime.datetime.now().strftime('%Y%m%d_%H%M%S'); idxp = f"_{idx}" if idx is not None else ""
        fn = f"{prefix}_{suf}{idxp}_{uuid.uuid4().hex[:6]}{ext}"
        path = os.path.join(PHOTO_SAVE_DIR, fn)
        file = await bot.get_file(fid)
        await bot.download_file(file.file_path, path); return fn, path
    if tp=="action" and fs["type"]=="photo":
        await wipe_messages(uid, cid)
        await try_delete(cid, msg.message_id)
        fid = msg.photo[-1].file_id; fn, fp = await save_file(fid, ".jpg", "driver_license")
        state["fields"][fs["field"]]=fn
        txt = get_form_conf("action")["shutter_text"](uid, state["fields"], state.get("req_username"), mode="processing")
        await try_delete(cid, state.get("shutter_id"))
        pm = await bot.send_photo(cid, FSInputFile(fp), caption=txt); state["shutter_id"] = pm.message_id
        state["step"]+=1; m = await bot.send_message(cid, "Проверьте вашу заявку\nЕсли всё верно нажмите кнопку 'Завершить'\nЕсли есть ошибки нажмите 'Удалить' и создайте новую заявку", reply_markup=get_confirm_kbd(tp))
        await wipe_messages(uid, cid)
        state["questions_msgs"].append(m.message_id); return
    if tp=="damage" and fs["type"]=="media":
        mgid = getattr(msg, "media_group_id", get_confirm_kbd(tp))
        async def parse_and_save(m, idx):
            if m.photo: return await save_file(m.photo[-1].file_id, ".jpg", "damage_photo", idx)
            if getattr(m, "video", None): return await save_file(m.video.file_id, ".mp4", "damage_video", idx)
            return None, None
        if mgid:
            media_group_batches[mgid].append(msg); media_group_to_user[mgid]=(uid, cid)
            await asyncio.sleep(0.5)
            if not media_group_batches[mgid]: return
            msgs=media_group_batches.pop(mgid)
            midu, mcid = media_group_to_user.pop(mgid,(uid,cid)); state = user_form_states[midu]
            paths, fns = [], []
            for ix, m in enumerate(msgs):
                fname, fpath = await parse_and_save(m, ix)
                if fname:
                    paths.append(fpath)
                    fns.append(fname)
                await try_delete(mcid, m.message_id)
            state.setdefault("media_files",[]).extend(paths)
            prev = state["fields"].get("media","")
            for fn in fns: prev = (prev+";"+fn) if prev else fn
            state["fields"]["media"]=prev
            await remove_shutter_msgs(uid,cid)
            await clear_old_shutters_and_confirmations(uid, cid, state)
            await show_media_group(mcid, state["media_files"], get_shutter_text(tp, midu, state["fields"], state.get("req_username"), mode="processing"), "Добавьте ещё файлы или проверьте вашу заявку\nЕсли всё верно нажмите кнопку 'Завершить'\nЕсли есть ошибки нажмите 'Удалить' и создайте новую заявку", get_confirm_kbd(tp), state)
            return
        ix = len(state.get("media_files", []))
        if msg.photo: fid, ext, prefix = msg.photo[-1].file_id, ".jpg", "damage_photo"
        elif getattr(msg, "video", None): fid, ext, prefix = msg.video.file_id, ".mp4", "damage_video"
        else:
            await msg.answer("Поддерживаются только фото и видео."); return
        fname, fpath = await save_file(fid, ext, prefix, ix)
        state.setdefault("media_files",[]).append(fpath)
        prev=state["fields"].get("media",""); state["fields"]["media"]= (prev+";"+fname) if prev else fname
        await try_delete(cid, msg.message_id)
        await show_media_group(cid, state["media_files"], get_shutter_text(tp, uid, state["fields"], state.get("req_username"), mode="processing"), "Добавьте ещё файлы или проверьте вашу заявку\nЕсли всё верно нажмите кнопку 'Завершить'\nЕсли есть ошибки нажмите 'Удалить' и создайте новую заявку", get_confirm_kbd(tp), state, store_id_field="shutter_media_msgs")
    return

async def try_edit_caption(chat_id, msg_id, caption):
    try:
        await bot.edit_message_caption(chat_id=chat_id, message_id=msg_id, caption=caption)
        return True
    except Exception as e:
        logging.exception(e)
        return False

user_requests = defaultdict(list)

@dp.callback_query(lambda c: c.data=="damage_skip")
async def cb_damage_skip(call: types.CallbackQuery):
    uid, cid = call.from_user.id, call.message.chat.id
    state = user_form_states.get(uid)
    if not state: await call.answer(); return
    conf = get_form_conf(state["form_type"])
    cf = conf["FIELDS"][state["step"]]["field"]; state["fields"][cf]="Пропущено"
    if cf=="media": state["media_files"]=[]; state["fields"]["media"]="Нет приложений"
    await try_delete(cid, call.message.message_id)
    await remove_shutter_msgs(uid, cid)
    await send_or_edit_shutter(uid, cid, conf, state)
    state["step"]+=1; step=state["step"]
    if step < len(conf["FIELDS"]): await show_next_question(uid, cid)
    else:
        await send_or_edit_shutter(uid, cid, conf, state)
        tp = state["form_type"]
        m=await bot.send_message(cid, "Добавьте ещё файлы или проверьте вашу заявку\nЕсли всё верно нажмите кнопку 'Завершить'\nЕсли есть ошибки нажмите 'Удалить' и создайте новую заявку", reply_markup=get_confirm_kbd(tp))
        state["questions_msgs"].append(m.message_id)
    await call.answer()

class UniversalFinish(Filter):
    keys = ("action_finish", "damage_finish")
    async def call(self, call: types.CallbackQuery): return call.data in self.keys
async def finish_form(user_id, chat_id):
    state = user_form_states.get(user_id)
    if not state: return
    tp = state["form_type"]
    conf = get_form_conf(tp)
    dt = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    fields = state["fields"]
    uname = state.get("req_username")
    await remove_shutter_msgs(user_id, chat_id, keep_damage_media=False)
    final = conf["shutter_text"](user_id, fields, uname, mode="ending", dtstr=dt)
    mids = []
    if tp == "action":
        pfn = fields.get("photo", "")
        is_label = (pfn == conf["FIELDS_BY_NAME"]["photo"]["label"])
        ppath = os.path.join(PHOTO_SAVE_DIR, pfn) if pfn and not is_label else None
        if ppath and os.path.exists(ppath): msg = await bot.send_photo(chat_id, photo=FSInputFile(ppath), caption=final)
        else: msg = await bot.send_message(chat_id, text=final)
        mids.append(msg.message_id)
    elif tp == "damage":
        media_files = state.get("media_files", [])
        sent_media_msgs = []
        if media_files:
            media = []
            for i, path in enumerate(media_files):
                fn = path.lower()
                if fn.endswith(('.jpg', '.jpeg', '.png')): m = InputMediaPhoto(media=FSInputFile(path), caption=final if i == 0 else None)
                elif fn.endswith('.mp4'): m = InputMediaVideo(media=FSInputFile(path), caption=final if i == 0 else None)
                else: m = None
                if m: media.append(m)
            if media:
                try:
                    resp = await bot.send_media_group(chat_id, media)
                    sent_media_msgs = [x.message_id for x in resp]
                except Exception as e: logging.exception(e)
    ws = await get_sheet_by_type(tp)
    row = conf["row_gen"](dt, uname, fields)
    await append_row_with_notify(ws, row, chat_id)
    m = await bot.send_message(chat_id, "Ваша заявка создана и отправлена сотрудникам ОККР!", reply_markup=get_main_kb(user_id))
    mids.append(m.message_id)
    request_info = {
        "row_num": None,
        "form_type": tp,
        "msg_id": mids[0],
        "last_status": "Создано",
        "last_row": row,}
    user_requests[user_id].append(request_info)
    user_form_states.pop(user_id, None)
    user_chosen_type.pop(user_id, None)

@dp.callback_query(lambda c: c.data in ("action_finish", "damage_finish"))
async def finish_form_inner(call: types.CallbackQuery):
    await finish_form(call.from_user.id, call.message.chat.id)
    await call.answer()

@dp.callback_query(lambda c: c.data in ("action_delete", "damage_delete"))
async def delete_form(call: types.CallbackQuery):
    uid, cid = call.from_user.id, call.message.chat.id
    await wipe_messages(uid, cid)
    await remove_shutter_msgs(uid, cid)
    user_form_states.pop(uid, None)
    user_chosen_type.pop(uid, None)
    msg = await bot.send_message(cid, "Заявка удалена.", reply_markup=get_main_kb(uid))
    user_info_msgs[uid] = [msg.message_id]
    await call.answer()

async def check_requests_status_periodically():
    await asyncio.sleep(5)
    while True:
        try:
            agc = await agcm.authorize()
            for form_type in FORMS_CONFIG:
                ws = await (await agc.open_by_key(SPREADSHEET_ID)).worksheet(FORMS_CONFIG[form_type]['title'])
                data = await ws.get_all_values()
                headers = data[0]
                try: status_idx = headers.index("Статус заявки")
                except ValueError:
                    logging.warning(f"Нет столбца 'Статус' в {form_type}")
                    continue
                for user_id, reqs in user_requests.items():
                    for req in reqs:
                        if req['form_type'] != form_type: continue
                        found_row_num = None
                        target_dt = req['last_row'][1]
                        target_un = req['last_row'][2]
                        for i, row in enumerate(data[1:], start=2):
                            if len(row) < 3: continue
                            if row[1] == target_dt and row[2] == target_un:
                                found_row_num = i
                                break
                            
                        if not found_row_num: continue
                        req["row_num"] = found_row_num
                        status = data[found_row_num - 1][status_idx] if found_row_num <= len(data) else None
                        if not status or status == req["last_status"]: continue
                        req["last_status"] = status
                        chat_id = user_id
                        reply_to = req["msg_id"]
                        row = data[found_row_num - 1]
                        txt = None
                        if status == "Выполнено":
                            txt = "Сотрудник выполнил операцию, если руководитель откажет, то будет проведена отмена"
                            comment = ""
                            if form_type == "action":
                                try: comment = row[15] if len(row) > 15 and row[15].strip() else ""
                                except Exception: comment = ""
                            elif form_type == "damage":
                                try: comment = row[13] if len(row) > 13 and row[13].strip() else ""
                                except Exception: comment = ""
                            if comment:
                                txt += f"\n\nКомментарий от сотрудника: {comment}"
                        elif status == "Одобрено": txt = "Руководитель одобрил вашу заявку"
                        elif status == "Отменено": txt = "Руководитель отказал вашей заявке, сотрудник уже провёл отмену операции"
                        else: continue
                        if txt:
                            try: await bot.send_message(chat_id, txt, reply_to_message_id=reply_to)
                            except Exception as e: logging.exception(e)
        except Exception as e: logging.exception(e)
        await asyncio.sleep(30)

async def main(): 
    asyncio.create_task(check_requests_status_periodically())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())