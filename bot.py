import os
import asyncio
import random
import sqlite3
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler


API_TOKEN = os.getenv("API_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

DB_PATH = "bot.db"

print("БОТ ЗАПУЩЕН ✅")


# =========================
# БАЗА ДАННЫХ
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS giveaway (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            active INTEGER NOT NULL DEFAULT 0,
            media_type TEXT,
            media_file_id TEXT,
            description TEXT,
            start_mode TEXT,
            start_datetime TEXT,
            end_mode TEXT,
            end_value INTEGER,
            end_datetime TEXT,
            winners_count INTEGER,
            code TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            user_id INTEGER PRIMARY KEY,
            username TEXT
        )
    """)

    cur.execute("""
        INSERT OR IGNORE INTO giveaway (id, active) VALUES (1, 0)
    """)

    conn.commit()
    conn.close()


def save_giveaway(data: dict):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE giveaway
        SET active = ?,
            media_type = ?,
            media_file_id = ?,
            description = ?,
            start_mode = ?,
            start_datetime = ?,
            end_mode = ?,
            end_value = ?,
            end_datetime = ?,
            winners_count = ?,
            code = ?
        WHERE id = 1
    """, (
        1 if data.get("active", False) else 0,
        data.get("media_type"),
        data.get("media_file_id"),
        data.get("description"),
        data.get("start_mode"),
        data.get("start_datetime"),
        data.get("end_mode"),
        data.get("end_value"),
        data.get("end_datetime"),
        data.get("winners_count"),
        data.get("code"),
    ))

    conn.commit()
    conn.close()


def load_giveaway() -> dict:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT active, media_type, media_file_id, description, start_mode,
               start_datetime, end_mode, end_value, end_datetime,
               winners_count, code
        FROM giveaway
        WHERE id = 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row:
        return {}

    return {
        "active": bool(row[0]),
        "media_type": row[1],
        "media_file_id": row[2],
        "description": row[3],
        "start_mode": row[4],
        "start_datetime": row[5],
        "end_mode": row[6],
        "end_value": row[7],
        "end_datetime": row[8],
        "winners_count": row[9],
        "code": row[10],
    }


def clear_giveaway_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE giveaway
        SET active = 0,
            media_type = NULL,
            media_file_id = NULL,
            description = NULL,
            start_mode = NULL,
            start_datetime = NULL,
            end_mode = NULL,
            end_value = NULL,
            end_datetime = NULL,
            winners_count = NULL,
            code = NULL
        WHERE id = 1
    """)

    conn.commit()
    conn.close()


def add_participant(user_id: int, username: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO participants (user_id, username)
        VALUES (?, ?)
    """, (user_id, username))
    conn.commit()
    conn.close()


def participant_exists(user_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_participants() -> list[tuple[int, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username FROM participants")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_participants_count() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM participants")
    count = cur.fetchone()[0]
    conn.close()
    return count


def clear_participants():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM participants")
    conn.commit()
    conn.close()


# =========================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# =========================
current_giveaway = {}
time_task = None


# =========================
# FSM ДЛЯ СОЗДАНИЯ РОЗЫГРЫША
# =========================
class GiveawayForm(StatesGroup):
    waiting_media = State()
    waiting_description = State()
    waiting_start_mode = State()
    waiting_start_datetime = State()
    waiting_end_mode = State()
    waiting_end_value = State()
    waiting_winners_count = State()
    waiting_code = State()
    waiting_confirm = State()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def build_caption(description: str) -> str:
    return (
        f"{description}\n\n"
        f"🎯 Как участвовать:\n"
        f"1. Подпишись на канал\n"
        f"2. Посмотри видео или пост\n"
        f"3. Найди код\n"
        f"4. Отправь код в бота\n\n"
        f"👉 После этого ты автоматически участвуешь"
    )


def parse_dt(value: str | None):
    if not value:
        return None
    return datetime.fromisoformat(value)


async def finish_giveaway(reason: str):
    global current_giveaway, time_task

    if not current_giveaway.get("active"):
        return

    participants = get_participants()
    winners_count = current_giveaway.get("winners_count", 1) or 1

    if not participants:
        await bot.send_message(
            ADMIN_ID,
            f"Розыгрыш завершён ({reason}), но участников нет ❌"
        )
    else:
        ids = [row[0] for row in participants]
        names = {row[0]: row[1] for row in participants}

        actual_winners_count = min(winners_count, len(ids))
        winner_ids = random.sample(ids, actual_winners_count)

        winners_text = []
        for i, winner_id in enumerate(winner_ids, start=1):
            winners_text.append(f"{i}. {names[winner_id]} (ID: {winner_id})")

        text = (
            f"🎉 Розыгрыш завершён!\n\n"
            f"Причина: {reason}\n"
            f"Участников: {len(participants)}\n"
            f"Победителей: {actual_winners_count}\n\n"
            f"🏆 Победители:\n" + "\n".join(winners_text)
        )

        await bot.send_message(ADMIN_ID, text)

    current_giveaway = {"active": False}
    if time_task:
        time_task.cancel()
        time_task = None

    clear_participants()
    clear_giveaway_db()


async def time_finish_worker():
    global current_giveaway

    while current_giveaway.get("active"):
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt and datetime.now() >= end_dt:
            await finish_giveaway("вышло время")
            return
        await asyncio.sleep(1)


async def start_giveaway():
    global current_giveaway, time_task

    clear_participants()

    media_type = current_giveaway["media_type"]
    media_file_id = current_giveaway["media_file_id"]
    description = current_giveaway["description"]
    caption = build_caption(description)

    current_giveaway["active"] = True
    save_giveaway(current_giveaway)

    if media_type == "video":
        await bot.send_video(
            chat_id=CHANNEL_ID,
            video=media_file_id,
            caption=caption
        )
    else:
        await bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=media_file_id,
            caption=caption
        )

    await bot.send_message(
        ADMIN_ID,
        "✅ Розыгрыш запущен и опубликован в канале"
    )

    if current_giveaway["end_mode"] == "time":
        time_task = asyncio.create_task(time_finish_worker())


async def scheduled_start():
    await start_giveaway()


# =========================
# КОМАНДЫ
# =========================
@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message):
    await message.answer(
        "Привет 👋\n\n"
        "Чтобы участвовать в розыгрыше:\n"
        "1. Подпишись на канал\n"
        "2. Найди код\n"
        "3. Отправь код сюда"
    )


@dp.message_handler(commands=['newgiveaway'])
async def new_giveaway(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if current_giveaway.get("active"):
        await message.answer("Сейчас уже есть активный розыгрыш ❗")
        return

    await GiveawayForm.waiting_media.set()
    await message.answer(
        "Отправь файл для поста.\n\n"
        "Можно:\n"
        "- видео\n"
        "- фото\n\n"
        "⚠️ Лучше отправлять файлом, а не ссылкой."
    )


@dp.message_handler(commands=['cancelsetup'], state='*')
async def cancel_setup(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await message.answer("Создание розыгрыша отменено ❌")


# =========================
# ШАГ 1: ФАЙЛ
# =========================
@dp.message_handler(content_types=['video', 'photo', 'document'], state=GiveawayForm.waiting_media)
async def process_media(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    media_type = None
    file_id = None

    if message.video:
        media_type = "video"
        file_id = message.video.file_id

    elif message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id

    elif message.document:
        name = (message.document.file_name or "").lower()

        if name.endswith(('.mp4', '.mov', '.mkv')):
            media_type = "video"
        elif name.endswith(('.jpg', '.jpeg', '.png', '.webp')):
            media_type = "photo"
        else:
            await message.answer("Подходит только видео или фото")
            return

        file_id = message.document.file_id

    else:
        await message.answer("Отправь видео или фото")
        return

    await state.update_data(media_type=media_type, media_file_id=file_id)

    await GiveawayForm.next()
    await message.answer("Теперь отправь описание поста")


# =========================
# ШАГ 2: ОПИСАНИЕ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_description)
async def process_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text.strip())

    await GiveawayForm.next()
    await message.answer(
        "Когда запускать розыгрыш?\n\n"
        "Напиши:\n"
        "- сейчас\n"
        "- по времени"
    )


# =========================
# ШАГ 3: РЕЖИМ СТАРТА
# =========================
@dp.message_handler(state=GiveawayForm.waiting_start_mode)
async def process_start_mode(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()

    if text == "сейчас":
        await state.update_data(start_mode="now", start_datetime=None)
        await GiveawayForm.waiting_end_mode.set()
        await message.answer(
            "Как завершать розыгрыш?\n\n"
            "Напиши:\n"
            "- по количеству\n"
            "- по времени"
        )
        return

    if text == "по времени":
        await state.update_data(start_mode="scheduled")
        await GiveawayForm.next()
        await message.answer(
            "Введи дату и время старта в формате:\n"
            "2026-03-20 21:30"
        )
        return

    await message.answer("Напиши только: сейчас или по времени")


# =========================
# ШАГ 4: ДАТА СТАРТА
# =========================
@dp.message_handler(state=GiveawayForm.waiting_start_datetime)
async def process_start_datetime(message: types.Message, state: FSMContext):
    try:
        start_dt = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
    except ValueError:
        await message.answer("Неверный формат. Пример: 2026-03-20 21:30")
        return

    if start_dt <= datetime.now():
        await message.answer("Время старта должно быть в будущем")
        return

    await state.update_data(start_datetime=start_dt.isoformat())

    await GiveawayForm.waiting_end_mode.set()
    await message.answer(
        "Как завершать розыгрыш?\n\n"
        "Напиши:\n"
        "- по количеству\n"
        "- по времени"
    )


# =========================
# ШАГ 5: РЕЖИМ ЗАВЕРШЕНИЯ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_end_mode)
async def process_end_mode(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()

    if text == "по количеству":
        await state.update_data(end_mode="count")
        await GiveawayForm.next()
        await message.answer("Сколько участников должно быть до завершения?")
        return

    if text == "по времени":
        await state.update_data(end_mode="time")
        await GiveawayForm.next()
        await message.answer("Сколько минут должен идти розыгрыш после старта?")
        return

    await message.answer("Напиши только: по количеству или по времени")


# =========================
# ШАГ 6: ЗНАЧЕНИЕ ЗАВЕРШЕНИЯ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_end_value)
async def process_end_value(message: types.Message, state: FSMContext):
    try:
        value = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно ввести число")
        return

    if value <= 0:
        await message.answer("Число должно быть больше нуля")
        return

    await state.update_data(end_value=value)

    await GiveawayForm.next()
    await message.answer("Сколько победителей выбрать?")


# =========================
# ШАГ 7: КОЛИЧЕСТВО ПОБЕДИТЕЛЕЙ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_winners_count)
async def process_winners_count(message: types.Message, state: FSMContext):
    try:
        winners_count = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно ввести число")
        return

    if winners_count <= 0:
        await message.answer("Количество победителей должно быть больше нуля")
        return

    await state.update_data(winners_count=winners_count)

    await GiveawayForm.next()
    await message.answer("Теперь отправь код, который участники будут вводить в бота")


# =========================
# ШАГ 8: КОД
# =========================
@dp.message_handler(state=GiveawayForm.waiting_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    if not code:
        await message.answer("Код не должен быть пустым")
        return

    data = await state.get_data()
    await state.update_data(code=code)

    start_mode = data.get("start_mode")
    start_datetime = data.get("start_datetime")
    end_mode = data.get("end_mode")
    end_value = data.get("end_value")
    winners_count = data.get("winners_count")
    media_type = data.get("media_type")
    description = data.get("description")

    summary = (
        f"Проверь настройки розыгрыша:\n\n"
        f"Файл: {media_type}\n"
        f"Описание: {description}\n"
        f"Старт: {'сейчас' if start_mode == 'now' else datetime.fromisoformat(start_datetime).strftime('%Y-%m-%d %H:%M')}\n"
        f"Завершение: {'по количеству' if end_mode == 'count' else 'по времени'}\n"
        f"Значение: {end_value}\n"
        f"Победителей: {winners_count}\n"
        f"Код: {code}\n\n"
        f"Напиши:\n"
        f"- да\n"
        f"- нет"
    )

    await GiveawayForm.next()
    await message.answer(summary)


# =========================
# ШАГ 9: ПОДТВЕРЖДЕНИЕ
# =========================
@dp.message_handler(state=GiveawayForm.waiting_confirm)
async def process_confirm(message: types.Message, state: FSMContext):
    global current_giveaway

    text = message.text.strip().lower()

    if text != "да":
        await state.finish()
        await message.answer("Создание розыгрыша отменено ❌")
        return

    data = await state.get_data()

    current_giveaway = {
        "active": False,
        "media_type": data["media_type"],
        "media_file_id": data["media_file_id"],
        "description": data["description"],
        "start_mode": data["start_mode"],
        "start_datetime": data.get("start_datetime"),
        "end_mode": data["end_mode"],
        "end_value": data["end_value"],
        "winners_count": data["winners_count"],
        "code": data["code"],
    }

    if current_giveaway["end_mode"] == "time":
        if current_giveaway["start_mode"] == "now":
            current_giveaway["end_datetime"] = (
                datetime.now() + timedelta(minutes=current_giveaway["end_value"])
            ).isoformat()
        else:
            start_dt = datetime.fromisoformat(current_giveaway["start_datetime"])
            current_giveaway["end_datetime"] = (
                start_dt + timedelta(minutes=current_giveaway["end_value"])
            ).isoformat()
    else:
        current_giveaway["end_datetime"] = None

    save_giveaway(current_giveaway)
    await state.finish()

    if current_giveaway["start_mode"] == "now":
        await start_giveaway()
    else:
        scheduler.add_job(
            scheduled_start,
            trigger="date",
            run_date=datetime.fromisoformat(current_giveaway["start_datetime"])
        )
        await message.answer(
            f"✅ Розыгрыш запланирован на "
            f"{datetime.fromisoformat(current_giveaway['start_datetime']).strftime('%Y-%m-%d %H:%M')}"
        )


# =========================
# СТАТУС И ОТМЕНА
# =========================
@dp.message_handler(commands=['status'])
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if not current_giveaway.get("active"):
        await message.answer("Сейчас активного розыгрыша нет")
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {get_participants_count()}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Завершение: {'по количеству' if current_giveaway.get('end_mode') == 'count' else 'по времени'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Цель: {current_giveaway.get('end_value')}"
    else:
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt:
            text += f"До: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}"

    await message.answer(text)


@dp.message_handler(commands=['cancel'])
async def cancel_cmd(message: types.Message):
    global current_giveaway, time_task

    if not is_admin(message.from_user.id):
        return

    if time_task:
        time_task.cancel()
        time_task = None

    clear_participants()
    clear_giveaway_db()
    current_giveaway = {"active": False}

    await message.answer("Активный розыгрыш отменён ❌")


# =========================
# УЧАСТИЕ ПОЛЬЗОВАТЕЛЕЙ
# =========================
@dp.message_handler()
async def check_code(message: types.Message):
    if not current_giveaway.get("active"):
        await message.answer("Сейчас нет активного розыгрыша")
        return

    user_id = message.from_user.id
    text = message.text.strip()

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await message.answer("Ошибка проверки подписки 😕")
        return

    if member.status not in ['member', 'creator', 'administrator']:
        await message.answer("Сначала подпишись на канал ❗")
        return

    if text != current_giveaway.get("code"):
        await message.answer("Неверный код ❌")
        return

    if participant_exists(user_id):
        await message.answer("Ты уже участвуешь 😉")
        return

    username = message.from_user.username
    full_name = message.from_user.full_name
    display_name = f"@{username}" if username else full_name

    add_participant(user_id, display_name)

    await message.answer("Ты участвуешь 🎉")

    if current_giveaway.get("end_mode") == "count":
        if get_participants_count() >= current_giveaway.get("end_value"):
            await finish_giveaway("набрано нужное количество участников")


async def restore_state():
    global current_giveaway, time_task

    current_giveaway = load_giveaway()

    if not current_giveaway:
        current_giveaway = {"active": False}
        return

    if current_giveaway.get("active") and current_giveaway.get("end_mode") == "time":
        end_dt = parse_dt(current_giveaway.get("end_datetime"))
        if end_dt and datetime.now() < end_dt:
            time_task = asyncio.create_task(time_finish_worker())
        elif end_dt and datetime.now() >= end_dt:
            await finish_giveaway("вышло время после перезапуска")


async def on_startup(_):
    init_db()
    scheduler.start()
    await restore_state()
    await bot.send_message(ADMIN_ID, "Планировщик запущен ✅")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)