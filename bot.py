import os
import asyncio
import random
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.types import InputFile
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

print("БОТ ЗАПУЩЕН ✅")


# =========================
# СОСТОЯНИЕ ТЕКУЩЕГО РОЗЫГРЫША
# =========================
giveaway_active = False
participants = {}          # user_id -> name
current_giveaway = {}      # словарь с настройками
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


async def finish_giveaway(reason: str):
    global giveaway_active, participants, current_giveaway, time_task

    if not giveaway_active:
        return

    winners_count = current_giveaway.get("winners_count", 1)

    if not participants:
        await bot.send_message(
            ADMIN_ID,
            f"Розыгрыш завершён ({reason}), но участников нет ❌"
        )
    else:
        ids = list(participants.keys())
        actual_winners_count = min(winners_count, len(ids))
        winner_ids = random.sample(ids, actual_winners_count)

        winners_text = []
        for i, winner_id in enumerate(winner_ids, start=1):
            winners_text.append(f"{i}. {participants[winner_id]} (ID: {winner_id})")

        text = (
            f"🎉 Розыгрыш завершён!\n\n"
            f"Причина: {reason}\n"
            f"Участников: {len(participants)}\n"
            f"Победителей: {actual_winners_count}\n\n"
            f"🏆 Победители:\n" + "\n".join(winners_text)
        )

        await bot.send_message(ADMIN_ID, text)

    giveaway_active = False
    participants = {}
    current_giveaway = {}
    time_task = None


async def time_finish_worker():
    global current_giveaway, giveaway_active

    while giveaway_active:
        end_dt = current_giveaway.get("end_datetime")
        if end_dt and datetime.now() >= end_dt:
            await finish_giveaway("вышло время")
            return
        await asyncio.sleep(1)


async def start_giveaway():
    global giveaway_active, participants, current_giveaway, time_task

    participants = {}
    giveaway_active = True

    media_type = current_giveaway["media_type"]
    media_path = current_giveaway["media_path"]
    description = current_giveaway["description"]
    caption = build_caption(description)

    if not os.path.exists(media_path):
        await bot.send_message(ADMIN_ID, f"❌ Файл не найден: {media_path}")
        giveaway_active = False
        current_giveaway = {}
        return

    if media_type == "video":
        await bot.send_video(
            chat_id=CHANNEL_ID,
            video=InputFile(media_path),
            caption=caption
        )
    else:
        await bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=InputFile(media_path),
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

    if giveaway_active:
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

    os.makedirs("media", exist_ok=True)

    media_type = None
    file_id = None
    filename = None

    if message.video:
        media_type = "video"
        file_id = message.video.file_id
        filename = f"video_{message.message_id}.mp4"

    elif message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
        filename = f"photo_{message.message_id}.jpg"

    elif message.document:
        name = message.document.file_name.lower()

        if name.endswith(('.mp4', '.mov', '.mkv')):
            media_type = "video"
        elif name.endswith(('.jpg', '.jpeg', '.png', '.webp')):
            media_type = "photo"
        else:
            await message.answer("Подходит только видео или фото")
            return

        file_id = message.document.file_id
        filename = message.document.file_name

    else:
        await message.answer("Отправь видео или фото")
        return

    path = os.path.join("media", filename)

    file = await bot.get_file(file_id)
    await bot.download_file(file.file_path, path)

    await state.update_data(media_type=media_type, media_path=path)

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

    await state.update_data(start_datetime=start_dt)

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
        f"Старт: {'сейчас' if start_mode == 'now' else start_datetime.strftime('%Y-%m-%d %H:%M')}\n"
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
        "media_type": data["media_type"],
        "media_path": data["media_path"],
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
            current_giveaway["end_datetime"] = datetime.now() + timedelta(
                minutes=current_giveaway["end_value"]
            )
        else:
            current_giveaway["end_datetime"] = current_giveaway["start_datetime"] + timedelta(
                minutes=current_giveaway["end_value"]
            )
    else:
        current_giveaway["end_datetime"] = None

    await state.finish()

    if current_giveaway["start_mode"] == "now":
        await start_giveaway()
    else:
        scheduler.add_job(
            scheduled_start,
            trigger='date',
            run_date=current_giveaway["start_datetime"]
        )
        await message.answer(
            f"✅ Розыгрыш запланирован на "
            f"{current_giveaway['start_datetime'].strftime('%Y-%m-%d %H:%M')}"
        )


# =========================
# СТАТУС И ОТМЕНА
# =========================
@dp.message_handler(commands=['status'])
async def status_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    if not giveaway_active:
        await message.answer("Сейчас активного розыгрыша нет")
        return

    text = (
        f"📊 Статус розыгрыша\n\n"
        f"Код: {current_giveaway.get('code')}\n"
        f"Участников: {len(participants)}\n"
        f"Победителей: {current_giveaway.get('winners_count')}\n"
        f"Завершение: {'по количеству' if current_giveaway.get('end_mode') == 'count' else 'по времени'}\n"
    )

    if current_giveaway.get("end_mode") == "count":
        text += f"Цель: {current_giveaway.get('end_value')}"
    else:
        end_dt = current_giveaway.get("end_datetime")
        if end_dt:
            text += f"До: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}"

    await message.answer(text)


@dp.message_handler(commands=['cancel'])
async def cancel_cmd(message: types.Message):
    global giveaway_active, participants, current_giveaway, time_task

    if not is_admin(message.from_user.id):
        return

    giveaway_active = False
    participants = {}
    current_giveaway = {}
    time_task = None

    await message.answer("Активный розыгрыш отменён ❌")


# =========================
# УЧАСТИЕ ПОЛЬЗОВАТЕЛЕЙ
# =========================
@dp.message_handler()
async def check_code(message: types.Message):
    global participants

    if not giveaway_active:
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

    if user_id in participants:
        await message.answer("Ты уже участвуешь 😉")
        return

    username = message.from_user.username
    full_name = message.from_user.full_name
    participants[user_id] = f"@{username}" if username else full_name

    await message.answer("Ты участвуешь 🎉")

    if current_giveaway.get("end_mode") == "count":
        if len(participants) >= current_giveaway.get("end_value"):
            await finish_giveaway("набрано нужное количество участников")


async def on_startup(_):
    scheduler.start()
    await bot.send_message(ADMIN_ID, "Планировщик запущен ✅")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)