import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils import executor
from pyrogram import Client
from pyrogram.errors import ChatAdminRequired, FloodWait
from pyrogram.types import Message

from dotenv import load_dotenv


load_dotenv()

# Настройки
API_TOKEN = os.getenv('API_TOKEN')
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
admin_id = os.getenv('ADMIN_ID')

# Инициализация клиента Pyrogram
app = Client("zakaz", api_id=api_id, api_hash=api_hash)

# База данных пользователей
full_base_file = 'full_base.json'
new_users_file = 'new_users.json'
channels_file = 'channels.json'
chats_file = 'chats.json'
acs_users_file = 'acs_users.json'
auto_parser_settings_file = 'auto_parser_settings.json'

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

channels = {}
chats = {}
users = set()
new_users = set()
info_users = {}
acs_users = set()
auto_parser_settings = {}


class Form(StatesGroup):
    addch = State()
    dellch = State()
    addchat = State()
    dellchat = State()
    acs = State()
    pars = State()
    full_pars_ch = State()
    auto_parser = State()
    auto_parser_time = State()


def load_users():
    users = set()
    try:
        with open(full_base_file, 'r') as f:
            for line in f:
                try:
                    user_data = json.loads(line)
                    user_id = user_data['user_id']
                    users.add(user_id)
                    info_users[user_id] = {
                        'username': user_data.get('username'),
                        'phone': user_data.get('phone')
                    }
                except json.JSONDecodeError:
                    logging.warning(f"Skipping invalid JSON line: {line}")
    except FileNotFoundError:
        logging.info("User database file not found. Starting with empty set.")
    return users


def save_users(users):
    with open(full_base_file, 'w') as f:
        for user_id in users:
            user_info = info_users.get(user_id, {})
            user_data = {
                "user_id": user_id,
                "username": user_info.get('username'),
                "phone": user_info.get('phone')
            }
            f.write(json.dumps({k: v for k, v in user_data.items() if v is not None and v != 'Not available'}) + '\n')


def update_user_info(user_id, username, phone):
    if user_id not in info_users:
        info_users[user_id] = {}

    if username and username != 'Not available':
        info_users[user_id]['username'] = username
    if phone and phone != 'Not available':
        info_users[user_id]['phone'] = phone


def save_new_users(new_users):
    with open(new_users_file, 'w') as f:
        for user_id in new_users:
            user_info = info_users.get(user_id, {})
            f.write(json.dumps({"user_id": user_id, "username": user_info.get('username', None),
                                "phone": user_info.get('phone', None)}) + '\n')


def load_acs_users():
    try:
        with open(acs_users_file, 'r') as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()


def save_acs_users(acs_users):
    with open(acs_users_file, 'w') as f:
        json.dump(list(acs_users), f)


def load_channels():
    try:
        with open(channels_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_channels(channels):
    with open(channels_file, 'w') as f:
        json.dump(channels, f)


def load_chats():
    try:
        with open(chats_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_chats(chats):
    with open(chats_file, 'w') as f:
        json.dump(chats, f)


def load_auto_parser_settings():
    try:
        with open(auto_parser_settings_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_auto_parser_settings(settings):
    with open(auto_parser_settings_file, 'w') as f:
        json.dump(settings, f)


@app.on_message()
def handle_message(client, message: Message):
    if message.from_user:
        user_id = message.from_user.id
        if user_id not in users:
            users.add(user_id)
            new_users.add(user_id)
            update_user_info(user_id, message.from_user.username, message.from_user.phone_number)


async def parse_channels(limit=50):
    async with app:
        for channel_username in channels.keys():
            if not parsing_in_progress:
                break
            users_data = []

            try:
                channel = await app.get_chat(channel_username)

                async for message in app.get_chat_history(channel.id, limit=limit):
                    if not parsing_in_progress:
                        break
                    try:
                        # Проверка на существование сообщения
                        if not await app.get_messages(channel.id, message.id):
                            logging.warning(f"Сообщение {message.id} не существует. Пропуск.")
                            continue

                        async for reply in app.get_discussion_replies(channel.id, message.id):
                            if reply.from_user:
                                user = reply.from_user
                                user_data = {
                                    'id': user.id,
                                    'username': user.username if user.username else 'Not available',
                                    'phone': user.phone_number if user.phone_number else 'Not available'
                                }
                                if user_data not in users_data:
                                    users_data.append(user_data)
                                    logging.info(f"Добавлен пользователь: {user_data['username']}")

                        logging.info(f"Обработано сообщение {message.id}")

                    except Exception as e:
                        if "FLOOD_WAIT" in str(e):
                            wait_time = int(str(e).split()[-2])
                            logging.warning(f"Достигнут лимит запросов. Ожидание {wait_time} секунд.")
                            await asyncio.sleep(wait_time)
                        elif "MSG_ID_INVALID" in str(e):
                            logging.warning(f"Недействительный ID сообщения {message.id}. Пропуск.")
                        else:
                            logging.warning(f"Ошибка при обработке сообщения {message.id}: {str(e)}")
                        continue

                    # Добавляем небольшую задержку между запросами
                    await asyncio.sleep(2)

                if users_data:
                    for user_data in users_data:
                        user_id = user_data['id']
                        users.add(user_id)
                        if user_id not in info_users:
                            new_users.add(user_id)
                        update_user_info(user_id, user_data['username'], user_data['phone'])
                    logging.info(f"Данные {len(users_data)} пользователей сохранены в базу")
                else:
                    logging.info(f"В канале {channel_username} не найдено комментариев с данными пользователей.")

            except Exception as e:
                logging.error(f"Произошла ошибка: {str(e)}")


async def parse_chat_members(chat_id):
    async with app:
        try:
            async for member in app.get_chat_members(chat_id):
                user_id = member.user.id
                users.add(user_id)
                if user_id not in info_users:
                    new_users.add(user_id)
                update_user_info(user_id, member.user.username, member.user.phone_number)
        except ChatAdminRequired:
            logging.warning(f"Необходимы права администратора для чата ID {chat_id}. Пропуск.")
        except FloodWait as e:
            logging.warning(f"Достигнут лимит запросов. Ожидание {e.value} секунд.")
            await asyncio.sleep(e.value)


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    if message.from_user.id not in acs_users:
        no_access_message = await message.reply("У вас нет доступа к этому боту.")
        await asyncio.sleep(3)
        await bot.delete_message(message.chat.id, no_access_message.message_id)
    else:
        await show_main_menu(message.from_user.id)


async def update_main_menu_text(user_id):
    user_count = len(users)
    last_parsed_count = len(new_users)
    while parsing_in_progress:
        await bot.edit_message_caption(
            chat_id=user_id,
            message_id=main_menu_message_id,
            caption=f"Главное меню:\n\nВ базе: {user_count} чел\nСпарсено за последний раз: {last_parsed_count} чел"
        )
        await asyncio.sleep(5)


@dp.callback_query_handler(lambda c: c.data == 'stop_parsing')
async def stop_parsing(callback_query: types.CallbackQuery):
    global parsing_in_progress
    await bot.answer_callback_query(callback_query.id)
    if parsing_in_progress:
        parsing_in_progress = False
        await bot.send_message(callback_query.from_user.id, "Парсинг остановлен по запросу пользователя.")
    else:
        await bot.send_message(callback_query.from_user.id, "Парсинг не выполняется.")


async def check_and_save_new_users_file():
    previous_size = None
    while True:
        # Ждем 1 минуту и 3 секунды перед следующей проверкой
        await asyncio.sleep(60)

        # Проверяем, существует ли файл new_users.json
        if os.path.exists('new_users.json'):
            # Получаем текущий размер файла
            current_size = os.path.getsize('new_users.json')

            # Если размер файла изменился
            if current_size != previous_size:
                # Получаем текущую дату и время
                current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

                # Создаем папку newFILES, если она не существует
                if not os.path.exists('newFILES'):
                    os.makedirs('newFILES')

                # Создаем имя для нового файла с датой и временем
                new_file_name = f"new_users_{current_time}.json"
                new_file_path = os.path.join('newFILES', new_file_name)

                # Копируем содержимое файла new_users.json в новый файл
                with open('new_users.json', 'r') as original_file:
                    with open(new_file_path, 'w') as new_file:
                        new_file.write(original_file.read())

                print(f"Создана копия файла new_users.json: {new_file_path}")

                # Проверяем, существует ли файл converted_new_users.txt
                if os.path.exists('converted_new_users.txt'):
                    # Создаем имя для нового файла converted_new_users.txt с датой и временем
                    converted_file_name = f"converted_new_users_{current_time}.txt"
                    converted_file_path = os.path.join('newFILES', converted_file_name)

                    # Копируем содержимое файла converted_new_users.txt в новый файл
                    with open('converted_new_users.txt', 'r') as original_converted_file:
                        with open(converted_file_path, 'w') as new_converted_file:
                            new_converted_file.write(original_converted_file.read())

                    print(f"Создана копия файла converted_new_users.txt: {converted_file_path}")

                # Обновляем предыдущий размер файла
                previous_size = current_size
        else:
            print("Файл new_users.json не найден.")


@dp.callback_query_handler(lambda c: c.data == 'convert')
async def process_convert(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await convert_and_send_files(callback_query.from_user.id)


async def convert_and_send_files(user_id):
    # Конвертация full_base.json
    converted_data = []
    try:
        with open(full_base_file, 'r') as f:
            for line in f:
                user_data = json.loads(line)
                username = user_data.get('username', '')
                phone = user_data.get('phone', '')
                if username and username != 'None':
                    username = f"@{username}"
                else:
                    username = ''
                if phone and phone != 'None':
                    phone = phone
                else:
                    phone = ''
                if username or phone:
                    converted_data.append(f"{username} {phone}")
    except FileNotFoundError:
        await bot.send_message(user_id, "Файл full_base.json не найден.")
        return

    # Сохранение конвертированных данных в файл
    with open('converted_full_base.txt', 'w') as f:
        for line in converted_data:
            f.write(line + '\n')

    # Отправка конвертированного файла
    with open('converted_full_base.txt', 'rb') as f:
        await bot.send_document(user_id, f)

    # Конвертация new_users.json
    converted_data = []
    try:
        if os.path.getsize(new_users_file) == 0:
            logging.info("Файл new_users.json пуст.")
            await bot.send_message(user_id, "Файл new_users.json пуст.")
            return

        with open(new_users_file, 'r') as f:
            for line in f:
                user_data = json.loads(line)
                username = user_data.get('username', '')
                phone = user_data.get('phone', '')
                user_id_value = user_data.get('user_id', '')
                if username and username != 'None':
                    username = f"@{username}"
                else:
                    username = ''
                if phone and phone != 'None':
                    phone = phone
                else:
                    phone = ''
                if username or phone:
                    converted_data.append(f"{username} {phone}")
                else:
                    converted_data.append(f"{username} {phone} {user_id_value}")
    except FileNotFoundError:
        logging.error("Файл new_users.json не найден.")
        await bot.send_message(user_id, "Файл new_users.json не найден.")
        return

    # Сохранение конвертированных данных в файл
    with open('converted_new_users.txt', 'w') as f:
        for line in converted_data:
            f.write(line + '\n')

    # Отправка конвертированного файла
    try:
        with open('converted_new_users.txt', 'rb') as f:
            await bot.send_document(user_id, f)
    except Exception as e:
        logging.error(f"Ошибка при отправке файла: {e}")
        await bot.send_message(user_id, f"Ошибка при отправке файла: {e}")


async def show_main_menu(user_id: int):
    global main_menu_message_id
    user_count = len(users)
    last_parsed_count = len(new_users)
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Начать парсинг", callback_data='pars'))
    keyboard.add(InlineKeyboardButton("Полный парсинг канала", callback_data='full_pars_ch'))
    keyboard.add(InlineKeyboardButton("Автономный парсер", callback_data='auto_parser'))
    #keyboard.add(InlineKeyboardButton("Конвертировать", callback_data='convert'))
    keyboard.add(InlineKeyboardButton("Настройки", callback_data='settings'))
    keyboard.add(InlineKeyboardButton("Файлы", callback_data='files'))
    main_menu_message = await bot.send_photo(user_id, photo=open('images/main.jpg', 'rb'),
                                             caption=f"Главное меню:\n\nВ базе: {user_count} чел\nСпарсено за последний раз: {last_parsed_count} чел",
                                             reply_markup=keyboard)
    main_menu_message_id = main_menu_message.message_id


def get_cancel_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Отмена", callback_data='cancel'))
    return keyboard


@dp.callback_query_handler(lambda c: c.data == 'settings')
async def process_settings(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Администрация", callback_data='admin'))
    keyboard.add(InlineKeyboardButton("Каналы", callback_data='channels'))
    keyboard.add(InlineKeyboardButton("Чаты", callback_data='chats'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='close'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/settings.jpg', 'rb'), caption="Настройки:",
                         reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == 'admin')
async def process_admin(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    admins = "\n".join(map(str, acs_users))
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Добавить", callback_data='add_acs'))
    keyboard.add(InlineKeyboardButton("Удалить", callback_data='del_acs'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='close'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/admin.jpg', 'rb'),
                         caption=f"Администраторы:\n{admins}", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == 'add_acs')
async def process_add_acs(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Отправьте user_id для добавления:\n/cancel - отмена",
                           reply_markup=get_cancel_keyboard())
    await Form.acs.set()


@dp.callback_query_handler(lambda c: c.data == 'del_acs')
async def process_del_acs(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Отправьте user_id для удаления:\n/cancel - отмена",
                           reply_markup=get_cancel_keyboard())
    await Form.acs.set()


@dp.message_handler(state=Form.acs)
async def process_acs(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            user_id = int(message.text)
            if user_id in acs_users:
                acs_users.remove(user_id)
                await message.reply(f"Пользователь {user_id} удален из доступа")
            else:
                acs_users.add(user_id)
                await message.reply(f"Пользователь {user_id} добавлен в доступ")
            save_acs_users(acs_users)
        except Exception as e:
            await message.reply(f"Ошибка: {e}. Пожалуйста, введите user_id корректно.")
        await state.finish()
        await show_main_menu(message.from_user.id)


@dp.callback_query_handler(lambda c: c.data == 'channels')
async def process_channels(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    channels_info = "\n".join([f"Канал: [{channel}](https://t.me/{channel})" for channel in channels.keys()])
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Добавить", callback_data='add_ch'))
    keyboard.add(InlineKeyboardButton("Удалить", callback_data='del_ch'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='close'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/channels.jpg', 'rb'),
                         caption=f"Каналы для парсинга:\n{channels_info}", reply_markup=keyboard, parse_mode='Markdown')


@dp.callback_query_handler(lambda c: c.data == 'add_ch')
async def process_add_ch(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Отправьте канал для добавления:",
                           reply_markup=get_cancel_keyboard())
    await Form.addch.set()


@dp.callback_query_handler(lambda c: c.data == 'del_ch')
async def process_dellch(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = InlineKeyboardMarkup()
    for channel in channels:
        keyboard.add(InlineKeyboardButton(channel, callback_data=f'del_ch_{channel}'))
    keyboard.add(InlineKeyboardButton("Отмена", callback_data='cancel'))
    await bot.send_message(callback_query.from_user.id, "Выберите канал для удаления:", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data.startswith('del_ch_'))
async def process_delete_channel(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    channel = callback_query.data.split('_', 2)[-1]  # Извлекаем полный идентификатор канала
    if channel in channels:
        del channels[channel]
        save_channels(channels)
        await bot.send_message(callback_query.from_user.id, f"Канал {channel} удален из парсера")
    else:
        await bot.send_message(callback_query.from_user.id, f"Канал {channel} не найден в парсере")
    await show_main_menu(callback_query.from_user.id)


@dp.callback_query_handler(lambda c: c.data == 'cancel', state='*')
async def cancel_handler(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Действие отменено.")
    await show_main_menu(callback_query.from_user.id)


@dp.message_handler(state=Form.addch)
async def process_addch(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            channel = message.text
            channels[channel] = True
            save_channels(channels)
            await message.reply(f"Канал {channel} добавлен в парсер")
        except Exception as e:
            await message.reply(f"Ошибка: {e}. Пожалуйста, введите канал корректно.")
        await state.finish()
        await show_main_menu(message.from_user.id)


@dp.message_handler(state=Form.dellch)
async def process_dellch(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            channel = message.text
            if channel in channels:
                del channels[channel]
                save_channels(channels)
                await message.reply(f"Канал {channel} удален из парсера")
            else:
                keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("ОК", callback_data='ok'))
                await message.reply(f"Канал {channel} не найден в парсере", reply_markup=keyboard)
        except Exception as e:
            await message.reply(f"Ошибка: {e}. Пожалуйста, введите канал корректно.")
        await state.finish()
        await show_main_menu(message.from_user.id)


@dp.callback_query_handler(lambda c: c.data == 'chats')
async def process_chats(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    chats_info = "\n".join([f"Чат: [{chat_id}](https://t.me/{chat_id})" for chat_id in chats.keys()])
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Добавить", callback_data='add_chat'))
    keyboard.add(InlineKeyboardButton("Удалить", callback_data='del_chat'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='close'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/chats.jpg', 'rb'),
                         caption=f"Чаты для парсинга:\n{chats_info}", reply_markup=keyboard, parse_mode='Markdown')


@dp.callback_query_handler(lambda c: c.data == 'ok')
async def process_ok(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)


@dp.callback_query_handler(lambda c: c.data == 'add_chat')
async def process_add_chat(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Отправьте chat_id для добавления:",
                           reply_markup=get_cancel_keyboard())
    await Form.addchat.set()


@dp.callback_query_handler(lambda c: c.data == 'del_chat')
async def process_dellchat(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = InlineKeyboardMarkup()
    for chat_id in chats:
        keyboard.add(InlineKeyboardButton(chat_id, callback_data=f'del_chat_{chat_id}'))
    keyboard.add(InlineKeyboardButton("Отмена", callback_data='cancel'))
    await bot.send_message(callback_query.from_user.id, "Выберите чат для удаления:", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data.startswith('del_chat_'))
async def process_delete_chat(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    chat_id = callback_query.data.split('_', 2)[-1]  # Извлекаем полный идентификатор чата
    if chat_id in chats:
        del chats[chat_id]
        save_chats(chats)
        await bot.send_message(callback_query.from_user.id, f"Чат {chat_id} удален из парсера")
    else:
        await bot.send_message(callback_query.from_user.id, f"Чат {chat_id} не найден в парсере")
    await show_main_menu(callback_query.from_user.id)


@dp.message_handler(state=Form.addchat)
async def process_addchat(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            chat_id = message.text
            chats[chat_id] = True
            save_chats(chats)
            await message.reply(f"Чат {chat_id} добавлен в парсер")
        except Exception as e:
            await message.reply(f"Ошибка: {e}. Пожалуйста, введите chat_id корректно.")
        await state.finish()
        await show_main_menu(message.from_user.id)


@dp.message_handler(state=Form.dellchat)
async def process_dellchat(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            chat_id = message.text
            if chat_id in chats:
                del chats[chat_id]
                save_chats(chats)
                await message.reply(f"Чат {chat_id} удален из парсера")
            else:
                keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("ОК", callback_data='ok'))
                await message.reply(f"Чат {chat_id} не найден в парсере", reply_markup=keyboard)
        except Exception as e:
            await message.reply(f"Ошибка: {e}. Пожалуйста, введите chat_id корректно.")
        await state.finish()
        await show_main_menu(message.from_user.id)


async def update_parsing_message(user_id, message_id, start_time):
    while parsing_in_progress:
        elapsed_time = int(time.time() - start_time)
        await bot.edit_message_caption(
            chat_id=user_id,
            message_id=message_id,
            caption=f"Парсинг начат..\nПрошло времени: {elapsed_time} секунд",
            reply_markup=get_stop_parsing_keyboard()
        )
        await asyncio.sleep(1)


@dp.callback_query_handler(lambda c: c.data == 'pars')
async def process_pars(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    user_id = callback_query.from_user.id

    # Формируем информацию о каналах и чатах
    channels_info = "\n".join([f"Канал: [{channel}](https://t.me/{channel})" for channel in channels.keys()])
    chats_info = "\n".join([f"Чат: [{chat_id}](https://t.me/{chat_id})" for chat_id in chats.keys()])

    # Создаем клавиатуру с двумя кнопками
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Подтвердить запуск", callback_data='confirm_pars'))
    keyboard.add(InlineKeyboardButton("Отмена", callback_data='cancel'))

    # Отправляем сообщение с текстом и клавиатурой
    await bot.send_message(user_id, f"Ссылки на каналы/чаты:\n\n{channels_info}\n{chats_info}\n\nПодтвердить запуск?",
                           reply_markup=keyboard, parse_mode='Markdown')


def get_stop_parsing_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Остановить парсинг", callback_data='stop_parsing'))
    return keyboard


@dp.callback_query_handler(lambda c: c.data == 'stop_parsing')
async def stop_parsing_handler(callback_query: types.CallbackQuery):
    global parsing_in_progress
    await bot.answer_callback_query(callback_query.id)
    if parsing_in_progress:
        await stop_parsing()
        await bot.send_message(callback_query.from_user.id, "Парсинг остановлен по запросу пользователя.")
    else:
        await bot.send_message(callback_query.from_user.id, "Парсинг не выполняется.")


@dp.callback_query_handler(lambda c: c.data == 'confirm_pars')
async def confirm_pars(callback_query: types.CallbackQuery):
    global parsing_in_progress
    await bot.answer_callback_query(callback_query.id)
    user_id = callback_query.from_user.id

    # Отправляем сообщение с фотографией stats.jpg
    parsing_message = await bot.send_photo(
        user_id,
        photo=open('images/stats.jpg', 'rb'),
        caption="Парсинг начат",
        reply_markup=get_stop_parsing_keyboard()
    )

    start_time = time.time()

    parsing_in_progress = True
    asyncio.create_task(update_main_menu_text(user_id))

    # Запускаем задачу обновления сообщения
    asyncio.create_task(update_parsing_message(user_id, parsing_message.message_id, start_time))

    global users, new_users, info_users
    users = load_users()
    new_users = set()
    #info_users = {}
    await parse_channels(limit=50)
    for chat_id in chats:
        await parse_chat_members(chat_id)
    save_users(users)
    save_new_users(new_users)

    parsing_in_progress = False

    # Формируем сообщение для отправки
    summary_message = (
        f"<b>Всего пользователей:</b> {len(users)}\n"
        f"<b>Новые пользователи:</b> {len(new_users)}\n"
        f"<b>Парсинг завершен:</b> {datetime.now().strftime('%d.%m.%Y:%H:%M')}"
    )
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Получить файл", callback_data='convert'))
    await bot.send_photo(user_id, photo=open('images/done.jpg', 'rb'), caption=summary_message, parse_mode='HTML',
                         reply_markup=keyboard)

    # Отправляем файлы, если они не пусты
    #with open(full_base_file, 'rb') as f:
    #     await bot.send_document(user_id, f)

    # if os.path.getsize(new_users_file) > 0:
    #     with open(new_users_file, 'rb') as f:
    #         await bot.send_document(user_id, f)
    # else:
    #    await bot.send_message(user_id, "Файл new_users.json пуст.")

    await bot.delete_message(user_id, parsing_message.message_id)
    #await show_main_menu(user_id)


@dp.callback_query_handler(lambda c: c.data == 'full_pars_ch')
async def process_full_pars_ch(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = InlineKeyboardMarkup()
    for channel in channels:
        keyboard.add(InlineKeyboardButton(channel, callback_data=f'full_pars_{channel}'))
    keyboard.add(InlineKeyboardButton("Отмена", callback_data='cancel'))
    await bot.send_message(callback_query.from_user.id, "Выберите канал для полного парсинга:", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data.startswith('full_pars_'))
async def process_full_pars_channel(callback_query: types.CallbackQuery):
    global parsing_in_progress
    await bot.answer_callback_query(callback_query.id)
    channel = callback_query.data.split('_')[-1]
    user_id = callback_query.from_user.id
    await bot.send_message(user_id, f"Начало полного парсинга канала {channel}...")

    # Отправляем сообщение с фотографией stats.jpg
    parsing_message = await bot.send_photo(
        user_id,
        photo=open('images/stats.jpg', 'rb'),
        caption="Парсинг начат",
        reply_markup=get_stop_parsing_keyboard()
    )

    start_time = time.time()

    parsing_in_progress = True
    asyncio.create_task(update_main_menu_text(user_id))

    # Запускаем задачу обновления сообщения
    asyncio.create_task(update_parsing_message(user_id, parsing_message.message_id, start_time))

    global users, new_users, info_users
    users = load_users()
    new_users = set()
    #info_users = {}
    await parse_channels(limit=None)  # Парсинг всех постов
    save_users(users)
    save_new_users(new_users)

    parsing_in_progress = False

    # Формируем сообщение для отправки
    summary_message = (
        f"<b>Всего пользователей:</b> {len(users)}\n"
        f"<b>Новые пользователи:</b> {len(new_users)}\n"
        f"<b>Парсинг завершен:</b> {datetime.now().strftime('%d.%m.%Y:%H:%M')}"
    )
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Конвертировать", callback_data='convert'))
    await bot.send_photo(user_id, photo=open('images/done.jpg', 'rb'), caption=summary_message, parse_mode='HTML',
                         reply_markup=keyboard)

    # Отправляем файлы, если они не пусты
    #with open(full_base_file, 'rb') as f:
    #    await bot.send_document(user_id, f)

    #  if os.path.getsize(new_users_file) > 0:
    #       with open(new_users_file, 'rb') as f:
    #          await bot.send_document(user_id, f)
    #    else:
    #     await bot.send_message(user_id, "Файл new_users.json пуст.")

    await bot.delete_message(user_id, parsing_message.message_id)
    #await show_main_menu(user_id)


@dp.callback_query_handler(lambda c: c.data == 'files')
async def process_files(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Получить базу", callback_data='get_base'))
    keyboard.add(InlineKeyboardButton("Получить файлы", callback_data='get_files'))
    keyboard.add(InlineKeyboardButton("Удалить файлы", callback_data='dell_files'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='main_menu'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/files.jpg', 'rb'), caption="Файлы:",
                         reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == 'get_files')
async def process_get_files(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    user_id = callback_query.from_user.id

    # Получение последних 10 файлов из папки newFILES
    latest_files = get_latest_files('newFILES')
    for file_name in latest_files:
        file_path = os.path.join('newFILES', file_name)
        with open(file_path, 'rb') as f:
            await bot.send_document(user_id, f)


def get_latest_files(directory, num_files=10):
    files = os.listdir(directory)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return files[:num_files]


@dp.callback_query_handler(lambda c: c.data == 'view_files')
async def process_view_files(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    files = os.listdir()
    file_list = "\n".join(files)
    await bot.send_message(callback_query.from_user.id, f"Файлы:\n{file_list}")


@dp.callback_query_handler(lambda c: c.data == 'get_base')
async def process_get_base(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    user_id = callback_query.from_user.id

    # Отправка файла с общей базой
    with open(full_base_file, 'rb') as f:
        await bot.send_document(user_id, f)

    # Получение последних 10 файлов из папки newFILES
    latest_files = get_latest_files('newFILES')
    for file_name in latest_files:
        file_path = os.path.join('newFILES', file_name)
        with open(file_path, 'rb') as f:
            await bot.send_document(user_id, f)


@dp.callback_query_handler(lambda c: c.data == 'dell_files')
async def process_dell_files(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    try:
        os.remove(full_base_file)
        os.remove(new_users_file)
        # os.remove(channels_file)
        #os.remove(chats_file)
        await bot.send_message(callback_query.from_user.id, "JSON файлы удалены.")
    except Exception as e:
        await bot.send_message(callback_query.from_user.id, f"Ошибка при удалении файлов: {e}")
    await show_main_menu(callback_query.from_user.id)


@dp.callback_query_handler(lambda c: c.data == 'close')
async def process_close(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)


@dp.callback_query_handler(lambda c: c.data == 'main_menu')
async def process_main_menu(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await show_main_menu(callback_query.from_user.id)


@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    logging.error(f"Update {update} caused error {exception}")
    for admin in acs_users:
        await bot.send_message(admin, f"Произошла ошибка: {exception}")


@dp.callback_query_handler(lambda c: c.data == 'auto_parser')
async def process_auto_parser(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    settings = load_auto_parser_settings()
    status = settings.get('status', 'Off')
    time = settings.get('time', 'Not set')
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Включить/Выключить", callback_data='toggle_auto_parser'))
    keyboard.add(InlineKeyboardButton("Настроить время", callback_data='set_auto_parser_time'))
    keyboard.add(InlineKeyboardButton("Закрыть", callback_data='close'))
    await bot.send_photo(callback_query.from_user.id, photo=open('images/auto_parser.jpg', 'rb'),
                         caption=f"Автономный парсер:\n\nСтатус: {status}\nВремя: {time}", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == 'toggle_auto_parser')
async def toggle_auto_parser(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    settings = load_auto_parser_settings()
    current_status = settings.get('status', 'Off')
    new_status = 'On' if current_status == 'Off' else 'Off'
    settings['status'] = new_status
    save_auto_parser_settings(settings)
    await bot.send_message(callback_query.from_user.id, f"Автономный парсер теперь {new_status}")
    await show_main_menu(callback_query.from_user.id)


@dp.callback_query_handler(lambda c: c.data == 'set_auto_parser_time')
async def set_auto_parser_time(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "Отправьте время в формате HH:MM:\n/cancel - отмена",
                           reply_markup=get_cancel_keyboard())
    await Form.auto_parser_time.set()


@dp.message_handler(state=Form.auto_parser_time)
async def process_auto_parser_time(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await state.finish()
        await message.reply('Действие отменено.')
    else:
        try:
            time = datetime.strptime(message.text, '%H:%M').time()
            settings = load_auto_parser_settings()
            settings['time'] = message.text
            save_auto_parser_settings(settings)
            await message.reply(f"Время установлено на {message.text}")
        except ValueError:
            await message.reply("Неверный формат времени. Пожалуйста, используйте формат HH:MM.")
        await state.finish()
        await show_main_menu(message.from_user.id)


async def send_summary_message(user_id):
    summary_message = (
        f"<b>Всего пользователей:</b> {len(users)}\n"
        f"<b>Новые пользователи:</b> {len(new_users)}\n"
        f"<b>Парсинг завершен:</b> {datetime.now().strftime('%d.%m.%Y:%H:%M')}"
    )
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Конвертировать", callback_data='convert'))
    await bot.send_photo(user_id, photo=open('images/done.jpg', 'rb'), caption=summary_message, parse_mode='HTML',
                         reply_markup=keyboard)


async def send_files(user_id):
    with open(full_base_file, 'rb') as f:
        await bot.send_document(user_id, f)

    if os.path.getsize(new_users_file) > 0:
        with open(new_users_file, 'rb') as f:
            await bot.send_document(user_id, f)
    else:
        await bot.send_message(user_id, "Файл new_users.json пуст.")


async def schedule_auto_parser():
    while True:
        settings = load_auto_parser_settings()
        if settings.get('status', 'Off') == 'On':
            now = datetime.now()
            logging.info(f"Текущее время: {now}")
            scheduled_time = datetime.strptime(settings['time'], '%H:%M').time()
            next_run = now.replace(hour=scheduled_time.hour, minute=scheduled_time.minute, second=0, microsecond=0)
            if next_run <= now:
                logging.info(f"Запуск парсера в текущее время: {next_run}")
                await run_auto_parser()
                next_run += timedelta(days=1)
            logging.info(f"Следующий запуск парсера запланирован на {next_run}")
            await asyncio.sleep((next_run - now).total_seconds())
        else:
            logging.info("Автономный парсер выключен. Ожидание...")
            await asyncio.sleep(60)


async def run_auto_parser():
    global parsing_in_progress
    logging.info("Запуск автономного парсера...")
    parsing_in_progress = True
    global users, new_users, info_users
    users = load_users()
    new_users = set()
    await parse_channels(limit=50)
    for chat_id in chats:
        await parse_chat_members(chat_id)
    save_users(users)
    save_new_users(new_users)
    parsing_in_progress = False
    logging.info("Автономный парсер завершил работу.")

    # Отправка summary сообщения и файлов после завершения парсинга
    await send_summary_message(admin_id)
    await send_files(admin_id)


async def on_startup(dp):
    logging.info("Бот запущен")
    asyncio.create_task(schedule_auto_parser())
    asyncio.create_task(check_and_save_new_users_file())


if __name__ == '__main__':
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    channels = load_channels()
    chats = load_chats()
    users = load_users()
    acs_users = load_acs_users()
    auto_parser_settings = load_auto_parser_settings()

    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
