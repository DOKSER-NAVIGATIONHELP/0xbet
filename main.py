"""
╔══════════════════════════════════════════════╗
║         LUMINA — RP Black Market Bot         ║
║         Bot API 9.4 | aiogram 3.x            ║
╚══════════════════════════════════════════════╝

Требования:
  pip install aiogram aiohttp aiosqlite

Запуск:
  python bot.py

ВАЖНО: Замени BOT_TOKEN своим токеном.
"""

import asyncio
import logging
import random
import string
import aiosqlite
import aiohttp
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

logging.basicConfig(level=logging.INFO)

# ══════════════════════════════════════════════
#  КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN = "8694169660:AAEU58PhkQVGkbq5b3ycQBHxG86qVIu317U"  # ← замени после revoke старого
DB_PATH   = "lumina.db"
ADMIN_USERNAME = "@LomanTTGG"

# Суммарный оборот платформы (можно хранить в БД, пока хардкод для демо)
PLATFORM_TURNOVER = 567209.19

# ══════════════════════════════════════════════
#  PREMIUM EMOJI IDs
# ══════════════════════════════════════════════

E_HAND     = "5212920365432476767"   # ✋ приветствие
E_SHOP     = "4925115139503455129"   # 🛒 магазин (меню)
E_PROFILE  = "5215347090674192358"   # 👤 профиль
E_INFO     = "4926974091543447184"   # 📖 информация
E_QUEST1   = "4924866615515809418"   # ❓ выбор
E_SWORD    = "5436102448273965193"   # 🗡 оружие
E_PILL     = "5345982227538785769"   # 💊 наркотики
E_BOX      = "4924862170224658711"   # 📦 другое
E_TAG      = "4924753567681611811"   # 🔖 создать объявление
E_BACK     = "4924988781565577148"   # ◀️ назад
E_QUEST2   = "4925246707236603673"   # ❓ категория
E_QUEST3   = "4927187031727015348"   # ❓ название
E_BOOK1    = "4925075329451558632"   # 📖 описание
E_QUEST4   = "5213108227302003435"   # ❓ количество
E_QUEST5   = "4924988592587015783"   # ❓ цена
E_OK       = "5213056077809099062"   # ✅ успешно
E_LOCK     = "5213464610803323563"   # 🔒 мои объявления
E_ADS      = "4925246707236603673"   # ❓ ваши объявления
E_LISTING  = "4925090452031408197"   # 📋 название товара
E_DESC     = "4924922913947125599"   # 📜 описание
E_QTY      = "5213108227302003435"   # ❓ кол-во
E_PRICE    = "4924988592587015783"   # ❓ цена
E_REMOVE   = "4924821557013907177"   # ❌ снять объявление
E_EDIT     = "5215520061892103070"   # ✏️ изменить цену
E_BUY      = "4924998397997352581"   # 💳 купить
E_WAIT     = "4925163749943281335"   # 📦 ожидайте
E_MONEY    = "4927470722906850798"   # 💵 сделка пришла
E_UNLOCK   = "5213226497816434626"   # 🔓 сделка активна
E_SIDES    = "5213281821290173707"   # ❓ стороны
E_BOOKTWO  = "4925152621683017316"   # 📖 описание сделки
E_HISTORY  = "4927097116586674284"   # 🕐 история
E_MSG      = "4925090452031408197"   # ✉️ написать
E_COMPLETE = "5213464941515808062"   # 🛡 завершить
E_CANCEL   = "5213360384831952090"   # 🛡 отменить
E_SENT     = "5215480427933898474"   # ✅ отправлено
E_XMARK    = "5213175168662279435"   # ❌ нет
E_STAR     = "5213085536989780516"   # ⭐ репутация
E_CHART    = "5213399275760814479"   # 📊 статистика
E_WALLET   = "4925106025582823166"   # 💰 финансы
E_DEALS    = "4927373317343544956"   # 📋 сделки/отзывы
E_DEPOSIT  = "4925225477213259152"   # ➕ пополнить
E_WITHDRAW = "4924772027451049781"   # ➖ вывести
E_HISTFIN  = "4924866615515809418"   # 📖 история пополнений
E_SHIELD1  = "5213256712911363107"   # 🛡 администратор
E_SHIELD2  = "5213253010649551755"   # ❓ инструкция заголовок
E_REVIEW   = "4925090452031408197"   # ⭐ оставить отзыв
E_STARS    = "5213085536989780516"   # ⭐ звёзды отзыва


def pe(emoji_id: str, fallback: str = "•") -> str:
    """Возвращает HTML-тег для премиум-эмодзи."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


# ══════════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ══════════════════════════════════════════════

class CreateListing(StatesGroup):
    category    = State()
    name        = State()
    description = State()
    quantity    = State()
    price       = State()

class BuyFlow(StatesGroup):
    quantity = State()

class EditPrice(StatesGroup):
    new_price = State()

class SendDealMessage(StatesGroup):
    text = State()

class LeaveReview(StatesGroup):
    rating = State()
    text   = State()

class Withdraw(StatesGroup):
    amount = State()


# ══════════════════════════════════════════════
#  БД — ИНИЦИАЛИЗАЦИЯ
# ══════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT,
                balance       REAL    DEFAULT 0,
                total_deals   INTEGER DEFAULT 0,
                deals_as_buyer  INTEGER DEFAULT 0,
                deals_as_seller INTEGER DEFAULT 0,
                amount_as_buyer  REAL DEFAULT 0,
                amount_as_seller REAL DEFAULT 0,
                positive_reviews INTEGER DEFAULT 0,
                negative_reviews INTEGER DEFAULT 0,
                registered_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id   INTEGER,
                category    TEXT,
                name        TEXT,
                description TEXT,
                quantity    INTEGER,
                price       REAL,
                active      INTEGER DEFAULT 1,
                created_at  TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id            TEXT PRIMARY KEY,
                listing_id    INTEGER,
                buyer_id      INTEGER,
                seller_id     INTEGER,
                item_name     TEXT,
                quantity      INTEGER,
                total_amount  REAL,
                status        TEXT DEFAULT 'pending',
                complete_requested_by INTEGER DEFAULT NULL,
                created_at    TEXT,
                buyer_msg_id  INTEGER DEFAULT NULL,
                seller_msg_id INTEGER DEFAULT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS deal_messages (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id   TEXT,
                sender_id INTEGER,
                text      TEXT,
                sent_at   TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id     TEXT,
                reviewer_id INTEGER,
                target_id   INTEGER,
                rating      INTEGER,
                text        TEXT,
                created_at  TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                type       TEXT,
                amount     REAL,
                comment    TEXT,
                created_at TEXT
            )
        """)
        await db.commit()


# ══════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cur:
            return await cur.fetchone()

async def ensure_user(user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        exists = await (await db.execute(
            "SELECT 1 FROM users WHERE user_id=?", (user_id,)
        )).fetchone()
        if not exists:
            await db.execute(
                "INSERT INTO users (user_id,username,registered_at) VALUES (?,?,?)",
                (user_id, username or "", datetime.now().strftime("%d.%m.%Y"))
            )
            await db.commit()
        else:
            await db.execute(
                "UPDATE users SET username=? WHERE user_id=?",
                (username or "", user_id)
            )
            await db.commit()

async def get_listings(seller_id: int = None, category: str = None, active_only=True):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q = "SELECT * FROM listings WHERE 1=1"
        params = []
        if active_only:
            q += " AND active=1"
        if seller_id:
            q += " AND seller_id=?"
            params.append(seller_id)
        if category:
            q += " AND category=?"
            params.append(category)
        async with db.execute(q, params) as cur:
            return await cur.fetchall()

async def get_listing(listing_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM listings WHERE id=?", (listing_id,)) as cur:
            return await cur.fetchone()

async def get_active_deal_for_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deals WHERE (buyer_id=? OR seller_id=?) AND status IN ('pending','active')",
            (user_id, user_id)
        ) as cur:
            return await cur.fetchone()

async def get_deal(deal_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM deals WHERE id=?", (deal_id,)) as cur:
            return await cur.fetchone()

def gen_deal_id():
    return "G-" + str(random.randint(10000, 99999))

async def add_transaction(user_id, ttype, amount, comment=""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO transactions (user_id,type,amount,comment,created_at) VALUES (?,?,?,?,?)",
            (user_id, ttype, amount, comment, datetime.now().strftime("%d.%m.%Y %H:%M"))
        )
        await db.commit()


# ══════════════════════════════════════════════
#  BOT API 9.4 — прямые запросы (цветные кнопки)
# ══════════════════════════════════════════════

async def api_request(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            return await resp.json()


def build_inline_v2(rows: list) -> dict:
    """
    rows = [
      [{"text":"...", "callback_data":"...", "style":"success", "icon_custom_emoji_id":"..."}],
      ...
    ]
    Возвращает dict для reply_markup.
    """
    return {"inline_keyboard": rows}


async def send_v2(chat_id: int, text: str, keyboard: dict = None, parse_mode="HTML") -> dict:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if keyboard:
        payload["reply_markup"] = keyboard
    return await api_request("sendMessage", payload)


async def edit_v2(chat_id: int, message_id: int, text: str, keyboard: dict = None) -> dict:
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if keyboard:
        payload["reply_markup"] = keyboard
    return await api_request("editMessageText", payload)


async def answer_cb(callback_id: str, text: str = "", alert=False):
    await api_request("answerCallbackQuery", {
        "callback_query_id": callback_id,
        "text": text,
        "show_alert": alert,
    })


# ══════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def main_reply_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=f"{pe(E_SHOP,'🛒')} Магазин"),
                KeyboardButton(text=f"{pe(E_PROFILE,'👤')} Профиль"),
            ],
            [
                KeyboardButton(text=f"{pe(E_INFO,'📖')} Информация"),
            ],
        ],
        resize_keyboard=True,
    )


def btn(text: str, cb: str, style: str = None, icon: str = None) -> dict:
    b = {"text": text, "callback_data": cb}
    if style:
        b["style"] = style
    if icon:
        b["icon_custom_emoji_id"] = icon
    return b


def back_btn(cb="back_to_start") -> dict:
    return btn(f"{pe(E_BACK,'◀️')} Назад", cb, icon=E_BACK)


# ══════════════════════════════════════════════
#  ТЕКСТЫ
# ══════════════════════════════════════════════

WELCOME_TEXT = (
    f"{pe(E_HAND,'✋')} <b>Добро пожаловать в Lumina.</b>\n"
    f"╰ Приятных покупок"
)

SHOP_TEXT = f"{pe(E_QUEST1,'❓')} <b>Выберите что хотите приобрести</b>"

def profile_text(u, pos_rev, neg_rev):
    return (
        f"{pe(E_PROFILE,'👤')} <b>Информация</b>\n"
        f"├ Никнейм: @{u['username'] or 'без ника'}\n"
        f"├ ID: <code>{u['user_id']}</code>\n"
        f"╰ Кол-во сделок: <b>{u['total_deals']}</b>\n\n"
        f"{pe(E_STAR,'⭐')} <b>Репутация</b>\n"
        f"├ Депозит: $ {u['balance']:.2f}\n"
        f"├ Отзывы (+ / -): {pos_rev} / {neg_rev}\n"
        f"╰ Дата регистрации: {u['registered_at']}\n\n"
        f"{pe(E_CHART,'📊')} <b>Статистика сделок</b>\n"
        f"├ Сделки ({u['total_deals']}): $ {(u['amount_as_buyer']+u['amount_as_seller']):.2f}\n"
        f"├ Покупатель ({u['deals_as_buyer']}): $ {u['amount_as_buyer']:.2f}\n"
        f"╰ Продавец ({u['deals_as_seller']}): $ {u['amount_as_seller']:.2f}\n\n"
        f"{pe(E_WALLET,'💰')} <b>Финансы</b>\n"
        f"╰ Баланс: $ {u['balance']:.2f}"
    )

INSTRUCTION_TEXT = (
    f"{pe(E_SHIELD2,'❓')} <b>Как совершить безопасную сделку в Lumina?</b>\n\n"
    f"<b>1. Выберите товар в магазине.</b>\n"
    f"Перейдите в {pe(E_SHOP,'🛒')} <b>Магазин</b>, выберите нужную категорию и найдите интересующий товар.\n"
    f"<i>Совет: изучите профиль продавца и его отзывы перед покупкой.</i>\n\n"
    f"<b>2. Нажмите «Купить» и укажите количество.</b>\n"
    f"Деньги будут заморожены на платформе до завершения сделки — это гарантирует безопасность для обеих сторон.\n"
    f"<i>Совет: убедитесь, что на вашем балансе достаточно средств.</i>\n\n"
    f"<b>3. Используйте чат внутри сделки.</b>\n"
    f"После принятия сделки продавцом оба участника получают доступ к защищённому чату — договаривайтесь о деталях там.\n\n"
    f"<b>4. Завершите сделку правильно.</b>\n"
    f"{pe(E_SENT,'✅')} Если всё прошло по плану — нажмите <b>Завершить сделку</b>.\n"
    f"{pe(E_XMARK,'❌')} Если возникли проблемы — нажмите <b>Отменить сделку</b> и обратитесь к администратору.\n\n"
    f"<i>Совет: никогда не передавайте товар до подтверждения сделки через бота. Это ваша защита!</i>"
)


# ══════════════════════════════════════════════
#  РОУТЕР
# ══════════════════════════════════════════════

router = Router()


# ── /start ────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    uname = message.from_user.username or ""
    await ensure_user(uid, uname)
    await message.answer(
        WELCOME_TEXT,
        reply_markup=main_reply_keyboard(),
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════
#  МАГАЗИН
# ══════════════════════════════════════════════

async def send_shop_menu(chat_id: int, user_id: int):
    listings = await get_listings(seller_id=user_id)
    has_listings = len(listings) > 0

    if has_listings:
        row4 = btn(f"{pe(E_LOCK,'🔒')} Мои объявления", "my_listings", icon=E_LOCK)
    else:
        row4 = btn(f"{pe(E_TAG,'🔖')} Создать объявление", "create_listing", icon=E_TAG)

    kb = build_inline_v2([
        [btn(f"{pe(E_SWORD,'🗡')} Оружие", "cat_weapons", icon=E_SWORD),
         btn(f"{pe(E_PILL,'💊')} Наркотики", "cat_drugs", icon=E_PILL)],
        [btn(f"{pe(E_BOX,'📦')} Другое", "cat_other", icon=E_BOX)],
        [row4],
        [back_btn("back_main")],
    ])
    await send_v2(chat_id, SHOP_TEXT, kb)


@router.message(F.text.contains("Магазин"))
async def menu_shop(message: Message, state: FSMContext):
    await state.clear()
    await send_shop_menu(message.chat.id, message.from_user.id)


@router.callback_query(F.data == "back_main")
async def cb_back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await answer_cb(cb.id)
    await cb.message.delete()
    await cb.message.answer(WELCOME_TEXT, reply_markup=main_reply_keyboard(), parse_mode="HTML")


# ── Категории товаров ─────────────────────────

CAT_NAMES = {
    "cat_weapons": ("weapons", "Оружие"),
    "cat_drugs":   ("drugs",   "Наркотики"),
    "cat_other":   ("other",   "Другое"),
}

@router.callback_query(F.data.in_({"cat_weapons", "cat_drugs", "cat_other"}))
async def cb_category(cb: CallbackQuery):
    await answer_cb(cb.id)
    cat_key, cat_label = CAT_NAMES[cb.data]
    listings = await get_listings(category=cat_key)

    if not listings:
        kb = build_inline_v2([[back_btn("open_shop")]])
        await edit_v2(cb.message.chat.id, cb.message.message_id,
                      f"{pe(E_BOX,'📦')} <b>В категории «{cat_label}» пока нет товаров.</b>",
                      kb)
        return

    rows = []
    for l in listings:
        rows.append([btn(
            f"{pe(E_LISTING,'📋')} {l['name']} — {l['price']}$/шт [{l['quantity']} шт.]",
            f"view_listing_{l['id']}"
        )])
    rows.append([back_btn("open_shop")])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_QUEST1,'❓')} <b>Категория: {cat_label}</b>\n\nВыберите товар:",
                  build_inline_v2(rows))


@router.callback_query(F.data == "open_shop")
async def cb_open_shop(cb: CallbackQuery):
    await answer_cb(cb.id)
    await cb.message.delete()
    await send_shop_menu(cb.message.chat.id, cb.from_user.id)


# ── Просмотр объявления ───────────────────────

@router.callback_query(F.data.startswith("view_listing_"))
async def cb_view_listing(cb: CallbackQuery):
    await answer_cb(cb.id)
    listing_id = int(cb.data.split("_")[-1])
    l = await get_listing(listing_id)
    if not l:
        await answer_cb(cb.id, "Товар не найден.", alert=True)
        return

    text = (
        f"{pe(E_LISTING,'📋')} <b>{l['name']}</b>\n\n"
        f"{pe(E_DESC,'📜')} {l['description']}\n\n"
        f"{pe(E_QTY,'❓')} Количество: <b>{l['quantity']} шт.</b>\n"
        f"{pe(E_PRICE,'❓')} Цена за 1 шт.: <b>{l['price']}$</b>"
    )

    if l['seller_id'] == cb.from_user.id:
        # Продавец видит свой товар — кнопки управления
        kb = build_inline_v2([
            [btn(f"{pe(E_REMOVE,'❌')} Снять объявление", f"remove_listing_{listing_id}",
                 style="danger", icon=E_REMOVE)],
            [btn(f"{pe(E_EDIT,'✏️')} Изменить цену", f"edit_price_{listing_id}", icon=E_EDIT)],
            [back_btn("my_listings")],
        ])
    else:
        # Покупатель
        kb = build_inline_v2([
            [btn(f"{pe(E_BUY,'💳')} Купить", f"buy_{listing_id}",
                 style="success", icon=E_BUY)],
            [back_btn("open_shop")],
        ])

    await edit_v2(cb.message.chat.id, cb.message.message_id, text, kb)


# ── Снять объявление ──────────────────────────

@router.callback_query(F.data.startswith("remove_listing_"))
async def cb_remove_listing(cb: CallbackQuery):
    listing_id = int(cb.data.split("_")[-1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE listings SET active=0 WHERE id=?", (listing_id,))
        await db.commit()
    await answer_cb(cb.id, "Объявление снято.", alert=True)
    await cb.message.delete()
    await send_shop_menu(cb.message.chat.id, cb.from_user.id)


# ── Изменить цену ─────────────────────────────

@router.callback_query(F.data.startswith("edit_price_"))
async def cb_edit_price(cb: CallbackQuery, state: FSMContext):
    listing_id = int(cb.data.split("_")[-1])
    await state.set_state(EditPrice.new_price)
    await state.update_data(listing_id=listing_id)
    await answer_cb(cb.id)
    kb = build_inline_v2([[back_btn("my_listings")]])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_PRICE,'❓')} <b>Введите новую цену за 1 штуку (числом):</b>", kb)


@router.message(EditPrice.new_price)
async def fsm_edit_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
    except ValueError:
        await message.answer("Введите корректное число.")
        return
    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE listings SET price=? WHERE id=?", (price, data["listing_id"]))
        await db.commit()
    await state.clear()
    await message.answer(f"{pe(E_OK,'✅')} <b>Цена обновлена!</b>", parse_mode="HTML",
                         reply_markup=main_reply_keyboard())


# ══════════════════════════════════════════════
#  МОИ ОБЪЯВЛЕНИЯ
# ══════════════════════════════════════════════

@router.callback_query(F.data == "my_listings")
async def cb_my_listings(cb: CallbackQuery):
    await answer_cb(cb.id)
    listings = await get_listings(seller_id=cb.from_user.id)

    if not listings:
        kb = build_inline_v2([
            [btn(f"{pe(E_TAG,'🔖')} Создать объявление", "create_listing", icon=E_TAG)],
            [back_btn("open_shop")],
        ])
        await edit_v2(cb.message.chat.id, cb.message.message_id,
                      f"{pe(E_ADS,'❓')} <b>Ваши объявления.</b>\n\n— Нет объявлений —", kb)
        return

    rows = []
    for l in listings:
        rows.append([btn(
            f"{pe(E_LISTING,'📋')} {l['name']} — {l['price']}$/шт [{l['quantity']}шт.]",
            f"view_listing_{l['id']}"
        )])
    rows.append([btn(f"{pe(E_TAG,'🔖')} Создать объявление", "create_listing", icon=E_TAG)])
    rows.append([back_btn("open_shop")])

    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_ADS,'❓')} <b>Ваши объявления.</b>",
                  build_inline_v2(rows))


# ══════════════════════════════════════════════
#  СОЗДАТЬ ОБЪЯВЛЕНИЕ — FSM
# ══════════════════════════════════════════════

@router.callback_query(F.data == "create_listing")
async def cb_create_listing(cb: CallbackQuery, state: FSMContext):
    await answer_cb(cb.id)
    await state.set_state(CreateListing.category)
    kb = build_inline_v2([
        [btn(f"{pe(E_SWORD,'🗡')} Оружие", "lst_cat_weapons", icon=E_SWORD),
         btn(f"{pe(E_PILL,'💊')} Наркотики", "lst_cat_drugs", icon=E_PILL)],
        [btn(f"{pe(E_BOX,'📦')} Другое", "lst_cat_other", icon=E_BOX)],
        [back_btn("open_shop")],
    ])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_QUEST2,'❓')} <b>Выберите категорию, что будете продавать:</b>",
                  kb)


@router.callback_query(F.data.in_({"lst_cat_weapons", "lst_cat_drugs", "lst_cat_other"}))
async def cb_lst_category(cb: CallbackQuery, state: FSMContext):
    cat_map = {
        "lst_cat_weapons": "weapons",
        "lst_cat_drugs": "drugs",
        "lst_cat_other": "other",
    }
    await state.update_data(category=cat_map[cb.data])
    await state.set_state(CreateListing.name)
    await answer_cb(cb.id)
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_QUEST3,'❓')} <b>Напишите название товара:</b>")


@router.message(CreateListing.name)
async def fsm_listing_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(CreateListing.description)
    await message.answer(f"{pe(E_BOOK1,'📖')} <b>Напишите описание товара.</b>",
                         parse_mode="HTML")


@router.message(CreateListing.description)
async def fsm_listing_desc(message: Message, state: FSMContext):
    await state.update_data(description=message.text)
    await state.set_state(CreateListing.quantity)
    await message.answer(f"{pe(E_QUEST4,'❓')} <b>Напишите количество товара.</b>",
                         parse_mode="HTML")


@router.message(CreateListing.quantity)
async def fsm_listing_qty(message: Message, state: FSMContext):
    try:
        qty = int(message.text)
        if qty <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введите целое положительное число.")
        return
    await state.update_data(quantity=qty)
    await state.set_state(CreateListing.price)
    await message.answer(f"{pe(E_QUEST5,'❓')} <b>Напишите цену за 1 штуку.</b>",
                         parse_mode="HTML")


@router.message(CreateListing.price)
async def fsm_listing_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введите корректную цену (число больше 0).")
        return

    data = await state.get_data()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO listings (seller_id,category,name,description,quantity,price,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (message.from_user.id, data["category"], data["name"],
             data["description"], data["quantity"], price, now)
        )
        await db.commit()

    await state.clear()
    await message.answer(
        f"{pe(E_OK,'✅')} <b>Ваш товар успешно выставлен!</b>",
        parse_mode="HTML",
        reply_markup=main_reply_keyboard()
    )


# ══════════════════════════════════════════════
#  ПОКУПКА — FSM
# ══════════════════════════════════════════════

@router.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery, state: FSMContext):
    listing_id = int(cb.data.split("_")[-1])
    l = await get_listing(listing_id)
    if not l:
        await answer_cb(cb.id, "Товар не найден.", alert=True)
        return

    if l['seller_id'] == cb.from_user.id:
        await answer_cb(cb.id, "Нельзя купить собственный товар.", alert=True)
        return

    active = await get_active_deal_for_user(cb.from_user.id)
    if active:
        await answer_cb(cb.id,
                        f"У вас уже есть активная сделка {active['id']}. Завершите её сначала.",
                        alert=True)
        return

    await state.set_state(BuyFlow.quantity)
    await state.update_data(listing_id=listing_id)
    await answer_cb(cb.id)
    await edit_v2(
        cb.message.chat.id, cb.message.message_id,
        f"{pe(E_QUEST4,'❓')} <b>Сколько товара хотите купить?</b>\n"
        f"Всего доступно: <b>{l['quantity']} шт.</b>\n\n"
        f"Напишите количество:",
    )


@router.message(BuyFlow.quantity)
async def fsm_buy_qty(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    l = await get_listing(data["listing_id"])

    try:
        qty = int(message.text)
        if qty <= 0 or qty > l['quantity']:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer(f"Введите число от 1 до {l['quantity']}.")
        return

    total = round(qty * l['price'], 2)
    user = await get_user(message.from_user.id)

    if user['balance'] < total:
        await message.answer(
            f"❌ Недостаточно средств. Ваш баланс: <b>{user['balance']:.2f}$</b>, "
            f"нужно: <b>{total:.2f}$</b>",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Списываем деньги
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance=balance-? WHERE user_id=?",
                         (total, message.from_user.id))
        await db.commit()

    await add_transaction(message.from_user.id, "freeze", total,
                          f"Заморожено по сделке на {l['name']}")

    # Создаём сделку
    deal_id = gen_deal_id()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO deals (id,listing_id,buyer_id,seller_id,item_name,quantity,"
            "total_amount,status,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (deal_id, l['id'], message.from_user.id, l['seller_id'],
             l['name'], qty, total, 'pending', now)
        )
        await db.commit()

    await state.clear()

    # Сообщение покупателю
    kb_buyer = build_inline_v2([
        [btn(f"{pe(E_REMOVE,'❌')} Отменить", f"cancel_pending_{deal_id}",
             style="danger", icon=E_REMOVE)]
    ])
    buyer_msg = await send_v2(
        message.chat.id,
        f"{pe(E_WAIT,'📦')} <b>Ожидайте, когда продавец примёт сделку.</b>\n"
        f"Сделка: <b>{deal_id}</b>\n"
        f"Заморожено: <b>{total}$</b>",
        kb_buyer
    )

    # Сохраняем msg_id покупателя
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET buyer_msg_id=? WHERE id=?",
                         (buyer_msg.get("result", {}).get("message_id"), deal_id))
        await db.commit()

    # Сообщение продавцу
    kb_seller = build_inline_v2([
        [btn(f"{pe(E_REMOVE,'❌')} Отменить", f"seller_cancel_{deal_id}",
             style="danger", icon=E_REMOVE),
         btn(f"{pe(E_OK,'✅')} Принять", f"seller_accept_{deal_id}",
             style="success", icon=E_OK)],
    ])
    seller_msg = await send_v2(
        l['seller_id'],
        f"{pe(E_MONEY,'💵')} <b>Вам пришла сделка!</b>\n\n"
        f"├ Товар: <b>{l['name']}</b>\n"
        f"├ Количество: <b>{qty} шт.</b>\n"
        f"╰ Сумма: <b>{total}$</b>",
        kb_seller
    )

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET seller_msg_id=? WHERE id=?",
                         (seller_msg.get("result", {}).get("message_id"), deal_id))
        await db.commit()


# ── Покупатель отменяет pending сделку ────────

@router.callback_query(F.data.startswith("cancel_pending_"))
async def cb_cancel_pending(cb: CallbackQuery):
    deal_id = cb.data.replace("cancel_pending_", "")
    deal = await get_deal(deal_id)
    if not deal or deal['buyer_id'] != cb.from_user.id:
        await answer_cb(cb.id, "Ошибка.", alert=True)
        return
    if deal['status'] != 'pending':
        await answer_cb(cb.id, "Сделка уже не в статусе ожидания.", alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?",
                         (deal['total_amount'], deal['buyer_id']))
        await db.commit()

    await add_transaction(deal['buyer_id'], "refund", deal['total_amount'],
                          f"Возврат по отменённой сделке {deal_id}")

    await answer_cb(cb.id)
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_XMARK,'❌')} <b>Сделка {deal_id} отменена.</b>\n"
                  f"Средства <b>{deal['total_amount']}$</b> возвращены на баланс.")

    # Уведомляем продавца
    await send_v2(deal['seller_id'],
                  f"{pe(E_XMARK,'❌')} Покупатель отменил сделку <b>{deal_id}</b>.")


# ── Продавец отменяет ─────────────────────────

@router.callback_query(F.data.startswith("seller_cancel_"))
async def cb_seller_cancel(cb: CallbackQuery):
    deal_id = cb.data.replace("seller_cancel_", "")
    deal = await get_deal(deal_id)
    if not deal or deal['seller_id'] != cb.from_user.id:
        await answer_cb(cb.id, "Ошибка.", alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?",
                         (deal['total_amount'], deal['buyer_id']))
        await db.commit()

    await add_transaction(deal['buyer_id'], "refund", deal['total_amount'],
                          f"Возврат (продавец отменил) {deal_id}")

    await answer_cb(cb.id)
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_XMARK,'❌')} <b>Сделка {deal_id} отменена.</b>")

    await send_v2(deal['buyer_id'],
                  f"{pe(E_XMARK,'❌')} Продавец отменил сделку <b>{deal_id}</b>.\n"
                  f"Средства <b>{deal['total_amount']}$</b> возвращены.")


# ── Продавец принимает ────────────────────────

@router.callback_query(F.data.startswith("seller_accept_"))
async def cb_seller_accept(cb: CallbackQuery):
    deal_id = cb.data.replace("seller_accept_", "")
    deal = await get_deal(deal_id)
    if not deal or deal['seller_id'] != cb.from_user.id:
        await answer_cb(cb.id, "Ошибка.", alert=True)
        return
    if deal['status'] != 'pending':
        await answer_cb(cb.id, "Сделка уже обработана.", alert=True)
        return

    # Проверяем нет ли у продавца другой активной
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM deals WHERE seller_id=? AND status='active'",
            (deal['seller_id'],)
        )
        other = await cur.fetchone()
    if other:
        await answer_cb(cb.id,
                        f"У вас уже есть активная сделка {other['id']}.", alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET status='active' WHERE id=?", (deal_id,))
        await db.commit()

    await answer_cb(cb.id)

    # Получаем данные участников
    buyer = await get_user(deal['buyer_id'])
    seller = await get_user(deal['seller_id'])

    deal_text = (
        f"{pe(E_UNLOCK,'🔓')} <b>Сделка {deal_id}</b>\n"
        f"├ Статус: <b>Активна</b>\n"
        f"├ Сумма: <b>{deal['total_amount']}$</b>\n"
        f"╰ Дата создания: <b>{deal['created_at']}</b>\n\n"
        f"{pe(E_SIDES,'❓')} <b>Стороны сделки</b>\n"
        f"├ Покупатель: @{buyer['username'] or 'анон'} (<code>{deal['buyer_id']}</code>)\n"
        f"╰ Продавец: @{seller['username'] or 'анон'} (<code>{deal['seller_id']}</code>)\n\n"
        f"{pe(E_BOOKTWO,'📖')} Описание: Покупка {deal['quantity']} шт. — {deal['item_name']}"
    )

    deal_kb = build_inline_v2([
        [btn(f"{pe(E_HISTORY,'🕐')} История переписки", f"deal_history_{deal_id}", icon=E_HISTORY),
         btn(f"{pe(E_MSG,'✉️')} Написать сообщение", f"deal_msg_{deal_id}", icon=E_MSG)],
        [btn(f"{pe(E_COMPLETE,'🛡')} Завершить сделку", f"deal_complete_{deal_id}",
             style="success", icon=E_COMPLETE),
         btn(f"{pe(E_CANCEL,'🛡')} Отменить сделку", f"deal_cancel_{deal_id}",
             style="danger", icon=E_CANCEL)],
    ])

    await edit_v2(cb.message.chat.id, cb.message.message_id, deal_text, deal_kb)
    await send_v2(deal['buyer_id'], deal_text, deal_kb)


# ══════════════════════════════════════════════
#  СДЕЛКА — ЧАТЫ, ЗАВЕРШЕНИЕ, ОТМЕНА
# ══════════════════════════════════════════════

@router.callback_query(F.data.startswith("deal_history_"))
async def cb_deal_history(cb: CallbackQuery):
    deal_id = cb.data.replace("deal_history_", "")
    await answer_cb(cb.id)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deal_messages WHERE deal_id=? ORDER BY id", (deal_id,)
        ) as cur:
            msgs = await cur.fetchall()

    if not msgs:
        kb = build_inline_v2([[back_btn(f"back_to_deal_{deal_id}")]])
        await send_v2(cb.message.chat.id,
                      f"{pe(E_HISTORY,'🕐')} <b>История переписки {deal_id}</b>\n\n— Сообщений нет —",
                      kb)
        return

    lines = [f"{pe(E_HISTORY,'🕐')} <b>История переписки {deal_id}</b>\n"]
    for m in msgs:
        u = await get_user(m['sender_id'])
        uname = u['username'] if u else str(m['sender_id'])
        lines.append(f"<b>@{uname}</b> [{m['sent_at']}]:\n{m['text']}\n")

    kb = build_inline_v2([[back_btn(f"back_to_deal_{deal_id}")]])
    await send_v2(cb.message.chat.id, "\n".join(lines), kb)


@router.callback_query(F.data.startswith("deal_msg_"))
async def cb_deal_msg(cb: CallbackQuery, state: FSMContext):
    deal_id = cb.data.replace("deal_msg_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return
    await state.set_state(SendDealMessage.text)
    await state.update_data(deal_id=deal_id,
                             other_id=deal['buyer_id'] if cb.from_user.id == deal['seller_id']
                             else deal['seller_id'])
    await answer_cb(cb.id)
    kb = build_inline_v2([[back_btn(f"back_to_deal_{deal_id}")]])
    await send_v2(cb.message.chat.id,
                  f"{pe(E_MSG,'✉️')} <b>Напишите ваше сообщение для сделки {deal_id}:</b>",
                  kb)


@router.message(SendDealMessage.text)
async def fsm_send_deal_msg(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data["deal_id"]
    other_id = data["other_id"]
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO deal_messages (deal_id,sender_id,text,sent_at) VALUES (?,?,?,?)",
            (deal_id, message.from_user.id, message.text, now)
        )
        await db.commit()

    await state.clear()
    u = await get_user(message.from_user.id)
    await message.answer(f"{pe(E_OK,'✅')} <b>Сообщение отправлено.</b>",
                         parse_mode="HTML", reply_markup=main_reply_keyboard())

    # Пересылаем собеседнику
    await send_v2(other_id,
                  f"{pe(E_MSG,'✉️')} <b>Сообщение по сделке {deal_id}</b>\n"
                  f"От: @{u['username'] or message.from_user.id}\n\n"
                  f"{message.text}")


@router.callback_query(F.data.startswith("back_to_deal_"))
async def cb_back_to_deal(cb: CallbackQuery):
    deal_id = cb.data.replace("back_to_deal_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return
    await answer_cb(cb.id)

    buyer = await get_user(deal['buyer_id'])
    seller = await get_user(deal['seller_id'])

    deal_text = (
        f"{pe(E_UNLOCK,'🔓')} <b>Сделка {deal_id}</b>\n"
        f"├ Статус: <b>Активна</b>\n"
        f"├ Сумма: <b>{deal['total_amount']}$</b>\n"
        f"╰ Дата создания: <b>{deal['created_at']}</b>\n\n"
        f"{pe(E_SIDES,'❓')} <b>Стороны сделки</b>\n"
        f"├ Покупатель: @{buyer['username'] or 'анон'} (<code>{deal['buyer_id']}</code>)\n"
        f"╰ Продавец: @{seller['username'] or 'анон'} (<code>{deal['seller_id']}</code>)\n\n"
        f"{pe(E_BOOKTWO,'📖')} Описание: Покупка {deal['quantity']} шт. — {deal['item_name']}"
    )

    deal_kb = build_inline_v2([
        [btn(f"{pe(E_HISTORY,'🕐')} История переписки", f"deal_history_{deal_id}", icon=E_HISTORY),
         btn(f"{pe(E_MSG,'✉️')} Написать сообщение", f"deal_msg_{deal_id}", icon=E_MSG)],
        [btn(f"{pe(E_COMPLETE,'🛡')} Завершить сделку", f"deal_complete_{deal_id}",
             style="success", icon=E_COMPLETE),
         btn(f"{pe(E_CANCEL,'🛡')} Отменить сделку", f"deal_cancel_{deal_id}",
             style="danger", icon=E_CANCEL)],
    ])

    await send_v2(cb.message.chat.id, deal_text, deal_kb)


# ── Завершить сделку ──────────────────────────

@router.callback_query(F.data.startswith("deal_complete_"))
async def cb_deal_complete(cb: CallbackQuery):
    deal_id = cb.data.replace("deal_complete_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return
    if deal['status'] != 'active':
        await answer_cb(cb.id, "Сделка не активна.", alert=True)
        return

    await answer_cb(cb.id)

    other_id = deal['buyer_id'] if cb.from_user.id == deal['seller_id'] else deal['seller_id']

    # Запоминаем кто инициировал завершение
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET complete_requested_by=? WHERE id=?",
                         (cb.from_user.id, deal_id))
        await db.commit()

    await send_v2(cb.message.chat.id,
                  f"{pe(E_SENT,'✅')} <b>Предложение о завершении сделки {deal_id} успешно отправлено.</b>")

    kb_other = build_inline_v2([
        [btn(f"{pe(E_SENT,'✅')} Да", f"confirm_complete_{deal_id}",
             style="success", icon=E_SENT)],
        [btn(f"{pe(E_XMARK,'❌')} Нет", f"reject_complete_{deal_id}",
             style="danger", icon=E_XMARK)],
    ])
    await send_v2(other_id,
                  f"{pe(E_SENT,'✅')} <b>Хотите ли вы завершить сделку {deal_id}?</b>\n"
                  f"Убедитесь, что все условия выполнены.",
                  kb_other)


@router.callback_query(F.data.startswith("confirm_complete_"))
async def cb_confirm_complete(cb: CallbackQuery):
    deal_id = cb.data.replace("confirm_complete_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return

    await answer_cb(cb.id)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET status='completed' WHERE id=?", (deal_id,))
        # Начисляем деньги продавцу
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?",
                         (deal['total_amount'], deal['seller_id']))
        # Обновляем статистику покупателя
        await db.execute(
            "UPDATE users SET total_deals=total_deals+1, deals_as_buyer=deals_as_buyer+1, "
            "amount_as_buyer=amount_as_buyer+? WHERE user_id=?",
            (deal['total_amount'], deal['buyer_id'])
        )
        # Обновляем статистику продавца
        await db.execute(
            "UPDATE users SET total_deals=total_deals+1, deals_as_seller=deals_as_seller+1, "
            "amount_as_seller=amount_as_seller+? WHERE user_id=?",
            (deal['total_amount'], deal['seller_id'])
        )
        # Уменьшаем количество товара
        await db.execute(
            "UPDATE listings SET quantity=quantity-? WHERE id=?",
            (deal['quantity'], deal['listing_id'])
        )
        await db.execute(
            "UPDATE listings SET active=0 WHERE id=? AND quantity<=0",
            (deal['listing_id'],)
        )
        await db.commit()

    await add_transaction(deal['seller_id'], "income", deal['total_amount'],
                          f"Доход по сделке {deal_id}")

    # Покупателю — с кнопкой отзыва
    kb_buyer = build_inline_v2([
        [btn(f"{pe(E_REVIEW,'⭐')} Оставить отзыв", f"leave_review_{deal_id}",
             style="success", icon=E_STARS)],
    ])
    await send_v2(deal['buyer_id'],
                  f"{pe(E_SENT,'✅')} <b>Сделка {deal_id} успешно завершена.</b>\n"
                  f"Спасибо, что пользуетесь нашим гарант-сервисом.",
                  kb_buyer)

    # Продавцу
    await send_v2(deal['seller_id'],
                  f"{pe(E_SENT,'✅')} <b>Сделка {deal_id} успешно завершена.</b>\n"
                  f"Спасибо, что пользуетесь нашим гарант-сервисом.\n"
                  f"На ваш счёт начислено <b>{deal['total_amount']}$</b>.")


@router.callback_query(F.data.startswith("reject_complete_"))
async def cb_reject_complete(cb: CallbackQuery):
    deal_id = cb.data.replace("reject_complete_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return

    await answer_cb(cb.id)
    requester = deal['complete_requested_by']
    await send_v2(cb.message.chat.id,
                  f"{pe(E_XMARK,'❌')} <b>Вы отклонили завершение сделки {deal_id}.</b>")
    await send_v2(requester,
                  f"{pe(E_XMARK,'❌')} <b>Другая сторона отклонила завершение сделки {deal_id}.</b>")


# ── Отменить активную сделку ──────────────────

@router.callback_query(F.data.startswith("deal_cancel_"))
async def cb_deal_cancel(cb: CallbackQuery):
    deal_id = cb.data.replace("deal_cancel_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return
    if deal['status'] != 'active':
        await answer_cb(cb.id, "Сделка не активна.", alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deals SET status='cancelled' WHERE id=?", (deal_id,))
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?",
                         (deal['total_amount'], deal['buyer_id']))
        await db.commit()

    await add_transaction(deal['buyer_id'], "refund", deal['total_amount'],
                          f"Возврат по отменённой сделке {deal_id}")

    await answer_cb(cb.id)
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_XMARK,'❌')} <b>Сделка {deal_id} отменена.</b>")

    other_id = deal['buyer_id'] if cb.from_user.id == deal['seller_id'] else deal['seller_id']
    await send_v2(other_id,
                  f"{pe(E_XMARK,'❌')} <b>Сделка {deal_id} отменена другой стороной.</b>\n"
                  f"Средства возвращены покупателю.")


# ══════════════════════════════════════════════
#  ОТЗЫВЫ
# ══════════════════════════════════════════════

@router.callback_query(F.data.startswith("leave_review_"))
async def cb_leave_review(cb: CallbackQuery, state: FSMContext):
    deal_id = cb.data.replace("leave_review_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return

    await state.set_state(LeaveReview.rating)
    await state.update_data(deal_id=deal_id, seller_id=deal['seller_id'])
    await answer_cb(cb.id)

    kb = build_inline_v2([
        [btn("👍 Положительный", f"rate_pos_{deal_id}", style="success"),
         btn("👎 Отрицательный", f"rate_neg_{deal_id}", style="danger")],
    ])
    await send_v2(cb.message.chat.id,
                  f"{pe(E_STARS,'⭐')} <b>Оставьте отзыв о продавце</b>\n\n"
                  f"Выберите оценку для сделки <b>{deal_id}</b>:", kb)


@router.callback_query(F.data.startswith("rate_pos_") | F.data.startswith("rate_neg_"))
async def cb_rate(cb: CallbackQuery, state: FSMContext):
    is_pos = cb.data.startswith("rate_pos_")
    deal_id = cb.data.split("_", 2)[-1]
    await state.update_data(rating=1 if is_pos else -1)
    await state.set_state(LeaveReview.text)
    await answer_cb(cb.id)
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_STARS,'⭐')} <b>Напишите текст отзыва:</b>")


@router.message(LeaveReview.text)
async def fsm_review_text(message: Message, state: FSMContext):
    data = await state.get_data()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO reviews (deal_id,reviewer_id,target_id,rating,text,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (data["deal_id"], message.from_user.id, data["seller_id"],
             data["rating"], message.text, now)
        )
        if data["rating"] == 1:
            await db.execute("UPDATE users SET positive_reviews=positive_reviews+1 WHERE user_id=?",
                             (data["seller_id"],))
        else:
            await db.execute("UPDATE users SET negative_reviews=negative_reviews+1 WHERE user_id=?",
                             (data["seller_id"],))
        await db.commit()

    await state.clear()
    await message.answer(
        f"{pe(E_STARS,'⭐')} <b>Отзыв успешно оставлен!</b>\nСпасибо за обратную связь.",
        parse_mode="HTML", reply_markup=main_reply_keyboard()
    )


# ══════════════════════════════════════════════
#  ПРОФИЛЬ
# ══════════════════════════════════════════════

@router.message(F.text.contains("Профиль"))
async def menu_profile(message: Message):
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("Сначала напишите /start")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT SUM(CASE WHEN rating=1 THEN 1 ELSE 0 END) pos, "
            "SUM(CASE WHEN rating=-1 THEN 1 ELSE 0 END) neg "
            "FROM reviews WHERE target_id=?", (user['user_id'],)
        ) as cur:
            rev = await cur.fetchone()

    pos_rev = rev[0] or 0
    neg_rev = rev[1] or 0

    total_deals = user['total_deals']
    reviews_count = pos_rev + neg_rev

    kb = build_inline_v2([
        [btn(f"{pe(E_WALLET,'💰')} Кошелёк", "wallet", icon=E_WALLET)],
        [btn(f"{pe(E_DEALS,'📋')} Мои сделки • {total_deals}", "my_deals", icon=E_DEALS),
         btn(f"{pe(E_DEALS,'📋')} Мои отзывы • {reviews_count}", "my_reviews", icon=E_DEALS)],
    ])

    await send_v2(
        message.chat.id,
        profile_text(user, pos_rev, neg_rev),
        kb
    )


# ── Кошелёк ───────────────────────────────────

@router.callback_query(F.data == "wallet")
async def cb_wallet(cb: CallbackQuery):
    await answer_cb(cb.id)
    user = await get_user(cb.from_user.id)
    kb = build_inline_v2([
        [btn(f"{pe(E_DEPOSIT,'➕')} Пополнить", "deposit", style="success", icon=E_DEPOSIT),
         btn(f"{pe(E_WITHDRAW,'➖')} Вывести", "withdraw", style="danger", icon=E_WITHDRAW)],
        [btn(f"{pe(E_HISTFIN,'📖')} История пополнений", "fin_history", icon=E_HISTFIN)],
        [back_btn("back_profile")],
    ])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_WALLET,'💰')} <b>Финансы</b>\n"
                  f"╰ Баланс: <b>$ {user['balance']:.2f}</b>",
                  kb)


@router.callback_query(F.data == "back_profile")
async def cb_back_profile(cb: CallbackQuery):
    await answer_cb(cb.id)
    await cb.message.delete()
    user = await get_user(cb.from_user.id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT SUM(CASE WHEN rating=1 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN rating=-1 THEN 1 ELSE 0 END) FROM reviews WHERE target_id=?",
            (user['user_id'],)
        ) as cur:
            rev = await cur.fetchone()
    pos_rev = rev[0] or 0
    neg_rev = rev[1] or 0
    total_deals = user['total_deals']
    reviews_count = pos_rev + neg_rev
    kb = build_inline_v2([
        [btn(f"{pe(E_WALLET,'💰')} Кошелёк", "wallet", icon=E_WALLET)],
        [btn(f"{pe(E_DEALS,'📋')} Мои сделки • {total_deals}", "my_deals", icon=E_DEALS),
         btn(f"{pe(E_DEALS,'📋')} Мои отзывы • {reviews_count}", "my_reviews", icon=E_DEALS)],
    ])
    await send_v2(cb.message.chat.id, profile_text(user, pos_rev, neg_rev), kb)


@router.callback_query(F.data == "deposit")
async def cb_deposit(cb: CallbackQuery):
    await answer_cb(cb.id)
    # Временный: добавляет 100$ для теста. Подключишь свой крипто-бот позже.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance=balance+100 WHERE user_id=?",
                         (cb.from_user.id,))
        await db.commit()
    await add_transaction(cb.from_user.id, "deposit", 100, "Тестовое пополнение +100$")
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_OK,'✅')} <b>Начислено 100$ (тест).</b>\n\n"
                  f"<i>Реальное пополнение через крипту будет подключено позже.</i>")


@router.callback_query(F.data == "withdraw")
async def cb_withdraw(cb: CallbackQuery, state: FSMContext):
    await answer_cb(cb.id)
    await state.set_state(Withdraw.amount)
    kb = build_inline_v2([[back_btn("wallet")]])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_WITHDRAW,'➖')} <b>Введите сумму для вывода:</b>", kb)


@router.message(Withdraw.amount)
async def fsm_withdraw(message: Message, state: FSMContext):
    try:
        amount = float(message.text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введите корректную сумму.")
        return

    user = await get_user(message.from_user.id)
    if user['balance'] < amount:
        await message.answer(f"❌ Недостаточно средств. Баланс: {user['balance']:.2f}$")
        await state.clear()
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance=balance-? WHERE user_id=?",
                         (amount, message.from_user.id))
        await db.commit()
    await add_transaction(message.from_user.id, "withdraw", amount, "Вывод средств")
    await state.clear()
    await message.answer(
        f"{pe(E_OK,'✅')} <b>Заявка на вывод {amount:.2f}$ принята.</b>\n"
        f"<i>Обратитесь к администратору {ADMIN_USERNAME} для получения.</i>",
        parse_mode="HTML", reply_markup=main_reply_keyboard()
    )


@router.callback_query(F.data == "fin_history")
async def cb_fin_history(cb: CallbackQuery):
    await answer_cb(cb.id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT 20",
            (cb.from_user.id,)
        ) as cur:
            txs = await cur.fetchall()

    kb = build_inline_v2([[back_btn("wallet")]])

    if not txs:
        await edit_v2(cb.message.chat.id, cb.message.message_id,
                      f"{pe(E_HISTFIN,'📖')} <b>История операций</b>\n\n— Операций нет —", kb)
        return

    lines = [f"{pe(E_HISTFIN,'📖')} <b>История операций</b>\n"]
    type_map = {"deposit": "➕", "withdraw": "➖", "freeze": "🔒",
                "refund": "🔄", "income": "💰"}
    for t in txs:
        icon = type_map.get(t['type'], "•")
        lines.append(f"{icon} {t['comment']} — <b>{t['amount']:.2f}$</b> [{t['created_at']}]")

    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  "\n".join(lines), kb)


# ── Мои сделки ────────────────────────────────

@router.callback_query(F.data == "my_deals")
async def cb_my_deals(cb: CallbackQuery):
    await answer_cb(cb.id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM deals WHERE (buyer_id=? OR seller_id=?) ORDER BY rowid DESC",
            (cb.from_user.id, cb.from_user.id)
        ) as cur:
            deals = await cur.fetchall()

    user = await get_user(cb.from_user.id)
    kb_rows = []

    if deals:
        for d in deals:
            role = "🛒" if d['buyer_id'] == cb.from_user.id else "🏪"
            status_icon = {"pending": "⏳", "active": "🟢",
                           "completed": "✅", "cancelled": "❌"}.get(d['status'], "•")
            kb_rows.append([btn(
                f"{role} {d['id']} | {d['item_name']} | {d['total_amount']}$ {status_icon}",
                f"view_deal_{d['id']}"
            )])
    else:
        kb_rows.append([btn("— Нет сделок —", "noop")])

    kb_rows.append([back_btn("back_profile")])

    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_DEALS,'📋')} <b>Мои сделки</b>\n╰ Всего сделок: <b>{user['total_deals']}</b>",
                  build_inline_v2(kb_rows))


@router.callback_query(F.data.startswith("view_deal_"))
async def cb_view_deal(cb: CallbackQuery):
    deal_id = cb.data.replace("view_deal_", "")
    deal = await get_deal(deal_id)
    if not deal:
        await answer_cb(cb.id, "Сделка не найдена.", alert=True)
        return
    await answer_cb(cb.id)

    buyer = await get_user(deal['buyer_id'])
    seller = await get_user(deal['seller_id'])
    status_map = {"pending": "⏳ Ожидание", "active": "🟢 Активна",
                  "completed": "✅ Завершена", "cancelled": "❌ Отменена"}

    text = (
        f"{pe(E_UNLOCK,'🔓')} <b>Сделка {deal_id}</b>\n"
        f"├ Статус: <b>{status_map.get(deal['status'], deal['status'])}</b>\n"
        f"├ Сумма: <b>{deal['total_amount']}$</b>\n"
        f"╰ Дата: <b>{deal['created_at']}</b>\n\n"
        f"├ Покупатель: @{buyer['username'] if buyer else 'анон'}\n"
        f"╰ Продавец: @{seller['username'] if seller else 'анон'}\n\n"
        f"Товар: <b>{deal['item_name']}</b> × {deal['quantity']} шт."
    )

    rows = [[back_btn("my_deals")]]
    if deal['status'] == 'active':
        rows.insert(0, [
            btn(f"{pe(E_HISTORY,'🕐')} История переписки", f"deal_history_{deal_id}", icon=E_HISTORY),
            btn(f"{pe(E_MSG,'✉️')} Написать", f"deal_msg_{deal_id}", icon=E_MSG)
        ])
        rows.insert(1, [
            btn(f"{pe(E_COMPLETE,'🛡')} Завершить", f"deal_complete_{deal_id}",
                style="success", icon=E_COMPLETE),
            btn(f"{pe(E_CANCEL,'🛡')} Отменить", f"deal_cancel_{deal_id}",
                style="danger", icon=E_CANCEL)
        ])

    await edit_v2(cb.message.chat.id, cb.message.message_id, text, build_inline_v2(rows))


# ── Мои отзывы ────────────────────────────────

@router.callback_query(F.data == "my_reviews")
async def cb_my_reviews(cb: CallbackQuery):
    await answer_cb(cb.id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM reviews WHERE target_id=? ORDER BY id DESC",
            (cb.from_user.id,)
        ) as cur:
            reviews = await cur.fetchall()

    kb = build_inline_v2([[back_btn("back_profile")]])

    if not reviews:
        await edit_v2(cb.message.chat.id, cb.message.message_id,
                      f"{pe(E_STARS,'⭐')} <b>Мои отзывы</b>\n\n<b>Нет отзывов</b>", kb)
        return

    lines = [f"{pe(E_STARS,'⭐')} <b>Мои отзывы</b>\n"]
    for r in reviews:
        icon = "👍" if r['rating'] == 1 else "👎"
        reviewer = await get_user(r['reviewer_id'])
        rname = reviewer['username'] if reviewer else str(r['reviewer_id'])
        lines.append(
            f"{icon} Сделка <b>{r['deal_id']}</b> | @{rname}\n"
            f"   {r['text']} [{r['created_at']}]"
        )

    await edit_v2(cb.message.chat.id, cb.message.message_id, "\n".join(lines), kb)


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await answer_cb(cb.id)


# ══════════════════════════════════════════════
#  ИНФОРМАЦИЯ
# ══════════════════════════════════════════════

@router.message(F.text.contains("Информация"))
async def menu_info(message: Message):
    kb = build_inline_v2([
        [btn(f"{pe(E_SHIELD1,'🛡')} Администратор • {ADMIN_USERNAME}",
             "open_admin", icon=E_SHIELD1)],
        [btn(f"{pe(E_INFO,'📖')} Инструкция", "instruction", icon=E_INFO)],
    ])
    await send_v2(
        message.chat.id,
        f"{pe(E_INFO,'📖')} <b>Информация</b>\n\n"
        f"╰ Проведено сделок на сумму: <b>$ {PLATFORM_TURNOVER:,.2f}</b>",
        kb
    )


@router.callback_query(F.data == "open_admin")
async def cb_open_admin(cb: CallbackQuery):
    await answer_cb(cb.id)
    kb = build_inline_v2([[back_btn("back_info")]])
    await edit_v2(cb.message.chat.id, cb.message.message_id,
                  f"{pe(E_SHIELD1,'🛡')} <b>Администратор платформы</b>\n\n"
                  f"По всем вопросам, спорам и проблемам обращайтесь:\n"
                  f"<b>{ADMIN_USERNAME}</b>",
                  kb)


@router.callback_query(F.data == "instruction")
async def cb_instruction(cb: CallbackQuery):
    await answer_cb(cb.id)
    kb = build_inline_v2([[back_btn("back_info")]])
    await edit_v2(cb.message.chat.id, cb.message.message_id, INSTRUCTION_TEXT, kb)


@router.callback_query(F.data == "back_info")
async def cb_back_info(cb: CallbackQuery):
    await answer_cb(cb.id)
    kb = build_inline_v2([
        [btn(f"{pe(E_SHIELD1,'🛡')} Администратор • {ADMIN_USERNAME}",
             "open_admin", icon=E_SHIELD1)],
        [btn(f"{pe(E_INFO,'📖')} Инструкция", "instruction", icon=E_INFO)],
    ])
    await edit_v2(
        cb.message.chat.id, cb.message.message_id,
        f"{pe(E_INFO,'📖')} <b>Информация</b>\n\n"
        f"╰ Проведено сделок на сумму: <b>$ {PLATFORM_TURNOVER:,.2f}</b>",
        kb
    )


# ══════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    await init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    print("🚀 Lumina запущен!")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())