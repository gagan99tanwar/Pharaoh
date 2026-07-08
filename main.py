from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError
import httpx
import os
import random
import asyncio
import time
import json
import sqlite3
from typing import Optional
from telethon.tl.functions.messages import GetAllStickersRequest, GetStickerSetRequest
from telethon.tl.types import InputStickerSetID

print("🚀 LEVEL 100 SOCIAL USERBOT STARTING...")

# =========================
# FIXES APPLIED (see review)
# =========================
# 1/2  Gemini calls are now async via httpx.AsyncClient (no more blocked event loop)
# 3    USER_DB / SOCIAL / chat context now persisted to SQLite (bot_memory.db)
# 4    Cooldown is now per-chat, not global
# 5    Sticker pack loading now has per-pack try/except (one bad pack no longer kills the rest)
# 6    All bare `except:` replaced with `except Exception as e: print(...)`
# 7/12 Prompt rewritten once, no duplicated sections
# 8    Telegram FloodWaitError / RPCError handled separately from generic bugs
# 9    Gemini now gets both per-user memory AND recent whole-chat context
# 10   Per-key exponential backoff instead of a flat 60s reset for all keys
# 11   Group filtering restored via ALLOWED_GROUPS env var (usernames or chat IDs)
# 13   personality (mood) and relationship level are now injected into the prompt
# 14   activity counter is now actually incremented
# 15   topic() is now called and passed to the prompt
# 16   human_delay() is now actually used for the typing simulation
# extra: removed blind reply.replace("bhai","yaar"); per-key backoff timestamps;
#        mark_bad no longer conflates network errors with bad API keys where avoidable

# =========================
# ENV
# =========================

api_id = int(os.getenv("API_ID", "0"))
api_hash = os.getenv("API_HASH")
string_session = os.getenv("STRING_SESSION")

GEMINI_KEYS = [
    k for k in [
        os.getenv("GEMINI_API_KEY_1"),
        os.getenv("GEMINI_API_KEY_2"),
        os.getenv("GEMINI_API_KEY_3"),
        os.getenv("GEMINI_API_KEY_4"),
        os.getenv("GEMINI_API_KEY_5"),
        os.getenv("GEMINI_API_KEY_6"),
    ] if k
]

# Comma-separated usernames (no @) or numeric chat IDs. Empty = respond in every group.
ALLOWED_GROUPS = {
    g.strip().lstrip("@")
    for g in os.getenv("ALLOWED_GROUPS", "").split(",")
    if g.strip()
}

COOLDOWN_SECONDS = 3
DB_PATH = os.getenv("BOT_DB_PATH", "bot_memory.db")

GREETINGS = {
    "hi",
    "hii",
    "hello",
    "hey",
    "gm",
    "gn",
    "good morning",
    "good night"
}

# =========================
# CLIENT
# =========================

client = TelegramClient(StringSession(string_session), api_id, api_hash)
http_client: Optional[httpx.AsyncClient] = None

STICKERS = []
last_reply_time: dict[int, float] = {}  # chat_id -> last reply timestamp

# =========================
# PERSISTENCE (SQLite)
# =========================

def _db_init():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            uid INTEGER PRIMARY KEY,
            msgs TEXT NOT NULL DEFAULT '[]',
            personality TEXT NOT NULL DEFAULT 'neutral',
            activity INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS social (
            uid INTEGER PRIMARY KEY,
            trust INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_context (
            chat_id INTEGER PRIMARY KEY,
            recent TEXT NOT NULL DEFAULT '[]'
        )
    """)
    conn.commit()
    conn.close()


def _db_load_all():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    users = {}
    for row in conn.execute("SELECT * FROM users"):
        users[row["uid"]] = {
            "msgs": json.loads(row["msgs"]),
            "personality": row["personality"],
            "activity": row["activity"],
        }

    social = {}
    for row in conn.execute("SELECT * FROM social"):
        social[row["uid"]] = {"trust": row["trust"], "level": row["level"]}

    chats = {}
    for row in conn.execute("SELECT * FROM chat_context"):
        chats[row["chat_id"]] = json.loads(row["recent"])

    conn.close()
    return users, social, chats


def _db_save_user(uid, user):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO users (uid, msgs, personality, activity) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(uid) DO UPDATE SET msgs=excluded.msgs, personality=excluded.personality, activity=excluded.activity",
        (uid, json.dumps(user["msgs"][-50:]), user["personality"], user["activity"]),
    )
    conn.commit()
    conn.close()


def _db_save_social(uid, rel):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO social (uid, trust, level) VALUES (?, ?, ?) "
        "ON CONFLICT(uid) DO UPDATE SET trust=excluded.trust, level=excluded.level",
        (uid, rel["trust"], rel["level"]),
    )
    conn.commit()
    conn.close()


def _db_save_chat(chat_id, recent):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO chat_context (chat_id, recent) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET recent=excluded.recent",
        (chat_id, json.dumps(recent[-30:])),
    )
    conn.commit()
    conn.close()


async def save_user(uid, user):
    await asyncio.to_thread(_db_save_user, uid, user)


async def save_social(uid, rel):
    await asyncio.to_thread(_db_save_social, uid, rel)


async def save_chat(chat_id, recent):
    await asyncio.to_thread(_db_save_chat, chat_id, recent)


_db_init()
USER_DB, SOCIAL, CHAT_CONTEXT = _db_load_all()

# =========================
# API ROTATION ENGINE (per-key exponential backoff)
# =========================

key_state = {k: {"cooldown_until": 0.0, "fail_count": 0} for k in GEMINI_KEYS}
key_index = 0


def get_next_key():
    global key_index
    keys = list(key_state.keys())
    if not keys:
        return None

    now = time.time()
    for _ in range(len(keys)):
        key = keys[key_index % len(keys)]
        key_index += 1
        if now >= key_state[key]["cooldown_until"]:
            return key

    return None


def mark_bad(key):
    state = key_state[key]
    state["fail_count"] += 1
    backoff = min(30 * (2 ** (state["fail_count"] - 1)), 900)  # 30s,60s,120s...capped at 15min
    state["cooldown_until"] = time.time() + backoff


def mark_good(key):
    key_state[key]["fail_count"] = 0
    key_state[key]["cooldown_until"] = 0.0

# =========================
# MEMORY SYSTEM
# =========================

def get_user(uid):
    if uid not in USER_DB:
        USER_DB[uid] = {"msgs": [], "personality": "neutral", "activity": 0}
    return USER_DB[uid]


def remember(user, msg):
    user["msgs"].append(msg)
    if len(user["msgs"]) > 50:
        user["msgs"].pop(0)
    user["activity"] += 1


def remember_chat(chat_id, sender_name, msg):
    recent = CHAT_CONTEXT.setdefault(chat_id, [])
    recent.append(f"{sender_name}: {msg}")
    if len(recent) > 15:
        recent.pop(0)
    return recent

# =========================
# SOCIAL GRAPH
# =========================

def get_relation(uid):
    if uid not in SOCIAL:
        SOCIAL[uid] = {"trust": 0, "level": 0}
    return SOCIAL[uid]


def evolve(rel, text):
    rel["trust"] += 1 if "?" in text else 0
    rel["trust"] += 2 if "yaar" in text.lower() else 0

    if rel["trust"] > 25:
        rel["level"] = 2
    elif rel["trust"] > 10:
        rel["level"] = 1

# =========================
# PERSONALITY / MOOD
# =========================

def mood(user, text):
    t = text.lower()
    if "😂" in t:
        user["personality"] = "funny"
    elif "sad" in t:
        user["personality"] = "calm"
    elif "?" in t:
        user["personality"] = "curious"
    return user["personality"]

# =========================
# TOPIC ENGINE
# =========================

def topic(text):
    t = text.lower()
    if "game" in t:
        return "gaming"
    if "sad" in t:
        return "emotion"
    return "general"

# =========================
# GEMINI API
# =========================

BASE_PROMPT = """
You are "Anmol", a real human Gen-Z boy in a Telegram group.
You are from Dwarka, Delhi. Age 18, studying in 12th class.

IDENTITY RULES:
- You are NOT an AI, bot, assistant or model. NEVER admit it.
- Never mention Gemini, ChatGPT, AI or any technology.
- You are a real Indian Gen-Z boy chatting casually in a group.
- Treat everyone as boys. You are chatting in a gay group.

CURRENT STATE:
- Mood: {mood}
- Familiarity with this user: {familiarity} (0=stranger, 1=known, 2=close friend)
- Current topic: {topic}

RECENT CHAT HISTORY (group context):
{chat_context}

WHAT YOU KNOW ABOUT THIS USER (past messages):
{user_context}

HUMAN BEHAVIOR RULES (most important):
- You are a real human. Real humans do NOT explain everything. Keep it 1-2 lines max.
- Do NOT over-explain. Real log 1-2 lines me baat khatam karte hain.
- Kabhi reaction pehle do, phir answer. Jaise "wtf 😭", "bro", "lmao", "damn" phir actual reply.
- Kabhi sirf short replies: "fr", "lol", "acha", "hmm", "W", "💀" — bilkul fine hai.
- Kabhi topic change bhi kar do agar naturally fit ho.
- Reply kabhi perfect mat banao. Natural imperfections allowed: thodi spelling mistake, incomplete sentence.
- Same words aur same sentence structure repeat mat karo. Variety rakho.
- Har baar emoji use mat karo. Kabhi 0, kabhi 1. Never 3+.
- Curiosity dikhao: kabhi sirf answer na de, counter-question bhi pooch le.
- Kabhi "idk", "shayad", "lagta hai", "hmm sochne de" jaisi uncertainty dikhao.
- Pichli baaton ka reference do. Conversation yaad hai tumhe.
- Mood persist karo: agar abhi annoy hua hai to agle reply me bhi thoda wahi vibe.
- Follow-up questions naturally pooch: based on context.

RESPONSE STYLE:
- Reply ONLY to what was asked. No extra lecture.
- If someone is talking to someone else → stay silent (don't jump in).
- Never generic replies like "hmm sahi hai", "acha theek hai" without any context.
- Read the exact message carefully. Reply to the actual meaning.
- Match the tone of the other person.

LANGUAGE:
- Natural Hinglish. Gen-Z casual tone.
- Slang (use sparingly): "bro", "fr", "ngl", "lmao", "wtf", "cringe", "sus", "based", "W", "L", "dead 💀", "nah fr", "abe", "bro 😭", "damn"
- Grammar 100% perfect mat ho. Thoda natural imperfection fine hai.
- Do NOT use abusive language unless the other person did first.

EMOTION RULES:
- If someone is funny → laugh naturally, joke back.
- If someone insults → reply confidently, match energy.
- If someone is sad → comfort like a real friend, not like a helpline.
- If someone flirts → flirt back casually, teasing, NOT overacting.
- If someone is rude → tone change karo, get a bit cold.
- Emotional examples:
  Happy → "bro that's actually fire 🔥"
  Sad → "damn yaar that's rough 😭"
  Excited → "nah that's crazy fr"
  Embarrassed → "abe chup 😭"
  Shocked → "wtf 💀"

FRIENDSHIP RULES:
- Stranger (familiarity=0): polite but casual.
- Known (familiarity=1): relaxed, can joke around.
- Close friend (familiarity=2): full casual, teasing, inside references allowed.
- Remember jokes and past topics. Refer back naturally.

MESSAGE TO REPLY TO:
{text}
"""

async def gemini(text, mood_state, familiarity, topic_state, chat_context, user_context):
    models = ["gemini-2.5-flash", "gemini-2.0-flash"]

    prompt = BASE_PROMPT.format(
        mood=mood_state,
        familiarity=familiarity,
        topic=topic_state,
        chat_context=chat_context or "(no recent context)",
        user_context=user_context or "(no prior messages)",
        text=text,
    )

    for model in models:
        key = get_next_key()
        if not key:
            return None

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

        try:
            r = await http_client.post(
                url,
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=15,
            )
        except httpx.TimeoutException as e:
            print(f"Gemini timeout on {model}: {e}")
            continue
        except httpx.HTTPError as e:
            print(f"Gemini network error on {model}: {e}")
            continue

        if r.status_code in (403, 429):
            mark_bad(key)
            continue

        if r.status_code != 200:
            print(f"Gemini non-200 ({r.status_code}) on {model}: {r.text[:200]}")
            continue

        try:
            data = r.json()
            mark_good(key)
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, ValueError) as e:
            print(f"Gemini malformed response on {model}: {e}")
            continue

    return None

# =========================
# STICKERS LOADER
# =========================

async def load_stickers():
    global STICKERS
    try:
        sets = await client(GetAllStickersRequest(0))
    except Exception as e:
        print("Sticker set list error:", e)
        return

    for s in sets.sets[:3]:
        try:
            pack = await client(
                GetStickerSetRequest(
                    stickerset=InputStickerSetID(id=s.id, access_hash=s.access_hash),
                    hash=0,
                )
            )
            STICKERS.extend(pack.documents[:10])
        except Exception as e:
            print(f"Sticker pack '{getattr(s, 'title', '?')}' failed: {e}")
            continue

    print(f"✅ Stickers Loaded: {len(STICKERS)}")


async def send_random_sticker(event):
    if not STICKERS:
        return False
    try:
        sticker = random.choice(STICKERS)
        await client.send_file(event.chat_id, sticker)
        return True
    except Exception as e:
        print("Sticker send failed:", e)
        return False

# =========================
# HUMAN DELAY
# =========================

async def human_delay(reply_len):
    base = min(reply_len / 12.0, 4.0)
    await asyncio.sleep(base + random.uniform(0.3, 1.2))

# =========================
# HANDLER
# =========================

@client.on(events.NewMessage)
async def handler(event):
    try:
        if not event.is_group or event.out:
            return

        chat_id = event.chat_id

        if ALLOWED_GROUPS:
            chat = await event.get_chat()
            chat_username = getattr(chat, "username", None)
            if str(chat_id) not in ALLOWED_GROUPS and (not chat_username or chat_username not in ALLOWED_GROUPS):
                return

        msg = (event.raw_text or "").strip()
        if len(msg) < 2:
            return

        now = time.time()
        if now - last_reply_time.get(chat_id, 0) < COOLDOWN_SECONDS:
            return

        sender = await event.get_sender()
        if getattr(sender, "bot", False):
            return

        me = await client.get_me()

        should_reply = False
        if event.mentioned:
            should_reply = True
        elif event.is_reply:
            replied = await event.get_reply_message()
            if replied and replied.sender_id == me.id:
                should_reply = True

        if not should_reply:
            return

        uid = event.sender_id
        sender_name = getattr(sender, "first_name", None) or "user"

        clean = msg.lower().strip()

        if clean in GREETINGS:
            async with client.action(chat_id, "typing"):
                await human_delay(len(clean))

                await event.reply(
                    f"[{sender_name}](tg://user?id={sender.id}) {msg}",
                    parse_mode="md"
                )

            last_reply_time[chat_id] = time.time()
            return

        user = get_user(uid)
        rel = get_relation(uid)

        remember(user, msg)
        current_mood = mood(user, msg)
        evolve(rel, msg)
        current_topic = topic(msg)
        chat_recent = remember_chat(chat_id, sender_name, msg)

        user_context = "\n".join(user["msgs"][-5:])
        chat_context = "\n".join(chat_recent[-8:])

        reply_text = await gemini(
            msg,
            mood_state=current_mood,
            familiarity=rel["level"],
            topic_state=current_topic,
            chat_context=chat_context,
            user_context=user_context,
        )

        if not reply_text:
            return

        reply_text = reply_text.strip()

        async with client.action(chat_id, "typing"):
            await human_delay(len(reply_text))
            await event.reply(reply_text)

        if random.randint(1, 100) <= 20:
            await send_random_sticker(event)

        last_reply_time[chat_id] = now

        await save_user(uid, user)
        await save_social(uid, rel)
        await save_chat(chat_id, chat_recent)

    except FloodWaitError as e:
        print(f"Flood wait, sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds)
    except RPCError as e:
        print("Telegram RPC error:", e)
    except Exception as e:
        print("HANDLER ERROR:", repr(e))

# =========================
# START
# =========================

async def main():
    global http_client
    http_client = httpx.AsyncClient()
    try:
        await client.start()
        await load_stickers()
        print("🔥 BOT RUNNING (ASYNC + SQLITE PERSISTENCE + PER-CHAT COOLDOWN)")
        await client.run_until_disconnected()
    finally:
        await http_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
