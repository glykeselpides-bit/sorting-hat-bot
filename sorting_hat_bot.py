import os
import random
import sqlite3
from datetime import datetime

import discord
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN")  # set this in your environment

COMMAND_PREFIX = "!"
DB_FILE = "sorting_hat.sqlite3"

HOUSES = ["Gryffindor", "Hufflepuff", "Ravenclaw", "Slytherin"]

HOUSE_ROLE_COLORS = {
    "Gryffindor": discord.Color.red(),
    "Hufflepuff": discord.Color.gold(),
    "Ravenclaw": discord.Color.blue(),
    "Slytherin": discord.Color.green(),
}

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

# ----------------------------
# QUIZ CONFIG
# ----------------------------
QUIZ_TIMEOUT = 60  # seconds per question
ACTIVE_QUIZZES: set[int] = set()

QUIZ_QUESTIONS = [
    {
        "q": "You see someone being bullied. What do you do?",
        "options": {
            "A": ("Step in immediately, even if it’s risky.", {"Gryffindor": 3, "Hufflepuff": 1}),
            "B": ("Get help / rally people to stop it safely.", {"Hufflepuff": 3, "Ravenclaw": 1}),
            "C": ("Assess the situation and plan the most effective move.", {"Ravenclaw": 3, "Slytherin": 1}),
            "D": ("Use influence/pressure to make it stop—fast.", {"Slytherin": 3, "Gryffindor": 1}),
        },
    },
    {
        "q": "What do you value most?",
        "options": {
            "A": ("Bravery", {"Gryffindor": 3}),
            "B": ("Loyalty", {"Hufflepuff": 3}),
            "C": ("Knowledge", {"Ravenclaw": 3}),
            "D": ("Ambition", {"Slytherin": 3}),
        },
    },
    {
        "q": "Pick a class you’d never skip:",
        "options": {
            "A": ("Defense Against the Dark Arts", {"Gryffindor": 2, "Slytherin": 1}),
            "B": ("Herbology", {"Hufflepuff": 3}),
            "C": ("Charms", {"Ravenclaw": 3}),
            "D": ("Potions", {"Slytherin": 3}),
        },
    },
    {
        "q": "Your ideal weekend is:",
        "options": {
            "A": ("Adventure / exploring somewhere new", {"Gryffindor": 2, "Ravenclaw": 1}),
            "B": ("Cozy time with friends/family", {"Hufflepuff": 3}),
            "C": ("Learning something or a creative project", {"Ravenclaw": 3}),
            "D": ("Working on goals / leveling up", {"Slytherin": 3}),
        },
    },
]

# ----------------------------
# REACTION POINTS CONFIG
# ----------------------------
REACTION_POINTS = {
    "❤️": 1,
    "❤": 1,
    "😂": 1,
    "🤣": 1,
    "😍": 1,
    "👍": 1,
    "💯": 1,  # <-- added
    "😢": -1,
    "😭": -1,
    "👎": -1,
}

ALLOWED_REACTION_CHANNEL_IDS: set[int] = set()


def db():
    return sqlite3.connect(DB_FILE)


def init_db():
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                house TEXT,
                points INTEGER NOT NULL DEFAULT 0,
                sorted_at TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS points_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                moderator_user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reaction_awards (
                guild_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                reactor_user_id INTEGER NOT NULL,
                emoji TEXT NOT NULL,
                delta INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, message_id, reactor_user_id, emoji)
            )
        """)
        con.commit()


def get_user_record(guild_id: int, user_id: int):
    with db() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT house, points, sorted_at FROM users WHERE guild_id=? AND user_id=?",
            (guild_id, user_id),
        )
        return cur.fetchone()


def set_user_house(guild_id: int, user_id: int, house: str):
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO users (guild_id, user_id, house, points, sorted_at)
            VALUES (?, ?, ?, 0, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET house=excluded.house, sorted_at=excluded.sorted_at
        """, (guild_id, user_id, house, now))
        con.commit()


def add_points(guild_id: int, target_user_id: int, moderator_user_id: int, delta: int, reason: str | None):
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO users (guild_id, user_id, house, points, sorted_at)
            VALUES (?, ?, NULL, 0, NULL)
            ON CONFLICT(guild_id, user_id) DO NOTHING
        """, (guild_id, target_user_id))
        cur.execute("""
            UPDATE users SET points = points + ? WHERE guild_id=? AND user_id=?
        """, (delta, guild_id, target_user_id))
        cur.execute("""
            INSERT INTO points_log (guild_id, target_user_id, moderator_user_id, delta, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (guild_id, target_user_id, moderator_user_id, delta, reason, now))
        con.commit()


async def get_or_create_role(guild: discord.Guild, house: str) -> discord.Role:
    role = discord.utils.get(guild.roles, name=house)
    if role:
        return role
    return await guild.create_role(
        name=house,
        colour=HOUSE_ROLE_COLORS.get(house, discord.Color.default()),
        reason="Sorting Hat: create house role",
    )


async def assign_house_role(member: discord.Member, house: str):
    role = await get_or_create_role(member.guild, house)

    other_roles = [r for r in member.roles if r.name in HOUSES and r.name != house]
    if other_roles:
        await member.remove_roles(*other_roles, reason="Sorting Hat: changing house")

    if role not in member.roles:
        await member.add_roles(role, reason="Sorting Hat: assigned house")


# ----------------------------
# QUIZ ENGINE
# ----------------------------
async def run_sorting_quiz_for_user(user: discord.User) -> str:
    """DM-based quiz for a specific user. Returns house."""
    if user.id in ACTIVE_QUIZZES:
        raise RuntimeError("Quiz already running for that user.")

    ACTIVE_QUIZZES.add(user.id)
    scores = {h: 0 for h in HOUSES}

    try:
        dm = await user.create_dm()
        await dm.send(
            "🪄 **Sorting Hat Test**\n"
            "Reply with **A / B / C / D** for each question.\n"
            f"You have **{QUIZ_TIMEOUT}s** per question. Let’s begin!"
        )

        def check(m: discord.Message):
            return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)

        for i, item in enumerate(QUIZ_QUESTIONS, start=1):
            opts_text = "\n".join([f"**{k}** — {v[0]}" for k, v in item["options"].items()])
            await dm.send(f"**Q{i}.** {item['q']}\n{opts_text}")

            try:
                msg = await bot.wait_for("message", check=check, timeout=QUIZ_TIMEOUT)
            except TimeoutError:
                await dm.send("⌛ Time’s up. Run `!sort` again when you’re ready.")
                raise

            choice = msg.content.strip().upper()
            if choice not in item["options"]:
                await dm.send("❌ Please reply with **A / B / C / D** only. Run `!sort` again.")
                raise ValueError("Invalid choice")

            weights = item["options"][choice][1]
            for house, pts in weights.items():
                scores[house] += pts

        best = max(scores.values())
        tied = [h for h, s in scores.items() if s == best]
        house = random.choice(tied)

        await dm.send(f"✨ The Sorting Hat has decided… **{house}**!")
        return house

    finally:
        ACTIVE_QUIZZES.discard(user.id)


# ----------------------------
# REACTION AWARDS HELPERS
# ----------------------------
def _emoji_key(payload_emoji: discord.PartialEmoji) -> str:
    return str(payload_emoji)


def record_reaction_award(guild_id: int, message_id: int, reactor_id: int, emoji: str, delta: int) -> bool:
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        try:
            cur.execute("""
                INSERT INTO reaction_awards (guild_id, message_id, reactor_user_id, emoji, delta, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (guild_id, message_id, reactor_id, emoji, delta, now))
            con.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def remove_reaction_award(guild_id: int, message_id: int, reactor_id: int, emoji: str):
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT delta FROM reaction_awards
            WHERE guild_id=? AND message_id=? AND reactor_user_id=? AND emoji=?
        """, (guild_id, message_id, reactor_id, emoji))
        row = cur.fetchone()
        if not row:
            return None
        (delta,) = row
        cur.execute("""
            DELETE FROM reaction_awards
            WHERE guild_id=? AND message_id=? AND reactor_user_id=? AND emoji=?
        """, (guild_id, message_id, reactor_id, emoji))
        con.commit()
        return delta


# ----------------------------
# EVENTS
# ----------------------------
@bot.event
async def on_ready():
    init_db()
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == bot.user.id:
        return

    if ALLOWED_REACTION_CHANNEL_IDS and payload.channel_id not in ALLOWED_REACTION_CHANNEL_IDS:
        return

    emoji = _emoji_key(payload.emoji)
    if emoji not in REACTION_POINTS:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    channel = guild.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if message.author.bot:
        return
    if message.author.id == payload.user_id:
        return

    delta = REACTION_POINTS[emoji]

    if not record_reaction_award(payload.guild_id, payload.message_id, payload.user_id, emoji, delta):
        return

    add_points(payload.guild_id, message.author.id, payload.user_id, delta,
               f"Reaction {emoji} on msg {payload.message_id}")


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == bot.user.id:
        return

    if ALLOWED_REACTION_CHANNEL_IDS and payload.channel_id not in ALLOWED_REACTION_CHANNEL_IDS:
        return

    emoji = _emoji_key(payload.emoji)
    if emoji not in REACTION_POINTS:
        return

    previous_delta = remove_reaction_award(payload.guild_id, payload.message_id, payload.user_id, emoji)
    if previous_delta is None:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    channel = guild.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if message.author.bot:
        return
    if message.author.id == payload.user_id:
        return

    add_points(payload.guild_id, message.author.id, payload.user_id, -previous_delta,
               f"Removed reaction {emoji} on msg {payload.message_id}")


# ----------------------------
# COMMANDS
# ----------------------------
@bot.command(name="sort")
async def sort_me(ctx: commands.Context):
    record = get_user_record(ctx.guild.id, ctx.author.id)
    if record and record[0] in HOUSES:
        await ctx.reply(f"🪄 You’re already sorted into **{record[0]}**! Use `!resort` if you allow re-sorting.")
        return

    try:
        house = await run_sorting_quiz_for_user(ctx.author)
    except discord.Forbidden:
        await ctx.reply("❌ I can’t DM you. Please enable DMs from server members and try `!sort` again.")
        return
    except Exception:
        return

    set_user_house(ctx.guild.id, ctx.author.id, house)
    await assign_house_role(ctx.author, house)
    await ctx.reply(f"✨ The Sorting Hat has spoken! **{ctx.author.display_name}** → **{house}**")


@bot.command(name="resort")
@commands.has_permissions(manage_guild=True)
async def resort(ctx: commands.Context, member: discord.Member | None = None):
    """(Admin) Re-sort yourself or a mentioned member via DM quiz."""
    member = member or ctx.author

    try:
        house = await run_sorting_quiz_for_user(member)
    except discord.Forbidden:
        if member.id == ctx.author.id:
            await ctx.reply("❌ I can’t DM you. Please enable DMs from server members and try `!resort` again.")
        else:
            await ctx.reply(f"❌ I can’t DM **{member.display_name}**. They need to enable DMs from server members.")
        return
    except Exception:
        return

    set_user_house(ctx.guild.id, member.id, house)
    await assign_house_role(member, house)
    await ctx.reply(f"🔁 Re-sorted **{member.display_name}** into **{house}**")


@bot.group(name="points", invoke_without_command=True)
async def points_group(ctx: commands.Context):
    await ctx.reply("Use `!points add @user 10 reason` or `!points remove @user 5 reason`")


@points_group.command(name="add")
@commands.has_permissions(manage_messages=True)
async def points_add(ctx: commands.Context, member: discord.Member, amount: int, *, reason: str = None):
    if amount <= 0:
        await ctx.reply("Amount must be positive.")
        return
    add_points(ctx.guild.id, member.id, ctx.author.id, amount, reason)
    await ctx.reply(f"🏆 Added **{amount}** points to **{member.display_name}**. ({reason or 'no reason'})")


@points_group.command(name="remove")
@commands.has_permissions(manage_messages=True)
async def points_remove(ctx: commands.Context, member: discord.Member, amount: int, *, reason: str = None):
    if amount <= 0:
        await ctx.reply("Amount must be positive.")
        return
    add_points(ctx.guild.id, member.id, ctx.author.id, -amount, reason)
    await ctx.reply(f"🧨 Removed **{amount}** points from **{member.display_name}**. ({reason or 'no reason'})")


@bot.command(name="house")
async def my_house(ctx: commands.Context, member: discord.Member | None = None):
    member = member or ctx.author
    record = get_user_record(ctx.guild.id, member.id)
    if not record or not record[0]:
        await ctx.reply(f"❓ **{member.display_name}** isn’t sorted yet. Use `!sort`.")
        return
    house, points, sorted_at = record
    await ctx.reply(f"🏰 **{member.display_name}** → **{house}** | **{points}** points")


@bot.command(name="pointscheck")
async def points_check(ctx: commands.Context, member: discord.Member | None = None):
    member = member or ctx.author
    record = get_user_record(ctx.guild.id, member.id)
    if not record:
        await ctx.reply(f"❓ No record for **{member.display_name}** yet.")
        return
    house, points, _ = record
    await ctx.reply(f"🔎 **{member.display_name}** has **{points}** points. ({house or 'Unsorted'})")


@bot.command(name="leaderboard")
async def leaderboard(ctx: commands.Context, limit: int = 10):
    limit = max(1, min(limit, 25))
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT user_id, points, house
            FROM users
            WHERE guild_id=?
            ORDER BY points DESC
            LIMIT ?
        """, (ctx.guild.id, limit))
        rows = cur.fetchall()

    if not rows:
        await ctx.reply("No points yet.")
        return

    lines = []
    for i, (user_id, points, house) in enumerate(rows, start=1):
        user = ctx.guild.get_member(user_id)
        name = user.display_name if user else f"<@{user_id}>"
        lines.append(f"**{i}.** {name} — **{points}** ({house or 'Unsorted'})")

    await ctx.reply("📊 **Leaderboard**\n" + "\n".join(lines))


@bot.command(name="housecup")
async def house_cup(ctx: commands.Context):
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT house, COALESCE(SUM(points), 0) as total
            FROM users
            WHERE guild_id=? AND house IS NOT NULL
            GROUP BY house
            ORDER BY total DESC
        """, (ctx.guild.id,))
        rows = cur.fetchall()

    if not rows:
        await ctx.reply("No house totals yet. People need to `!sort` first.")
        return

    lines = [f"**{i}. {house}** — **{total}**" for i, (house, total) in enumerate(rows, start=1)]
    await ctx.reply("🏆 **House Cup Standings**\n" + "\n".join(lines))


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ You don’t have permission for that command (need **Manage Messages**).")
        return
    if isinstance(error, commands.MemberNotFound):
        await ctx.reply("❌ I can’t find that user. Try mentioning them like `@name`.")
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("❌ Missing info. Example: `!points remove @user 5 reason`")
        return
    if isinstance(error, commands.BadArgument):
        await ctx.reply("❌ Bad format. Example: `!points remove @user 5 reason`")
        return

    await ctx.reply(f"❌ Error: `{type(error).__name__}`")


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN environment variable.")
    bot.run(TOKEN)
import os
import random
import sqlite3
from datetime import datetime

import discord
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN")  # set this in your environment

COMMAND_PREFIX = "!"
DB_FILE = "sorting_hat.sqlite3"

HOUSES = ["Gryffindor", "Hufflepuff", "Ravenclaw", "Slytherin"]

HOUSE_ROLE_COLORS = {
    "Gryffindor": discord.Color.red(),
    "Hufflepuff": discord.Color.gold(),
    "Ravenclaw": discord.Color.blue(),
    "Slytherin": discord.Color.green(),
}

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True
intents.reactions = True  # <-- important for reaction points reliability

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

# ----------------------------
# QUIZ CONFIG
# ----------------------------
QUIZ_TIMEOUT = 60  # seconds per question
ACTIVE_QUIZZES: set[int] = set()

QUIZ_QUESTIONS = [
    {
        "q": "You see someone being bullied. What do you do?",
        "options": {
            "A": ("Step in immediately, even if it’s risky.", {"Gryffindor": 3, "Hufflepuff": 1}),
            "B": ("Get help / rally people to stop it safely.", {"Hufflepuff": 3, "Ravenclaw": 1}),
            "C": ("Assess the situation and plan the most effective move.", {"Ravenclaw": 3, "Slytherin": 1}),
            "D": ("Use influence/pressure to make it stop—fast.", {"Slytherin": 3, "Gryffindor": 1}),
        },
    },
    {
        "q": "What do you value most?",
        "options": {
            "A": ("Bravery", {"Gryffindor": 3}),
            "B": ("Loyalty", {"Hufflepuff": 3}),
            "C": ("Knowledge", {"Ravenclaw": 3}),
            "D": ("Ambition", {"Slytherin": 3}),
        },
    },
    {
        "q": "Pick a class you’d never skip:",
        "options": {
            "A": ("Defense Against the Dark Arts", {"Gryffindor": 2, "Slytherin": 1}),
            "B": ("Herbology", {"Hufflepuff": 3}),
            "C": ("Charms", {"Ravenclaw": 3}),
            "D": ("Potions", {"Slytherin": 3}),
        },
    },
    {
        "q": "Your ideal weekend is:",
        "options": {
            "A": ("Adventure / exploring somewhere new", {"Gryffindor": 2, "Ravenclaw": 1}),
            "B": ("Cozy time with friends/family", {"Hufflepuff": 3}),
            "C": ("Learning something or a creative project", {"Ravenclaw": 3}),
            "D": ("Working on goals / leveling up", {"Slytherin": 3}),
        },
    },
]

# ----------------------------
# REACTION POINTS CONFIG
# ----------------------------
REACTION_POINTS = {
    "❤️": 1,
    "❤": 1,   # <-- variant support
    "😂": 1,
    "🤣": 1,
    "😍": 1,
    "👍": 1,
    "😢": -1,
    "😭": -1,
    "👎": -1,
}

# If you want reactions to count only in certain channels, put their IDs here.
# Leave empty to allow anywhere the bot can see.
ALLOWED_REACTION_CHANNEL_IDS: set[int] = set()


def db():
    return sqlite3.connect(DB_FILE)


def init_db():
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                house TEXT,
                points INTEGER NOT NULL DEFAULT 0,
                sorted_at TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS points_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                moderator_user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reaction_awards (
                guild_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                reactor_user_id INTEGER NOT NULL,
                emoji TEXT NOT NULL,
                delta INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, message_id, reactor_user_id, emoji)
            )
        """)
        con.commit()


def get_user_record(guild_id: int, user_id: int):
    with db() as con:
        cur = con.cursor()
        cur.execute("SELECT house, points, sorted_at FROM users WHERE guild_id=? AND user_id=?",
                    (guild_id, user_id))
        return cur.fetchone()


def set_user_house(guild_id: int, user_id: int, house: str):
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO users (guild_id, user_id, house, points, sorted_at)
            VALUES (?, ?, ?, 0, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET house=excluded.house, sorted_at=excluded.sorted_at
        """, (guild_id, user_id, house, now))
        con.commit()


def add_points(guild_id: int, target_user_id: int, moderator_user_id: int, delta: int, reason: str | None):
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO users (guild_id, user_id, house, points, sorted_at)
            VALUES (?, ?, NULL, 0, NULL)
            ON CONFLICT(guild_id, user_id) DO NOTHING
        """, (guild_id, target_user_id))
        cur.execute("""
            UPDATE users SET points = points + ? WHERE guild_id=? AND user_id=?
        """, (delta, guild_id, target_user_id))
        cur.execute("""
            INSERT INTO points_log (guild_id, target_user_id, moderator_user_id, delta, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (guild_id, target_user_id, moderator_user_id, delta, reason, now))
        con.commit()


async def get_or_create_role(guild: discord.Guild, house: str) -> discord.Role:
    role = discord.utils.get(guild.roles, name=house)
    if role:
        return role
    return await guild.create_role(
        name=house,
        colour=HOUSE_ROLE_COLORS.get(house, discord.Color.default()),
        reason="Sorting Hat: create house role",
    )


async def assign_house_role(member: discord.Member, house: str):
    role = await get_or_create_role(member.guild, house)

    other_roles = [r for r in member.roles if r.name in HOUSES and r.name != house]
    if other_roles:
        await member.remove_roles(*other_roles, reason="Sorting Hat: changing house")

    if role not in member.roles:
        await member.add_roles(role, reason="Sorting Hat: assigned house")


# ----------------------------
# QUIZ ENGINE
# ----------------------------
async def run_sorting_quiz(ctx: commands.Context) -> str:
    user = ctx.author
    if user.id in ACTIVE_QUIZZES:
        raise RuntimeError("Quiz already running.")

    ACTIVE_QUIZZES.add(user.id)
    scores = {h: 0 for h in HOUSES}

    try:
        dm = await user.create_dm()
        await dm.send(
            "🪄 **Sorting Hat Test**\n"
            "Reply with **A / B / C / D** for each question.\n"
            f"You have **{QUIZ_TIMEOUT}s** per question. Let’s begin!"
        )

        def check(m: discord.Message):
            return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)

        for i, item in enumerate(QUIZ_QUESTIONS, start=1):
            opts_text = "\n".join([f"**{k}** — {v[0]}" for k, v in item["options"].items()])
            await dm.send(f"**Q{i}.** {item['q']}\n{opts_text}")

            try:
                msg = await bot.wait_for("message", check=check, timeout=QUIZ_TIMEOUT)
            except TimeoutError:
                await dm.send("⌛ Time’s up. Run `!sort` again when you’re ready.")
                raise

            choice = msg.content.strip().upper()
            if choice not in item["options"]:
                await dm.send("❌ Please reply with **A / B / C / D** only. Run `!sort` again.")
                raise ValueError("Invalid choice")

            weights = item["options"][choice][1]
            for house, pts in weights.items():
                scores[house] += pts

        best = max(scores.values())
        tied = [h for h, s in scores.items() if s == best]
        house = random.choice(tied)

        await dm.send(f"✨ The Sorting Hat has decided… **{house}**!")
        return house

    finally:
        ACTIVE_QUIZZES.discard(user.id)


# ----------------------------
# REACTION AWARDS HELPERS
# ----------------------------
def _emoji_key(payload_emoji: discord.PartialEmoji) -> str:
    return str(payload_emoji)


def record_reaction_award(guild_id: int, message_id: int, reactor_id: int, emoji: str, delta: int) -> bool:
    now = datetime.utcnow().isoformat()
    with db() as con:
        cur = con.cursor()
        try:
            cur.execute("""
                INSERT INTO reaction_awards (guild_id, message_id, reactor_user_id, emoji, delta, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (guild_id, message_id, reactor_id, emoji, delta, now))
            con.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def remove_reaction_award(guild_id: int, message_id: int, reactor_id: int, emoji: str):
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT delta FROM reaction_awards
            WHERE guild_id=? AND message_id=? AND reactor_user_id=? AND emoji=?
        """, (guild_id, message_id, reactor_id, emoji))
        row = cur.fetchone()
        if not row:
            return None
        (delta,) = row
        cur.execute("""
            DELETE FROM reaction_awards
            WHERE guild_id=? AND message_id=? AND reactor_user_id=? AND emoji=?
        """, (guild_id, message_id, reactor_id, emoji))
        con.commit()
        return delta


# ----------------------------
# EVENTS
# ----------------------------
@bot.event
async def on_ready():
    init_db()
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == bot.user.id:
        return

    if ALLOWED_REACTION_CHANNEL_IDS and payload.channel_id not in ALLOWED_REACTION_CHANNEL_IDS:
        return

    emoji = _emoji_key(payload.emoji)
    if emoji not in REACTION_POINTS:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    # Get channel (works for threads too via fetch_channel fallback)
    channel = guild.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if message.author.bot:
        return
    if message.author.id == payload.user_id:
        return  # no self-react farming

    delta = REACTION_POINTS[emoji]

    if not record_reaction_award(payload.guild_id, payload.message_id, payload.user_id, emoji, delta):
        return

    add_points(payload.guild_id, message.author.id, payload.user_id, delta,
               f"Reaction {emoji} on msg {payload.message_id}")


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == bot.user.id:
        return

    if ALLOWED_REACTION_CHANNEL_IDS and payload.channel_id not in ALLOWED_REACTION_CHANNEL_IDS:
        return

    emoji = _emoji_key(payload.emoji)
    if emoji not in REACTION_POINTS:
        return

    previous_delta = remove_reaction_award(payload.guild_id, payload.message_id, payload.user_id, emoji)
    if previous_delta is None:
        return

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return

    channel = guild.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return

    if message.author.bot:
        return
    if message.author.id == payload.user_id:
        return

    add_points(payload.guild_id, message.author.id, payload.user_id, -previous_delta,
               f"Removed reaction {emoji} on msg {payload.message_id}")


# ----------------------------
# COMMANDS
# ----------------------------
@bot.command(name="sort")
async def sort_me(ctx: commands.Context):
    record = get_user_record(ctx.guild.id, ctx.author.id)
    if record and record[0] in HOUSES:
        await ctx.reply(f"🪄 You’re already sorted into **{record[0]}**! Use `!resort` if you allow re-sorting.")
        return

    try:
        house = await run_sorting_quiz(ctx)
    except discord.Forbidden:
        await ctx.reply("❌ I can’t DM you. Please enable DMs from server members and try `!sort` again.")
        return
    except Exception:
        return

    set_user_house(ctx.guild.id, ctx.author.id, house)
    await assign_house_role(ctx.author, house)
    await ctx.reply(f"✨ The Sorting Hat has spoken! **{ctx.author.display_name}** → **{house}**")


@bot.command(name="resort")
@commands.has_permissions(manage_guild=True)
async def resort(ctx: commands.Context, member: discord.Member | None = None):
    member = member or ctx.author
    if member.id != ctx.author.id:
        await ctx.reply("❌ For now, `!resort` only works on yourself (quiz happens in DM).")
        return

    try:
        house = await run_sorting_quiz(ctx)
    except discord.Forbidden:
        await ctx.reply("❌ I can’t DM you. Please enable DMs from server members and try `!resort` again.")
        return
    except Exception:
        return

    set_user_house(ctx.guild.id, member.id, house)
    await assign_house_role(member, house)
    await ctx.reply(f"🔁 Re-sorted **{member.display_name}** into **{house}**")


@bot.group(name="points", invoke_without_command=True)
async def points_group(ctx: commands.Context):
    await ctx.reply("Use `!points add @user 10 reason` or `!points remove @user 5 reason`")


@points_group.command(name="add")
@commands.has_permissions(manage_messages=True)
async def points_add(ctx: commands.Context, member: discord.Member, amount: int, *, reason: str = None):
    if amount <= 0:
        await ctx.reply("Amount must be positive.")
        return
    add_points(ctx.guild.id, member.id, ctx.author.id, amount, reason)
    await ctx.reply(f"🏆 Added **{amount}** points to **{member.display_name}**. ({reason or 'no reason'})")


@points_group.command(name="remove")
@commands.has_permissions(manage_messages=True)
async def points_remove(ctx: commands.Context, member: discord.Member, amount: int, *, reason: str = None):
    if amount <= 0:
        await ctx.reply("Amount must be positive.")
        return
    add_points(ctx.guild.id, member.id, ctx.author.id, -amount, reason)
    await ctx.reply(f"🧨 Removed **{amount}** points from **{member.display_name}**. ({reason or 'no reason'})")


@bot.command(name="house")
async def my_house(ctx: commands.Context, member: discord.Member | None = None):
    member = member or ctx.author
    record = get_user_record(ctx.guild.id, member.id)
    if not record or not record[0]:
        await ctx.reply(f"❓ **{member.display_name}** isn’t sorted yet. Use `!sort`.")
        return
    house, points, sorted_at = record
    await ctx.reply(f"🏰 **{member.display_name}** → **{house}** | **{points}** points")


@bot.command(name="pointscheck")
async def points_check(ctx: commands.Context, member: discord.Member | None = None):
    """Quick points sanity check (no house needed)."""
    member = member or ctx.author
    record = get_user_record(ctx.guild.id, member.id)
    if not record:
        await ctx.reply(f"❓ No record for **{member.display_name}** yet.")
        return
    house, points, _ = record
    await ctx.reply(f"🔎 **{member.display_name}** has **{points}** points. ({house or 'Unsorted'})")


@bot.command(name="leaderboard")
async def leaderboard(ctx: commands.Context, limit: int = 10):
    limit = max(1, min(limit, 25))
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT user_id, points, house
            FROM users
            WHERE guild_id=?
            ORDER BY points DESC
            LIMIT ?
        """, (ctx.guild.id, limit))
        rows = cur.fetchall()

    if not rows:
        await ctx.reply("No points yet.")
        return

    lines = []
    for i, (user_id, points, house) in enumerate(rows, start=1):
        user = ctx.guild.get_member(user_id)
        name = user.display_name if user else f"<@{user_id}>"
        lines.append(f"**{i}.** {name} — **{points}** ({house or 'Unsorted'})")

    await ctx.reply("📊 **Leaderboard**\n" + "\n".join(lines))


@bot.command(name="housecup")
async def house_cup(ctx: commands.Context):
    with db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT house, COALESCE(SUM(points), 0) as total
            FROM users
            WHERE guild_id=? AND house IS NOT NULL
            GROUP BY house
            ORDER BY total DESC
        """, (ctx.guild.id,))
        rows = cur.fetchall()

    if not rows:
        await ctx.reply("No house totals yet. People need to `!sort` first.")
        return

    lines = [f"**{i}. {house}** — **{total}**" for i, (house, total) in enumerate(rows, start=1)]
    await ctx.reply("🏆 **House Cup Standings**\n" + "\n".join(lines))


# ----------------------------
# FRIENDLY COMMAND ERRORS (so you see why remove fails)
# ----------------------------
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ You don’t have permission for that command (need **Manage Messages**).")
        return
    if isinstance(error, commands.MemberNotFound):
        await ctx.reply("❌ I can’t find that user. Try mentioning them like `@name`.")
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("❌ Missing info. Example: `!points remove @user 5 reason`")
        return
    if isinstance(error, commands.BadArgument):
        await ctx.reply("❌ Bad format. Example: `!points remove @user 5 reason`")
        return

    # show generic error name (helps debugging without digging)
    await ctx.reply(f"❌ Error: `{type(error).__name__}`")


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN environment variable.")
    bot.run(TOKEN)
