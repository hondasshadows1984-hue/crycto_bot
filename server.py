"""
Crypto Trading Bot - FastAPI backend mejorado.
- IA: Groq API (gratis) con llama-3.1-70b
- Indicadores: EMA 50/100/200, RSI, MACD, Bollinger Bands, ATR, Volumen
- Solo LONG en Spot (sin apalancamiento)
- Una sola posición abierta a la vez
- Stop loss dinámico con ATR
- Circuit breaker diario 5%
- Ratio mínimo 1:2
- Alertas Telegram en español
- Binance Testnet por defecto
"""
import os
import json
import uuid
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Literal

import httpx
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, ConfigDict

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("trading-bot")

# ── Config desde .env ────────────────────────────────────────────────────────
MONGO_URL        = os.environ["MONGO_URL"]
DB_NAME          = os.environ.get("DB_NAME", "trading_bot")
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL       = "llama-3.1-70b-versatile"
GROQ_API_URL     = "https://api.groq.com/openai/v1/chat/completions"

# Telegram leído directamente de variables de entorno
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT    = os.environ.get("TELEGRAM_CHAT_ID", "")

# Binance Testnet por defecto — cambia a False para usar real
USE_TESTNET      = os.environ.get("USE_TESTNET", "true").lower() == "true"
BINANCE_BASE     = "https://testnet.binance.vision" if USE_TESTNET else "https://api.binance.com"
BINANCE_DATA     = "https://data-api.binance.vision"  # datos públicos siempre desde aquí

mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client[DB_NAME]

app = FastAPI(title="Crypto Trading Bot API")
api = APIRouter(prefix="/api")

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

# ── Helpers ──────────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def new_id() -> str:
    return str(uuid.uuid4())

def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ── Modelos ──────────────────────────────────────────────────────────────────
class Settings(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = "singleton"
    mode: Literal["paper", "live"] = "paper"
    starting_balance: float = 10000.0
    risk_per_trade_pct: float = 1.0        # Conservador: 1% por trade
    take_profit_multiplier: float = 2.0    # TP = 2x el SL (ratio 1:2 mínimo)
    stop_loss_atr_multiplier: float = 1.5  # SL = 1.5x ATR (dinámico)
    circuit_breaker_pct: float = 5.0       # Para el bot si pierde 5% en el día
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    symbols: List[str] = Field(default_factory=lambda: list(DEFAULT_SYMBOLS))
    strategy: Literal["technical", "ai", "combined"] = "combined"
    tick_interval_seconds: int = 300
    min_confidence: float = 0.65           # Confianza mínima para abrir posición
    use_testnet: bool = True

class BotState(BaseModel):
    id: str = "singleton"
    running: bool = False
    started_at: Optional[str] = None
    last_tick_at: Optional[str] = None
    daily_loss_pct: float = 0.0
    daily_loss_date: str = Field(default_factory=today_utc)
    circuit_breaker_tripped: bool = False

class Signal(BaseModel):
    id: str = Field(default_factory=new_id)
    symbol: str
    timeframe: str
    action: Literal["BUY", "SELL", "HOLD"]
    confidence: float
    price: float
    reason: str
    indicators: dict
    source: Literal["technical", "ai", "combined"]
    created_at: str = Field(default_factory=now_iso)

class Position(BaseModel):
    id: str = Field(default_factory=new_id)
    symbol: str
    side: Literal["LONG"] = "LONG"         # Solo LONG
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    atr_at_entry: float = 0.0
    status: Literal["OPEN", "CLOSED"] = "OPEN"
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    commission_paid: float = 0.0
    opened_at: str = Field(default_factory=now_iso)
    closed_at: Optional[str] = None
    mode: Literal["paper", "live"] = "paper"
    reason_open: str = ""
    reason_close: Optional[str] = None

class SignalRequest(BaseModel):
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"

class BacktestRequest(BaseModel):
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    limit: int = 500
    starting_balance: float = 10000.0

class SettingsUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore")
    mode: Optional[Literal["paper", "live"]] = None
    starting_balance: Optional[float] = None
    risk_per_trade_pct: Optional[float] = None
    take_profit_multiplier: Optional[float] = None
    stop_loss_atr_multiplier: Optional[float] = None
    circuit_breaker_pct: Optional[float] = None
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    symbols: Optional[List[str]] = None
    strategy: Optional[Literal["technical", "ai", "combined"]] = None
    tick_interval_seconds: Optional[int] = None
    min_confidence: Optional[float] = None
    use_testnet: Optional[bool] = None

# ── Binance datos públicos ────────────────────────────────────────────────────
async def fetch_klines(symbol: str, interval: str = "1h", limit: int = 250) -> List[dict]:
    """Obtiene velas históricas de Binance (datos públicos, sin API key)."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{BINANCE_DATA}/api/v3/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )
        r.raise_for_status()
        raw = r.json()
    return [
        {
            "open_time": int(k[0]),
            "open":  float(k[1]),
            "high":  float(k[2]),
            "low":   float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
        }
        for k in raw
    ]

async def fetch_tickers(symbols: List[str]) -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{BINANCE_DATA}/api/v3/ticker/24hr")
        r.raise_for_status()
        all_data = r.json()
    sym_set = {s.upper() for s in symbols}
    out = {}
    for d in all_data:
        if d["symbol"] in sym_set:
            out[d["symbol"]] = {
                "symbol": d["symbol"],
                "price": float(d["lastPrice"]),
                "change_pct": float(d["priceChangePercent"]),
                "high_24h": float(d["highPrice"]),
                "low_24h": float(d["lowPrice"]),
                "volume_24h": float(d["volume"]),
                "quote_volume_24h": float(d["quoteVolume"]),
            }
    return out

# ── Indicadores técnicos mejorados ───────────────────────────────────────────
def compute_indicators(klines: List[dict]) -> dict:
    """
    Calcula: EMA 50/100/200, RSI 14, MACD, Bollinger Bands, ATR 14, Volumen.
    Necesita mínimo 210 velas para EMA 200 estable.
    """
    df = pd.DataFrame(klines)
    if df.empty or len(df) < 210:
        return {}

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    volume = df["volume"].astype(float)

    # EMAs
    ema_50  = close.ewm(span=50,  adjust=False).mean()
    ema_100 = close.ewm(span=100, adjust=False).mean()
    ema_200 = close.ewm(span=200, adjust=False).mean()

    # MACD (12, 26, 9)
    ema_12     = close.ewm(span=12, adjust=False).mean()
    ema_26     = close.ewm(span=26, adjust=False).mean()
    macd_line  = ema_12 - ema_26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist  = macd_line - signal_line

    # RSI 14
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))

    # Bollinger Bands (20, 2)
    sma_20   = close.rolling(20).mean()
    std_20   = close.rolling(20).std()
    bb_upper = sma_20 + 2 * std_20
    bb_lower = sma_20 - 2 * std_20
    bb_pct   = (close - bb_lower) / (bb_upper - bb_lower)  # 0=bottom, 1=top

    # ATR 14 (para stop loss dinámico)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    # Volumen relativo (vs media 20 períodos)
    vol_avg    = volume.rolling(20).mean()
    vol_ratio  = volume / vol_avg.replace(0, np.nan)

    # Tendencia EMA (alineación)
    def ema_trend():
        e50  = float(ema_50.iloc[-1])
        e100 = float(ema_100.iloc[-1])
        e200 = float(ema_200.iloc[-1])
        price = float(close.iloc[-1])
        if price > e50 > e100 > e200:
            return "fuerte_alcista"
        elif price > e200:
            return "alcista"
        elif price < e50 < e100 < e200:
            return "fuerte_bajista"
        else:
            return "bajista"

    def f(x):
        try:
            v = float(x)
            return None if (np.isnan(v) or np.isinf(v)) else round(v, 4)
        except Exception:
            return None

    last = -1
    return {
        "last_close":   f(close.iloc[last]),
        "ema_50":       f(ema_50.iloc[last]),
        "ema_100":      f(ema_100.iloc[last]),
        "ema_200":      f(ema_200.iloc[last]),
        "macd":         f(macd_line.iloc[last]),
        "macd_signal":  f(signal_line.iloc[last]),
        "macd_hist":    f(macd_hist.iloc[last]),
        "rsi":          f(rsi.iloc[last]),
        "bb_upper":     f(bb_upper.iloc[last]),
        "bb_middle":    f(sma_20.iloc[last]),
        "bb_lower":     f(bb_lower.iloc[last]),
        "bb_pct":       f(bb_pct.iloc[last]),
        "atr":          f(atr.iloc[last]),
        "vol_ratio":    f(vol_ratio.iloc[last]),
        "trend":        ema_trend(),
    }

# ── Señal técnica mejorada ───────────────────────────────────────────────────
def technical_signal(ind: dict) -> dict:
    """
    Sistema de puntuación conservador.
    Solo genera BUY si el mercado está en tendencia alcista (EMA 200 filter).
    Necesita confirmación de múltiples indicadores.
    """
    if not ind:
        return {"action": "HOLD", "confidence": 0.0, "reason": "Sin datos suficientes"}

    price   = ind.get("last_close")
    ema_50  = ind.get("ema_50")
    ema_100 = ind.get("ema_100")
    ema_200 = ind.get("ema_200")
    rsi     = ind.get("rsi")
    macd    = ind.get("macd")
    macd_sig = ind.get("macd_signal")
    macd_hist = ind.get("macd_hist")
    bb_pct  = ind.get("bb_pct")
    vol_ratio = ind.get("vol_ratio")
    trend   = ind.get("trend", "bajista")

    # ── FILTRO PRINCIPAL: No comprar si mercado bajista ──────────────────────
    # Si el precio está por debajo de EMA 200, el mercado está en tendencia
    # bajista y no debemos comprar. Esta es la regla más importante.
    if price and ema_200 and price < ema_200:
        return {
            "action": "HOLD",
            "confidence": 0.0,
            "reason": f"Precio bajo EMA200 ({ema_200:.2f}) — mercado bajista, no se compra",
        }

    score = 0
    reasons = []
    max_score = 8

    # ── EMA alignment (peso alto) ────────────────────────────────────────────
    if ema_50 and ema_100 and ema_200:
        if ema_50 > ema_100 > ema_200:
            score += 3
            reasons.append("EMAs 50>100>200 alineadas alcistas")
        elif ema_50 > ema_200:
            score += 1
            reasons.append("EMA50 > EMA200")

    # ── RSI ──────────────────────────────────────────────────────────────────
    if rsi is not None:
        if 40 <= rsi <= 60:
            score += 1
            reasons.append(f"RSI neutral ({rsi:.1f})")
        elif rsi < 40:
            score += 2
            reasons.append(f"RSI sobrevendido ({rsi:.1f}) — posible rebote")
        elif rsi > 70:
            score -= 2
            reasons.append(f"RSI sobrecomprado ({rsi:.1f}) — no entrar")

    # ── MACD ─────────────────────────────────────────────────────────────────
    if macd is not None and macd_sig is not None:
        if macd > macd_sig and macd_hist and macd_hist > 0:
            score += 2
            reasons.append("MACD cruce alcista confirmado")
        elif macd > macd_sig:
            score += 1
            reasons.append("MACD por encima de señal")
        else:
            score -= 1

    # ── Bollinger Bands ──────────────────────────────────────────────────────
    if bb_pct is not None:
        if bb_pct < 0.2:
            score += 1
            reasons.append("Precio cerca banda inferior BB — zona de compra")
        elif bb_pct > 0.8:
            score -= 1
            reasons.append("Precio cerca banda superior BB — zona saturada")

    # ── Volumen ──────────────────────────────────────────────────────────────
    if vol_ratio is not None:
        if vol_ratio > 1.5:
            score += 1
            reasons.append(f"Volumen alto ({vol_ratio:.1f}x media) — confirma movimiento")
        elif vol_ratio < 0.7:
            score -= 1
            reasons.append("Volumen bajo — señal débil")

    # ── Decisión ─────────────────────────────────────────────────────────────
    confidence = round(min(1.0, max(0.0, score / max_score)), 2)

    if score >= 5:
        action = "BUY"
    else:
        action = "HOLD"

    return {
        "action": action,
        "confidence": confidence,
        "reason": " | ".join(reasons) if reasons else "Condiciones neutrales",
        "score": score,
    }

# ── IA con Groq (gratis) ─────────────────────────────────────────────────────
async def ai_signal(symbol: str, timeframe: str, ind: dict, technical: dict) -> dict:
    """
    Llama a Groq API (llama-3.1-70b) para análisis adicional.
    Groq es gratis con 14,400 requests/día.
    """
    if not GROQ_API_KEY:
        return {"action": "HOLD", "confidence": 0.0, "reason": "Groq API key no configurada"}

    # No gastar llamada de IA si el filtro EMA200 ya rechazó
    if technical.get("action") == "HOLD" and "bajo EMA200" in technical.get("reason", ""):
        return {"action": "HOLD", "confidence": 0.0, "reason": "Mercado bajista — IA omitida"}

    system_prompt = (
        "Eres un analista cuantitativo de crypto conservador. "
        "Tu prioridad es PROTEGER el capital, no maximizar ganancias. "
        "Analiza los indicadores técnicos y responde SOLO con JSON válido con estos campos: "
        '"action" (solo "BUY" o "HOLD", nunca SELL porque solo hacemos LONG en spot), '
        '"confidence" (número entre 0 y 1), '
        '"reason" (explicación corta en español, máximo 120 caracteres). '
        "Solo recomienda BUY si hay señales CLARAS y MÚLTIPLES. En duda, HOLD."
    )

    user_text = (
        f"Símbolo: {symbol} | Timeframe: {timeframe}\n"
        f"Indicadores: {json.dumps(ind, ensure_ascii=False)}\n"
        f"Análisis técnico previo: {json.dumps(technical, ensure_ascii=False)}\n"
        "Responde solo con el JSON."
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": GROQ_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_text},
                    ],
                    "max_tokens": 200,
                    "temperature": 0.1,  # Bajo para respuestas consistentes
                },
            )
            r.raise_for_status()
            data = r.json()

        text = data["choices"][0]["message"]["content"].strip()
        start = text.find("{")
        end   = text.rfind("}")
        if start != -1 and end != -1:
            parsed = json.loads(text[start:end + 1])
            action = str(parsed.get("action", "HOLD")).upper()
            if action not in {"BUY", "HOLD"}:
                action = "HOLD"
            conf   = float(parsed.get("confidence", 0.0))
            reason = str(parsed.get("reason", ""))[:200]
            return {
                "action": action,
                "confidence": round(max(0.0, min(1.0, conf)), 2),
                "reason": reason,
            }
    except Exception as exc:
        logger.warning("Error Groq API: %s", exc)
        return {"action": "HOLD", "confidence": 0.0, "reason": f"Error IA: {str(exc)[:80]}"}

    return {"action": "HOLD", "confidence": 0.0, "reason": "IA sin respuesta válida"}

def combine_signals(tech: dict, ai: dict) -> dict:
    """
    Combina señales técnica + IA.
    Ambas deben coincidir en BUY para abrir posición (más conservador).
    """
    if tech["action"] == "BUY" and ai["action"] == "BUY":
        avg_conf = round((tech["confidence"] + ai["confidence"]) / 2 + 0.1, 2)
        return {
            "action": "BUY",
            "confidence": min(1.0, avg_conf),
            "reason": f"✅ Técnico + IA de acuerdo | {tech['reason']} | IA: {ai['reason']}",
        }
    if tech["action"] == "BUY" and ai["action"] == "HOLD":
        return {
            "action": "HOLD",
            "confidence": round(tech["confidence"] * 0.6, 2),
            "reason": f"⚠ Técnico dice BUY pero IA dice HOLD — esperando confirmación",
        }
    return {
        "action": "HOLD",
        "confidence": 0.0,
        "reason": "Sin consenso suficiente para entrar",
    }

# ── Circuit Breaker ──────────────────────────────────────────────────────────
async def check_circuit_breaker(settings: Settings) -> bool:
    """
    Verifica si se superó el límite de pérdida diaria.
    Devuelve True si el circuit breaker está activo (bot debe parar).
    """
    state = await get_bot_state()

    # Reset diario
    if state.daily_loss_date != today_utc():
        state.daily_loss_pct = 0.0
        state.daily_loss_date = today_utc()
        state.circuit_breaker_tripped = False
        await save_bot_state(state)
        return False

    if state.circuit_breaker_tripped:
        return True

    if state.daily_loss_pct >= settings.circuit_breaker_pct:
        state.circuit_breaker_tripped = True
        state.running = False
        await save_bot_state(state)
        logger.warning(
            "⚡ Circuit breaker activado — pérdida diaria %.2f%% >= límite %.2f%%",
            state.daily_loss_pct, settings.circuit_breaker_pct
        )
        await send_telegram(
            f"🛑 *CIRCUIT BREAKER ACTIVADO*\n"
            f"Pérdida diaria: {state.daily_loss_pct:.2f}%\n"
            f"Límite: {settings.circuit_breaker_pct}%\n"
            f"Bot detenido automáticamente. Revisa las posiciones."
        )
        return True

    return False

async def update_daily_loss(pnl: float, settings: Settings) -> None:
    """Actualiza el % de pérdida diaria acumulada."""
    if pnl >= 0:
        return  # Solo cuenta pérdidas
    state = await get_bot_state()
    if state.daily_loss_date != today_utc():
        state.daily_loss_pct = 0.0
        state.daily_loss_date = today_utc()
    loss_pct = abs(pnl) / settings.starting_balance * 100
    state.daily_loss_pct += loss_pct
    await save_bot_state(state)

# ── Settings & Bot state ─────────────────────────────────────────────────────
async def get_settings() -> Settings:
    doc = await db.settings.find_one({"id": "singleton"}, {"_id": 0})
    if not doc:
        s = Settings()
        await db.settings.insert_one(s.model_dump())
        return s
    return Settings(**doc)

async def save_settings(updated: Settings) -> None:
    await db.settings.update_one(
        {"id": "singleton"},
        {"$set": updated.model_dump()},
        upsert=True,
    )

async def get_bot_state() -> BotState:
    doc = await db.bot_state.find_one({"id": "singleton"}, {"_id": 0})
    if not doc:
        s = BotState()
        await db.bot_state.insert_one(s.model_dump())
        return s
    return BotState(**doc)

async def save_bot_state(state: BotState) -> None:
    await db.bot_state.update_one(
        {"id": "singleton"},
        {"$set": state.model_dump()},
        upsert=True,
    )

# ── Motor de paper trading ───────────────────────────────────────────────────
BINANCE_COMMISSION = 0.001  # 0.1% por lado

async def count_open_positions() -> int:
    """Cuenta posiciones abiertas. El bot solo permite UNA a la vez."""
    return await db.positions.count_documents({"status": "OPEN"})

async def open_position(
    symbol: str,
    price: float,
    settings: Settings,
    reason: str,
    atr: float,
) -> Optional[Position]:
    """
    Abre posición LONG con:
    - Stop loss dinámico basado en ATR
    - Take profit mínimo 1:2
    - Position sizing basado en % de riesgo
    - Comisiones descontadas
    """
    # ── Una sola posición a la vez ────────────────────────────────────────────
    open_count = await count_open_positions()
    if open_count > 0:
        logger.info("Ya hay una posición abierta — no se abre nueva en %s", symbol)
        return None

    # ── Stop loss dinámico con ATR ────────────────────────────────────────────
    sl_distance = atr * settings.stop_loss_atr_multiplier
    stop_loss   = price - sl_distance
    take_profit = price + (sl_distance * settings.take_profit_multiplier)

    # Verificar ratio mínimo 1:2
    rr_ratio = (take_profit - price) / (price - stop_loss) if price > stop_loss else 0
    if rr_ratio < 1.9:
        logger.info("Ratio R/R %.2f < 1:2 en %s — no se abre posición", rr_ratio, symbol)
        return None

    # ── Position sizing ────────────────────────────────────────────────────────
    risk_amount  = settings.starting_balance * (settings.risk_per_trade_pct / 100.0)
    quantity     = round(risk_amount / sl_distance, 6) if sl_distance > 0 else 0

    if quantity <= 0:
        return None

    # Descontar comisión de entrada (0.1%)
    commission_entry = price * quantity * BINANCE_COMMISSION
    commission_exit  = take_profit * quantity * BINANCE_COMMISSION
    total_commission = round(commission_entry + commission_exit, 4)

    pos = Position(
        symbol=symbol,
        side="LONG",
        entry_price=round(price, 4),
        quantity=quantity,
        stop_loss=round(stop_loss, 4),
        take_profit=round(take_profit, 4),
        atr_at_entry=round(atr, 4),
        status="OPEN",
        mode=settings.mode,
        reason_open=reason[:300],
        commission_paid=total_commission,
    )
    await db.positions.insert_one(pos.model_dump())
    logger.info(
        "✅ Posición abierta: %s LONG @ %.2f | SL: %.2f | TP: %.2f | R/R: 1:%.1f",
        symbol, price, stop_loss, take_profit, rr_ratio
    )
    return pos

async def close_position(pos: Position, current_price: float, reason: str) -> Position:
    """Cierra posición y calcula P&L neto (descontando comisiones)."""
    pnl_bruto = (current_price - pos.entry_price) * pos.quantity
    pnl_neto  = pnl_bruto - pos.commission_paid
    pnl_pct   = (pnl_neto / (pos.entry_price * pos.quantity)) * 100

    pos.status      = "CLOSED"
    pos.exit_price  = round(current_price, 4)
    pos.pnl         = round(pnl_neto, 4)
    pos.pnl_pct     = round(pnl_pct, 4)
    pos.closed_at   = now_iso()
    pos.reason_close = reason[:300]

    await db.positions.update_one({"id": pos.id}, {"$set": pos.model_dump()})
    logger.info(
        "📊 Posición cerrada: %s @ %.2f | P&L: %.4f (%.2f%%) | Motivo: %s",
        pos.symbol, current_price, pnl_neto, pnl_pct, reason
    )
    return pos

async def evaluate_open_positions(tickers: dict, settings: Settings) -> List[Position]:
    """Revisa posiciones abiertas y cierra las que tocaron SL o TP."""
    closed: List[Position] = []
    async for doc in db.positions.find({"status": "OPEN"}, {"_id": 0}):
        pos = Position(**doc)
        t   = tickers.get(pos.symbol)
        if not t:
            continue
        price  = t["price"]
        reason = None

        if price <= pos.stop_loss:
            reason = f"🛑 Stop loss tocado @ {price:.2f}"
        elif price >= pos.take_profit:
            reason = f"✅ Take profit alcanzado @ {price:.2f}"

        if reason:
            closed_pos = await close_position(pos, price, reason)
            closed.append(closed_pos)
            await update_daily_loss(closed_pos.pnl or 0, settings)

    return closed

# ── Telegram en español ──────────────────────────────────────────────────────
async def send_telegram(message: str) -> bool:
    # Lee directo de variables de entorno — no depende de MongoDB
    token   = TELEGRAM_TOKEN
    chat_id = TELEGRAM_CHAT
    if not token or not chat_id:
        logger.warning("Telegram no configurado — TOKEN o CHAT_ID vacíos")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                },
            )
            return r.status_code == 200
    except Exception as exc:
        logger.warning("Error Telegram: %s", exc)
        return False

# ── Routes — Market ──────────────────────────────────────────────────────────
@api.get("/")
async def root():
    return {"name": "Crypto Trading Bot", "status": "ok", "version": "2.0"}

@api.get("/market/klines")
async def market_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 250):
    if limit > 1000:
        limit = 1000
    klines = await fetch_klines(symbol, interval, limit)
    return {"symbol": symbol.upper(), "interval": interval, "klines": klines}

@api.get("/market/tickers")
async def market_tickers(symbols: Optional[str] = None):
    syms = symbols.split(",") if symbols else DEFAULT_SYMBOLS
    data = await fetch_tickers(syms)
    return {"tickers": list(data.values())}

@api.get("/market/indicators")
async def market_indicators(symbol: str = "BTCUSDT", interval: str = "1h"):
    klines = await fetch_klines(symbol, interval, 250)
    ind = compute_indicators(klines)
    return {"symbol": symbol.upper(), "interval": interval, "indicators": ind}

# ── Routes — Signals ─────────────────────────────────────────────────────────
@api.post("/signals/generate")
async def generate_signal(req: SignalRequest):
    settings = await get_settings()
    klines   = await fetch_klines(req.symbol, req.timeframe, 250)
    if not klines:
        raise HTTPException(status_code=400, detail="Sin datos de mercado")
    ind  = compute_indicators(klines)
    if not ind:
        raise HTTPException(status_code=400, detail="Datos insuficientes para calcular indicadores (necesita 210 velas)")
    tech = technical_signal(ind)
    ai   = {"action": "HOLD", "confidence": 0.0, "reason": "IA desactivada"}

    if settings.strategy in ("ai", "combined"):
        ai = await ai_signal(req.symbol, req.timeframe, ind, tech)

    if settings.strategy == "technical":
        combined = tech
        source   = "technical"
    elif settings.strategy == "ai":
        combined = ai
        source   = "ai"
    else:
        combined = combine_signals(tech, ai)
        source   = "combined"

    sig = Signal(
        symbol=req.symbol.upper(),
        timeframe=req.timeframe,
        action=combined["action"],
        confidence=combined["confidence"],
        price=float(klines[-1]["close"]),
        reason=combined["reason"],
        indicators=ind,
        source=source,
    )
    await db.signals.insert_one(sig.model_dump())
    return {"signal": sig.model_dump(), "technical": tech, "ai": ai}

@api.get("/signals")
async def list_signals(limit: int = 50):
    cur = db.signals.find({}, {"_id": 0}).sort("created_at", -1).limit(limit)
    return {"signals": await cur.to_list(length=limit)}

# ── Routes — Posiciones ──────────────────────────────────────────────────────
@api.get("/positions")
async def list_positions(status: Optional[str] = None, limit: int = 100):
    q = {}
    if status:
        q["status"] = status.upper()
    cur = db.positions.find(q, {"_id": 0}).sort("opened_at", -1).limit(limit)
    return {"positions": await cur.to_list(length=limit)}

@api.post("/positions/{position_id}/close")
async def manual_close(position_id: str):
    doc = await db.positions.find_one({"id": position_id, "status": "OPEN"}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Posición abierta no encontrada")
    pos     = Position(**doc)
    tickers = await fetch_tickers([pos.symbol])
    t       = tickers.get(pos.symbol)
    if not t:
        raise HTTPException(status_code=400, detail="No se pudo obtener el precio")
    settings = await get_settings()
    closed   = await close_position(pos, t["price"], "Cerrada manualmente")
    await update_daily_loss(closed.pnl or 0, settings)
    return {"position": closed.model_dump()}

@api.get("/portfolio")
async def portfolio():
    settings     = await get_settings()
    state        = await get_bot_state()
    open_docs    = await db.positions.find({"status": "OPEN"},   {"_id": 0}).to_list(500)
    closed_docs  = await db.positions.find({"status": "CLOSED"}, {"_id": 0}).to_list(2000)
    realized_pnl = sum((d.get("pnl") or 0.0) for d in closed_docs)
    total_comm   = sum((d.get("commission_paid") or 0.0) for d in closed_docs)

    syms        = list({d["symbol"] for d in open_docs})
    unreal_total = 0.0
    if syms:
        tickers = await fetch_tickers(syms)
        for d in open_docs:
            t = tickers.get(d["symbol"])
            if not t:
                continue
            price = t["price"]
            entry = d["entry_price"]
            qty   = d["quantity"]
            unreal = (price - entry) * qty - (d.get("commission_paid") or 0)
            unreal_total += unreal
            d["current_price"]       = price
            d["unrealized_pnl"]      = round(unreal, 4)
            d["unrealized_pnl_pct"]  = round((price - entry) / entry * 100, 4)

    wins     = [d for d in closed_docs if (d.get("pnl") or 0) > 0]
    losses   = [d for d in closed_docs if (d.get("pnl") or 0) < 0]
    win_rate = round(len(wins) / len(closed_docs) * 100, 2) if closed_docs else 0.0
    equity   = settings.starting_balance + realized_pnl + unreal_total

    # Semáforo de salud del bot
    if win_rate >= 55 and realized_pnl >= 0:
        health = "green"
        health_msg = "✅ Bot funcionando bien"
    elif win_rate >= 45 or realized_pnl >= 0:
        health = "yellow"
        health_msg = "⚠️ Rendimiento moderado — vigila"
    else:
        health = "red"
        health_msg = "🔴 Bot perdiendo — considera pausarlo"

    return {
        "starting_balance":  settings.starting_balance,
        "equity":            round(equity, 4),
        "realized_pnl":      round(realized_pnl, 4),
        "unrealized_pnl":    round(unreal_total, 4),
        "total_commissions": round(total_comm, 4),
        "open_positions":    open_docs,
        "total_trades":      len(closed_docs),
        "winning_trades":    len(wins),
        "losing_trades":     len(losses),
        "win_rate":          win_rate,
        "daily_loss_pct":    state.daily_loss_pct,
        "circuit_breaker":   state.circuit_breaker_tripped,
        "health":            health,
        "health_msg":        health_msg,
    }

# ── Routes — Bot ─────────────────────────────────────────────────────────────
@api.get("/bot/status")
async def bot_status():
    state    = await get_bot_state()
    settings = await get_settings()
    return {"state": state.model_dump(), "settings": settings.model_dump()}

@api.post("/bot/start")
async def bot_start():
    state = await get_bot_state()
    if state.circuit_breaker_tripped:
        raise HTTPException(
            status_code=400,
            detail="Circuit breaker activo. Resetea el bot antes de continuar."
        )
    state.running    = True
    state.started_at = now_iso()
    await save_bot_state(state)
    await send_telegram("🟢 *Bot iniciado*\nModo: paper trading\nVigilando el mercado...")
    return {"state": state.model_dump()}

@api.post("/bot/stop")
async def bot_stop():
    state         = await get_bot_state()
    state.running = False
    await save_bot_state(state)
    await send_telegram("🔴 *Bot detenido* manualmente.")
    return {"state": state.model_dump()}

@api.post("/bot/reset-circuit-breaker")
async def reset_circuit_breaker():
    """Resetea el circuit breaker manualmente tras revisión."""
    state = await get_bot_state()
    state.circuit_breaker_tripped = False
    state.daily_loss_pct          = 0.0
    state.daily_loss_date         = today_utc()
    await save_bot_state(state)
    return {"message": "Circuit breaker reseteado", "state": state.model_dump()}

@api.post("/bot/panic")
async def panic_stop():
    """Para el bot y cierra todas las posiciones abiertas."""
    settings = await get_settings()
    state    = await get_bot_state()
    state.running = False
    await save_bot_state(state)

    tickers = await fetch_tickers(settings.symbols)
    closed  = []
    async for doc in db.positions.find({"status": "OPEN"}, {"_id": 0}):
        pos = Position(**doc)
        t   = tickers.get(pos.symbol)
        if t:
            c = await close_position(pos, t["price"], "🛑 Pánico manual")
            closed.append(c.model_dump())

    await send_telegram(
        f"🛑 *PÁNICO MANUAL ACTIVADO*\n"
        f"Bot detenido. {len(closed)} posición(es) cerrada(s)."
    )
    return {"message": "Bot detenido y posiciones cerradas", "closed": closed}

@api.post("/bot/tick")
async def bot_tick(background_tasks: BackgroundTasks):
    result, telegram_msgs = await run_tick_iteration()
    for msg in telegram_msgs:
        background_tasks.add_task(send_telegram, msg)
    return result

async def run_tick_iteration() -> tuple:
    """Lógica principal del bot en cada tick."""
    settings = await get_settings()
    state    = await get_bot_state()
    state.last_tick_at = now_iso()
    await save_bot_state(state)

    # Verificar circuit breaker antes de hacer nada
    if await check_circuit_breaker(settings):
        return {"message": "Circuit breaker activo — tick omitido"}, []

    tickers       = await fetch_tickers(settings.symbols)
    closed        = await evaluate_open_positions(tickers, settings)
    opened: List[Position] = []
    new_signals: List[Signal] = []
    telegram_msgs: List[str] = []

    for sym in settings.symbols:
        try:
            # Verificar de nuevo circuit breaker en cada símbolo
            if await check_circuit_breaker(settings):
                break

            klines = await fetch_klines(sym, "1h", 250)
            ind    = compute_indicators(klines)
            if not ind:
                continue

            tech = technical_signal(ind)
            ai   = {"action": "HOLD", "confidence": 0.0, "reason": "IA desactivada"}

            if settings.strategy in ("ai", "combined"):
                ai = await ai_signal(sym, "1h", ind, tech)

            if settings.strategy == "technical":
                combined = tech
                source   = "technical"
            elif settings.strategy == "ai":
                combined = ai
                source   = "ai"
            else:
                combined = combine_signals(tech, ai)
                source   = "combined"

            price = float(klines[-1]["close"])
            atr   = ind.get("atr") or 0

            sig = Signal(
                symbol=sym,
                timeframe="1h",
                action=combined["action"],
                confidence=combined["confidence"],
                price=price,
                reason=combined["reason"],
                indicators=ind,
                source=source,
            )
            await db.signals.insert_one(sig.model_dump())
            new_signals.append(sig)

            # Abrir posición solo si:
            # 1. Bot está corriendo
            # 2. Señal es BUY con confianza suficiente
            # 3. ATR disponible para stop loss
            if (
                state.running
                and combined["action"] == "BUY"
                and combined["confidence"] >= settings.min_confidence
                and atr > 0
            ):
                pos = await open_position(sym, price, settings, combined["reason"], atr)
                if pos is not None:
                    opened.append(pos)
                    rr = round((pos.take_profit - pos.entry_price) / (pos.entry_price - pos.stop_loss), 1)
                    telegram_msgs.append(
                        f"🟢 *Nueva operación abierta*\n"
                        f"Par: `{sym}`\n"
                        f"Precio entrada: ${pos.entry_price:,.2f}\n"
                        f"Stop Loss: ${pos.stop_loss:,.2f}\n"
                        f"Take Profit: ${pos.take_profit:,.2f}\n"
                        f"Ratio R/R: 1:{rr}\n"
                        f"Confianza: {combined['confidence']*100:.0f}%\n"
                        f"Motivo: {combined['reason'][:100]}"
                    )

        except Exception as exc:
            logger.warning("Error en tick para %s: %s", sym, exc)

    for c in closed:
        emoji  = "✅" if (c.pnl or 0) >= 0 else "❌"
        telegram_msgs.append(
            f"{emoji} *Posición cerrada*\n"
            f"Par: `{c.symbol}`\n"
            f"Precio salida: ${c.exit_price:,.2f}\n"
            f"P&L neto: ${c.pnl:+.4f} ({c.pnl_pct:+.2f}%)\n"
            f"Motivo: {c.reason_close}"
        )

    return (
        {
            "opened":  [p.model_dump() for p in opened],
            "closed":  [p.model_dump() for p in closed],
            "signals": [s.model_dump() for s in new_signals],
        },
        telegram_msgs,
    )

# ── Routes — Settings ────────────────────────────────────────────────────────
@api.get("/settings")
async def get_settings_route():
    s = await get_settings()
    return s.model_dump()

@api.put("/settings")
async def update_settings_route(upd: SettingsUpdate):
    cur  = await get_settings()
    data = cur.model_dump()
    for k, v in upd.model_dump(exclude_unset=True).items():
        if v is not None:
            data[k] = v
    new = Settings(**data)
    await save_settings(new)
    return new.model_dump()

# ── Routes — Backtest ────────────────────────────────────────────────────────
@api.post("/backtest")
async def backtest(req: BacktestRequest):
    """
    Backtest con estrategia mejorada.
    Incluye comisiones (0.1% por lado) y filtro EMA 200.
    """
    klines = await fetch_klines(req.symbol, req.timeframe, min(req.limit, 1000))
    if len(klines) < 220:
        raise HTTPException(status_code=400, detail="Se necesitan al menos 220 velas")

    settings     = await get_settings()
    equity       = req.starting_balance
    open_trade   = None
    trades       = []
    equity_curve = []

    df     = pd.DataFrame(klines)
    closes = df["close"].astype(float).values

    for i in range(210, len(klines)):
        window = klines[max(0, i - 210): i + 1]
        ind    = compute_indicators(window)
        if not ind:
            continue
        tech  = technical_signal(ind)
        price = float(closes[i])
        atr   = ind.get("atr") or 0

        if open_trade and atr > 0:
            sl = open_trade["sl"]
            tp = open_trade["tp"]
            exit_price = None
            reason     = None

            if price <= sl:
                exit_price = sl
                reason = "SL"
            elif price >= tp:
                exit_price = tp
                reason = "TP"

            if exit_price is not None:
                qty       = open_trade["qty"]
                pnl_bruto = (exit_price - open_trade["entry"]) * qty
                comm      = open_trade["entry"] * qty * BINANCE_COMMISSION + exit_price * qty * BINANCE_COMMISSION
                pnl_neto  = pnl_bruto - comm
                equity   += pnl_neto
                trades.append({
                    "entry":  open_trade["entry"],
                    "exit":   exit_price,
                    "pnl":    round(pnl_neto, 4),
                    "reason": reason,
                    "time":   klines[i]["open_time"],
                })
                open_trade = None

        if not open_trade and tech["action"] == "BUY" and tech["confidence"] >= 0.65 and atr > 0:
            sl_dist = atr * settings.stop_loss_atr_multiplier
            sl      = price - sl_dist
            tp      = price + sl_dist * settings.take_profit_multiplier
            rr      = (tp - price) / (price - sl) if price > sl else 0
            if rr >= 1.9:
                risk_amt   = equity * (settings.risk_per_trade_pct / 100)
                qty        = risk_amt / sl_dist if sl_dist > 0 else 0
                if qty > 0:
                    open_trade = {"entry": price, "qty": qty, "sl": sl, "tp": tp}

        equity_curve.append({"t": klines[i]["open_time"], "equity": round(equity, 4)})

    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    total_comm = sum(
        t["entry"] * (req.starting_balance * settings.risk_per_trade_pct / 100 / t["entry"] if t["entry"] > 0 else 0) * BINANCE_COMMISSION * 2
        for t in trades
    )

    # Calcular max drawdown
    peak = req.starting_balance
    max_dd = 0.0
    for pt in equity_curve:
        if pt["equity"] > peak:
            peak = pt["equity"]
        dd = (peak - pt["equity"]) / peak * 100
        if dd > max_dd:
            max_dd = dd

    return {
        "symbol":           req.symbol.upper(),
        "timeframe":        req.timeframe,
        "starting_balance": req.starting_balance,
        "final_equity":     round(equity, 4),
        "total_pnl":        round(equity - req.starting_balance, 4),
        "return_pct":       round((equity - req.starting_balance) / req.starting_balance * 100, 2),
        "total_trades":     len(trades),
        "wins":             len(wins),
        "losses":           len(losses),
        "win_rate":         round(len(wins) / len(trades) * 100, 2) if trades else 0,
        "max_drawdown_pct": round(max_dd, 2),
        "commissions_paid": round(total_comm, 4),
        "trades":           trades[-30:],
        "equity_curve":     equity_curve[-200:],
    }

# ── Routes — Tests ────────────────────────────────────────────────────────────
@api.post("/telegram/test")
async def telegram_test():
    ok = await send_telegram(
        "✅ *Bot conectado correctamente*\n"
        "Las notificaciones funcionan.\n"
        "Estás listo para empezar."
    )
    return {"sent": ok}

@api.post("/groq/test")
async def groq_test():
    """Verifica que Groq API funciona correctamente."""
    if not GROQ_API_KEY:
        return {"ok": False, "error": "GROQ_API_KEY no configurada en .env"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": "Responde solo: OK"}],
                    "max_tokens": 10,
                },
            )
            r.raise_for_status()
            return {"ok": True, "model": GROQ_MODEL, "response": r.json()["choices"][0]["message"]["content"]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

# ── Scheduler automático ──────────────────────────────────────────────────────
_scheduler_task: Optional[asyncio.Task] = None

async def scheduler_loop():
    logger.info("⏰ Scheduler iniciado")
    await asyncio.sleep(10)
    elapsed = 0
    while True:
        try:
            state    = await get_bot_state()
            settings = await get_settings()
            interval = max(60, settings.tick_interval_seconds)

            if state.running and elapsed >= interval:
                logger.info("🔄 Auto-tick (intervalo=%ss)", interval)
                _, telegram_msgs = await run_tick_iteration()
                for msg in telegram_msgs:
                    asyncio.create_task(send_telegram(msg))
                elapsed = 0
            elif not state.running:
                elapsed = 0

            await asyncio.sleep(10)
            elapsed += 10

        except asyncio.CancelledError:
            logger.info("Scheduler cancelado")
            raise
        except Exception as exc:
            logger.warning("Error en scheduler: %s", exc)
            await asyncio.sleep(15)

# ── App setup ─────────────────────────────────────────────────────────────────
app.include_router(api)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    global _scheduler_task
    _scheduler_task = asyncio.create_task(scheduler_loop())
    logger.info("🚀 Trading Bot arrancado")

@app.on_event("shutdown")
async def shutdown():
    global _scheduler_task
    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    mongo_client.close()
