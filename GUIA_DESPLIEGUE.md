# 🚀 Guía de despliegue — Crypto Trading Bot

## Lo que vas a tener al final
- Bot corriendo 24/7 en la nube (gratis)
- Alertas en Telegram cuando opera
- Dashboard en tu móvil
- Binance Testnet (dinero ficticio, precios reales)

---

## PASO 1 — MongoDB Atlas (base de datos gratis)

1. Ve a **mongodb.com/atlas** → "Try Free"
2. Regístrate con Google
3. Elige **M0 Free** (gratis para siempre)
4. Región: **AWS / N. Virginia (us-east-1)** → Create
5. Espera 2 minutos a que se cree el cluster
6. Click en **"Connect"** → "Drivers"
7. Copia la URL que tiene este formato:
   ```
   mongodb+srv://usuario:password@cluster0.xxxxx.mongodb.net/
   ```
8. Guárdala — la necesitas en el paso 4

**Importante:** En "Network Access" → "Add IP Address" → "Allow Access from Anywhere"

---

## PASO 2 — Groq API (IA gratis)

1. Ve a **console.groq.com**
2. Regístrate con Google
3. Ve a **"API Keys"** → **"Create API Key"**
4. Ponle nombre: "trading-bot"
5. Copia la key (empieza por `gsk_...`)
6. Guárdala — la necesitas en el paso 4

---

## PASO 3 — Telegram Bot

1. Abre Telegram en tu móvil
2. Busca **@BotFather**
3. Escribe: `/newbot`
4. Ponle nombre: `Mi Trading Bot`
5. Ponle username: `mitradinbgot_bot` (tiene que terminar en _bot)
6. Copia el **token** que te da (formato: `1234567890:ABCdef...`)
7. Busca **@userinfobot** en Telegram
8. Escribe cualquier cosa y te dirá tu **Chat ID** (un número)
9. Guarda ambos

---

## PASO 4 — GitHub (subir el código)

1. Ve a **github.com** → regístrate gratis
2. Click en **"New repository"**
3. Nombre: `crypto-bot`
4. Público o privado (da igual)
5. Create repository
6. Sube estos 3 archivos:
   - `server.py`
   - `requirements.txt`
   - `railway.json`

Para subir archivos: en el repo → "Add file" → "Upload files"

---

## PASO 5 — Railway (servidor gratis 24/7)

1. Ve a **railway.app**
2. "Start a New Project" → Login with GitHub
3. "Deploy from GitHub repo" → selecciona `crypto-bot`
4. Click en el proyecto → "Variables" → "Add Variable"
5. Añade estas variables una por una:

```
MONGO_URL       = mongodb+srv://... (la de MongoDB Atlas)
DB_NAME         = trading_bot
GROQ_API_KEY    = gsk_... (la de Groq)
USE_TESTNET     = true
TELEGRAM_BOT_TOKEN = 1234567890:ABCdef...
TELEGRAM_CHAT_ID   = 123456789
CORS_ORIGINS    = *
```

6. Railway desplegará solo — espera 2 minutos
7. Ve a "Settings" → "Domains" → "Generate Domain"
8. Copia tu URL (formato: `crypto-bot-production.up.railway.app`)

---

## PASO 6 — Verificar que funciona

Abre en el navegador:
```
https://TU-URL.up.railway.app/api/
```
Debes ver: `{"name":"Crypto Trading Bot","status":"ok","version":"2.0"}`

Prueba Telegram:
```
https://TU-URL.up.railway.app/api/telegram/test
```
Si recibes mensaje en Telegram → ✅ Todo funciona

Prueba Groq:
```
https://TU-URL.up.railway.app/api/groq/test
```
Debes ver: `{"ok":true,...}`

---

## PASO 7 — Usar el bot

**Ver señales:**
```
https://TU-URL.up.railway.app/api/signals/generate?symbol=BTCUSDT
```

**Iniciar el bot:**
```
POST https://TU-URL.up.railway.app/api/bot/start
```

**Ver portfolio:**
```
https://TU-URL.up.railway.app/api/portfolio
```

**Parar de emergencia:**
```
POST https://TU-URL.up.railway.app/api/bot/panic
```

---

## ⚠️ Recuerda

- `USE_TESTNET=true` significa dinero ficticio — no pierdes nada real
- Deja el bot en testnet mínimo 1 mes antes de cambiar a real
- El circuit breaker para el bot si pierde 5% en un día
- Solo abre UNA posición a la vez (más seguro)
- Las alertas de Telegram están en español

---

## 📱 Instalar el dashboard en el móvil (PWA)

1. Abre Chrome en tu móvil Android
2. Ve al dashboard (te lo daré en el siguiente paso)
3. Toca los 3 puntos → "Añadir a pantalla de inicio"
4. Se instala como si fuera una app normal
