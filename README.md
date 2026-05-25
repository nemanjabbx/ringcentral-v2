# RingCentral Availability API

A lightweight Node.js server that checks agent availability directly via **RingCentral**. Works standalone — no Ringba required. Any system (dialer, IVR, custom app) can ping this API to check whether agents are available before routing a call.

## How It Works

1. Your system sends a GET request to `/availability?state=TX` or `/queue?name=Sales Team`
2. The server authenticates with RingCentral using JWT (token cached 55 min)
3. It finds the matching call queue on the RC account
4. Checks presence of all queue members in parallel
5. Returns `{ "available": true }` if at least one agent is: **Available + TakeAllCalls + NoCall**

---

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check — returns `{"status":"ok"}` |
| `GET /availability?state=TX` | Check availability by 2-letter US state code |
| `GET /queue?name=Sales Team` | Check availability by exact queue name |

---

## Environment Variables

Set these on your hosting platform (Render, Railway, etc.):

| Variable | Description |
|----------|-------------|
| `RC_CLIENT_ID` | RingCentral App Client ID |
| `RC_CLIENT_SECRET` | RingCentral App Client Secret |
| `RC_JWT` | RingCentral JWT token (must match the app above) |

---

## Setup Guide

### 1. RingCentral — Create App & Get Credentials

1. Go to **https://developers.ringcentral.com** → Login with the target RC account
2. **Console → Apps → Create App**
   - App Type: **REST API App**
   - Auth: **JWT auth flow** ← required
   - Issue refresh tokens: **Off**
   - Who can access: **Private**
   - Scopes: `Read Accounts`, `Read Presence`, `Call Queues`
3. After creation → **Credentials tab** → copy `Client ID` and `Client Secret`
4. Click **"Create JWT"** → copy the token (starts with `eyJ...`)
   - If no JWT button visible: go to **https://developers.ringcentral.com/console/jwt** → Create JWT → select your app under Authorized Apps

> **Important:** Must be created on the RingCentral account where the call queues exist. Each RC account needs its own set of credentials.

---

### 2. Deploy to Render

1. Go to **https://render.com** → **New → Web Service** → connect GitHub repo
2. Configure:
   - **Runtime:** Node
   - **Build Command:** `npm install`
   - **Start Command:** `node index.js`
   - **Instance Type:** Starter ($7/mo) — required, no cold starts
3. **Environment tab** → add:

   | KEY | VALUE |
   |-----|-------|
   | `RC_CLIENT_ID` | from step 1 |
   | `RC_CLIENT_SECRET` | from step 1 |
   | `RC_JWT` | from step 1 |

4. **Save Changes** → auto redeploy

---

### 3. Test

```bash
# Health check
curl https://your-service.onrender.com/health

# By state
curl https://your-service.onrender.com/availability?state=MI

# By queue name
curl "https://your-service.onrender.com/queue?name=Michigan"
```

Expected response:
```json
{"available":true,"agents":3,"state":"MI","queue":"Michigan","total_members":5}
```

---

## Token Expiry & Renewal

If you see `invalid_grant` error:

1. RC Developer Portal → your app → Credentials → **Create new JWT**
2. Update `RC_JWT` in Render → Environment → Save
3. Render auto-redeploys

---

## File Structure

```
index.js      — Main server (state + queue availability, port 3000)
package.json  — Node dependencies
README.md     — This file
```
