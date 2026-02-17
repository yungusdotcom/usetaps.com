# TAPS — Web Application

Live inventory intelligence and purchasing system for Thrive Cannabis Marketplace.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   runtaps.com    │────▶│   TAPS API       │────▶│  Flowhub API    │
│   React App      │◀────│   FastAPI/Python  │◀────│  Inventory +    │
│                  │     │   Railway/Fly.io  │     │  Orders         │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

- **Frontend**: React app hosted on Vercel/Netlify at runtaps.com
- **Backend**: FastAPI on Railway — handles Flowhub API, caching, TAPS engine
- **Data**: Flowhub API for live inventory + cached sales

## Quick Start (Local)

### Backend
```bash
cd api
pip install -r requirements.txt
export FLOWHUB_CLIENT_ID="your-id"
export FLOWHUB_API_KEY="your-key"
uvicorn main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5173

## Deploy

### Backend (Railway)
1. Push to GitHub
2. Connect repo to Railway
3. Set environment variables: FLOWHUB_CLIENT_ID, FLOWHUB_API_KEY
4. Deploy — it auto-detects the Dockerfile

### Frontend (Vercel)
1. Push to GitHub
2. Import to Vercel
3. Set API_BASE env var to your Railway URL
4. Deploy

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/taps` | GET | Full TAPS analysis (inventory + sales + engine) |
| `/api/taps?refresh_inventory=true` | GET | Force fresh inventory pull |
| `/api/inventory` | GET | Pull current inventory from Flowhub |
| `/api/refresh-sales` | POST | Trigger background sales pull (~10 min) |
| `/api/sales-status` | GET | Check if sales pull is running |
| `/api/status` | GET | System status |

## Buyer Workflow

1. Open runtaps.com
2. Dashboard loads with cached data
3. Click **↻ Inventory** for real-time on-hand counts (2-3 sec)
4. Click **↻ Sales** to refresh sales data (runs in background)
5. Filter, sort, generate POs
6. All tabs: Command Center, Revenue, Par Levels, Stockouts, Overstock, Dead Weight, Store View, POs
