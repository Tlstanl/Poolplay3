# Pool Play Seed Projections — Self-Hosted

Live USSSA pool standings + GameChanger scores, served from your own machine.
Identical UI and behavior to the original xelite.poolplaytool.com.

---

## Files

```
poolplay/
├── server.py            ← Python backend (all config is here)
├── requirements.txt     ← pip dependencies (just aiohttp)
├── README.md            ← this file
└── templates/
    └── index.html       ← exact original frontend (do not edit)
```

---

## Quick start (your laptop)

**Requirements:** Python 3.10 or newer — https://python.org

```bash
# 1. Install the one dependency
pip install -r requirements.txt

# 2. Start the server
python server.py

# 3. Open in browser
open http://localhost:8000
```

Share with parents on your local Wi-Fi:
- Find your computer's IP address (System Settings → Wi-Fi → Details, or run `ipconfig` on Windows / `ifconfig` on Mac)
- Give them: `http://192.168.x.x:8000`

---

## Adding GameChanger teams

Open `server.py` and find the `GC_TEAMS` section near the top:

```python
GC_TEAMS: dict[str, str] = {
    "patriots white": "z415Y79XAsjY",   # already added
    # Add more teams here:
    # "rebels":  "aBcDeFgHiJkL",
    # "tigers":  "mNoPqRsTuVwX",
}
```

To find a team's public_id, look at their GameChanger URL:
```
https://web.gc.com/teams/z415Y79XAsjY/2026-spring-patriots-white-11u
                          ^^^^^^^^^^^^
                          copy this part
```

The key (e.g. `"patriots white"`) is any part of the USSSA team name,
lowercase. It just needs to be unique enough to identify the team.

Restart the server after editing (`Ctrl+C` then `python server.py`).

---

## Changing to a different tournament

Edit the top section of `server.py`:

```python
EVENT_ID    = 409034       # from the USSSA URL
DIVISION_ID = 2691056      # from the USSSA URL
AGE         = 11
AGE_CLASS   = "Open"

EVENT_NAME      = "Arkansas Premier East / West Shootout"
EVENT_LOCATION  = "Springdale/Bentonville/Fayetteville, AR"
EVENT_DATES     = "May 10–11, 2026"   # optional
```

Find the IDs in the USSSA Game Center URL:
```
?eventID=409034&divisionID=2691056
```

---

## Running on a server (so anyone can access from anywhere)

Any $5/month Linux VPS works (DigitalOcean, Linode, Vultr, etc.).

```bash
# Upload the poolplay folder to your VPS, then:
pip install -r requirements.txt

# Create a systemd service so it starts automatically:
sudo nano /etc/systemd/system/poolplay.service
```

Paste this (change `/home/ubuntu/poolplay` to your actual path):
```ini
[Unit]
Description=Pool Play Seed Projections
After=network.target

[Service]
WorkingDirectory=/home/ubuntu/poolplay
ExecStart=/usr/bin/python3 server.py
Restart=always
User=ubuntu

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now poolplay
# Now running at http://your-server-ip:8000
```

### Add a domain + HTTPS (optional but recommended)

Install nginx and certbot, then:

```nginx
# /etc/nginx/sites-available/poolplay
server {
    listen 80;
    server_name yourname.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;

        # SSE requires buffering disabled:
        proxy_buffering    off;
        proxy_cache        off;
        proxy_read_timeout 3600s;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/poolplay /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d yourname.com
```

---

## How it works

```
Browser ←── SSE ──── /api/events ←─── background poller (every 20–300s)
        ←── JSON ─── /api/state         │
                                         ├── USSSA API  (standings + scores)
                                         └── GameChanger API (live scores)
```

- The poller runs in the background, fetching USSSA + GC every N seconds
- When content changes it bumps a revision number and pushes a tiny SSE notification
- Each browser tab receives the notification and fetches the full `/api/state`
- Open `<details>` panels (game lists) stay open during live refreshes
- Poll interval: 20s during live games, 2min between games, 5min when idle
- Seed ranges use Monte Carlo simulation (5,000 scenarios) just like the original
