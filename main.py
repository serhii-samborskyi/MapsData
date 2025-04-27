
from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import init_db, get_db
from typing import List, Union

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Initialize database
init_db()

@app.get("/", response_class=HTMLResponse)
async def get_campaigns(request: Request):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                sc.*,
                COUNT(DISTINCT r.id) as total_requests,
                COUNT(DISTINCT c.id) as total_contacts,
                SUM(CASE WHEN r.status = 'completed' THEN 1 ELSE 0 END) as completed_requests
            FROM search_campaigns sc
            LEFT JOIN requests r ON sc.id = r.campaign_id
            LEFT JOIN contacts c ON sc.id = c.campaign_id
            GROUP BY sc.id
        """)
        campaigns = [dict(row) for row in cursor.fetchall()]
    return templates.TemplateResponse("index.html", {"request": request, "campaigns": campaigns})

@app.post("/create_campaign")
async def create_campaign(name: str = Form(...), search_phrases: str = Form(...)):
    phrases = [p.strip() for p in search_phrases.split("\n") if p.strip()]
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO search_campaigns (name, status) VALUES (?, ?)",
            (name, "active")
        )
        campaign_id = cursor.lastrowid
        for phrase in phrases:
            cursor.execute(
                "INSERT INTO requests (campaign_id, req_text, status) VALUES (?, ?, ?)",
                (campaign_id, phrase, "pending")
            )
        conn.commit()
    return {"status": "Campaign created", "campaign_id": campaign_id}

@app.get("/api/reserve_phrase/{campaign_id}")
async def reserve_phrase(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, req_text FROM requests WHERE campaign_id = ? AND status = 'pending' LIMIT 1",
            (campaign_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No pending phrases")
        phrase_id = row["id"]
        cursor.execute(
            "UPDATE requests SET status = 'reserved' WHERE id = ?",
            (phrase_id,)
        )
        conn.commit()
        return {"phrase_id": phrase_id, "req_text": row["req_text"]}

@app.post("/api/store_contact")
async def store_contact(
    campaign_id: int = Form(...),
    phrase_id: int = Form(...),
    business_name: str = Form(...),
    review_count: int = Form(...),
    phone: str = Form(None),
    domain: str = Form(None),
    email: str = Form(None)
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO contacts (campaign_id, business_name, review_count, phone, domain, email, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (campaign_id, business_name, review_count, phone, domain, email, "pending")
        )
        cursor.execute(
            "UPDATE requests SET status = 'completed' WHERE id = ?",
            (phrase_id,)
        )
        conn.commit()
    return {"status": "Contact stored"}

@app.get("/api/campaign/{campaign_id}")
async def get_campaign_status(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM search_campaigns WHERE id = ?", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        cursor.execute("SELECT * FROM requests WHERE campaign_id = ?", (campaign_id,))
        requests = [dict(row) for row in cursor.fetchall()]
        cursor.execute("SELECT * FROM contacts WHERE campaign_id = ?", (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]
        return {"campaign": dict(campaign), "requests": requests, "contacts": contacts}
