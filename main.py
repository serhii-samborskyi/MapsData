
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

@app.get("/docs/api", response_class=HTMLResponse)
async def get_api_docs(request: Request):
    return templates.TemplateResponse("docs.html", {"request": request})

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
        campaigns = []
        for row in cursor.fetchall():
            campaign = dict(row)
            cursor.execute("""
                SELECT r.*, COUNT(c.id) as contact_count 
                FROM requests r 
                LEFT JOIN contacts c ON r.campaign_id = c.campaign_id 
                WHERE r.campaign_id = ? 
                GROUP BY r.id
            """, (campaign['id'],))
            campaign['requests'] = [dict(r) for r in cursor.fetchall()]
            
            cursor.execute("SELECT * FROM contacts WHERE campaign_id = ?", (campaign['id'],))
            campaign['contacts'] = [dict(r) for r in cursor.fetchall()]
            
            campaigns.append(campaign)
    return templates.TemplateResponse("index.html", {"request": request, "campaigns": campaigns})

@app.post("/update_campaign_status/{campaign_id}")
async def update_campaign_status(campaign_id: int, status: str = Form(...)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE search_campaigns SET status = ? WHERE id = ?",
            (status, campaign_id)
        )
        conn.commit()
    return {"status": "Campaign status updated"}

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

@app.get("/api/campaigns/active")
async def get_active_campaigns():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                sc.*,
                COUNT(DISTINCT r.id) as total_requests,
                COUNT(DISTINCT c.id) as total_contacts
            FROM search_campaigns sc
            LEFT JOIN requests r ON sc.id = r.campaign_id
            LEFT JOIN contacts c ON sc.id = c.campaign_id
            WHERE sc.status = 'active'
            GROUP BY sc.id
        """)
        campaigns = [dict(row) for row in cursor.fetchall()]
        return {"campaigns": campaigns}

@app.get("/api/campaign/{campaign_name}/requests")
async def get_campaign_requests(campaign_name: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.*, sc.name as campaign_name
            FROM requests r
            JOIN search_campaigns sc ON r.campaign_id = sc.id
            WHERE sc.name = ?
        """, (campaign_name,))
        requests = [dict(row) for row in cursor.fetchall()]
        if not requests:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return {"requests": requests}

@app.post("/api/contacts")
async def save_contact(
    campaign_id: int = Form(...),
    request_id: int = Form(...),
    business_name: str = Form(...),
    review_count: int = Form(...),
    phone: str = Form(None),
    domain: str = Form(None),
    email: str = Form(None)
):
    with get_db() as conn:
        cursor = conn.cursor()
        # Verify campaign exists and is active
        cursor.execute("SELECT status FROM search_campaigns WHERE id = ?", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        if campaign['status'] != 'active':
            raise HTTPException(status_code=400, detail="Campaign is not active")
        
        # Verify request belongs to campaign
        cursor.execute("SELECT id FROM requests WHERE id = ? AND campaign_id = ?", (request_id, campaign_id))
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail="Invalid request ID for this campaign")
        
        cursor.execute(
            """INSERT INTO contacts 
               (campaign_id, business_name, review_count, phone, domain, email, status) 
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, business_name, review_count, phone, domain, email, "pending")
        )
        conn.commit()
        return {"status": "Contact saved successfully", "contact_id": cursor.lastrowid}

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
