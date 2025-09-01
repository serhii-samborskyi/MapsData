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
async def get_campaigns(request: Request, partial: bool = False):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                sc.*,
                COUNT(DISTINCT r.id) as total_requests,
                COUNT(DISTINCT c.id) as total_contacts,
                (SELECT COUNT(*) FROM requests WHERE campaign_id = sc.id AND status = 'completed') as completed_requests,
                (SELECT COUNT(*) FROM contacts WHERE campaign_id = sc.id AND email IS NOT NULL AND email != '') as email_count
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
                LEFT JOIN contacts c ON r.campaign_id = c.campaign_id AND r.id = c.request_id
                WHERE r.campaign_id = ? 
                GROUP BY r.id
            """, (campaign['id'],))
            campaign['requests'] = [dict(r) for r in cursor.fetchall()]

            cursor.execute("SELECT * FROM contacts WHERE campaign_id = ?", (campaign['id'],))
            campaign['contacts'] = [dict(r) for r in cursor.fetchall()]

            campaigns.append(campaign)
    template = "index.html" if not partial else "partials/table.html"
    return templates.TemplateResponse(template, {"request": request, "campaigns": campaigns})

@app.delete("/api/campaign/{campaign_id}")
async def delete_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM contacts WHERE campaign_id = ?", (campaign_id,))
        cursor.execute("DELETE FROM requests WHERE campaign_id = ?", (campaign_id,))
        cursor.execute("DELETE FROM search_campaigns WHERE id = ?", (campaign_id,))
        conn.commit()
    return {"status": "Campaign deleted successfully"}

@app.get("/update_campaign_status/{campaign_id}/{status}")
async def update_campaign_status(campaign_id: int, status: str):
    if status not in ['active', 'inactive', 'completed']:
        raise HTTPException(status_code=400, detail="Invalid status. Must be 'active', 'inactive' or 'completed'")

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE search_campaigns SET status = ? WHERE id = ?",
            (status, campaign_id)
        )
        conn.commit()
    return {"status": "Campaign status updated"}

@app.get("/api/campaign/{campaign_id}/complete")
async def complete_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM search_campaigns WHERE id = ?", (campaign_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Campaign not found")

        cursor.execute(
            "UPDATE search_campaigns SET status = 'completed' WHERE id = ?",
            (campaign_id,)
        )
        conn.commit()
    return {"status": "Campaign marked as completed"}

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
            WHERE sc.name = ? AND r.status = 'pending'
        """, (campaign_name,))
        requests = [dict(row) for row in cursor.fetchall()]
        if not requests:
            raise HTTPException(status_code=404, detail="No pending requests found")
        return {"requests": requests}

@app.get("/api/request/{request_id}/status/{status}")
async def update_request_status(request_id: int, status: str):
    if status not in ['inuse', 'completed']:
        raise HTTPException(status_code=400, detail="Invalid status. Must be 'inuse' or 'completed'")

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM requests WHERE id = ?", (request_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Request not found")

        cursor.execute(
            "UPDATE requests SET status = ? WHERE id = ?",
            (status, request_id)
        )
        conn.commit()
        return {"status": "Request status updated successfully"}

@app.post("/api/contacts")
async def save_contacts(request: Request):
    data = await request.json()
    contacts = data if isinstance(data, list) else [data]

    saved_contacts = []
    with get_db() as conn:
        cursor = conn.cursor()

        for contact in contacts:
            campaign_id = contact.get('campaign_id')
            request_id = contact.get('request_id')

            # Verify campaign exists and is active
            cursor.execute("SELECT status FROM search_campaigns WHERE id = ?", (campaign_id,))
            campaign = cursor.fetchone()
            if not campaign:
                raise HTTPException(status_code=404, detail=f"Campaign {campaign_id} not found")
            if campaign['status'] != 'active':
                raise HTTPException(status_code=400, detail=f"Campaign {campaign_id} is not active")

            # Verify request belongs to campaign
            cursor.execute("SELECT id FROM requests WHERE id = ? AND campaign_id = ?", (request_id, campaign_id))
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail=f"Invalid request ID {request_id} for campaign {campaign_id}")

            cursor.execute(
                """INSERT INTO contacts 
                   (address, business_name, campaign_id, category, domain, email, facebook, instagram, phone, place_id, rating, request_id, review_count, twitter, yelp, status) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    contact.get('address'),
                    contact.get('business_name', contact.get('title')),
                    campaign_id,
                    contact.get('category'),
                    contact.get('domain', contact.get('website')),
                    contact.get('email'),
                    contact.get('facebook'),
                    contact.get('instagram'),
                    contact.get('phone'),
                    contact.get('place_id', contact.get('url', '').split('/place/')[-1].split('/')[0] if contact.get('url') else ''),
                    contact.get('rating'),
                    request_id,
                    contact.get('review_count', contact.get('reviewsCount', 0)),
                    contact.get('twitter'),
                    contact.get('yelp'),
                    "pending"
                )
            )
            saved_contacts.append({
                "contact_id": cursor.lastrowid,
                "campaign_id": campaign_id,
                "request_id": request_id
            })

        conn.commit()
    return {"status": "Contacts saved successfully", "saved_contacts": saved_contacts}

@app.post("/api/campaign/{campaign_id}/email_update")
async def update_contact_email(campaign_id: int, data: dict):
    # Check if it's a batch update or single update
    if 'contacts' in data:
        # Batch update
        contacts = data.get('contacts', [])
        if not contacts:
            raise HTTPException(status_code=400, detail="No contacts provided in batch")
        if len(contacts) > 100:
            raise HTTPException(status_code=400, detail="Batch size cannot exceed 100 contacts")
            
        updated_count = 0
        failed_updates = []
        
        with get_db() as conn:
            cursor = conn.cursor()
            for contact in contacts:
                contact_id = contact.get('id')
                email = contact.get('email')
                
                if not contact_id or not email:
                    failed_updates.append({"id": contact_id, "error": "Missing id or email"})
                    continue
                    
                cursor.execute("""
                    UPDATE contacts 
                    SET email = ? 
                    WHERE id = ? AND campaign_id = ?
                """, (email, contact_id, campaign_id))
                
                if cursor.rowcount > 0:
                    updated_count += 1
                else:
                    failed_updates.append({"id": contact_id, "error": "Contact not found"})
            
            conn.commit()
            
        return {
            "status": "Batch update completed",
            "updated_count": updated_count,
            "failed_count": len(failed_updates),
            "failed_updates": failed_updates
        }
    else:
        # Single update (backward compatibility)
        contact_id = data.get('id')
        email = data.get('email')
        
        if not contact_id or not email:
            raise HTTPException(status_code=400, detail="Missing id or email in request body")
            
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE contacts 
                SET email = ? 
                WHERE id = ? AND campaign_id = ?
            """, (email, contact_id, campaign_id))
            conn.commit()
            
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Contact not found")
                
            return {"status": "Email updated successfully"}

@app.get("/api/campaign/{campaign_id}/nomail")
async def get_random_contact_without_email(campaign_id: int, batch: int = 1):
    if batch < 0 or batch > 1000:
        raise HTTPException(status_code=400, detail="Batch size must be between 0 and 1000")
        
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, domain FROM contacts 
            WHERE campaign_id = ? 
            AND (email IS NULL OR email = '')
            AND domain IS NOT NULL 
            AND domain != ''
            ORDER BY RANDOM() LIMIT ?
        """, (campaign_id, batch))
        contacts = cursor.fetchall()
        if not contacts:
            raise HTTPException(status_code=404, detail="No contacts found matching criteria")
            
        results = []
        for contact in contacts:
            # Clean domain: remove protocol, www, and URL parameters
            domain = contact["domain"]
            domain = domain.replace("http://", "").replace("https://", "").replace("www.", "")
            domain = domain.split("?")[0].split("/")[0]
            results.append({"id": str(contact["id"]), "domain": domain})
        
        return {"contacts": results, "count": len(results)}

@app.post("/api/campaign/{campaign_id}/remove_duplicates")
async def remove_duplicate_contacts(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Remove duplicates by domain only
        cursor.execute("""
            DELETE FROM contacts 
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM contacts
                WHERE campaign_id = ? 
                AND domain IS NOT NULL
                GROUP BY domain
            )
            AND campaign_id = ?
            AND domain IS NOT NULL
        """, (campaign_id, campaign_id))
        duplicate_count = cursor.rowcount
        
        conn.commit()
        return {
            "status": "success", 
            "removed_duplicates": duplicate_count
        }

@app.post("/api/campaign/{campaign_id}/remove_empty_domains")
async def remove_empty_domains(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM contacts 
            WHERE campaign_id = ?
            AND (domain IS NULL OR domain = '')
        """, (campaign_id,))
        deleted_count = cursor.rowcount
        conn.commit()
        return {"status": "success", "removed_contacts": deleted_count}

@app.post("/api/campaign/{campaign_id}/remove_filtered")
async def remove_filtered_contacts(campaign_id: int, data: dict):
    keywords = data.get('keywords', [])
    if not keywords:
        return {"status": "error", "message": "No keywords provided"}
    
    with get_db() as conn:
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in keywords])
        like_conditions = []
        params = []
        
        for keyword in keywords:
            like_conditions.extend([
                "business_name LIKE ?",
                "domain LIKE ?",
                "email LIKE ?",
            ])
            params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
        
        params.append(campaign_id)
        
        query = f"""
            DELETE FROM contacts 
            WHERE ({' OR '.join(like_conditions)})
            AND campaign_id = ?
        """
        
        cursor.execute(query, params)
        deleted_count = cursor.rowcount
        conn.commit()
        return {"status": "success", "removed_contacts": deleted_count}

@app.post("/api/campaign/{campaign_id}/duplicate")
async def duplicate_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get original campaign
        cursor.execute("SELECT * FROM search_campaigns WHERE id = ?", (campaign_id,))
        original_campaign = cursor.fetchone()
        if not original_campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        # Find next available number for campaign name
        base_name = original_campaign["name"]
        counter = 1
        new_name = f"{base_name} {counter}"
        
        while True:
            cursor.execute("SELECT id FROM search_campaigns WHERE name = ?", (new_name,))
            if not cursor.fetchone():
                break
            counter += 1
            new_name = f"{base_name} {counter}"
        
        # Create new campaign
        cursor.execute(
            "INSERT INTO search_campaigns (name, status) VALUES (?, ?)",
            (new_name, "active")
        )
        new_campaign_id = cursor.lastrowid
        
        # Copy all requests from original campaign
        cursor.execute("SELECT req_text FROM requests WHERE campaign_id = ?", (campaign_id,))
        requests = cursor.fetchall()
        
        for request in requests:
            cursor.execute(
                "INSERT INTO requests (campaign_id, req_text, status) VALUES (?, ?, ?)",
                (new_campaign_id, request["req_text"], "pending")
            )
        
        conn.commit()
        return {
            "status": "success", 
            "new_campaign_id": new_campaign_id,
            "new_campaign_name": new_name,
            "copied_requests": len(requests)
        }

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