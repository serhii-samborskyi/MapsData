from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import init_db, get_db
from templates import TemplateManager, ManyReachIntegration, SmartLeadIntegration
from email_verification import EmailVerificationManager, EmailVerificationService, MyEmailVerifierIntegration
from typing import List, Union, Any, Optional
from psycopg2 import DataError, IntegrityError
import json
import time
from datetime import datetime, timedelta
import threading
from uuid import uuid4

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

PUBLIC_EMAIL_DOMAINS = [
    'gmail.com',
    'yahoo.com',
    'outlook.com',
    'hotmail.com',
    'icloud.com',
    'aol.com',
    'mail.com',
    'proton.me',
    'protonmail.com',
    'live.com',
    'msn.com',
    'gmx.com',
    'zoho.com',
    'yandex.com',
    'yandex.ru',
    'mail.ru',
    'fastmail.com',
    'tutanota.com',
    'hushmail.com',
    'qq.com',
    '126.com',
    '163.com'
]

# Initialize database
init_db()

# In-memory verification job state (single-process runtime)
verification_jobs = {}
verification_jobs_lock = threading.Lock()
MAX_VERIFICATION_LOGS = 200


def _append_verification_log(job: dict, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    job["logs"].append(f"{timestamp} - {message}")
    if len(job["logs"]) > MAX_VERIFICATION_LOGS:
        job["logs"] = job["logs"][-MAX_VERIFICATION_LOGS:]


def _serialize_verification_job(job: dict):
    return {
        "job_id": job["job_id"],
        "campaign_id": job["campaign_id"],
        "template_id": job["template_id"],
        "status": job["status"],
        "total_emails": job["total_emails"],
        "processed_emails": job["processed_emails"],
        "verified_emails": job["verified_emails"],
        "invalid_emails": job["invalid_emails"],
        "failed_emails": job["failed_emails"],
        "current_email": job["current_email"],
        "message": job["message"],
        "cancel_requested": job.get("cancel_requested", False),
        "skip_public_providers": job.get("skip_public_providers", False),
        "started_at": job["started_at"],
        "completed_at": job["completed_at"],
        "logs": list(job["logs"]),
    }


def _mark_job_cancelled(job: dict):
    if job.get("status") == "cancelled":
        return
    job["status"] = "cancelled"
    job["message"] = "Verification stopped by user"
    job["current_email"] = "-"
    job["completed_at"] = datetime.now().isoformat()
    _append_verification_log(job, "Verification stopped by user")


def _run_verification_job(job_id: str):
    with verification_jobs_lock:
        job = verification_jobs.get(job_id)
        if not job:
            return
        if job.get("cancel_requested"):
            _mark_job_cancelled(job)
            return
        job["status"] = "running"
        job["message"] = "Verification in progress"
        job["started_at"] = datetime.now().isoformat()
        _append_verification_log(job, "Verification started")
        if job.get("skip_public_providers"):
            _append_verification_log(job, "Skipping public email providers")

    try:
        template = EmailVerificationManager.get_template(job["template_id"])
        if not template:
            raise RuntimeError("Template not found")

        verification_service = EmailVerificationService()

        with get_db() as conn:
            cursor = conn.cursor()
            conditions = [
                "campaign_id = %s",
                "email IS NOT NULL",
                "email != ''",
                "(email_status IS NULL OR email_status = 'unverified')"
            ]
            params = [job["campaign_id"]]

            if job.get("skip_public_providers"):
                placeholders = ','.join(['%s' for _ in PUBLIC_EMAIL_DOMAINS])
                conditions.append(f"lower(split_part(email, '@', 2)) NOT IN ({placeholders})")
                params.extend(PUBLIC_EMAIL_DOMAINS)

            query = f"""
                SELECT id, email
                FROM contacts
                WHERE {' AND '.join(conditions)}
                ORDER BY id
            """
            cursor.execute(query, tuple(params))
            contacts = cursor.fetchall()

        with verification_jobs_lock:
            job = verification_jobs.get(job_id)
            if not job:
                return
            job["total_emails"] = len(contacts)
            if len(contacts) == 0:
                job["status"] = "completed"
                job["message"] = "No unverified emails found"
                job["completed_at"] = datetime.now().isoformat()
                _append_verification_log(job, "No unverified emails found")
                return

        cancelled = False
        for index, contact in enumerate(contacts):
            contact_id = contact["id"]
            email = contact["email"]

            with verification_jobs_lock:
                job = verification_jobs.get(job_id)
                if not job:
                    return
                if job.get("cancel_requested"):
                    _mark_job_cancelled(job)
                    cancelled = True
                    break
                job["current_email"] = email
                _append_verification_log(job, f"Verifying: {email}")

            result = verification_service.verify_batch([email], template, 0)[0]
            mapped_status = result["mapped_status"] if result["success"] else "Unknown"

            with get_db() as conn:
                cursor = conn.cursor()
                if result["success"]:
                    cursor.execute("""
                        UPDATE contacts
                        SET email_status = %s
                        WHERE id = %s AND campaign_id = %s
                    """, (mapped_status, contact_id, job["campaign_id"]))
                conn.commit()

            with verification_jobs_lock:
                job = verification_jobs.get(job_id)
                if not job:
                    return

                job["processed_emails"] += 1
                job["message"] = f"Processed {job['processed_emails']} of {job['total_emails']}"
                status_lower = mapped_status.lower()
                if status_lower in ["valid", "verified"]:
                    job["verified_emails"] += 1
                    _append_verification_log(job, f"✓ {email} - {mapped_status}")
                elif status_lower in ["invalid", "bounced"]:
                    job["invalid_emails"] += 1
                    _append_verification_log(job, f"✗ {email} - {mapped_status}")
                elif not result["success"]:
                    job["failed_emails"] += 1
                    _append_verification_log(job, f"! {email} - Failed: {result.get('error', 'Unknown error')}")
                else:
                    _append_verification_log(job, f"? {email} - {mapped_status}")

            if index < len(contacts) - 1:
                remaining_delay = max(0.0, float(job["delay"]))
                while remaining_delay > 0:
                    sleep_step = min(0.2, remaining_delay)
                    time.sleep(sleep_step)
                    remaining_delay -= sleep_step

                    with verification_jobs_lock:
                        job = verification_jobs.get(job_id)
                        if not job:
                            return
                        if job.get("cancel_requested"):
                            _mark_job_cancelled(job)
                            cancelled = True
                            break
                if cancelled:
                    break

        if cancelled:
            with verification_jobs_lock:
                job = verification_jobs.get(job_id)
                if not job:
                    return
                processed = job["processed_emails"]
                verified = job["verified_emails"]
                invalid = job["invalid_emails"]
                campaign_id = job["campaign_id"]
                template_id = job["template_id"]

            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO email_verification_logs
                    (campaign_id, template_id, emails_processed, emails_verified, emails_invalid, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    campaign_id,
                    template_id,
                    processed,
                    verified,
                    invalid,
                    "cancelled",
                    "Stopped by user"
                ))
                conn.commit()
            return

        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO email_verification_logs
                (campaign_id, template_id, emails_processed, emails_verified, emails_invalid, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                job["campaign_id"],
                job["template_id"],
                job["processed_emails"],
                job["verified_emails"],
                job["invalid_emails"],
                "completed"
            ))
            conn.commit()

        with verification_jobs_lock:
            job = verification_jobs.get(job_id)
            if not job:
                return
            job["status"] = "completed"
            job["message"] = "Verification completed successfully"
            job["current_email"] = "-"
            job["completed_at"] = datetime.now().isoformat()
            _append_verification_log(job, f"Completed: {job['processed_emails']} emails processed")

    except Exception as e:
        failed_campaign_id = None
        failed_template_id = None
        with verification_jobs_lock:
            job = verification_jobs.get(job_id)
            if job:
                failed_campaign_id = job["campaign_id"]
                failed_template_id = job["template_id"]
                job["status"] = "failed"
                job["message"] = str(e)
                job["current_email"] = "-"
                job["completed_at"] = datetime.now().isoformat()
                _append_verification_log(job, f"Error: {str(e)}")

        if failed_campaign_id and failed_template_id:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO email_verification_logs
                    (campaign_id, template_id, emails_processed, emails_verified, emails_invalid, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    failed_campaign_id,
                    failed_template_id,
                    0,
                    0,
                    0,
                    "failed",
                    str(e)
                ))
                conn.commit()

@app.get("/docs/api", response_class=HTMLResponse)
async def get_api_docs(request: Request):
    return templates.TemplateResponse("docs.html", {"request": request})

@app.get("/export", response_class=HTMLResponse)
async def get_export_page(request: Request):
    return templates.TemplateResponse("export.html", {"request": request})

@app.get("/verify", response_class=HTMLResponse)
async def get_verify_page(request: Request):
    return templates.TemplateResponse("verify.html", {"request": request})

@app.get("/", response_class=HTMLResponse)
async def get_campaigns(request: Request, partial: bool = False):
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Single optimized query to get all campaign data at once
        cursor.execute("""
            SELECT 
                sc.id,
                sc.name,
                sc.status,
                COUNT(DISTINCT r.id) as total_requests,
                COUNT(DISTINCT c.id) as total_contacts,
                COUNT(DISTINCT CASE WHEN r.status = 'completed' THEN r.id END) as completed_requests,
                COUNT(DISTINCT CASE WHEN c.email IS NOT NULL AND c.email != '' THEN c.id END) as email_count,
                COUNT(DISTINCT CASE WHEN c.email IS NOT NULL AND c.email != '' AND c.email_status = 'Valid' THEN c.id END) as valid_email_count
            FROM search_campaigns sc
            LEFT JOIN requests r ON sc.id = r.campaign_id
            LEFT JOIN contacts c ON sc.id = c.campaign_id
            GROUP BY sc.id, sc.name, sc.status
            ORDER BY sc.id DESC
        """)
        
        campaigns = []
        for row in cursor.fetchall():
            campaign = dict(row)
            
            # Only load detailed data if specifically requested (not for main list view)
            if not partial:
                # Get requests for this campaign (simplified)
                cursor.execute("""
                    SELECT id, req_text, status
                    FROM requests 
                    WHERE campaign_id = %s 
                    ORDER BY id
                """, (campaign['id'],))
                campaign['requests'] = [dict(r) for r in cursor.fetchall()]

                # Get sample contacts (limit to first 100 for performance)
                cursor.execute("""
                    SELECT * FROM contacts 
                    WHERE campaign_id = %s 
                    ORDER BY id 
                    LIMIT 100
                """, (campaign['id'],))
                campaign['contacts'] = [dict(r) for r in cursor.fetchall()]
            else:
                # For partial loads (table refresh), skip detailed data
                campaign['requests'] = []
                campaign['contacts'] = []

            campaigns.append(campaign)

    template = "index.html" if not partial else "partials/table.html"
    return templates.TemplateResponse(template, {"request": request, "campaigns": campaigns})

@app.delete("/api/campaign/{campaign_id}")
async def delete_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM contacts WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM requests WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM search_campaigns WHERE id = %s", (campaign_id,))
        conn.commit()
    return {"status": "Campaign deleted successfully"}

@app.delete("/api/campaigns")
async def delete_all_campaigns():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS count FROM search_campaigns")
        campaigns_count = cursor.fetchone()["count"]
        cursor.execute("SELECT COUNT(*) AS count FROM requests")
        requests_count = cursor.fetchone()["count"]
        cursor.execute("SELECT COUNT(*) AS count FROM contacts")
        contacts_count = cursor.fetchone()["count"]
        cursor.execute("SELECT COUNT(*) AS count FROM export_templates")
        export_templates_count = cursor.fetchone()["count"]
        cursor.execute("SELECT COUNT(*) AS count FROM email_verification_templates")
        verification_templates_count = cursor.fetchone()["count"]

        # Delete campaign-related records in FK-safe order.
        cursor.execute("DELETE FROM contacts")
        cursor.execute("DELETE FROM requests")
        cursor.execute("DELETE FROM export_logs")
        cursor.execute("DELETE FROM email_verification_logs")
        cursor.execute("DELETE FROM search_campaigns")
        cursor.execute("DELETE FROM export_templates")
        cursor.execute("DELETE FROM email_verification_templates")
        conn.commit()

    with verification_jobs_lock:
        verification_jobs.clear()

    return {
        "status": "All campaigns and related data deleted successfully",
        "deleted": {
            "campaigns": campaigns_count,
            "requests": requests_count,
            "contacts": contacts_count,
            "export_templates": export_templates_count,
            "verification_templates": verification_templates_count
        }
    }

@app.get("/update_campaign_status/{campaign_id}/{status}")
async def update_campaign_status(campaign_id: int, status: str):
    if status not in ['active', 'inactive', 'completed']:
        raise HTTPException(status_code=400, detail="Invalid status. Must be 'active', 'inactive' or 'completed'")

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE search_campaigns SET status = %s WHERE id = %s",
            (status, campaign_id)
        )
        conn.commit()
    return {"status": "Campaign status updated"}

@app.get("/api/campaign/{campaign_id}/complete")
async def complete_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM search_campaigns WHERE id = %s", (campaign_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Campaign not found")

        cursor.execute(
            "UPDATE search_campaigns SET status = 'completed' WHERE id = %s",
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
            "INSERT INTO search_campaigns (name, status) VALUES (%s, %s) RETURNING id",
            (name, "active")
        )
        campaign_id = cursor.fetchone()['id']
        for phrase in phrases:
            cursor.execute(
                "INSERT INTO requests (campaign_id, req_text, status) VALUES (%s, %s, %s)",
                (campaign_id, phrase, "pending")
            )
        conn.commit()
    return {"status": "Campaign created", "campaign_id": campaign_id}

@app.get("/api/reserve_phrase/{campaign_id}")
async def reserve_phrase(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, req_text FROM requests WHERE campaign_id = %s AND status = 'pending' LIMIT 1",
            (campaign_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No pending phrases")
        phrase_id = row["id"]
        cursor.execute(
            "UPDATE requests SET status = 'reserved' WHERE id = %s",
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
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (campaign_id, business_name, review_count, phone, domain, email, "pending")
        )
        cursor.execute(
            "UPDATE requests SET status = 'completed' WHERE id = %s",
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

@app.get("/api/campaigns/all")
async def get_all_campaigns():
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
            GROUP BY sc.id
            ORDER BY sc.status = 'active' DESC, sc.name ASC
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
            WHERE sc.name = %s AND r.status = 'pending'
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
        cursor.execute("SELECT id FROM requests WHERE id = %s", (request_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Request not found")

        cursor.execute(
            "UPDATE requests SET status = %s WHERE id = %s",
            (status, request_id)
        )
        conn.commit()
        return {"status": "Request status updated successfully"}

@app.post("/api/contacts")
async def save_contacts(request: Request):
    data = await request.json()
    envelope_defaults = {}
    if isinstance(data, dict) and isinstance(data.get("contacts"), list):
        contacts = data.get("contacts", [])
        envelope_defaults = {
            "campaign_id": data.get("campaign_id", data.get("campaignId")),
            "request_id": data.get("request_id", data.get("requestId"))
        }
    elif isinstance(data, list):
        contacts = data
    elif isinstance(data, dict):
        contacts = [data]
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid payload. Expected a contact object, a list of contacts, or an object containing a contacts array."
        )

    if not contacts:
        raise HTTPException(status_code=400, detail="No contacts provided")

    def _first_present(*values: Any) -> Optional[Any]:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str):
                stripped = value.strip()
                if stripped == "":
                    continue
                return stripped
            return value
        return None

    def _to_int(value: Any, field: str, contact_index: int, default: Optional[int] = None, allow_none: bool = False) -> Optional[int]:
        candidate = _first_present(value)
        if candidate is None:
            if default is not None:
                return default
            if allow_none:
                return None
            raise HTTPException(status_code=400, detail=f"Contact {contact_index}: missing required field '{field}'")

        if isinstance(candidate, bool):
            raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

        if isinstance(candidate, int):
            return candidate

        if isinstance(candidate, float):
            if candidate.is_integer():
                return int(candidate)
            raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

        if isinstance(candidate, str):
            normalized = candidate.replace(",", "").strip().lower()
            if normalized in {"none", "null", "n/a", "na"}:
                if default is not None:
                    return default
                if allow_none:
                    return None
            try:
                return int(normalized)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

        raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

    def _to_float(value: Any, field: str, contact_index: int, allow_none: bool = True) -> Optional[float]:
        candidate = _first_present(value)
        if candidate is None:
            if allow_none:
                return None
            raise HTTPException(status_code=400, detail=f"Contact {contact_index}: missing required field '{field}'")

        if isinstance(candidate, bool):
            raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

        if isinstance(candidate, (int, float)):
            return float(candidate)

        if isinstance(candidate, str):
            normalized = candidate.replace(",", "").strip().lower()
            if normalized in {"none", "null", "n/a", "na"} and allow_none:
                return None
            try:
                return float(normalized)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

        raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid '{field}' value")

    saved_contacts = []
    with get_db() as conn:
        cursor = conn.cursor()

        for contact_index, contact in enumerate(contacts, start=1):
            if not isinstance(contact, dict):
                raise HTTPException(status_code=400, detail=f"Contact {contact_index}: each contact must be an object")

            campaign_id = _to_int(
                _first_present(contact.get("campaign_id"), contact.get("campaignId"), envelope_defaults.get("campaign_id")),
                "campaign_id",
                contact_index
            )
            request_id = _to_int(
                _first_present(contact.get("request_id"), contact.get("requestId"), envelope_defaults.get("request_id")),
                "request_id",
                contact_index
            )
            business_name = _first_present(
                contact.get("business_name"),
                contact.get("businessName"),
                contact.get("title"),
                contact.get("name")
            )
            if business_name is None:
                raise HTTPException(status_code=400, detail=f"Contact {contact_index}: missing required field 'business_name'")

            review_count = _to_int(
                _first_present(
                    contact.get("review_count"),
                    contact.get("reviewCount"),
                    contact.get("reviewsCount"),
                    contact.get("reviews")
                ),
                "review_count",
                contact_index,
                default=0
            )
            rating = _to_float(
                _first_present(contact.get("rating"), contact.get("averageRating")),
                "rating",
                contact_index,
                allow_none=True
            )
            time_zone_offset_min = _to_int(
                _first_present(contact.get("time_zone_offset_min"), contact.get("timeZoneOffsetMin")),
                "time_zone_offset_min",
                contact_index,
                allow_none=True
            )
            domain = _first_present(contact.get("domain"), contact.get("website"))
            if isinstance(domain, str) and domain.lower() in {"none", "null", "n/a", "na"}:
                domain = None
            place_id = _first_present(contact.get("place_id"), contact.get("placeId"))
            if place_id is None:
                url = _first_present(contact.get("url"))
                if url:
                    place_id = str(url).split('/place/')[-1].split('/')[0]
                else:
                    place_id = ""

            # Verify campaign exists and is active
            cursor.execute("SELECT status FROM search_campaigns WHERE id = %s", (campaign_id,))
            campaign = cursor.fetchone()
            if not campaign:
                raise HTTPException(status_code=404, detail=f"Campaign {campaign_id} not found")
            if campaign['status'] != 'active':
                raise HTTPException(status_code=400, detail=f"Campaign {campaign_id} is not active")

            # Verify request belongs to campaign
            cursor.execute("SELECT id FROM requests WHERE id = %s AND campaign_id = %s", (request_id, campaign_id))
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail=f"Invalid request ID {request_id} for campaign {campaign_id}")

            try:
                cursor.execute(
                    """INSERT INTO contacts 
                       (address, business_name, campaign_id, category, domain, email, facebook, instagram, phone, place_id, rating, request_id, review_count, twitter, yelp, status,
                        full_name, industry, city, www, firstname, lastname, company, country, company_social, company_size, personal_job_position, personal_prospect_location,
                        personal_user_social, screenshot, logo, state, icebreaker, time_zone_offset_min, notes, tags_import,
                        custom_1, custom_2, custom_3, custom_4, custom_5, custom_6, custom_7, custom_8, custom_9, custom_10,
                        custom_11, custom_12, custom_13, custom_14, custom_15, custom_16, custom_17, custom_18, custom_19, custom_20, email_status) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (
                        contact.get('address'),
                        business_name,
                        campaign_id,
                        contact.get('category'),
                        domain,
                        contact.get('email'),
                        contact.get('facebook'),
                        contact.get('instagram'),
                        contact.get('phone'),
                        place_id,
                        rating,
                        request_id,
                        review_count,
                        contact.get('twitter'),
                        contact.get('yelp'),
                        "pending",
                        # New fields
                        contact.get('full_name', contact.get('fullName')),
                        contact.get('industry'),
                        contact.get('city'),
                        contact.get('www'),
                        contact.get('firstname', contact.get('firstName')),
                        contact.get('lastname', contact.get('lastName')),
                        contact.get('company'),
                        contact.get('country'),
                        contact.get('company_social', contact.get('companySocial')),
                        contact.get('company_size', contact.get('companySize')),
                        contact.get('personal_job_position', contact.get('personalJobPosition')),
                        contact.get('personal_prospect_location', contact.get('personalProspectLocation')),
                        contact.get('personal_user_social', contact.get('personalUserSocial')),
                        contact.get('screenshot'),
                        contact.get('logo'),
                        contact.get('state'),
                        contact.get('icebreaker'),
                        time_zone_offset_min,
                        contact.get('notes'),
                        contact.get('tags_import', contact.get('tagsImport')),
                        contact.get('custom_1'),
                        contact.get('custom_2'),
                        contact.get('custom_3'),
                        contact.get('custom_4'),
                        contact.get('custom_5'),
                        contact.get('custom_6'),
                        contact.get('custom_7'),
                        contact.get('custom_8'),
                        contact.get('custom_9'),
                        contact.get('custom_10'),
                        contact.get('custom_11'),
                        contact.get('custom_12'),
                        contact.get('custom_13'),
                        contact.get('custom_14'),
                        contact.get('custom_15'),
                        contact.get('custom_16'),
                        contact.get('custom_17'),
                        contact.get('custom_18'),
                        contact.get('custom_19'),
                        contact.get('custom_20'),
                        contact.get('email_status', contact.get('emailStatus', 'unverified'))
                    )
                )
            except (DataError, IntegrityError) as e:
                conn.rollback()
                reason = str(e).splitlines()[0]
                raise HTTPException(status_code=400, detail=f"Contact {contact_index}: invalid payload ({reason})")
            contact_id = cursor.fetchone()['id']
            saved_contacts.append({
                "contact_id": contact_id,
                "campaign_id": campaign_id,
                "request_id": request_id
            })

        conn.commit()
    return {"status": "Contacts saved successfully", "saved_contacts": saved_contacts}

@app.post("/api/campaign/{campaign_id}/email_verify")
async def update_email_verification_status(campaign_id: int, request: Request):
    data = await request.json()

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
                email_status = contact.get('email_status')

                if not contact_id or not email_status:
                    failed_updates.append({"id": contact_id, "error": "Missing id or email_status"})
                    continue

                if email_status not in ['unverified', 'Valid', 'Invalid', 'Catch-all', 'Unknown']:
                    failed_updates.append({"id": contact_id, "error": "Invalid email_status"})
                    continue

                cursor.execute("""
                    UPDATE contacts 
                    SET email_status = %s 
                    WHERE id = %s AND campaign_id = %s
                """, (email_status, contact_id, campaign_id))

                if cursor.rowcount > 0:
                    updated_count += 1
                else:
                    failed_updates.append({"id": contact_id, "error": "Contact not found"})

            conn.commit()

        return {
            "status": "Batch verification update completed",
            "updated_count": updated_count,
            "failed_count": len(failed_updates),
            "failed_updates": failed_updates
        }
    else:
        # Single update
        contact_id = data.get('id')
        email_status = data.get('email_status')

        if not contact_id or not email_status:
            raise HTTPException(status_code=400, detail="Missing id or email_status in request body")

        if email_status not in ['unverified', 'Valid', 'Invalid', 'Catch-all', 'Unknown']:
            raise HTTPException(status_code=400, detail="Invalid email_status. Must be: unverified, Valid, Invalid, Catch-all, or Unknown")

        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE contacts 
                SET email_status = %s 
                WHERE id = %s AND campaign_id = %s
            """, (email_status, contact_id, campaign_id))
            conn.commit()

            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Contact not found")

            return {"status": "Email verification status updated successfully"}

@app.post("/api/campaign/{campaign_id}/email_update")
async def update_contact_email(campaign_id: int, request: Request):
    data = await request.json()
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
                    SET email = %s 
                    WHERE id = %s AND campaign_id = %s
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
                SET email = %s 
                WHERE id = %s AND campaign_id = %s
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
            WITH picked AS (
                SELECT id, domain
                FROM contacts
                WHERE campaign_id = %s
                AND (email IS NULL OR email = '')
                AND domain IS NOT NULL
                AND domain != ''
                ORDER BY
                    CASE WHEN nomail_pulled_at IS NULL THEN 0 ELSE 1 END,
                    nomail_pulled_at ASC NULLS FIRST,
                    id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE contacts
            SET nomail_pulled_at = NOW()
            WHERE id IN (SELECT id FROM picked)
            RETURNING id, domain
        """, (campaign_id, batch))
        contacts = cursor.fetchall()
        conn.commit()
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
async def remove_duplicate_contacts(campaign_id: int, request: Request = None):
    field = "domain"  # default field

    if request:
        try:
            data = await request.json()
            field = data.get('field', 'domain')
        except:
            pass

    # Validate field
    valid_fields = ["domain", "business_name", "email", "phone"]
    if field not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Invalid field. Must be one of: {valid_fields}")

    with get_db() as conn:
        cursor = conn.cursor()

        # Remove duplicates by specified field
        cursor.execute(f"""
            DELETE FROM contacts 
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM contacts
                WHERE campaign_id = %s 
                AND {field} IS NOT NULL
                AND {field} != ''
                GROUP BY {field}
            )
            AND campaign_id = %s
            AND {field} IS NOT NULL
            AND {field} != ''
        """, (campaign_id, campaign_id))
        duplicate_count = cursor.rowcount

        conn.commit()
        return {
            "status": "success", 
            "removed_duplicates": duplicate_count,
            "field_used": field
        }

@app.post("/api/campaign/{campaign_id}/remove_empty_domains")
async def remove_empty_domains(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM contacts 
            WHERE campaign_id = %s
            AND (domain IS NULL OR domain = '')
        """, (campaign_id,))
        deleted_count = cursor.rowcount
        conn.commit()
        return {"status": "success", "removed_contacts": deleted_count}

@app.post("/api/campaign/{campaign_id}/remove_filtered")
async def remove_filtered_contacts(campaign_id: int, request: Request):
    data = await request.json()
    keywords = data.get('keywords', [])
    if not keywords:
        return {"status": "error", "message": "No keywords provided"}

    with get_db() as conn:
        cursor = conn.cursor()
        like_conditions = []
        params = []

        for keyword in keywords:
            like_conditions.extend([
                "business_name ILIKE %s",
                "domain ILIKE %s",
                "email ILIKE %s",
            ])
            params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])

        params.append(campaign_id)

        query = f"""
            DELETE FROM contacts 
            WHERE ({' OR '.join(like_conditions)})
            AND campaign_id = %s
        """

        cursor.execute(query, tuple(params))
        deleted_count = cursor.rowcount
        conn.commit()
        return {"status": "success", "removed_contacts": deleted_count}

@app.post("/api/campaign/{campaign_id}/duplicate")
async def duplicate_campaign(campaign_id: int, request: Request):
    try:
        data = await request.json()
        custom_name = data.get('name', '').strip()
        contact_filters = data.get('contactFilters', {})
    except:
        custom_name = ''
        contact_filters = {}

    with get_db() as conn:
        cursor = conn.cursor()

        # Get original campaign
        cursor.execute("SELECT * FROM search_campaigns WHERE id = %s", (campaign_id,))
        original_campaign = cursor.fetchone()
        if not original_campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        # Determine new campaign name
        if custom_name:
            # Check if custom name already exists
            cursor.execute("SELECT id FROM search_campaigns WHERE name = %s", (custom_name,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Campaign name already exists")
            new_name = custom_name
        else:
            # Find next available number for campaign name
            base_name = original_campaign["name"]
            counter = 1
            new_name = f"{base_name} {counter}"

            while True:
                cursor.execute("SELECT id FROM search_campaigns WHERE name = %s", (new_name,))
                if not cursor.fetchone():
                    break
                counter += 1
                new_name = f"{base_name} {counter}"

        # Create new campaign
        cursor.execute(
            "INSERT INTO search_campaigns (name, status) VALUES (%s, %s) RETURNING id",
            (new_name, "active")
        )
        new_campaign_id = cursor.fetchone()['id']

        # Copy all requests from original campaign
        cursor.execute("SELECT req_text FROM requests WHERE campaign_id = %s", (campaign_id,))
        requests = cursor.fetchall()

        for request in requests:
            cursor.execute(
                "INSERT INTO requests (campaign_id, req_text, status) VALUES (%s, %s, %s)",
                (new_campaign_id, request["req_text"], "pending")
            )

        # Copy contacts based on filters
        copied_contacts = 0
        if contact_filters:
            # Check if "All contacts" is selected - if so, copy all contacts
            if contact_filters.get('keepAllContacts', False):
                cursor.execute("""
                    SELECT address, business_name, category, domain, email, facebook, 
                           instagram, phone, place_id, rating, review_count, twitter, yelp, status
                    FROM contacts 
                    WHERE campaign_id = %s
                """, (campaign_id,))

                contacts_to_copy = cursor.fetchall()

                for contact in contacts_to_copy:
                    cursor.execute("""
                        INSERT INTO contacts 
                        (address, business_name, campaign_id, category, domain, email, facebook, 
                         instagram, phone, place_id, rating, request_id, review_count, twitter, yelp, status) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        contact["address"],
                        contact["business_name"],
                        new_campaign_id,
                        contact["category"],
                        contact["domain"],
                        contact["email"],
                        contact["facebook"],
                        contact["instagram"],
                        contact["phone"],
                        contact["place_id"],
                        contact["rating"],
                        None,  # request_id will be None for duplicated contacts
                        contact["review_count"],
                        contact["twitter"],
                        contact["yelp"],
                        contact["status"]
                    ))
                    copied_contacts += 1
            else:
                conditions = []

                # Build the WHERE clause based on filters
                domain_conditions = []
                email_conditions = []
                phone_conditions = []

                if contact_filters.get('keepContactsWithDomain', False):
                    domain_conditions.append("(domain IS NOT NULL AND domain != '')")
                if contact_filters.get('keepContactsWithoutDomain', False):
                    domain_conditions.append("(domain IS NULL OR domain = '')")

                if contact_filters.get('keepContactsWithEmail', False):
                    email_conditions.append("(email IS NOT NULL AND email != '')")
                if contact_filters.get('keepContactsWithValidEmail', False):
                    email_conditions.append("(email IS NOT NULL AND email != '' AND email_status = 'Valid')")
                if contact_filters.get('keepContactsWithoutEmail', False):
                    email_conditions.append("(email IS NULL OR email = '')")

                if contact_filters.get('keepContactsWithPhone', False):
                    phone_conditions.append("(phone IS NOT NULL AND phone != '')")

                # Review count filters
                review_conditions = []
                if contact_filters.get('keepContactsWithLessReviews', False):
                    less_count = contact_filters.get('lessReviewsCount', 0)
                    review_conditions.append(f"(review_count IS NOT NULL AND review_count < {less_count})")
                
                if contact_filters.get('keepContactsWithMoreReviews', False):
                    more_count = contact_filters.get('moreReviewsCount', 0)
                    review_conditions.append(f"(review_count IS NOT NULL AND review_count > {more_count})")

                # Combine conditions
                if domain_conditions:
                    conditions.append(f"({' OR '.join(domain_conditions)})")
                if email_conditions:
                    conditions.append(f"({' OR '.join(email_conditions)})")
                if phone_conditions:
                    conditions.append(f"({' OR '.join(phone_conditions)})")
                if review_conditions:
                    conditions.append(f"({' OR '.join(review_conditions)})")

                if conditions:
                    where_clause = f"campaign_id = %s AND ({' AND '.join(conditions)})"
                    cursor.execute(f"""
                        SELECT address, business_name, category, domain, email, facebook, 
                               instagram, phone, place_id, rating, review_count, twitter, yelp, status
                        FROM contacts 
                        WHERE {where_clause}
                    """, (campaign_id,))

                    contacts_to_copy = cursor.fetchall()

                    for contact in contacts_to_copy:
                        cursor.execute("""
                            INSERT INTO contacts 
                            (address, business_name, campaign_id, category, domain, email, facebook, 
                             instagram, phone, place_id, rating, request_id, review_count, twitter, yelp, status) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            contact["address"],
                            contact["business_name"],
                            new_campaign_id,
                            contact["category"],
                            contact["domain"],
                            contact["email"],
                            contact["facebook"],
                            contact["instagram"],
                            contact["phone"],
                            contact["place_id"],
                            contact["rating"],
                            None,  # request_id will be None for duplicated contacts
                            contact["review_count"],
                            contact["twitter"],
                            contact["yelp"],
                            contact["status"]
                        ))
                        copied_contacts += 1

        conn.commit()
        return {
            "status": "success", 
            "new_campaign_id": new_campaign_id,
            "new_campaign_name": new_name,
            "copied_requests": len(requests),
            "copied_contacts": copied_contacts
        }

@app.post("/api/campaign/{campaign_id}/exclude")
async def exclude_contacts_from_campaigns(campaign_id: int, request: Request):
    try:
        data = await request.json()
        exclude_all = data.get('excludeAll', False)
        exclude_campaigns = data.get('excludeCampaigns', [])
    except:
        raise HTTPException(status_code=400, detail="Invalid request body")

    if not exclude_all and not exclude_campaigns:
        raise HTTPException(status_code=400, detail="No campaigns specified for exclusion")

    with get_db() as conn:
        cursor = conn.cursor()

        excluded_count = 0

        if exclude_all:
            # Remove contacts from current campaign that exist in ANY other campaign
            cursor.execute("""
                DELETE FROM contacts 
                WHERE campaign_id = %s 
                AND id IN (
                    SELECT c1.id 
                    FROM contacts c1 
                    WHERE c1.campaign_id = %s 
                    AND EXISTS (
                        SELECT 1 FROM contacts c2 
                        WHERE c2.campaign_id != %s 
                        AND (
                            (c1.domain IS NOT NULL AND c1.domain != '' AND c1.domain = c2.domain) OR
                            (c1.email IS NOT NULL AND c1.email != '' AND c1.email = c2.email) OR
                            (c1.business_name IS NOT NULL AND c1.business_name != '' AND c1.business_name = c2.business_name) OR
                            (c1.phone IS NOT NULL AND c1.phone != '' AND c1.phone = c2.phone)
                        )
                    )
                )
            """, (campaign_id, campaign_id, campaign_id))
            excluded_count = cursor.rowcount
        else:
            # Remove contacts from current campaign that exist in specific campaigns
            campaign_placeholders = ','.join(['%s' for _ in exclude_campaigns])
            params = [campaign_id, campaign_id] + exclude_campaigns + [campaign_id]

            cursor.execute(f"""
                DELETE FROM contacts 
                WHERE campaign_id = %s 
                AND id IN (
                    SELECT c1.id 
                    FROM contacts c1 
                    WHERE c1.campaign_id = %s 
                    AND EXISTS (
                        SELECT 1 FROM contacts c2 
                        WHERE c2.campaign_id IN ({campaign_placeholders}) 
                        AND c2.campaign_id != %s
                        AND (
                            (c1.domain IS NOT NULL AND c1.domain != '' AND c1.domain = c2.domain) OR
                            (c1.email IS NOT NULL AND c1.email != '' AND c1.email = c2.email) OR
                            (c1.business_name IS NOT NULL AND c1.business_name != '' AND c1.business_name = c2.business_name) OR
                            (c1.phone IS NOT NULL AND c1.phone != '' AND c1.phone = c2.phone)
                        )
                    )
                )
            """, params)
            excluded_count = cursor.rowcount

        conn.commit()
        return {
            "status": "success", 
            "excluded_contacts": excluded_count,
            "exclude_type": "all other campaigns" if exclude_all else f"{len(exclude_campaigns)} selected campaigns"
        }

@app.get("/api/campaign/{campaign_id}")
async def get_campaign_status(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM search_campaigns WHERE id = %s", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        cursor.execute("SELECT * FROM requests WHERE campaign_id = %s", (campaign_id,))
        requests = [dict(row) for row in cursor.fetchall()]
        cursor.execute("SELECT * FROM contacts WHERE campaign_id = %s", (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]
        return {"campaign": dict(campaign), "requests": requests, "contacts": contacts}

# Export Template Management
@app.get("/api/templates")
async def get_templates():
    templates = TemplateManager.get_all_templates()
    return {"templates": templates}

@app.get("/api/templates/{template_id}")
async def get_template(template_id: int):
    template = TemplateManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template

@app.post("/api/templates")
async def create_template(request: Request):
    data = await request.json()

    required_fields = ['name', 'service', 'field_mappings', 'api_config']
    for field in required_fields:
        if field not in data:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    template_id = TemplateManager.create_template(
        data['name'],
        data['service'],
        data['field_mappings'],
        data['api_config']
    )

    return {"status": "Template created", "template_id": template_id}

@app.put("/api/templates/{template_id}")
async def update_template(template_id: int, request: Request):
    data = await request.json()

    # Check if template exists
    template = TemplateManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # Use existing service if not provided in update
    service = data.get('service', template['service'])

    TemplateManager.update_template(
        template_id,
        data['name'],
        data['field_mappings'],
        data['api_config'],
        service
    )

    return {"status": "Template updated"}

@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: int):
    if not TemplateManager.get_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")

    TemplateManager.delete_template(template_id)
    return {"status": "Template deleted"}

@app.post("/api/export/provider-campaigns")
async def get_provider_campaigns(request: Request):
    data = await request.json()
    service = str(data.get("service", "")).strip().lower()
    api_key = str(data.get("api_key", "")).strip()
    active_only_raw = data.get("active_only", True)
    if isinstance(active_only_raw, bool):
        active_only = active_only_raw
    else:
        active_only = str(active_only_raw).strip().lower() in ["1", "true", "yes", "y"]

    if service not in ["manyreach", "smartlead"]:
        raise HTTPException(status_code=400, detail="Unsupported service")
    if not api_key:
        raise HTTPException(status_code=400, detail="API key is required")

    try:
        if service == "manyreach":
            integration = ManyReachIntegration(api_key)
            campaigns = integration.get_campaigns()
        else:
            integration = SmartLeadIntegration(api_key)
            campaigns = integration.get_campaigns(active_only=active_only)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {service} campaigns: {str(e)}")

    return {
        "service": service,
        "campaigns": campaigns
    }

# Export functionality
@app.get("/api/campaign/{campaign_id}/export/preview")
async def preview_export(
    campaign_id: int,
    template_id: int,
    valid_only: bool = False,
    include_catch_all: bool = False,
    exclude_public_emails: bool = False
):
    """Preview what the export will look like"""
    template = TemplateManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    with get_db() as conn:
        cursor = conn.cursor()
        conditions = [
            "campaign_id = %s",
            "email IS NOT NULL",
            "email != ''"
        ]
        params = [campaign_id]

        if valid_only:
            if include_catch_all:
                conditions.append("email_status IN ('Valid', 'Catch-all')")
            else:
                conditions.append("email_status = 'Valid'")

        if exclude_public_emails:
            placeholders = ','.join(['%s' for _ in PUBLIC_EMAIL_DOMAINS])
            conditions.append(f"lower(split_part(email, '@', 2)) NOT IN ({placeholders})")
            params.extend(PUBLIC_EMAIL_DOMAINS)

        query = f"""
            SELECT * FROM contacts
            WHERE {' AND '.join(conditions)}
            LIMIT 5
        """
        cursor.execute(query, tuple(params))
        contacts = [dict(row) for row in cursor.fetchall()]

    if template['service'] == 'manyreach':
        integration = ManyReachIntegration("")
        manyreach_campaign_id = template['api_config'].get('manyreach_campaign_id', '')
        preview_data = []
        for contact in contacts:
            transformed = integration.transform_contact(contact, template['field_mappings'], manyreach_campaign_id)
            preview_data.append(transformed)

        return {
            "template_name": template['name'],
            "service": template['service'],
            "total_contacts": len(contacts),
            "preview_data": preview_data,
            "field_mappings": template['field_mappings']
        }

    if template['service'] == 'smartlead':
        integration = SmartLeadIntegration("")
        preview_data = []
        for contact in contacts:
            transformed = integration.transform_contact(contact, template['field_mappings'])
            preview_data.append(transformed)

        return {
            "template_name": template['name'],
            "service": template['service'],
            "total_contacts": len(contacts),
            "preview_data": preview_data,
            "field_mappings": template['field_mappings']
        }

    return {"error": "Service not supported"}

@app.post("/api/campaign/{campaign_id}/export")
async def export_campaign(campaign_id: int, request: Request):
    data = await request.json()
    template_id = data.get('template_id')
    batch_size = data.get('batch_size', 10)

    if not template_id:
        raise HTTPException(status_code=400, detail="Template ID required")

    template = TemplateManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # Use field_mappings from request if provided, otherwise use template's field_mappings
    field_mappings = data.get('field_mappings', template['field_mappings'])

    # Rate limiting check
    now = datetime.now()
    rate_limit_window = now - timedelta(minutes=1)

    with get_db() as conn:
        cursor = conn.cursor()

        # Check recent exports for rate limiting
        cursor.execute("""
            SELECT COUNT(*) as recent_exports
            FROM export_logs 
            WHERE created_at > %s AND template_id = %s
        """, (rate_limit_window.isoformat(), template_id))

        recent_exports = cursor.fetchone()['recent_exports']

        if template['service'] == 'manyreach':
            integration = ManyReachIntegration(template['api_config'].get('api_key', ''))
            if recent_exports >= integration.rate_limit:
                raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Max {integration.rate_limit} exports per minute.")
        elif template['service'] == 'smartlead':
            integration = SmartLeadIntegration(template['api_config'].get('api_key', ''))
            if recent_exports >= integration.rate_limit:
                raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Max {integration.rate_limit} exports per minute.")

        # Get batch offset and export filters from request data
        offset = data.get('offset', 0)
        export_valid_only = data.get('export_valid_only', False)
        export_catch_all = data.get('export_catch_all', False)
        exclude_public_emails = data.get('exclude_public_emails', False)

        conditions = [
            "campaign_id = %s",
            "email IS NOT NULL",
            "email != ''"
        ]
        params = [campaign_id]

        if export_valid_only:
            if export_catch_all:
                conditions.append("email_status IN ('Valid', 'Catch-all')")
            else:
                conditions.append("email_status = 'Valid'")

        if exclude_public_emails:
            placeholders = ','.join(['%s' for _ in PUBLIC_EMAIL_DOMAINS])
            conditions.append(f"lower(split_part(email, '@', 2)) NOT IN ({placeholders})")
            params.extend(PUBLIC_EMAIL_DOMAINS)

        query = f"""
            SELECT * FROM contacts
            WHERE {' AND '.join(conditions)}
            ORDER BY id
            LIMIT %s OFFSET %s
        """
        params.extend([batch_size, offset])

        cursor.execute(query, tuple(params))
        contacts = [dict(row) for row in cursor.fetchall()]

        if not contacts:
            raise HTTPException(status_code=404, detail="No contacts with email found")

        # Get newListName from request data
        new_list_name = data.get('newListName', '')

        # Transform contacts using field_mappings from request (or template default)
        if template['service'] == 'manyreach':
            integration = ManyReachIntegration(template['api_config'].get('api_key', ''))
            manyreach_campaign_id = template['api_config'].get('manyreach_campaign_id', '')
            transformed_contacts = []

            for contact in contacts:
                if integration.validate_contact(contact):
                    transformed = integration.transform_contact(contact, field_mappings, manyreach_campaign_id, new_list_name)
                    transformed_contacts.append(transformed)

            # Format for bulk API - wrap contacts in array for bulk endpoint
            bulk_data = {
                "apikey": template['api_config'].get('api_key', ''),
                "campaignid": manyreach_campaign_id,
                "prospects": transformed_contacts
            }

            # Add newListName if provided
            if new_list_name:
                bulk_data["newListName"] = new_list_name

            # Make real API call to ManyReach
            try:
                api_response = integration.export_to_manyreach_bulk(bulk_data)

                # Log the successful export
                cursor.execute("""
                    INSERT INTO export_logs (campaign_id, template_id, contacts_exported, status)
                    VALUES (%s, %s, %s, %s)
                """, (campaign_id, template_id, len(transformed_contacts), "completed"))
                conn.commit()

                return {
                    "status": "Export completed successfully",
                    "service": "manyreach",
                    "contacts_exported": len(transformed_contacts),
                    "api_response": api_response,
                    "endpoint": "https://app.manyreach.com/api/campaigns/prospects/add/bulk"
                }
            except Exception as e:
                # Log the failed export
                cursor.execute("""
                    INSERT INTO export_logs (campaign_id, template_id, contacts_exported, status)
                    VALUES (%s, %s, %s, %s)
                """, (campaign_id, template_id, 0, f"failed: {str(e)}"))
                conn.commit()

                raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

        if template['service'] == 'smartlead':
            integration = SmartLeadIntegration(template['api_config'].get('api_key', ''))
            smartlead_campaign_id = str(template['api_config'].get('smartlead_campaign_id', '')).strip()

            if not smartlead_campaign_id:
                raise HTTPException(status_code=400, detail="Smartlead campaign ID is missing in template configuration")

            if batch_size > integration.max_leads_per_request:
                raise HTTPException(
                    status_code=400,
                    detail=f"Smartlead supports max {integration.max_leads_per_request} leads per request"
                )

            transformed_contacts = []
            for contact in contacts:
                transformed = integration.transform_contact(contact, field_mappings)
                if integration.validate_contact(transformed):
                    transformed_contacts.append(transformed)

            if not transformed_contacts:
                raise HTTPException(status_code=400, detail="No valid contacts to export (email is required)")

            settings = template['api_config'].get('settings', {})

            try:
                # Requirement: only export to active/running Smartlead campaigns.
                integration.ensure_active_campaign(smartlead_campaign_id)
                api_response = integration.export_to_smartlead_bulk(
                    smartlead_campaign_id,
                    transformed_contacts,
                    settings=settings
                )

                cursor.execute("""
                    INSERT INTO export_logs (campaign_id, template_id, contacts_exported, status)
                    VALUES (%s, %s, %s, %s)
                """, (campaign_id, template_id, len(transformed_contacts), "completed"))
                conn.commit()

                return {
                    "status": "Export completed successfully",
                    "service": "smartlead",
                    "contacts_exported": len(transformed_contacts),
                    "api_response": api_response,
                    "endpoint": "https://server.smartlead.ai/api/v1/campaigns/{id}/leads"
                }
            except Exception as e:
                cursor.execute("""
                    INSERT INTO export_logs (campaign_id, template_id, contacts_exported, status)
                    VALUES (%s, %s, %s, %s)
                """, (campaign_id, template_id, 0, f"failed: {str(e)}"))
                conn.commit()
                raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

    return {"error": "Service not supported"}

@app.get("/api/campaign/{campaign_id}/export/history")
async def get_export_history(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT el.*, et.name as template_name, et.service
            FROM export_logs el
            JOIN export_templates et ON el.template_id = et.id
            WHERE el.campaign_id = %s
            ORDER BY el.created_at DESC
        """, (campaign_id,))
        return {"history": [dict(row) for row in cursor.fetchall()]}

@app.get("/api/campaign/{campaign_id}/export/test-leads")
async def get_export_test_leads(campaign_id: int, q: str = "", limit: int = 100):
    """Return campaign leads to pick from when testing export mapping."""
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be at least 1")
    limit = min(limit, 200)

    with get_db() as conn:
        cursor = conn.cursor()
        conditions = ["campaign_id = %s"]
        params = [campaign_id]

        if q and q.strip():
            like_value = f"%{q.strip()}%"
            conditions.append("(business_name ILIKE %s OR email ILIKE %s OR domain ILIKE %s OR phone ILIKE %s)")
            params.extend([like_value, like_value, like_value, like_value])

        query = f"""
            SELECT id, business_name, email, domain, phone, email_status
            FROM contacts
            WHERE {' AND '.join(conditions)}
            ORDER BY id DESC
            LIMIT %s
        """
        params.append(limit)
        cursor.execute(query, tuple(params))
        leads = [dict(row) for row in cursor.fetchall()]

    return {"leads": leads}

@app.post("/api/campaign/{campaign_id}/export/test")
async def test_export_lead(campaign_id: int, request: Request):
    """Send one selected lead to provider API using current mapping/template."""
    data = await request.json()
    template_id = data.get("template_id")
    contact_id = data.get("contact_id")
    new_list_name = data.get("newListName", "")

    if not template_id:
        raise HTTPException(status_code=400, detail="Template ID required")
    if not contact_id:
        raise HTTPException(status_code=400, detail="contact_id required")

    template = TemplateManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # Allow UI to test with currently edited mapping without persisting template changes.
    field_mappings = data.get("field_mappings", template["field_mappings"])

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM contacts
            WHERE id = %s AND campaign_id = %s
        """, (contact_id, campaign_id))
        contact = cursor.fetchone()

        if not contact:
            raise HTTPException(status_code=404, detail="Lead not found in selected campaign")

        contact = dict(contact)

    if template["service"] == "manyreach":
        integration = ManyReachIntegration(template["api_config"].get("api_key", ""))
        manyreach_campaign_id = str(template["api_config"].get("manyreach_campaign_id", "")).strip()
        if not manyreach_campaign_id:
            raise HTTPException(status_code=400, detail="ManyReach campaign ID is missing in template configuration")

        if not integration.validate_contact(contact):
            raise HTTPException(status_code=400, detail="Selected lead is missing email")

        transformed = integration.transform_contact(contact, field_mappings, manyreach_campaign_id, new_list_name)
        bulk_data = {
            "apikey": template["api_config"].get("api_key", ""),
            "campaignid": manyreach_campaign_id,
            "prospects": [transformed]
        }
        if new_list_name:
            bulk_data["newListName"] = new_list_name

        try:
            api_response = integration.export_to_manyreach_bulk(bulk_data)
            return {
                "status": "Test export completed successfully",
                "service": "manyreach",
                "contact_id": contact_id,
                "transformed_contact": transformed,
                "api_response": api_response
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Test export failed: {str(e)}")

    if template["service"] == "smartlead":
        integration = SmartLeadIntegration(template["api_config"].get("api_key", ""))
        smartlead_campaign_id = str(template["api_config"].get("smartlead_campaign_id", "")).strip()

        if not smartlead_campaign_id:
            raise HTTPException(status_code=400, detail="Smartlead campaign ID is missing in template configuration")

        transformed = integration.transform_contact(contact, field_mappings)
        if not integration.validate_contact(transformed):
            raise HTTPException(status_code=400, detail="Selected lead is missing email")

        settings = template["api_config"].get("settings", {})

        try:
            integration.ensure_active_campaign(smartlead_campaign_id)
            api_response = integration.export_to_smartlead_bulk(
                smartlead_campaign_id,
                [transformed],
                settings=settings
            )
            return {
                "status": "Test export completed successfully",
                "service": "smartlead",
                "contact_id": contact_id,
                "transformed_contact": transformed,
                "api_response": api_response
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Test export failed: {str(e)}")

    raise HTTPException(status_code=400, detail=f"Unsupported service: {template['service']}")

# Email Verification Template Management
@app.get("/api/email-verification/templates")
async def get_verification_templates():
    templates = EmailVerificationManager.get_all_templates()
    return {"templates": templates}

@app.get("/api/email-verification/templates/{template_id}")
async def get_verification_template(template_id: int):
    template = EmailVerificationManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template

@app.post("/api/email-verification/templates")
async def create_verification_template(request: Request):
    data = await request.json()

    required_fields = ['name', 'service', 'api_config', 'status_mapping']
    for field in required_fields:
        if field not in data:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    template_id = EmailVerificationManager.create_template(
        data['name'],
        data['service'],
        data['api_config'],
        data['status_mapping']
    )

    return {"status": "Template created", "template_id": template_id}

@app.delete("/api/email-verification/templates/{template_id}")
async def delete_verification_template(template_id: int):
    if not EmailVerificationManager.get_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")

    EmailVerificationManager.delete_template(template_id)
    return {"status": "Template deleted"}

@app.post("/api/campaign/{campaign_id}/verify-emails/start")
async def start_verification_job(campaign_id: int, request: Request):
    data = await request.json()
    template_id = data.get('template_id')
    delay = float(data.get('delay', 2))
    skip_public_providers = bool(data.get('skip_public_providers', False))

    if not template_id:
        raise HTTPException(status_code=400, detail="Template ID required")

    if delay < 0:
        raise HTTPException(status_code=400, detail="Delay must be zero or positive")

    template = EmailVerificationManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    with verification_jobs_lock:
        for existing_job in verification_jobs.values():
            if existing_job["campaign_id"] == campaign_id and existing_job["status"] in ["queued", "running"]:
                response = _serialize_verification_job(existing_job)
                response["already_running"] = True
                return response

        job_id = str(uuid4())
        verification_jobs[job_id] = {
            "job_id": job_id,
            "campaign_id": campaign_id,
            "template_id": template_id,
            "delay": delay,
            "skip_public_providers": skip_public_providers,
            "cancel_requested": False,
            "status": "queued",
            "total_emails": 0,
            "processed_emails": 0,
            "verified_emails": 0,
            "invalid_emails": 0,
            "failed_emails": 0,
            "current_email": "-",
            "message": "Queued",
            "started_at": None,
            "completed_at": None,
            "logs": [],
        }

    thread = threading.Thread(target=_run_verification_job, args=(job_id,), daemon=True)
    thread.start()

    response = _serialize_verification_job(verification_jobs[job_id])
    response["already_running"] = False
    return response

@app.get("/api/verification-jobs/{job_id}")
async def get_verification_job_status(job_id: str):
    with verification_jobs_lock:
        job = verification_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Verification job not found")
        return _serialize_verification_job(job)

@app.get("/api/campaign/{campaign_id}/verification-job/active")
async def get_active_verification_job(campaign_id: int):
    with verification_jobs_lock:
        active_jobs = [
            job for job in verification_jobs.values()
            if job["campaign_id"] == campaign_id and job["status"] in ["queued", "running"]
        ]
        if not active_jobs:
            return {"job": None}

        active_job = sorted(
            active_jobs,
            key=lambda j: j["started_at"] or "",
            reverse=True
        )[0]
        return {"job": _serialize_verification_job(active_job)}

@app.post("/api/verification-jobs/{job_id}/stop")
async def stop_verification_job(job_id: str):
    with verification_jobs_lock:
        job = verification_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Verification job not found")

        if job["status"] in ["completed", "failed", "cancelled"]:
            return _serialize_verification_job(job)

        job["cancel_requested"] = True
        if job["status"] == "queued":
            _mark_job_cancelled(job)
        else:
            job["message"] = "Stopping verification..."
            _append_verification_log(job, "Stop requested by user")

        return _serialize_verification_job(job)

# Single Email Verification for real-time progress
@app.post("/api/campaign/{campaign_id}/verify-single-email")
async def verify_single_email(campaign_id: int, request: Request):
    data = await request.json()
    template_id = data.get('template_id')
    contact_id = data.get('contact_id')
    email = data.get('email')

    if not template_id or not contact_id or not email:
        raise HTTPException(status_code=400, detail="Template ID, contact ID, and email required")

    template = EmailVerificationManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # Initialize verification service
    verification_service = EmailVerificationService()

    try:
        # Verify single email
        results = verification_service.verify_batch([email], template, 0)
        result = results[0] if results else None

        if not result:
            raise HTTPException(status_code=500, detail="No result from verification service")

        with get_db() as conn:
            cursor = conn.cursor()

            if result['success']:
                email_status = result['mapped_status']

                # Update database with result
                cursor.execute("""
                    UPDATE contacts 
                    SET email_status = %s 
                    WHERE id = %s AND campaign_id = %s
                """, (email_status, contact_id, campaign_id))

                conn.commit()

                return {
                    "status": email_status,
                    "email": email,
                    "success": True,
                    "details": result.get('diagnosis', ''),
                    "raw_response": result.get('raw_response', {})
                }
            else:
                return {
                    "status": "unknown",
                    "email": email,
                    "success": False,
                    "error": result.get('error', 'Unknown error'),
                    "details": "Verification failed"
                }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

# Email Verification
@app.post("/api/campaign/{campaign_id}/verify-emails")
async def verify_campaign_emails(campaign_id: int, request: Request):
    data = await request.json()
    template_id = data.get('template_id')
    batch_size = data.get('batch_size', 25)
    delay = data.get('delay', 1.0)
    verify_all = data.get('verify_all', False)

    if not template_id:
        raise HTTPException(status_code=400, detail="Template ID required")

    template = EmailVerificationManager.get_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # Get unverified emails from campaign
    with get_db() as conn:
        cursor = conn.cursor()

        if verify_all:
            cursor.execute("""
                SELECT id, email FROM contacts 
                WHERE campaign_id = %s 
                AND email IS NOT NULL 
                AND email != ''
                AND (email_status IS NULL OR email_status = 'unverified')
            """, (campaign_id,))
        else:
            cursor.execute("""
                SELECT id, email FROM contacts 
                WHERE campaign_id = %s 
                AND email IS NOT NULL 
                AND email != ''
                AND (email_status IS NULL OR email_status = 'unverified')
                LIMIT %s
            """, (campaign_id, batch_size))

        contacts = cursor.fetchall()

        if not contacts:
            raise HTTPException(status_code=404, detail="No unverified emails found")

        # Extract emails for verification
        emails = [contact['email'] for contact in contacts]
        contact_map = {contact['email']: contact['id'] for contact in contacts}

        # Initialize verification service
        verification_service = EmailVerificationService()

        # Verify emails
        try:
            results = verification_service.verify_batch(emails, template, delay)

            # Update database with results
            verified_count = 0
            invalid_count = 0

            for result in results:
                if result['success']:
                    email_status = result['mapped_status']
                    contact_id = contact_map[result['email']]

                    cursor.execute("""
                        UPDATE contacts 
                        SET email_status = %s 
                        WHERE id = %s AND campaign_id = %s
                    """, (email_status, contact_id, campaign_id))

                    if email_status == 'verified':
                        verified_count += 1
                    elif email_status == 'invalid':
                        invalid_count += 1

            # Log the verification
            cursor.execute("""
                INSERT INTO email_verification_logs 
                (campaign_id, template_id, emails_processed, emails_verified, emails_invalid, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (campaign_id, template_id, len(emails), verified_count, invalid_count, "completed"))

            conn.commit()

            return {
                "status": "Verification completed",
                "emails_processed": len(emails),
                "emails_verified": verified_count,
                "emails_invalid": invalid_count,
                "template_name": template['name']
            }

        except Exception as e:
            # Log the failed verification
            cursor.execute("""
                INSERT INTO email_verification_logs 
                (campaign_id, template_id, emails_processed, emails_verified, emails_invalid, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (campaign_id, template_id, 0, 0, 0, "failed", str(e)))
            conn.commit()

            raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@app.get("/api/campaign/{campaign_id}/verification-history")
async def get_verification_history(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vl.*, vt.name as template_name, vt.service
            FROM email_verification_logs vl
            JOIN email_verification_templates vt ON vl.template_id = vt.id
            WHERE vl.campaign_id = %s
            ORDER BY vl.created_at DESC
        """, (campaign_id,))
        return {"history": [dict(row) for row in cursor.fetchall()]}

@app.delete("/api/campaign/{campaign_id}/contact/{contact_id}")
async def remove_contact(campaign_id: int, contact_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM contacts 
            WHERE id = %s AND campaign_id = %s
        """, (contact_id, campaign_id))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Contact not found")
        
        conn.commit()
        return {"status": "Contact removed successfully"}

@app.delete("/api/campaign/{campaign_id}/contact/{contact_id}/email")
async def remove_contact_email(campaign_id: int, contact_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE contacts
            SET email = NULL,
                email_status = 'unverified'
            WHERE id = %s AND campaign_id = %s
        """, (contact_id, campaign_id))

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Contact not found")

        conn.commit()
        return {"status": "Email removed successfully"}

@app.get("/api/campaign/{campaign_id}/email-statuses")
async def get_email_statuses(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT email_status, COUNT(*) as count
            FROM contacts 
            WHERE campaign_id = %s AND email IS NOT NULL AND email != ''
            GROUP BY email_status
        """, (campaign_id,))
        statuses = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT id, email, email_status
            FROM contacts 
            WHERE campaign_id = %s AND email IS NOT NULL AND email != ''
            ORDER BY email_status, id
        """, (campaign_id,))
        details = [dict(row) for row in cursor.fetchall()]
        
        return {"status_counts": statuses, "contact_details": details}

@app.get("/api/campaign/{campaign_id}/export/csv")
async def export_campaign_csv(campaign_id: int, fields: str = None):
    from fastapi.responses import StreamingResponse
    import csv
    import io
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get campaign name
        cursor.execute("SELECT name FROM search_campaigns WHERE id = %s", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        # Define available fields and their database column names
        available_fields = {
            'id': 'id',
            'business_name': 'business_name',
            'address': 'address',
            'category': 'category',
            'phone': 'phone',
            'email': 'email',
            'domain': 'domain',
            'rating': 'rating',
            'review_count': 'review_count',
            'facebook': 'facebook',
            'instagram': 'instagram',
            'twitter': 'twitter',
            'yelp': 'yelp',
            'email_status': 'email_status',
            'status': 'status',
            'place_id': 'place_id',
            'full_name': 'full_name',
            'industry': 'industry',
            'city': 'city',
            'country': 'country',
            'www': 'www',
            'firstname': 'firstname',
            'lastname': 'lastname',
            'company': 'company'
        }
        
        # Determine which fields to include
        if fields:
            selected_fields = [f.strip() for f in fields.split(',') if f.strip() in available_fields]
            if not selected_fields:
                selected_fields = list(available_fields.keys())
        else:
            # Default fields if none specified
            selected_fields = list(available_fields.keys())
        
        # Build SQL query with selected fields
        field_columns = [available_fields[field] for field in selected_fields]
        query = f"SELECT {', '.join(field_columns)} FROM contacts WHERE campaign_id = %s ORDER BY id"
        
        cursor.execute(query, (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]
        
        if not contacts:
            raise HTTPException(status_code=404, detail="No contacts found for this campaign")
        
        # Create CSV in memory with selected fields only
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=selected_fields)
        writer.writeheader()
        
        # Map database column names back to field names for CSV
        for contact in contacts:
            csv_row = {}
            for field in selected_fields:
                db_column = available_fields[field]
                csv_row[field] = contact.get(db_column, '')
            writer.writerow(csv_row)
        
        # Create response
        csv_content = output.getvalue()
        output.close()
        
        # Create filename with campaign name
        safe_campaign_name = "".join(c for c in campaign['name'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"campaign_{campaign_id}_{safe_campaign_name.replace(' ', '_')}.csv"
        
        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

# Initialize default export templates
@app.on_event("startup")
async def create_default_templates():
    with get_db() as conn:
        cursor = conn.cursor()

        # Check if ManyReach template already exists
        cursor.execute("SELECT id FROM export_templates WHERE service = 'manyreach'")
        if not cursor.fetchone():
            integration = ManyReachIntegration("")
            default_mapping = integration.get_default_field_mapping()

            TemplateManager.create_template(
                "ManyReach Default",
                "manyreach",
                default_mapping,
                {
                    "api_key": "",
                    "manyreach_campaign_id": "",
                    "endpoint": "/api/campaigns/prospects/add/bulk",
                    "method": "POST"
                }
            )

        # Check if Smartlead template already exists
        cursor.execute("SELECT id FROM export_templates WHERE service = 'smartlead'")
        if not cursor.fetchone():
            integration = SmartLeadIntegration("")
            default_mapping = integration.get_default_field_mapping()

            TemplateManager.create_template(
                "Smartlead Default",
                "smartlead",
                default_mapping,
                {
                    "api_key": "",
                    "smartlead_campaign_id": "",
                    "settings": {
                        "ignore_global_block_list": True,
                        "ignore_community_bounce_list": False,
                        "ignore_unsubscribe_list": True,
                        "ignore_duplicate_leads_in_other_campaign": False,
                        "ignore_duplicate_contacts_within_campaign": True
                    },
                    "endpoint": "/campaigns/{campaign_id}/leads",
                    "method": "POST"
                }
            )

        # Check if MyEmailVerifier template already exists
        cursor.execute("SELECT id FROM email_verification_templates WHERE service = 'myemailverifier'")
        if not cursor.fetchone():
            integration = MyEmailVerifierIntegration("")
            default_status_mapping = integration.get_default_status_mapping()

            EmailVerificationManager.create_template(
                "MyEmailVerifier Default",
                "myemailverifier",
                {
                    "api_key": "XbK0x309Xoe06IU1"
                },
                default_status_mapping
            )
