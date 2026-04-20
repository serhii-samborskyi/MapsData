from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import init_db, get_db
from templates import TemplateManager, ManyReachIntegration, SmartLeadIntegration, SendReadIntegration, extract_city_from_address
from email_verification import EmailVerificationManager, EmailVerificationService, MyEmailVerifierIntegration
from typing import List, Union, Any, Optional
from psycopg2 import DataError, IntegrityError
from psycopg2.extras import Json
import json
import re
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

PIPELINE_STATUSES = ("pending", "running", "completed", "failed", "canceled")
PIPELINE_ACTIVE_STATUSES = {"pending", "running"}
PIPELINE_TERMINAL_STATUSES = {"completed", "failed", "canceled"}
PIPELINE_STAGES = (
    "maps_scrape",
    "cleanup_contacts",
    "email_fast",
    "email_fallback",
    "finalize",
)
PIPELINE_STAGE_INDEX = {stage: index for index, stage in enumerate(PIPELINE_STAGES)}
PIPELINE_DEFAULT_LEASE_SECONDS = 120
PIPELINE_MIN_LEASE_SECONDS = 30
PIPELINE_MAX_LEASE_SECONDS = 900
PIPELINE_ALLOWED_CLAIM_ACTORS = {"daemon", "dashboard", "system", "worker"}


def _now_utc() -> datetime:
    return datetime.utcnow()


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _normalize_domain(value: Any) -> str:
    if value is None:
        return ""
    domain = str(value).strip().lower()
    if not domain:
        return ""

    domain = re.sub(r"^\s*https?://", "", domain)
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.split("/")[0].split("?")[0].split("#")[0].strip(".")
    return domain.strip()


def _is_valid_domain(domain: str) -> bool:
    if not domain:
        return False
    if len(domain) > 253 or "_" in domain or " " in domain:
        return False
    if "." not in domain:
        return False
    if not re.fullmatch(r"[a-z0-9.-]+", domain):
        return False

    labels = domain.split(".")
    if any(not label for label in labels):
        return False
    if any(label.startswith("-") or label.endswith("-") for label in labels):
        return False
    tld = labels[-1]
    if len(tld) < 2 or not tld.isalpha():
        return False
    return True


def _normalized_contact_key(contact: dict) -> str:
    email = str(contact.get("email") or "").strip().lower()
    if email:
        return f"email:{email}"

    normalized_domain = _normalize_domain(contact.get("domain"))
    if _is_valid_domain(normalized_domain):
        return f"domain:{normalized_domain}"

    phone = re.sub(r"\D+", "", str(contact.get("phone") or ""))
    if phone:
        return f"phone:{phone}"

    place_id = str(contact.get("place_id") or "").strip().lower()
    if place_id:
        return f"place:{place_id}"

    business_name = str(contact.get("business_name") or "").strip().lower()
    if business_name:
        return f"name:{business_name}"

    return f"id:{contact.get('id')}"


def _compute_campaign_stats_from_contacts(contacts: List[dict], last_updated_at: Optional[str] = None) -> dict:
    total_contacts = len(contacts)
    unique_keys = {_normalized_contact_key(contact) for contact in contacts}
    unique_contacts = len(unique_keys)
    duplicates_removed = max(0, total_contacts - unique_contacts)

    contacts_with_domain = 0
    contacts_without_domain = 0
    contacts_with_email = 0
    contacts_without_email = 0

    for contact in contacts:
        normalized_domain = _normalize_domain(contact.get("domain"))
        if _is_valid_domain(normalized_domain):
            contacts_with_domain += 1
        else:
            contacts_without_domain += 1

        email = str(contact.get("email") or "").strip()
        if email:
            contacts_with_email += 1
        else:
            contacts_without_email += 1

    return {
        "total_contacts": total_contacts,
        "unique_contacts": unique_contacts,
        "contacts_with_domain": contacts_with_domain,
        "contacts_without_domain": contacts_without_domain,
        "contacts_with_email": contacts_with_email,
        "contacts_without_email": contacts_without_email,
        "duplicates_removed": duplicates_removed,
        "last_updated_at": last_updated_at,
    }


def _next_pipeline_stage(stage: str) -> Optional[str]:
    if stage not in PIPELINE_STAGE_INDEX:
        return None
    next_index = PIPELINE_STAGE_INDEX[stage] + 1
    if next_index >= len(PIPELINE_STAGES):
        return None
    return PIPELINE_STAGES[next_index]


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_lease_seconds(value: Any) -> int:
    lease_seconds = _safe_int(value, PIPELINE_DEFAULT_LEASE_SECONDS)
    if lease_seconds < PIPELINE_MIN_LEASE_SECONDS:
        return PIPELINE_MIN_LEASE_SECONDS
    if lease_seconds > PIPELINE_MAX_LEASE_SECONDS:
        return PIPELINE_MAX_LEASE_SECONDS
    return lease_seconds


async def _read_json_body(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _serialize_pipeline_stage(stage_row: dict) -> dict:
    return {
        "stage": stage_row["stage"],
        "stage_order": stage_row["stage_order"],
        "status": stage_row["status"],
        "retries": stage_row.get("retries", 0),
        "actor": stage_row.get("actor"),
        "worker_id": stage_row.get("worker_id"),
        "worker_metadata": stage_row.get("worker_metadata"),
        "started_at": _iso(stage_row.get("started_at")),
        "completed_at": _iso(stage_row.get("completed_at")),
        "failed_at": _iso(stage_row.get("failed_at")),
        "canceled_at": _iso(stage_row.get("canceled_at")),
        "last_heartbeat_at": _iso(stage_row.get("last_heartbeat_at")),
        "error_message": stage_row.get("error_message"),
        "error_payload": stage_row.get("error_payload"),
        "updated_at": _iso(stage_row.get("updated_at")),
    }


def _serialize_pipeline_status(campaign_id: int, run: Optional[dict], stages: List[dict]) -> dict:
    if not run:
        return {
            "campaign_id": campaign_id,
            "run_id": None,
            "status": "pending",
            "current_stage": None,
            "stages": [],
            "counts": {
                "total_stages": len(PIPELINE_STAGES),
                "pending": len(PIPELINE_STAGES),
                "running": 0,
                "completed": 0,
                "failed": 0,
                "canceled": 0,
                "completed_stages": 0,
            },
            "retries": 0,
            "timestamps": {},
            "latest_error": None,
            "worker": None,
            "lease_expires_at": None,
            "last_heartbeat_at": None,
        }

    stage_rows = [_serialize_pipeline_stage(stage) for stage in stages]
    stage_status_counts = {status: 0 for status in PIPELINE_STATUSES}
    for stage in stage_rows:
        stage_status_counts[stage["status"]] = stage_status_counts.get(stage["status"], 0) + 1

    return {
        "campaign_id": campaign_id,
        "run_id": run["id"],
        "status": run["status"],
        "current_stage": run.get("current_stage"),
        "stages": stage_rows,
        "counts": {
            "total_stages": len(PIPELINE_STAGES),
            "pending": stage_status_counts.get("pending", 0),
            "running": stage_status_counts.get("running", 0),
            "completed": stage_status_counts.get("completed", 0),
            "failed": stage_status_counts.get("failed", 0),
            "canceled": stage_status_counts.get("canceled", 0),
            "completed_stages": stage_status_counts.get("completed", 0),
        },
        "retries": run.get("retries", 0),
        "timestamps": {
            "created_at": _iso(run.get("created_at")),
            "updated_at": _iso(run.get("updated_at")),
            "started_at": _iso(run.get("started_at")),
            "completed_at": _iso(run.get("completed_at")),
            "failed_at": _iso(run.get("failed_at")),
            "canceled_at": _iso(run.get("canceled_at")),
        },
        "latest_error": run.get("latest_error"),
        "worker": {
            "worker_id": run.get("worker_id"),
            "worker_metadata": run.get("worker_metadata"),
            "actor": run.get("actor"),
        },
        "lease_expires_at": _iso(run.get("lease_expires_at")),
        "last_heartbeat_at": _iso(run.get("last_heartbeat_at")),
    }

def _normalize_status_value(value: Any) -> str:
    return ''.join(ch for ch in str(value or '').lower() if ch.isalpha())

def _resolve_contact_email_status(contact: dict) -> str:
    email_status = contact.get("email_status")
    if email_status is not None and str(email_status).strip():
        return _normalize_status_value(email_status)
    return _normalize_status_value(contact.get("status"))

def _is_valid_email_status(normalized_status: str) -> bool:
    return normalized_status.startswith("valid") or normalized_status == "verified"

def _is_catch_all_email_status(normalized_status: str) -> bool:
    return "catchall" in normalized_status

def _matches_export_status_filter(contact: dict, valid_only: bool, include_catch_all: bool, catch_all_only: bool) -> bool:
    normalized_status = _resolve_contact_email_status(contact)
    is_valid = _is_valid_email_status(normalized_status)
    is_catch_all = _is_catch_all_email_status(normalized_status)

    if catch_all_only:
        return is_catch_all
    if valid_only:
        return is_valid or (include_catch_all and is_catch_all)
    return True

def _is_public_email_address(email: str) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.rsplit("@", 1)[-1].strip().lower()
    return domain in PUBLIC_EMAIL_DOMAINS

def _compute_campaign_email_metrics(cursor, campaign_ids: Optional[List[int]] = None) -> dict:
    """
    Return per-campaign email metrics using the same normalization rules as export logic.
    This avoids inconsistencies from join-heavy aggregate queries.
    """
    query = """
        SELECT campaign_id, email_status, status
        FROM contacts
        WHERE campaign_id IS NOT NULL
          AND email IS NOT NULL
          AND btrim(email) != ''
    """
    params: List[Any] = []
    if campaign_ids is not None:
        if not campaign_ids:
            return {}
        placeholders = ",".join(["%s"] * len(campaign_ids))
        query += f" AND campaign_id IN ({placeholders})"
        params.extend(campaign_ids)

    cursor.execute(query, tuple(params))

    metrics = {}
    for row in cursor.fetchall():
        campaign_id = row.get("campaign_id")
        if campaign_id is None:
            continue

        campaign_metrics = metrics.setdefault(campaign_id, {"email_count": 0, "valid_email_count": 0})
        campaign_metrics["email_count"] += 1

        normalized_status = _resolve_contact_email_status(row)
        if _is_valid_email_status(normalized_status):
            campaign_metrics["valid_email_count"] += 1

    return metrics

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
async def get_campaigns(
    request: Request,
    partial: bool = False,
    page: int = 1,
    per_page: int = 3,
    search: str = ""
):
    page = max(1, int(page or 1))
    per_page = int(per_page or 3)
    per_page = min(max(per_page, 1), 25)
    search_term = str(search or "").strip()
    search_clause = ""
    search_params: List[Any] = []
    if search_term:
        search_clause = " WHERE sc.name ILIKE %s"
        search_params = [f"%{search_term}%"]

    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(
            f"SELECT COUNT(*) AS count FROM search_campaigns sc{search_clause}",
            tuple(search_params)
        )
        total_campaigns = int((cursor.fetchone() or {}).get("count") or 0)
        total_pages = max(1, (total_campaigns + per_page - 1) // per_page)
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * per_page
        
        # Single optimized query to get all campaign data at once
        cursor.execute(f"""
            SELECT 
                sc.id,
                sc.name,
                sc.status,
                COUNT(DISTINCT r.id) as total_requests,
                COUNT(DISTINCT c.id) as total_contacts,
                COUNT(DISTINCT CASE WHEN r.status = 'completed' THEN r.id END) as completed_requests,
                COUNT(DISTINCT CASE WHEN c.email IS NOT NULL AND btrim(c.email) != '' THEN c.id END) as email_count,
                COUNT(
                    DISTINCT CASE
                        WHEN c.email IS NOT NULL
                             AND btrim(c.email) != ''
                             AND coalesce(nullif(btrim(c.email_status), ''), 'unverified') != 'unverified'
                        THEN c.id
                    END
                ) AS verification_processed_count,
                0 as valid_email_count
            FROM search_campaigns sc
            LEFT JOIN requests r ON sc.id = r.campaign_id
            LEFT JOIN contacts c ON sc.id = c.campaign_id
            {search_clause}
            GROUP BY sc.id, sc.name, sc.status
            ORDER BY sc.id DESC
            LIMIT %s
            OFFSET %s
        """, tuple(search_params + [per_page, offset]))

        campaign_rows = cursor.fetchall()
        campaigns = []
        campaign_ids = [int(row["id"]) for row in campaign_rows]
        email_metrics = _compute_campaign_email_metrics(cursor, campaign_ids)

        with verification_jobs_lock:
            active_verification_jobs = {}
            latest_verification_jobs = {}
            for job in verification_jobs.values():
                campaign_id = job.get("campaign_id")
                if campaign_id is None:
                    continue

                status = str(job.get("status") or "")
                sort_key = job.get("started_at") or job.get("completed_at") or ""
                existing_latest = latest_verification_jobs.get(campaign_id)
                if not existing_latest or sort_key > (existing_latest.get("started_at") or existing_latest.get("completed_at") or ""):
                    latest_verification_jobs[campaign_id] = dict(job)

                if status in {"queued", "running"}:
                    existing_active = active_verification_jobs.get(campaign_id)
                    if not existing_active or sort_key > (existing_active.get("started_at") or ""):
                        active_verification_jobs[campaign_id] = dict(job)

        for row in campaign_rows:
            campaign = dict(row)
            metrics = email_metrics.get(campaign["id"], {"email_count": 0, "valid_email_count": 0})
            campaign["email_count"] = metrics["email_count"]
            campaign["valid_email_count"] = metrics["valid_email_count"]

            active_job = active_verification_jobs.get(campaign["id"])
            latest_job = latest_verification_jobs.get(campaign["id"])
            total_emails = int(campaign.get("email_count") or 0)
            processed_from_contacts = int(campaign.get("verification_processed_count") or 0)

            verification_status = "idle"
            verification_total = total_emails
            verification_processed = min(processed_from_contacts, total_emails) if total_emails > 0 else 0
            verification_message = "Not started"

            if active_job:
                verification_status = str(active_job.get("status") or "running")
                verification_total = int(active_job.get("total_emails") or total_emails)
                if verification_total <= 0:
                    verification_total = total_emails
                active_processed = int(active_job.get("processed_emails") or 0)
                if verification_total > 0:
                    verification_processed = min(active_processed, verification_total)
                else:
                    verification_processed = max(active_processed, 0)
                verification_message = str(active_job.get("message") or "").strip() or "Verification in progress"
            elif latest_job and str(latest_job.get("status") or "") in {"completed", "failed", "cancelled"}:
                verification_status = str(latest_job.get("status") or "idle")
                verification_message = str(latest_job.get("message") or "").strip() or verification_status.capitalize()
                if verification_status == "completed" and verification_total > 0:
                    verification_processed = verification_total
            elif verification_total > 0 and verification_processed >= verification_total:
                verification_status = "completed"
                verification_message = "All emails processed"

            verification_percent = 0
            if verification_total > 0:
                verification_percent = round(min(100, (verification_processed / verification_total) * 100), 2)

            campaign["verification_status"] = verification_status
            campaign["verification_total_emails"] = verification_total
            campaign["verification_processed_emails"] = verification_processed
            campaign["verification_progress_percent"] = verification_percent
            campaign["verification_message"] = verification_message
            
            # Dashboard details are lazy-loaded via dedicated endpoints.
            campaign['requests'] = []
            campaign['contacts'] = []

            campaigns.append(campaign)

    template = "index.html" if not partial else "partials/table.html"
    return templates.TemplateResponse(template, {
        "request": request,
        "campaigns": campaigns,
        "page": page,
        "per_page": per_page,
        "total_campaigns": total_campaigns,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "search_term": search_term
    })


def _compute_verification_progress_payload(
    campaign_id: int,
    total_emails: int,
    processed_emails: int,
    active_job: Optional[dict],
    latest_job: Optional[dict]
) -> dict:
    fallback_total_emails = max(0, int(total_emails or 0))
    total_emails = fallback_total_emails
    processed_emails = max(0, int(processed_emails or 0))

    status = "idle"
    message = "Not started"
    current_email = "-"
    active = False
    job_id = None

    if active_job:
        active = True
        status = str(active_job.get("status") or "running")
        job_id = active_job.get("job_id")
        total_emails = int(active_job.get("total_emails") or total_emails)
        if total_emails <= 0:
            total_emails = fallback_total_emails
        processed_emails = int(active_job.get("processed_emails") or 0)
        if total_emails > 0:
            processed_emails = min(max(0, processed_emails), total_emails)
        message = str(active_job.get("message") or "").strip() or "Verification in progress"
        current_email = str(active_job.get("current_email") or "-")
    elif latest_job and str(latest_job.get("status") or "") in {"completed", "failed", "cancelled"}:
        status = str(latest_job.get("status") or "idle")
        job_id = latest_job.get("job_id")
        message = str(latest_job.get("message") or "").strip() or status.capitalize()
        current_email = str(latest_job.get("current_email") or "-")
        if status == "completed" and total_emails > 0:
            processed_emails = total_emails
    elif total_emails > 0 and processed_emails >= total_emails:
        status = "completed"
        message = "All emails processed"
        processed_emails = total_emails

    progress_percent = 0
    if total_emails > 0:
        progress_percent = round(min(100, (processed_emails / total_emails) * 100), 2)

    return {
        "campaign_id": campaign_id,
        "job_id": job_id,
        "active": active,
        "status": status,
        "total_emails": total_emails,
        "processed_emails": processed_emails,
        "progress_percent": progress_percent,
        "current_email": current_email,
        "message": message
    }


@app.get("/api/campaign/{campaign_id}/details")
async def get_campaign_details(campaign_id: int, limit: int = 100):
    capped_limit = min(max(int(limit or 100), 1), 300)
    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)

        cursor.execute("""
            SELECT
                r.id,
                r.req_text,
                r.status,
                COUNT(c.id) AS contact_count
            FROM requests r
            LEFT JOIN contacts c ON c.request_id = r.id
            WHERE r.campaign_id = %s
            GROUP BY r.id, r.req_text, r.status
            ORDER BY r.id DESC
        """, (campaign_id,))
        requests = [dict(row) for row in cursor.fetchall()]

        cursor.execute("""
            SELECT
                id,
                business_name,
                category,
                domain,
                email,
                address,
                phone,
                facebook,
                instagram,
                twitter,
                yelp,
                status,
                email_status
            FROM contacts
            WHERE campaign_id = %s
            ORDER BY id DESC
            LIMIT %s
        """, (campaign_id, capped_limit))
        contacts = [dict(row) for row in cursor.fetchall()]

    emails = [contact for contact in contacts if str(contact.get("email") or "").strip()]
    return {
        "campaign_id": campaign_id,
        "limit": capped_limit,
        "requests": requests,
        "contacts": contacts,
        "emails": emails
    }


@app.get("/api/dashboard/runtime-status")
async def get_dashboard_runtime_status(campaign_ids: str = ""):
    ids = []
    for raw_id in str(campaign_ids or "").split(","):
        token = raw_id.strip()
        if not token:
            continue
        try:
            ids.append(int(token))
        except ValueError:
            continue

    seen = set()
    campaign_id_list = []
    for campaign_id in ids:
        if campaign_id not in seen:
            seen.add(campaign_id)
            campaign_id_list.append(campaign_id)

    if not campaign_id_list:
        return {"campaigns": {}}

    with get_db() as conn:
        cursor = conn.cursor()
        placeholders = ",".join(["%s"] * len(campaign_id_list))

        cursor.execute(f"""
            SELECT DISTINCT ON (campaign_id)
                *
            FROM pipeline_runs
            WHERE campaign_id IN ({placeholders})
            ORDER BY campaign_id, created_at DESC, id DESC
        """, tuple(campaign_id_list))
        latest_runs_raw = [dict(row) for row in cursor.fetchall()]
        latest_run_by_campaign = {row["campaign_id"]: row for row in latest_runs_raw}

        run_ids = [int(run["id"]) for run in latest_runs_raw]
        stage_rows_by_run = {}
        if run_ids:
            stage_placeholders = ",".join(["%s"] * len(run_ids))
            cursor.execute(f"""
                SELECT *
                FROM pipeline_run_stages
                WHERE run_id IN ({stage_placeholders})
                ORDER BY run_id ASC, stage_order ASC
            """, tuple(run_ids))
            for stage_row in cursor.fetchall():
                stage = dict(stage_row)
                stage_rows_by_run.setdefault(stage["run_id"], []).append(stage)

        cursor.execute(f"""
            SELECT
                campaign_id,
                id,
                domain,
                email,
                phone,
                place_id,
                business_name,
                nomail_pulled_at,
                email_status,
                status
            FROM contacts
            WHERE campaign_id IN ({placeholders})
        """, tuple(campaign_id_list))
        contacts_by_campaign = {}
        for row in cursor.fetchall():
            contact = dict(row)
            contacts_by_campaign.setdefault(contact["campaign_id"], []).append(contact)

        cursor.execute(f"""
            SELECT
                campaign_id,
                MAX(updated_at) AS pipeline_updated_at
            FROM pipeline_runs
            WHERE campaign_id IN ({placeholders})
            GROUP BY campaign_id
        """, tuple(campaign_id_list))
        pipeline_updated_at_map = {
            int(row["campaign_id"]): row.get("pipeline_updated_at")
            for row in cursor.fetchall()
        }

    with verification_jobs_lock:
        active_jobs = {}
        latest_jobs = {}
        for job in verification_jobs.values():
            campaign_id = job.get("campaign_id")
            if campaign_id not in seen:
                continue

            sort_key = job.get("started_at") or job.get("completed_at") or ""
            latest_existing = latest_jobs.get(campaign_id)
            if not latest_existing or sort_key > (latest_existing.get("started_at") or latest_existing.get("completed_at") or ""):
                latest_jobs[campaign_id] = dict(job)

            if job.get("status") in {"queued", "running"}:
                active_existing = active_jobs.get(campaign_id)
                if not active_existing or sort_key > (active_existing.get("started_at") or ""):
                    active_jobs[campaign_id] = dict(job)

    result = {}
    for campaign_id in campaign_id_list:
        run = latest_run_by_campaign.get(campaign_id)
        stages = stage_rows_by_run.get(run["id"], []) if run else []
        pipeline = _serialize_pipeline_status(campaign_id, run, stages)

        contacts = contacts_by_campaign.get(campaign_id, [])
        pipeline_updated_at = pipeline_updated_at_map.get(campaign_id)
        max_nomail = None
        for contact in contacts:
            nomail_pulled_at = contact.get("nomail_pulled_at")
            if nomail_pulled_at and (max_nomail is None or nomail_pulled_at > max_nomail):
                max_nomail = nomail_pulled_at

        last_updated_at_dt = None
        for candidate in [max_nomail, pipeline_updated_at]:
            if candidate and (last_updated_at_dt is None or candidate > last_updated_at_dt):
                last_updated_at_dt = candidate

        stats = _compute_campaign_stats_from_contacts(
            contacts,
            last_updated_at=_iso(last_updated_at_dt),
        )

        total_emails = 0
        processed_emails = 0
        for contact in contacts:
            email = str(contact.get("email") or "").strip()
            if not email:
                continue
            total_emails += 1
            if _resolve_contact_email_status(contact) != "unverified":
                processed_emails += 1

        verification = _compute_verification_progress_payload(
            campaign_id=campaign_id,
            total_emails=total_emails,
            processed_emails=processed_emails,
            active_job=active_jobs.get(campaign_id),
            latest_job=latest_jobs.get(campaign_id),
        )

        result[str(campaign_id)] = {
            "pipeline": pipeline,
            "stats": stats,
            "verification": verification,
        }

    return {"campaigns": result}

@app.delete("/api/campaign/{campaign_id}")
async def delete_campaign(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM search_campaigns WHERE id = %s", (campaign_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Campaign not found")

        # Delete child rows first to satisfy FK constraints.
        cursor.execute("DELETE FROM pipeline_run_locks WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM pipeline_run_stages WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM pipeline_runs WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM export_logs WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM email_verification_logs WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM contacts WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM requests WHERE campaign_id = %s", (campaign_id,))
        cursor.execute("DELETE FROM search_campaigns WHERE id = %s", (campaign_id,))
        conn.commit()

    with verification_jobs_lock:
        for job_id, job in list(verification_jobs.items()):
            if job.get("campaign_id") == campaign_id:
                verification_jobs.pop(job_id, None)

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
        cursor.execute("DELETE FROM pipeline_run_locks")
        cursor.execute("DELETE FROM pipeline_run_stages")
        cursor.execute("DELETE FROM pipeline_runs")
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
        email_metrics = _compute_campaign_email_metrics(cursor)
        for campaign in campaigns:
            metrics = email_metrics.get(campaign["id"], {"email_count": 0, "valid_email_count": 0})
            campaign["email_count"] = metrics["email_count"]
            campaign["valid_email_count"] = metrics["valid_email_count"]
        return {"campaigns": campaigns}


def _ensure_campaign_exists(cursor, campaign_id: int):
    cursor.execute("SELECT id FROM search_campaigns WHERE id = %s", (campaign_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Campaign not found")


def _load_latest_pipeline_run(cursor, campaign_id: int) -> Optional[dict]:
    cursor.execute("""
        SELECT *
        FROM pipeline_runs
        WHERE campaign_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """, (campaign_id,))
    run = cursor.fetchone()
    return dict(run) if run else None


def _load_pipeline_run(cursor, run_id: int) -> Optional[dict]:
    cursor.execute("SELECT * FROM pipeline_runs WHERE id = %s", (run_id,))
    run = cursor.fetchone()
    return dict(run) if run else None


def _load_run_stages(cursor, run_id: int) -> List[dict]:
    cursor.execute("""
        SELECT *
        FROM pipeline_run_stages
        WHERE run_id = %s
        ORDER BY stage_order ASC
    """, (run_id,))
    return [dict(row) for row in cursor.fetchall()]


def _current_stage_row(cursor, run_id: int, current_stage: str) -> Optional[dict]:
    cursor.execute("""
        SELECT *
        FROM pipeline_run_stages
        WHERE run_id = %s AND stage = %s
        LIMIT 1
    """, (run_id, current_stage))
    stage_row = cursor.fetchone()
    return dict(stage_row) if stage_row else None


def _claim_lock_is_active(lock_row: Optional[dict], worker_id: str, now: datetime) -> bool:
    if not lock_row:
        return False
    lock_owner = lock_row.get("worker_id")
    lease_expires_at = lock_row.get("lease_expires_at")
    if not lock_owner or lease_expires_at is None:
        return False
    if lock_owner == worker_id:
        return False
    return lease_expires_at > now


def _upsert_pipeline_lock(cursor, run_id: int, campaign_id: int, worker_id: str, lease_expires_at: datetime, metadata: dict):
    lock_token = str(uuid4())
    cursor.execute("""
        INSERT INTO pipeline_run_locks (run_id, campaign_id, worker_id, lock_token, metadata, lease_expires_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE
        SET campaign_id = EXCLUDED.campaign_id,
            worker_id = EXCLUDED.worker_id,
            lock_token = EXCLUDED.lock_token,
            metadata = EXCLUDED.metadata,
            lease_expires_at = EXCLUDED.lease_expires_at,
            updated_at = EXCLUDED.updated_at
    """, (run_id, campaign_id, worker_id, lock_token, Json(metadata), lease_expires_at, _now_utc()))


def _load_run_stages_for_update(cursor, run_id: int) -> List[dict]:
    cursor.execute("""
        SELECT *
        FROM pipeline_run_stages
        WHERE run_id = %s
        ORDER BY stage_order ASC
        FOR UPDATE
    """, (run_id,))
    return [dict(row) for row in cursor.fetchall()]


def _resolve_run_stage_pointer(run: dict, stage_rows: List[dict]) -> Optional[dict]:
    if not stage_rows:
        return None

    current_stage = run.get("current_stage")
    stage_by_name = {row["stage"]: row for row in stage_rows}
    current_row = stage_by_name.get(current_stage)
    if current_row and current_row["status"] != "completed":
        return current_row

    for row in stage_rows:
        if row["status"] != "completed":
            return row

    return None


def _is_actor_allowed_for_claim(actor: str) -> bool:
    return actor in PIPELINE_ALLOWED_CLAIM_ACTORS


@app.post("/api/campaign/{campaign_id}/pipeline/start")
async def start_campaign_pipeline(campaign_id: int, request: Request):
    payload = await _read_json_body(request)
    actor = str(payload.get("actor") or "dashboard").strip() or "dashboard"
    worker_metadata = payload.get("worker_metadata")
    retry_requested = bool(payload.get("retry", False))

    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)

        cursor.execute("""
            SELECT *
            FROM pipeline_runs
            WHERE campaign_id = %s
              AND status IN ('pending', 'running')
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """, (campaign_id,))
        existing_active_run = cursor.fetchone()
        if existing_active_run:
            run = dict(existing_active_run)
            stage_row = _current_stage_row(cursor, run["id"], run["current_stage"])
            conn.commit()
            return {
                "status": "existing",
                "idempotent": True,
                "run_id": run["id"],
                "campaign_id": campaign_id,
                "pipeline_status": run["status"],
                "current_stage": run["current_stage"],
                "current_stage_status": stage_row["status"] if stage_row else run["status"],
            }

        retries = 0
        if retry_requested:
            cursor.execute("""
                SELECT retries
                FROM pipeline_runs
                WHERE campaign_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (campaign_id,))
            previous_run = cursor.fetchone()
            if previous_run:
                retries = int(previous_run.get("retries", 0)) + 1

        now = _now_utc()
        try:
            cursor.execute("""
                INSERT INTO pipeline_runs (
                    campaign_id,
                    status,
                    current_stage,
                    retries,
                    actor,
                    worker_metadata,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'pending', %s, %s, %s, %s, %s, %s)
                RETURNING id, status, current_stage
            """, (
                campaign_id,
                PIPELINE_STAGES[0],
                retries,
                actor,
                Json(worker_metadata) if worker_metadata is not None else None,
                now,
                now,
            ))
            created_run = dict(cursor.fetchone())
        except IntegrityError:
            conn.rollback()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT *
                FROM pipeline_runs
                WHERE campaign_id = %s
                  AND status IN ('pending', 'running')
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (campaign_id,))
            race_existing = cursor.fetchone()
            if not race_existing:
                raise
            run = dict(race_existing)
            stage_row = _current_stage_row(cursor, run["id"], run["current_stage"])
            conn.commit()
            return {
                "status": "existing",
                "idempotent": True,
                "run_id": run["id"],
                "campaign_id": campaign_id,
                "pipeline_status": run["status"],
                "current_stage": run["current_stage"],
                "current_stage_status": stage_row["status"] if stage_row else run["status"],
            }
        run_id = created_run["id"]

        for stage_order, stage in enumerate(PIPELINE_STAGES):
            cursor.execute("""
                INSERT INTO pipeline_run_stages (
                    run_id,
                    campaign_id,
                    stage,
                    stage_order,
                    status,
                    retries,
                    actor,
                    worker_metadata,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, 'pending', 0, %s, %s, %s, %s)
            """, (
                run_id,
                campaign_id,
                stage,
                stage_order,
                actor,
                Json(worker_metadata) if worker_metadata is not None else None,
                now,
                now,
            ))

        conn.commit()

    return {
        "status": "created",
        "idempotent": False,
        "run_id": run_id,
        "campaign_id": campaign_id,
        "pipeline_status": created_run["status"],
        "current_stage": created_run["current_stage"],
    }


@app.get("/api/campaign/{campaign_id}/pipeline/status")
async def get_campaign_pipeline_status(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)

        run = _load_latest_pipeline_run(cursor, campaign_id)
        stages: List[dict] = []
        if run:
            stages = _load_run_stages(cursor, run["id"])

        result = _serialize_pipeline_status(campaign_id, run, stages)
        conn.commit()
        return result


@app.post("/api/pipeline/claim")
async def claim_pipeline_stage(request: Request):
    payload = await _read_json_body(request)
    worker_id = str(payload.get("worker_id") or "").strip() or f"worker-{uuid4()}"
    actor = str(payload.get("actor") or "daemon").strip() or "daemon"
    lease_seconds = _coerce_lease_seconds(payload.get("lease_seconds"))
    worker_metadata = payload.get("worker_metadata")
    now = _now_utc()
    lease_expires_at = now + timedelta(seconds=lease_seconds)

    if not _is_actor_allowed_for_claim(actor):
        return {
            "claimed": False,
            "reason": "actor_not_allowed",
            "run_id": None,
            "campaign_id": None,
            "stage": None,
        }

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT prl.run_id, prl.worker_id, prl.lease_expires_at
            FROM pipeline_run_locks prl
            JOIN pipeline_runs pr ON pr.id = prl.run_id
            WHERE pr.status IN ('pending', 'running')
              AND prl.lease_expires_at IS NOT NULL
              AND prl.lease_expires_at > %s
        """, (now,))
        active_lock_rows = [dict(row) for row in cursor.fetchall()]
        current_worker_active_run_ids = {
            int(row["run_id"])
            for row in active_lock_rows
            if str(row.get("worker_id") or "").strip() == worker_id
        }
        other_active_workers = {
            str(row.get("worker_id") or "").strip()
            for row in active_lock_rows
            if str(row.get("worker_id") or "").strip() and str(row.get("worker_id") or "").strip() != worker_id
        }
        fairness_block_second_distinct_run = bool(current_worker_active_run_ids and other_active_workers)

        cursor.execute("""
            SELECT *
            FROM pipeline_runs
            WHERE status IN ('pending', 'running')
            ORDER BY
                CASE WHEN status = 'running' THEN 0 ELSE 1 END,
                updated_at ASC,
                id ASC
            FOR UPDATE SKIP LOCKED
        """)
        candidate_runs = [dict(row) for row in cursor.fetchall()]

        if not candidate_runs:
            conn.commit()
            return {
                "claimed": False,
                "reason": "run_not_started",
                "run_id": None,
                "campaign_id": None,
                "stage": None,
            }

        saw_all_leased = False
        saw_non_claimable_stage = False
        deferred_current_worker_runs = []

        def _claim_run_stage(run: dict, stage_row: dict) -> dict:
            run_id = run["id"]
            campaign_id = run["campaign_id"]

            _upsert_pipeline_lock(
                cursor,
                run_id=run_id,
                campaign_id=campaign_id,
                worker_id=worker_id,
                lease_expires_at=lease_expires_at,
                metadata=worker_metadata if isinstance(worker_metadata, dict) else {},
            )

            cursor.execute("""
                UPDATE pipeline_runs
                SET status = 'running',
                    current_stage = %s,
                    worker_id = %s,
                    actor = %s,
                    worker_metadata = %s,
                    lease_expires_at = %s,
                    last_heartbeat_at = %s,
                    started_at = COALESCE(started_at, %s),
                    updated_at = %s
                WHERE id = %s
            """, (
                stage_row["stage"],
                worker_id,
                actor,
                Json(worker_metadata) if worker_metadata is not None else None,
                lease_expires_at,
                now,
                now,
                now,
                run_id,
            ))

            if stage_row["status"] == "pending":
                cursor.execute("""
                    UPDATE pipeline_run_stages
                    SET status = 'running',
                        worker_id = %s,
                        actor = %s,
                        worker_metadata = %s,
                        started_at = COALESCE(started_at, %s),
                        last_heartbeat_at = %s,
                        updated_at = %s
                    WHERE run_id = %s AND stage = %s
                """, (
                    worker_id,
                    actor,
                    Json(worker_metadata) if worker_metadata is not None else None,
                    now,
                    now,
                    now,
                    run_id,
                    stage_row["stage"],
                ))
            else:
                cursor.execute("""
                    UPDATE pipeline_run_stages
                    SET worker_id = %s,
                        actor = %s,
                        worker_metadata = %s,
                        last_heartbeat_at = %s,
                        updated_at = %s
                    WHERE run_id = %s AND stage = %s
                """, (
                    worker_id,
                    actor,
                    Json(worker_metadata) if worker_metadata is not None else None,
                    now,
                    now,
                    run_id,
                    stage_row["stage"],
                ))

            return {
                "claimed": True,
                "run_id": run_id,
                "campaign_id": campaign_id,
                "stage": stage_row["stage"],
                "pipeline_status": "running",
                "lease_expires_at": _iso(lease_expires_at),
                "worker_id": worker_id,
            }

        for run in candidate_runs:
            run_id = run["id"]
            stage_rows = _load_run_stages_for_update(cursor, run_id)
            if not stage_rows:
                saw_non_claimable_stage = True
                continue

            cursor.execute("SELECT * FROM pipeline_run_locks WHERE run_id = %s FOR UPDATE", (run_id,))
            lock_row_raw = cursor.fetchone()
            lock_row = dict(lock_row_raw) if lock_row_raw else None
            lock_active_other_worker = _claim_lock_is_active(lock_row, worker_id, now)
            lock_owner = str(lock_row.get("worker_id") or "").strip() if lock_row else ""
            lock_owned_by_current_worker = bool(
                lock_owner == worker_id
                and lock_row
                and lock_row.get("lease_expires_at")
                and lock_row.get("lease_expires_at") > now
            )

            stage_row = _resolve_run_stage_pointer(run, stage_rows)
            if not stage_row:
                cursor.execute("""
                    UPDATE pipeline_runs
                    SET status = 'completed',
                        completed_at = COALESCE(completed_at, %s),
                        lease_expires_at = NULL,
                        updated_at = %s
                    WHERE id = %s
                """, (now, now, run_id))
                cursor.execute("DELETE FROM pipeline_run_locks WHERE run_id = %s", (run_id,))
                saw_non_claimable_stage = True
                continue

            if run.get("current_stage") != stage_row["stage"]:
                cursor.execute("""
                    UPDATE pipeline_runs
                    SET current_stage = %s,
                        updated_at = %s
                    WHERE id = %s
                """, (stage_row["stage"], now, run_id))

            if stage_row["status"] not in {"pending", "running"}:
                saw_non_claimable_stage = True
                continue

            if lock_active_other_worker and stage_row["status"] == "pending":
                saw_all_leased = True
                continue

            if stage_row["status"] == "running" and lock_active_other_worker:
                saw_all_leased = True
                continue

            # Fairness: defer runs already owned by this worker so unclaimed/other-owned
            # claimable runs are considered first.
            if lock_owned_by_current_worker:
                deferred_current_worker_runs.append((run, stage_row))
                continue

            # Fairness: if this worker already holds an active run and other workers are
            # active, avoid assigning another distinct run to this worker.
            if fairness_block_second_distinct_run and run_id not in current_worker_active_run_ids:
                saw_all_leased = True
                continue

            claim_response = _claim_run_stage(run, stage_row)
            conn.commit()
            return claim_response

        # Fallback pass: allow reclaim/renew of runs already leased by this worker.
        for run, stage_row in deferred_current_worker_runs:
            claim_response = _claim_run_stage(run, stage_row)
            conn.commit()
            return claim_response

        conn.commit()
        reason = "all_leased" if saw_all_leased else ("no_pending_stages" if saw_non_claimable_stage else "run_not_started")
        return {
            "claimed": False,
            "reason": reason,
            "run_id": None,
            "campaign_id": None,
            "stage": None,
        }


@app.get("/api/pipeline/debug/claimability")
async def debug_pipeline_claimability(campaign_id: Optional[int] = None, worker_id: str = "debug-worker", actor: str = "daemon"):
    now = _now_utc()
    actor_allowed = _is_actor_allowed_for_claim(actor)

    with get_db() as conn:
        cursor = conn.cursor()
        params: List[Any] = []
        campaign_clause = ""
        if campaign_id is not None:
            campaign_clause = "AND pr.campaign_id = %s"
            params.append(campaign_id)

        cursor.execute(f"""
            SELECT pr.*, prl.worker_id AS lock_worker_id, prl.lease_expires_at AS lock_lease_expires_at
            FROM pipeline_runs pr
            LEFT JOIN pipeline_run_locks prl ON prl.run_id = pr.id
            WHERE pr.status IN ('pending', 'running', 'failed', 'completed', 'canceled')
            {campaign_clause}
            ORDER BY pr.updated_at DESC, pr.id DESC
            LIMIT 100
        """, tuple(params))
        runs = [dict(row) for row in cursor.fetchall()]

        diagnostics = []
        for run in runs:
            cursor.execute("""
                SELECT stage, stage_order, status, worker_id, last_heartbeat_at, error_message
                FROM pipeline_run_stages
                WHERE run_id = %s
                ORDER BY stage_order ASC
            """, (run["id"],))
            stages = [dict(row) for row in cursor.fetchall()]
            stage_row = _resolve_run_stage_pointer(run, stages)

            reason = None
            claimable = False
            if run["status"] not in PIPELINE_ACTIVE_STATUSES:
                reason = "run_not_started"
            elif not actor_allowed:
                reason = "actor_not_allowed"
            elif not stage_row:
                reason = "no_pending_stages"
            elif stage_row["status"] not in {"pending", "running"}:
                reason = "no_pending_stages"
            else:
                lock_row = {
                    "worker_id": run.get("lock_worker_id"),
                    "lease_expires_at": run.get("lock_lease_expires_at"),
                }
                if _claim_lock_is_active(lock_row, worker_id, now):
                    reason = "all_leased"
                else:
                    claimable = True

            diagnostics.append({
                "run_id": run["id"],
                "campaign_id": run["campaign_id"],
                "run_status": run["status"],
                "current_stage": run.get("current_stage"),
                "resolved_stage": stage_row["stage"] if stage_row else None,
                "resolved_stage_status": stage_row["status"] if stage_row else None,
                "lock_worker_id": run.get("lock_worker_id"),
                "lock_lease_expires_at": _iso(run.get("lock_lease_expires_at")),
                "claimable": claimable,
                "not_claimable_reason": None if claimable else reason,
                "stages": [
                    {
                        "stage": stage["stage"],
                        "status": stage["status"],
                        "worker_id": stage.get("worker_id"),
                        "last_heartbeat_at": _iso(stage.get("last_heartbeat_at")),
                        "error_message": stage.get("error_message"),
                    }
                    for stage in stages
                ],
            })

        return {
            "timestamp": _iso(now),
            "actor": actor,
            "actor_allowed": actor_allowed,
            "worker_id": worker_id,
            "campaign_id_filter": campaign_id,
            "active_runs_count": len([run for run in diagnostics if run["run_status"] in PIPELINE_ACTIVE_STATUSES]),
            "runs": diagnostics,
        }


@app.post("/api/pipeline/{run_id}/heartbeat")
async def pipeline_heartbeat(run_id: int, request: Request):
    payload = await _read_json_body(request)
    worker_id = str(payload.get("worker_id") or "").strip()
    lease_seconds = _coerce_lease_seconds(payload.get("lease_seconds"))
    stage = payload.get("stage")
    worker_metadata = payload.get("worker_metadata")
    now = _now_utc()
    lease_expires_at = now + timedelta(seconds=lease_seconds)

    with get_db() as conn:
        cursor = conn.cursor()
        run = _load_pipeline_run(cursor, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        if run["status"] in PIPELINE_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"Run already {run['status']}")

        claimed_stage = stage or run["current_stage"]
        if claimed_stage not in PIPELINE_STAGE_INDEX:
            raise HTTPException(status_code=400, detail="Invalid stage")

        cursor.execute("SELECT * FROM pipeline_run_locks WHERE run_id = %s FOR UPDATE", (run_id,))
        lock_raw = cursor.fetchone()
        lock_row = dict(lock_raw) if lock_raw else None
        if _claim_lock_is_active(lock_row, worker_id, now):
            raise HTTPException(status_code=409, detail="Run currently leased by another worker")

        if not worker_id:
            worker_id = str(lock_row["worker_id"]) if lock_row else str(run.get("worker_id") or "")
        if not worker_id:
            worker_id = f"worker-{uuid4()}"

        _upsert_pipeline_lock(
            cursor,
            run_id=run_id,
            campaign_id=run["campaign_id"],
            worker_id=worker_id,
            lease_expires_at=lease_expires_at,
            metadata=worker_metadata if isinstance(worker_metadata, dict) else {},
        )

        cursor.execute("""
            UPDATE pipeline_runs
            SET status = 'running',
                worker_id = %s,
                worker_metadata = %s,
                lease_expires_at = %s,
                last_heartbeat_at = %s,
                updated_at = %s
            WHERE id = %s
        """, (
            worker_id,
            Json(worker_metadata) if worker_metadata is not None else None,
            lease_expires_at,
            now,
            now,
            run_id,
        ))

        cursor.execute("""
            UPDATE pipeline_run_stages
            SET status = CASE WHEN status = 'pending' THEN 'running' ELSE status END,
                worker_id = %s,
                worker_metadata = %s,
                last_heartbeat_at = %s,
                started_at = CASE WHEN status = 'pending' THEN COALESCE(started_at, %s) ELSE started_at END,
                updated_at = %s
            WHERE run_id = %s AND stage = %s
        """, (
            worker_id,
            Json(worker_metadata) if worker_metadata is not None else None,
            now,
            now,
            now,
            run_id,
            claimed_stage,
        ))

        conn.commit()
        return {
            "status": "ok",
            "run_id": run_id,
            "stage": claimed_stage,
            "lease_expires_at": _iso(lease_expires_at),
            "worker_id": worker_id,
        }


@app.post("/api/pipeline/{run_id}/stage-complete")
async def complete_pipeline_stage(run_id: int, request: Request):
    payload = await _read_json_body(request)
    worker_id = str(payload.get("worker_id") or "").strip()
    stage = str(payload.get("stage") or "").strip()
    actor = str(payload.get("actor") or "daemon").strip() or "daemon"
    worker_metadata = payload.get("worker_metadata")
    now = _now_utc()

    with get_db() as conn:
        cursor = conn.cursor()
        run = _load_pipeline_run(cursor, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        if run["status"] in PIPELINE_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"Run already {run['status']}")

        current_stage = run["current_stage"]
        stage_to_complete = stage or current_stage
        if stage_to_complete not in PIPELINE_STAGE_INDEX:
            raise HTTPException(status_code=400, detail="Invalid stage")
        if stage_to_complete != current_stage:
            raise HTTPException(status_code=409, detail=f"Current stage is '{current_stage}'")

        cursor.execute("SELECT * FROM pipeline_run_locks WHERE run_id = %s FOR UPDATE", (run_id,))
        lock_raw = cursor.fetchone()
        lock_row = dict(lock_raw) if lock_raw else None
        effective_worker_id = worker_id or str(run.get("worker_id") or "")
        if _claim_lock_is_active(lock_row, effective_worker_id, now):
            raise HTTPException(status_code=409, detail="Run currently leased by another worker")

        if not worker_id:
            worker_id = str(run.get("worker_id") or "")

        cursor.execute("""
            UPDATE pipeline_run_stages
            SET status = 'completed',
                worker_id = COALESCE(%s, worker_id),
                actor = %s,
                worker_metadata = %s,
                completed_at = %s,
                last_heartbeat_at = %s,
                error_message = NULL,
                error_payload = NULL,
                updated_at = %s
            WHERE run_id = %s
              AND stage = %s
              AND status IN ('pending', 'running', 'failed')
        """, (
            worker_id or None,
            actor,
            Json(worker_metadata) if worker_metadata is not None else None,
            now,
            now,
            now,
            run_id,
            stage_to_complete,
        ))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=409, detail="Stage is not claimable for completion")

        next_stage = _next_pipeline_stage(stage_to_complete)
        if next_stage is None:
            cursor.execute("""
                UPDATE pipeline_runs
                SET status = 'completed',
                    current_stage = %s,
                    completed_at = %s,
                    latest_error = NULL,
                    error_payload = NULL,
                    lease_expires_at = NULL,
                    worker_id = COALESCE(%s, worker_id),
                    worker_metadata = %s,
                    last_heartbeat_at = %s,
                    updated_at = %s
                WHERE id = %s
            """, (
                stage_to_complete,
                now,
                worker_id or None,
                Json(worker_metadata) if worker_metadata is not None else None,
                now,
                now,
                run_id,
            ))
        else:
            cursor.execute("""
                UPDATE pipeline_runs
                SET status = 'running',
                    current_stage = %s,
                    latest_error = NULL,
                    error_payload = NULL,
                    lease_expires_at = NULL,
                    worker_id = COALESCE(%s, worker_id),
                    worker_metadata = %s,
                    last_heartbeat_at = %s,
                    updated_at = %s
                WHERE id = %s
            """, (
                next_stage,
                worker_id or None,
                Json(worker_metadata) if worker_metadata is not None else None,
                now,
                now,
                run_id,
            ))

            cursor.execute("""
                UPDATE pipeline_run_stages
                SET status = CASE
                        WHEN status IN ('completed', 'canceled') THEN status
                        ELSE 'pending'
                    END,
                    worker_id = NULL,
                    actor = NULL,
                    worker_metadata = NULL,
                    last_heartbeat_at = NULL,
                    error_message = NULL,
                    error_payload = NULL,
                    failed_at = NULL,
                    updated_at = %s
                WHERE run_id = %s
                  AND stage = %s
            """, (now, run_id, next_stage))

        cursor.execute("DELETE FROM pipeline_run_locks WHERE run_id = %s", (run_id,))

        conn.commit()
        return {
            "status": "ok",
            "run_id": run_id,
            "completed_stage": stage_to_complete,
            "next_stage": next_stage,
            "pipeline_status": "completed" if next_stage is None else "running",
        }


@app.post("/api/pipeline/{run_id}/fail")
async def fail_pipeline_run(run_id: int, request: Request):
    payload = await _read_json_body(request)
    worker_id = str(payload.get("worker_id") or "").strip()
    stage = str(payload.get("stage") or "").strip()
    actor = str(payload.get("actor") or "daemon").strip() or "daemon"
    worker_metadata = payload.get("worker_metadata")
    error_message = str(payload.get("error") or payload.get("message") or "Pipeline stage failed").strip()
    error_payload = payload.get("error_payload")
    now = _now_utc()

    with get_db() as conn:
        cursor = conn.cursor()
        run = _load_pipeline_run(cursor, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        if run["status"] in PIPELINE_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"Run already {run['status']}")

        current_stage = run["current_stage"]
        stage_to_fail = stage or current_stage
        if stage_to_fail not in PIPELINE_STAGE_INDEX:
            raise HTTPException(status_code=400, detail="Invalid stage")

        cursor.execute("SELECT * FROM pipeline_run_locks WHERE run_id = %s FOR UPDATE", (run_id,))
        lock_raw = cursor.fetchone()
        lock_row = dict(lock_raw) if lock_raw else None
        effective_worker_id = worker_id or str(run.get("worker_id") or "")
        if _claim_lock_is_active(lock_row, effective_worker_id, now):
            raise HTTPException(status_code=409, detail="Run currently leased by another worker")

        cursor.execute("""
            UPDATE pipeline_run_stages
            SET status = 'failed',
                retries = retries + 1,
                worker_id = COALESCE(%s, worker_id),
                actor = %s,
                worker_metadata = %s,
                failed_at = %s,
                last_heartbeat_at = %s,
                error_message = %s,
                error_payload = %s,
                updated_at = %s
            WHERE run_id = %s
              AND stage = %s
              AND status IN ('pending', 'running', 'failed')
        """, (
            worker_id or None,
            actor,
            Json(worker_metadata) if worker_metadata is not None else None,
            now,
            now,
            error_message,
            Json(error_payload) if error_payload is not None else None,
            now,
            run_id,
            stage_to_fail,
        ))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=409, detail="Stage is not claimable for failure")

        cursor.execute("""
            UPDATE pipeline_runs
            SET status = 'failed',
                current_stage = %s,
                retries = retries + 1,
                worker_id = COALESCE(%s, worker_id),
                actor = %s,
                worker_metadata = %s,
                latest_error = %s,
                error_payload = %s,
                failed_at = %s,
                lease_expires_at = NULL,
                last_heartbeat_at = %s,
                updated_at = %s
            WHERE id = %s
        """, (
            stage_to_fail,
            worker_id or None,
            actor,
            Json(worker_metadata) if worker_metadata is not None else None,
            error_message,
            Json(error_payload) if error_payload is not None else None,
            now,
            now,
            now,
            run_id,
        ))

        cursor.execute("DELETE FROM pipeline_run_locks WHERE run_id = %s", (run_id,))

        conn.commit()
        return {
            "status": "failed",
            "run_id": run_id,
            "stage": stage_to_fail,
            "error": error_message,
        }


@app.post("/api/campaign/{campaign_id}/contacts/cleanup")
async def cleanup_campaign_contacts(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)

        cursor.execute("""
            SELECT id, domain, email, phone, place_id, business_name, status
            FROM contacts
            WHERE campaign_id = %s
            ORDER BY id ASC
        """, (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]
        before_count = len(contacts)

        seen_keys = set()
        duplicate_ids: List[int] = []
        invalid_domain_ids: List[int] = []

        for contact in contacts:
            normalized_domain = _normalize_domain(contact.get("domain"))
            if not _is_valid_domain(normalized_domain):
                invalid_domain_ids.append(contact["id"])

            key = _normalized_contact_key(contact)
            if key in seen_keys:
                duplicate_ids.append(contact["id"])
            else:
                seen_keys.add(key)

        if invalid_domain_ids:
            cursor.execute("""
                UPDATE contacts
                SET status = 'invalid_domain'
                WHERE campaign_id = %s
                  AND id = ANY(%s)
            """, (campaign_id, invalid_domain_ids))

        if duplicate_ids:
            cursor.execute("""
                DELETE FROM contacts
                WHERE campaign_id = %s
                  AND id = ANY(%s)
            """, (campaign_id, duplicate_ids))

        cursor.execute("SELECT COUNT(*) AS count FROM contacts WHERE campaign_id = %s", (campaign_id,))
        after_count = int(cursor.fetchone()["count"])
        conn.commit()

        return {
            "campaign_id": campaign_id,
            "before_count": before_count,
            "after_count": after_count,
            "duplicates_removed": len(duplicate_ids),
            "flagged_invalid_domain_count": len(invalid_domain_ids),
        }


@app.get("/api/campaign/{campaign_id}/stats")
async def get_campaign_stats(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)

        cursor.execute("""
            SELECT id, domain, email, phone, place_id, business_name, nomail_pulled_at
            FROM contacts
            WHERE campaign_id = %s
        """, (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]

        cursor.execute("""
            SELECT
                MAX(updated_at) AS pipeline_updated_at
            FROM pipeline_runs
            WHERE campaign_id = %s
        """, (campaign_id,))
        pipeline_timestamp = cursor.fetchone()
        pipeline_updated_at = pipeline_timestamp.get("pipeline_updated_at") if pipeline_timestamp else None

        max_nomail = None
        for contact in contacts:
            nomail_pulled_at = contact.get("nomail_pulled_at")
            if nomail_pulled_at and (max_nomail is None or nomail_pulled_at > max_nomail):
                max_nomail = nomail_pulled_at

        last_updated_at_dt = None
        for candidate in [max_nomail, pipeline_updated_at]:
            if candidate and (last_updated_at_dt is None or candidate > last_updated_at_dt):
                last_updated_at_dt = candidate

        return _compute_campaign_stats_from_contacts(
            contacts,
            last_updated_at=_iso(last_updated_at_dt),
        )

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
    active_only_raw = data.get("active_only", False)
    if isinstance(active_only_raw, bool):
        active_only = active_only_raw
    else:
        active_only = str(active_only_raw).strip().lower() in ["1", "true", "yes", "y"]

    if service not in ["manyreach", "smartlead", "sendread_campaign", "sendread_list"]:
        raise HTTPException(status_code=400, detail="Unsupported service")
    if not api_key:
        raise HTTPException(status_code=400, detail="API key is required")

    try:
        if service == "manyreach":
            integration = ManyReachIntegration(api_key)
            campaigns = integration.get_campaigns()
        elif service == "smartlead":
            integration = SmartLeadIntegration(api_key)
            campaigns = integration.get_campaigns(active_only=active_only)
        elif service == "sendread_campaign":
            integration = SendReadIntegration(api_key)
            campaigns = integration.get_campaigns()
        else:
            integration = SendReadIntegration(api_key)
            campaigns = integration.get_ab_test_lists()
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
    catch_all_only: bool = False,
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
            "btrim(email) != ''"
        ]
        params = [campaign_id]

        if exclude_public_emails:
            placeholders = ','.join(['%s' for _ in PUBLIC_EMAIL_DOMAINS])
            conditions.append(f"lower(split_part(email, '@', 2)) NOT IN ({placeholders})")
            params.extend(PUBLIC_EMAIL_DOMAINS)

        query = f"""
            SELECT * FROM contacts
            WHERE {' AND '.join(conditions)}
            ORDER BY id
        """
        cursor.execute(query, tuple(params))
        contacts = [dict(row) for row in cursor.fetchall()]

    filtered_contacts = [
        contact for contact in contacts
        if _matches_export_status_filter(contact, valid_only, include_catch_all, catch_all_only)
    ]
    preview_contacts = filtered_contacts[:5]

    if template['service'] == 'manyreach':
        integration = ManyReachIntegration("")
        manyreach_campaign_id = template['api_config'].get('manyreach_campaign_id', '')
        preview_data = []
        for contact in preview_contacts:
            transformed = integration.transform_contact(contact, template['field_mappings'], manyreach_campaign_id)
            preview_data.append(transformed)

        return {
            "template_name": template['name'],
            "service": template['service'],
            "total_contacts": len(filtered_contacts),
            "preview_data": preview_data,
            "field_mappings": template['field_mappings']
        }

    if template['service'] == 'smartlead':
        integration = SmartLeadIntegration("")
        preview_data = []
        for contact in preview_contacts:
            transformed = integration.transform_contact(contact, template['field_mappings'])
            preview_data.append(transformed)

        return {
            "template_name": template['name'],
            "service": template['service'],
            "total_contacts": len(filtered_contacts),
            "preview_data": preview_data,
            "field_mappings": template['field_mappings']
        }

    if template['service'] in ['sendread_campaign', 'sendread_list']:
        integration = SendReadIntegration("")
        preview_data = []
        for contact in preview_contacts:
            transformed = integration.transform_contact(contact, template['field_mappings'])
            preview_data.append(transformed)

        return {
            "template_name": template['name'],
            "service": template['service'],
            "total_contacts": len(filtered_contacts),
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
        elif template['service'] in ['sendread_campaign', 'sendread_list']:
            integration = SendReadIntegration(template['api_config'].get('api_key', ''))
            if recent_exports >= integration.rate_limit:
                raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Max {integration.rate_limit} exports per minute.")

        # Get batch offset and export filters from request data
        offset = data.get('offset', 0)
        export_valid_only = data.get('export_valid_only', False)
        export_catch_all = data.get('export_catch_all', False)
        export_catch_all_only = data.get('export_catch_all_only', False)
        exclude_public_emails = data.get('exclude_public_emails', False)

        conditions = [
            "campaign_id = %s",
            "email IS NOT NULL",
            "btrim(email) != ''"
        ]
        params = [campaign_id]

        if exclude_public_emails:
            placeholders = ','.join(['%s' for _ in PUBLIC_EMAIL_DOMAINS])
            conditions.append(f"lower(split_part(email, '@', 2)) NOT IN ({placeholders})")
            params.extend(PUBLIC_EMAIL_DOMAINS)

        query = f"""
            SELECT * FROM contacts
            WHERE {' AND '.join(conditions)}
            ORDER BY id
        """

        cursor.execute(query, tuple(params))
        all_candidates = [dict(row) for row in cursor.fetchall()]
        eligible_contacts = [
            contact for contact in all_candidates
            if _matches_export_status_filter(contact, export_valid_only, export_catch_all, export_catch_all_only)
        ]
        contacts = eligible_contacts[offset:offset + batch_size]

        if not contacts:
            cursor.execute("""
                SELECT * FROM contacts
                WHERE campaign_id = %s
                  AND email IS NOT NULL
                  AND btrim(email) != ''
            """, (campaign_id,))
            all_email_contacts = [dict(row) for row in cursor.fetchall()]

            contacts_with_email = len(all_email_contacts)
            contacts_valid_email = 0
            contacts_catch_all_email = 0
            contacts_valid_or_catch = 0
            for contact in all_email_contacts:
                normalized_status = _resolve_contact_email_status(contact)
                is_valid = _is_valid_email_status(normalized_status)
                is_catch_all = _is_catch_all_email_status(normalized_status)
                if is_valid:
                    contacts_valid_email += 1
                if is_catch_all:
                    contacts_catch_all_email += 1
                if is_valid or is_catch_all:
                    contacts_valid_or_catch += 1

            summary = {
                "total_contacts": len(all_email_contacts),
                "contacts_with_email": contacts_with_email,
                "contacts_valid_email": contacts_valid_email,
                "contacts_catch_all_email": contacts_catch_all_email,
                "contacts_valid_or_catch": contacts_valid_or_catch
            }

            has_status_match_before_public = any(
                _matches_export_status_filter(contact, export_valid_only, export_catch_all, export_catch_all_only)
                for contact in all_email_contacts
            )
            has_status_match_after_public = any(
                _matches_export_status_filter(contact, export_valid_only, export_catch_all, export_catch_all_only)
                and not _is_public_email_address(contact.get("email", ""))
                for contact in all_email_contacts
            )

            if summary["contacts_with_email"] == 0:
                detail = f"No contacts with email found in campaign (total contacts: {summary['total_contacts']})"
            elif export_catch_all_only and summary["contacts_catch_all_email"] == 0:
                detail = (
                    "No contacts with catch-all email found for export filters "
                    f"(contacts with email: {summary['contacts_with_email']}, catch-all: {summary['contacts_catch_all_email']})"
                )
            elif export_valid_only and export_catch_all and summary["contacts_valid_or_catch"] == 0:
                detail = (
                    "No contacts with valid/catch-all email found for export filters "
                    f"(contacts with email: {summary['contacts_with_email']}, valid/catch-all: {summary['contacts_valid_or_catch']})"
                )
            elif export_valid_only and summary["contacts_valid_email"] == 0:
                detail = (
                    "No contacts with valid email found for export filters "
                    f"(contacts with email: {summary['contacts_with_email']}, valid: {summary['contacts_valid_email']})"
                )
            elif exclude_public_emails and has_status_match_before_public and not has_status_match_after_public:
                detail = (
                    "No contacts left after excluding public email providers "
                    f"(contacts with email before filter: {summary['contacts_with_email']})"
                )
            else:
                detail = (
                    "No contacts found for current export filters "
                    f"(contacts with email: {summary['contacts_with_email']})"
                )

            raise HTTPException(status_code=404, detail=detail)

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

        if template['service'] in ['sendread_campaign', 'sendread_list']:
            integration = SendReadIntegration(template['api_config'].get('api_key', ''))
            target_id = str(template['api_config'].get('sendread_target_id', '')).strip()
            target_type = str(template['api_config'].get('sendread_target_type', '')).strip().lower()

            if not target_id:
                raise HTTPException(status_code=400, detail="SendRead target ID is missing in template configuration")

            if not target_type:
                target_type = "campaign" if template['service'] == "sendread_campaign" else "ab_test_list"

            transformed_contacts = []
            for contact in contacts:
                transformed = integration.transform_contact(contact, field_mappings)
                if integration.validate_contact(transformed):
                    transformed_contacts.append(transformed)

            if not transformed_contacts:
                raise HTTPException(status_code=400, detail="No valid contacts to export (email is required)")

            try:
                if target_type == "campaign":
                    api_response = integration.export_to_campaign(target_id, transformed_contacts)
                    endpoint = "https://app.sendread.co/api/public/campaigns/{id}/leads"
                elif target_type == "ab_test_list":
                    api_response = integration.export_to_ab_test_list(target_id, transformed_contacts)
                    endpoint = "https://app.sendread.co/api/public/ab-test-lists/{id}/leads"
                else:
                    raise HTTPException(status_code=400, detail="Invalid SendRead target type in template configuration")

                cursor.execute("""
                    INSERT INTO export_logs (campaign_id, template_id, contacts_exported, status)
                    VALUES (%s, %s, %s, %s)
                """, (campaign_id, template_id, len(transformed_contacts), "completed"))
                conn.commit()

                return {
                    "status": "Export completed successfully",
                    "service": template['service'],
                    "contacts_exported": len(transformed_contacts),
                    "api_response": api_response,
                    "endpoint": endpoint
                }
            except HTTPException:
                raise
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

    if template["service"] in ["sendread_campaign", "sendread_list"]:
        integration = SendReadIntegration(template["api_config"].get("api_key", ""))
        target_id = str(template["api_config"].get("sendread_target_id", "")).strip()
        target_type = str(template["api_config"].get("sendread_target_type", "")).strip().lower()

        if not target_id:
            raise HTTPException(status_code=400, detail="SendRead target ID is missing in template configuration")

        if not target_type:
            target_type = "campaign" if template["service"] == "sendread_campaign" else "ab_test_list"

        transformed = integration.transform_contact(contact, field_mappings)
        if not integration.validate_contact(transformed):
            raise HTTPException(status_code=400, detail="Selected lead is missing email")

        try:
            if target_type == "campaign":
                api_response = integration.export_to_campaign(target_id, [transformed])
            elif target_type == "ab_test_list":
                api_response = integration.export_to_ab_test_list(target_id, [transformed])
            else:
                raise HTTPException(status_code=400, detail="Invalid SendRead target type in template configuration")

            return {
                "status": "Test export completed successfully",
                "service": template["service"],
                "contact_id": contact_id,
                "transformed_contact": transformed,
                "api_response": api_response
            }
        except HTTPException:
            raise
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

@app.get("/api/campaign/{campaign_id}/verification-progress")
async def get_campaign_verification_progress(campaign_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        _ensure_campaign_exists(cursor, campaign_id)
        cursor.execute("""
            SELECT
                COUNT(*) FILTER (
                    WHERE email IS NOT NULL AND btrim(email) != ''
                ) AS total_emails,
                COUNT(*) FILTER (
                    WHERE email IS NOT NULL
                      AND btrim(email) != ''
                      AND coalesce(nullif(btrim(email_status), ''), 'unverified') != 'unverified'
                ) AS processed_emails
            FROM contacts
            WHERE campaign_id = %s
        """, (campaign_id,))
        counts = cursor.fetchone() or {}

    total_emails = int(counts.get("total_emails") or 0)
    processed_emails = int(counts.get("processed_emails") or 0)
    latest_job = None
    active_job = None

    with verification_jobs_lock:
        for job in verification_jobs.values():
            if job.get("campaign_id") != campaign_id:
                continue
            timestamp = job.get("started_at") or job.get("completed_at") or ""
            if latest_job is None or timestamp > (latest_job.get("started_at") or latest_job.get("completed_at") or ""):
                latest_job = dict(job)
            if job.get("status") in {"queued", "running"}:
                if active_job is None or timestamp > (active_job.get("started_at") or ""):
                    active_job = dict(job)

    return _compute_verification_progress_payload(
        campaign_id=campaign_id,
        total_emails=total_emails,
        processed_emails=processed_emails,
        active_job=active_job,
        latest_job=latest_job,
    )

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
            SELECT
                coalesce(nullif(btrim(email_status), ''), nullif(btrim(status), ''), 'unknown') AS email_status,
                COUNT(*) as count
            FROM contacts 
            WHERE campaign_id = %s AND email IS NOT NULL AND btrim(email) != ''
            GROUP BY email_status
        """, (campaign_id,))
        statuses = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT
                id,
                email,
                coalesce(nullif(btrim(email_status), ''), nullif(btrim(status), ''), 'unknown') AS email_status,
                status AS raw_status
            FROM contacts 
            WHERE campaign_id = %s AND email IS NOT NULL AND btrim(email) != ''
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
        
        # Build SQL query with selected fields. Use aliases matching CSV field names.
        select_expressions = [
            f"{available_fields[field]} AS {field}"
            for field in selected_fields
        ]

        # If city is requested but address is not, include address as hidden helper field
        # so city can be derived from address for export output.
        needs_city_fallback = 'city' in selected_fields
        if needs_city_fallback and 'address' not in selected_fields:
            select_expressions.append("address AS __address_fallback")

        query = f"SELECT {', '.join(select_expressions)} FROM contacts WHERE campaign_id = %s ORDER BY id"
        
        cursor.execute(query, (campaign_id,))
        contacts = [dict(row) for row in cursor.fetchall()]
        
        if not contacts:
            raise HTTPException(status_code=404, detail="No contacts found for this campaign")
        
        # Create CSV in memory with selected fields only
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=selected_fields)
        writer.writeheader()
        
        for contact in contacts:
            if needs_city_fallback and not str(contact.get('city') or '').strip():
                fallback_address = contact.get('address') if 'address' in selected_fields else contact.get('__address_fallback')
                derived_city = extract_city_from_address(fallback_address)
                if derived_city:
                    contact['city'] = derived_city

            csv_row = {}
            for field in selected_fields:
                value = contact.get(field, '')
                csv_row[field] = '' if value is None else value
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
                        "ignore_duplicate_leads_in_other_campaign": False
                    },
                    "endpoint": "/campaigns/{campaign_id}/leads",
                    "method": "POST"
                }
            )

        # Check if SendRead campaign template already exists
        cursor.execute("SELECT id FROM export_templates WHERE service = 'sendread_campaign'")
        if not cursor.fetchone():
            integration = SendReadIntegration("")
            default_mapping = integration.get_default_field_mapping()

            TemplateManager.create_template(
                "SendRead Campaign Default",
                "sendread_campaign",
                default_mapping,
                {
                    "api_key": "",
                    "sendread_target_id": "",
                    "sendread_target_type": "campaign",
                    "endpoint": "/api/public/campaigns/{campaign_id}/leads",
                    "method": "POST"
                }
            )

        # Check if SendRead list template already exists
        cursor.execute("SELECT id FROM export_templates WHERE service = 'sendread_list'")
        if not cursor.fetchone():
            integration = SendReadIntegration("")
            default_mapping = integration.get_default_field_mapping()

            TemplateManager.create_template(
                "SendRead AB List Default",
                "sendread_list",
                default_mapping,
                {
                    "api_key": "",
                    "sendread_target_id": "",
                    "sendread_target_type": "ab_test_list",
                    "endpoint": "/api/public/ab-test-lists/{list_id}/leads",
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
