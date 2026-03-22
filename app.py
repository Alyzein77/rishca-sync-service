"""
Rishca OS Financial Sync Service
Runs alongside N8N on Railway. Receives financial snapshot JSON,
updates Excel files and pitch deck, returns download links.
"""

import os
import json
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx
import base64
import logging

from sync_budget import update_budget_xlsx
from sync_model import update_financial_model
from sync_slides import update_pitch_slides

logger = logging.getLogger("rishca-sync")

app = FastAPI(title="Rishca Sync Service", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILES_DIR = Path(os.getenv("FILES_DIR", "/app/files"))
TEMPLATES_DIR = FILES_DIR / "templates"
OUTPUT_DIR = FILES_DIR / "output"
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Supabase config (set via Railway env vars)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://llrvrcgwhvcaqvscpnsi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
SYNC_API_KEY = os.getenv("SYNC_API_KEY", "rishca-sync-2026")

# Default hourly rates (EGP) per team member — used when Vitalis only has hours
DEFAULT_HOURLY_RATES = {
    "Rana El Sobky": 250, "Abaza": 200, "Alaa Ashraf": 200,
    "Nada Amin": 200, "Ahmed Hamdy": 200, "Yasseen Nouh": 200,
    "Amr Tarek": 200, "Tarek Mohamed": 200, "Amal Hamdy": 150,
    "Aml Hamdy": 150, "Anas Emad": 150, "Bahaa Mohamed": 150,
    "Bahaa Lashin": 150, "Aly Zein Eldin": 0,  # founder — no hourly cost
}
DEFAULT_EGP_TO_USD = 0.020  # 1 EGP ≈ 0.02 USD (50 EGP/USD)

# Google Drive config
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "1OQ_sebrvhbrbZQMVMDpCUTTKIKCbu7nJ")
GDRIVE_SERVICE_ACCOUNT_JSON = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "")


# ============================================================================
# GOOGLE DRIVE UPLOAD HELPER
# ============================================================================

def _get_gdrive_service():
    """Build Google Drive API service from service account credentials."""
    if not GDRIVE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        # Credentials can be base64-encoded JSON or raw JSON
        try:
            cred_json = json.loads(base64.b64decode(GDRIVE_SERVICE_ACCOUNT_JSON))
        except Exception:
            cred_json = json.loads(GDRIVE_SERVICE_ACCOUNT_JSON)

        credentials = service_account.Credentials.from_service_account_info(
            cred_json,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        return build("drive", "v3", credentials=credentials)
    except Exception as e:
        logger.warning(f"Google Drive init failed: {e}")
        return None


def upload_to_gdrive(file_path: str, folder_id: str = None) -> dict:
    """Upload a file to Google Drive. Returns file metadata or error."""
    service = _get_gdrive_service()
    if not service:
        return {"error": "Google Drive not configured (set GDRIVE_SERVICE_ACCOUNT_JSON)"}

    from googleapiclient.http import MediaFileUpload

    folder = folder_id or GDRIVE_FOLDER_ID
    filename = Path(file_path).name

    # Check if file already exists in folder (update instead of duplicate)
    # Search by name prefix (without timestamp) to find previous versions
    base_name = filename.split("_202")[0]  # e.g. "Team_Budget_Clean"
    query = f"'{folder}' in parents and name contains '{base_name}' and trashed=false"

    try:
        existing = service.files().list(q=query, fields="files(id,name)").execute()
        existing_files = existing.get("files", [])
    except Exception:
        existing_files = []

    # Determine MIME type
    if filename.endswith(".xlsx"):
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif filename.endswith(".pptx"):
        mime = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    else:
        mime = "application/octet-stream"

    media = MediaFileUpload(file_path, mimetype=mime, resumable=True)

    try:
        if existing_files:
            # Update the most recent existing file
            file_id = existing_files[0]["id"]
            updated = service.files().update(
                fileId=file_id,
                media_body=media,
                body={"name": filename},
                fields="id,name,webViewLink",
            ).execute()
            return {"id": updated["id"], "name": updated["name"],
                    "url": updated.get("webViewLink", ""), "action": "updated"}
        else:
            # Create new file
            file_metadata = {"name": filename, "parents": [folder]}
            created = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id,name,webViewLink",
            ).execute()
            return {"id": created["id"], "name": created["name"],
                    "url": created.get("webViewLink", ""), "action": "created"}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# VITALIS DATA TRANSFORMER
# ============================================================================

def transform_vitalis_to_snapshot(payload: dict) -> dict:
    """
    Transform raw Vitalis API response (from N8N) into the snapshot format
    expected by sync_budget.py and sync_model.py.

    Vitalis entries have: logged_at, duration_minutes, user.name, task, workspace_name
    Snapshot expects: team_costs.monthly_summary[].by_member[].{name, year, month, hours, amount, hourly_rate}
    """
    vitalis_data = payload.get("vitalis_data", {})
    entries = vitalis_data.get("entries", [])

    # If vitalis_data is a list (the entries themselves), handle that too
    if isinstance(vitalis_data, list):
        entries = vitalis_data
        vitalis_data = {}

    egp_to_usd = DEFAULT_EGP_TO_USD

    # Group entries by year-month and member
    monthly_buckets = {}  # key: "YYYY-MM" -> {member_name: {minutes, entries_count}}

    for entry in entries:
        logged_at = entry.get("logged_at", "")
        if not logged_at:
            continue

        # Parse date: "2026-03-21T00:00:00+00:00"
        date_part = logged_at[:10]  # "2026-03-21"
        parts = date_part.split("-")
        if len(parts) < 3:
            continue
        year, month = int(parts[0]), int(parts[1])
        month_key = f"{year}-{month:02d}"

        # Get member name
        user_info = entry.get("user", {})
        if isinstance(user_info, str):
            member_name = user_info
        else:
            member_name = user_info.get("name", "Unknown")

        duration_minutes = float(entry.get("duration_minutes", 0))

        if month_key not in monthly_buckets:
            monthly_buckets[month_key] = {}
        if member_name not in monthly_buckets[month_key]:
            monthly_buckets[month_key][member_name] = 0.0
        monthly_buckets[month_key][member_name] += duration_minutes

    # Build monthly_summary in the format sync_budget expects
    monthly_summary = []
    annual_totals_egp = {}

    for month_key in sorted(monthly_buckets.keys()):
        parts = month_key.split("-")
        year, month = int(parts[0]), int(parts[1])
        total_egp = 0.0
        by_member = []

        for member_name, total_minutes in monthly_buckets[month_key].items():
            hours = total_minutes / 60.0
            hourly_rate = DEFAULT_HOURLY_RATES.get(member_name, 175)
            amount = hours * hourly_rate

            by_member.append({
                "name": member_name,
                "year": year,
                "month": month,
                "hours": round(hours, 2),
                "hourly_rate": hourly_rate,
                "amount": round(amount, 2),
                "employment_type": "Hourly",
            })
            total_egp += amount

        total_usd = total_egp * egp_to_usd

        monthly_summary.append({
            "month": month_key,
            "total_egp": round(total_egp, 2),
            "total_usd": round(total_usd, 2),
            "by_member": by_member,
        })

        # Accumulate annual totals
        fy = f"fy{year - 2000}"
        annual_totals_egp[fy] = annual_totals_egp.get(fy, 0) + total_egp

    # Build the full snapshot
    snapshot = {
        "generated_at": payload.get("generated_at", datetime.utcnow().isoformat() + "Z"),
        "scenario": payload.get("scenario", "base"),
        "currency": {"egp_to_usd": egp_to_usd},
        "team_costs": {
            "monthly_summary": monthly_summary,
            "annual_totals_egp": annual_totals_egp,
            "annual_totals_usd": {k: v * egp_to_usd for k, v in annual_totals_egp.items()},
        },
        "vitalis_summary": {
            "total_entries": len(entries),
            "total_hours": vitalis_data.get("total_hours", 0),
            "team_count": len(vitalis_data.get("team_summary", [])),
        },
    }

    # Pass through optional fields from webhook body
    if payload.get("assumptions"):
        snapshot["assumptions"] = payload["assumptions"]
    if payload.get("customer_growth"):
        snapshot["customer_growth"] = payload["customer_growth"]
    if payload.get("tool_costs"):
        snapshot["tool_costs"] = payload["tool_costs"]

    return snapshot


# ============================================================================
# MODELS
# ============================================================================

class TeamCostEntry(BaseModel):
    name: str
    year: int
    month: int
    hours: float
    amount: float
    hourly_rate: float
    employment_type: str = "Hourly"

class MonthlyTeamCost(BaseModel):
    month: str  # "2025-03"
    total_egp: float
    total_usd: float
    by_member: list[TeamCostEntry] = []

class Assumptions(BaseModel):
    pricing_light: float = 100
    pricing_growth: float = 180
    pricing_pro: float = 350
    pricing_enterprise: float = 999
    churn_rate: float = 0.03
    egp_to_usd: float = 0.020

class CustomerGrowth(BaseModel):
    prefund_quarterly: list[int] = []
    postfund_annual: list[int] = []

class PandL(BaseModel):
    scenario: str = "base"
    revenue: dict = {}
    costs: dict = {}
    net_income: dict = {}
    closing_cash: dict = {}

class Customers(BaseModel):
    by_plan: dict = {}
    by_geo: dict = {}
    totals: dict = {}

class FinancialSnapshot(BaseModel):
    generated_at: str = ""
    scenario: str = "base"
    team_costs: dict = {}
    tool_costs: dict = {}
    assumptions: Optional[Assumptions] = None
    customer_growth: Optional[CustomerGrowth] = None
    p_and_l: Optional[PandL] = None
    customers: Optional[Customers] = None


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/")
async def health():
    return {
        "service": "rishca-sync",
        "version": "1.2.0",
        "status": "healthy",
        "vitalis_transform": True,
        "gdrive_configured": bool(GDRIVE_SERVICE_ACCOUNT_JSON),
        "templates_loaded": {
            "budget": (TEMPLATES_DIR / "Team_Budget_Clean.xlsx").exists(),
            "model": (TEMPLATES_DIR / "Rishca_OS_Financial_Model.xlsx").exists(),
            "slides": (TEMPLATES_DIR / "Rishca_OS_Financial_Pitch_Slides.pptx").exists(),
        }
    }


# ============================================================================
# TEMPLATE UPLOAD (one-time setup)
# ============================================================================

@app.post("/upload-templates")
async def upload_templates(
    budget: UploadFile = File(None),
    model: UploadFile = File(None),
    slides: UploadFile = File(None),
):
    """Upload template Excel/PPTX files. Do this once when setting up the service."""
    results = {}
    for name, file in [("Team_Budget_Clean.xlsx", budget),
                        ("Rishca_OS_Financial_Model.xlsx", model),
                        ("Rishca_OS_Financial_Pitch_Slides.pptx", slides)]:
        if file:
            dest = TEMPLATES_DIR / name
            with open(dest, "wb") as f:
                shutil.copyfileobj(file.file, f)
            results[name] = "uploaded"
    return {"status": "ok", "uploaded": results}


# ============================================================================
# FETCH DATA FROM SUPABASE (called by N8N or directly)
# ============================================================================

@app.get("/api/fetch-snapshot")
async def fetch_snapshot(scenario: str = "base"):
    """Fetch all data from Supabase and return a financial snapshot JSON."""
    if not SUPABASE_KEY:
        raise HTTPException(400, "SUPABASE_SERVICE_KEY not configured")

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    async with httpx.AsyncClient(base_url=SUPABASE_URL) as client:
        # Fetch time entries
        r = await client.get("/rest/v1/time_entries?select=*", headers=headers)
        time_entries = r.json() if r.status_code == 200 else []

        # Fetch team members
        r = await client.get("/rest/v1/team_members?select=*", headers=headers)
        team_members = r.json() if r.status_code == 200 else []

        # Fetch FX rates (latest)
        r = await client.get("/rest/v1/fx_rates?order=rate_date.desc&limit=1", headers=headers)
        fx_data = r.json() if r.status_code == 200 else []
        egp_to_usd = fx_data[0]["usd_egp"] if fx_data else 50.0
        egp_to_usd = 1.0 / egp_to_usd if egp_to_usd > 1 else egp_to_usd

    # Aggregate team costs by month
    monthly = {}
    for entry in time_entries:
        key = f"{entry.get('year', 0)}-{entry.get('month', 0):02d}"
        if key not in monthly:
            monthly[key] = {"month": key, "total_egp": 0, "total_usd": 0, "by_member": []}
        amount = float(entry.get("amount", 0))
        monthly[key]["total_egp"] += amount
        monthly[key]["total_usd"] += amount * egp_to_usd
        monthly[key]["by_member"].append({
            "name": entry.get("member_name", ""),
            "year": entry.get("year", 0),
            "month": entry.get("month", 0),
            "hours": float(entry.get("hours", 0)),
            "amount": amount,
            "hourly_rate": float(entry.get("hourly_rate", 0)),
        })

    # Aggregate by fiscal year (FY26 = Jul 2025 – Jun 2026, etc.)
    annual = {}
    for key, data in monthly.items():
        parts = key.split("-")
        year, month = int(parts[0]), int(parts[1])
        # Simple FY mapping: calendar year = FY
        fy = f"fy{year - 2000}"
        if fy not in annual:
            annual[fy] = 0
        annual[fy] += data["total_egp"]

    snapshot = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "scenario": scenario,
        "currency": {"egp_to_usd": egp_to_usd},
        "team_costs": {
            "monthly_summary": list(monthly.values()),
            "annual_totals_egp": annual,
            "annual_totals_usd": {k: v * egp_to_usd for k, v in annual.items()},
        },
        "team_members": team_members,
    }
    return snapshot


# ============================================================================
# MAIN SYNC ENDPOINT (called by N8N with full snapshot)
# ============================================================================

@app.post("/api/sync")
async def sync_all(snapshot: dict):
    """
    Receives a financial snapshot JSON (from N8N or direct call).
    If the payload contains 'vitalis_data', transforms it first.
    Updates Budget XLSX → Financial Model XLSX → Pitch Slides PPTX.
    Returns paths to generated files.
    """
    # Auto-detect and transform Vitalis API format
    if "vitalis_data" in snapshot:
        snapshot = transform_vitalis_to_snapshot(snapshot)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    results = {"timestamp": timestamp, "files": {}}

    # Step 1: Update Team Budget
    budget_template = TEMPLATES_DIR / "Team_Budget_Clean.xlsx"
    if budget_template.exists():
        budget_output = OUTPUT_DIR / f"Team_Budget_Clean_{timestamp}.xlsx"
        try:
            update_budget_xlsx(str(budget_template), str(budget_output), snapshot)
            results["files"]["budget"] = str(budget_output)
        except Exception as e:
            results["files"]["budget_error"] = str(e)

    # Step 2: Update Financial Model
    model_template = TEMPLATES_DIR / "Rishca_OS_Financial_Model.xlsx"
    if model_template.exists():
        model_output = OUTPUT_DIR / f"Rishca_OS_Financial_Model_{timestamp}.xlsx"
        try:
            update_financial_model(str(model_template), str(model_output), snapshot)
            results["files"]["model"] = str(model_output)
        except Exception as e:
            results["files"]["model_error"] = str(e)

    # Step 3: Update Pitch Slides
    # IMPORTANT: Use the TEMPLATE model (not output) because openpyxl
    # wipes cached formula results on save. The template has correct
    # values from when it was last saved in Excel.
    slides_template = TEMPLATES_DIR / "Rishca_OS_Financial_Pitch_Slides.pptx"
    model_for_slides = str(model_template)
    if slides_template.exists() and Path(model_for_slides).exists():
        slides_output = OUTPUT_DIR / f"Rishca_OS_Financial_Pitch_Slides_{timestamp}.pptx"
        try:
            update_pitch_slides(model_for_slides, str(slides_template), str(slides_output))
            results["files"]["slides"] = str(slides_output)
        except Exception as e:
            results["files"]["slides_error"] = str(e)

    # Return download URLs
    results["download_urls"] = {}
    for key, path in results["files"].items():
        if not key.endswith("_error"):
            filename = Path(path).name
            results["download_urls"][key] = f"/download/{filename}"

    # Auto-upload to Google Drive if configured
    if GDRIVE_SERVICE_ACCOUNT_JSON:
        results["gdrive"] = {}
        for key, path in results["files"].items():
            if not key.endswith("_error") and Path(path).exists():
                try:
                    gdrive_result = upload_to_gdrive(path)
                    results["gdrive"][key] = gdrive_result
                except Exception as e:
                    results["gdrive"][f"{key}_error"] = str(e)

    return results


# ============================================================================
# SIMPLE SYNC (no JSON needed — fetches from Supabase directly)
# ============================================================================

@app.post("/api/sync-from-db")
async def sync_from_db(scenario: str = "base"):
    """One-click sync: fetches data from Supabase, runs full pipeline."""
    snapshot = await fetch_snapshot(scenario)
    return await sync_all(snapshot)


# ============================================================================
# ONE-CLICK SYNC VIA VITALIS API (no N8N needed)
# ============================================================================

VITALIS_API_URL = os.getenv(
    "VITALIS_API_URL",
    "https://llrvrcgwhvcaqvscpnsi.supabase.co/functions/v1/team-time-log"
)
VITALIS_API_KEY = os.getenv("VITALIS_API_KEY", "TiQwbG47NLUKYhg")


@app.post("/api/sync-now")
async def sync_now(scenario: str = "base", days: int = 365):
    """
    One-click sync: fetches time data from Vitalis Edge Function,
    transforms it, and runs the full Budget → Model → Slides pipeline.
    This is the endpoint the Lovable dashboard 'Sync Now' button should call.
    """
    # Step 1: Fetch from Vitalis API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            r = await client.get(
                f"{VITALIS_API_URL}?days={days}",
                headers={"x-api-key": VITALIS_API_KEY},
            )
            r.raise_for_status()
            vitalis_data = r.json()
        except Exception as e:
            raise HTTPException(502, f"Failed to fetch from Vitalis API: {e}")

    # Step 2: Build payload in the format transform_vitalis_to_snapshot expects
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "scenario": scenario,
        "source": "sync-now",
        "vitalis_data": vitalis_data,
    }

    # Step 3: Transform and run full sync pipeline
    snapshot = transform_vitalis_to_snapshot(payload)
    return await sync_all(snapshot)


@app.get("/api/sync-now")
async def sync_now_get(scenario: str = "base", days: int = 365):
    """GET version of sync-now for easy browser/button triggers."""
    return await sync_now(scenario=scenario, days=days)


# ============================================================================
# FILE DOWNLOAD
# ============================================================================

@app.get("/download/{filename}")
async def download_file(filename: str):
    filepath = OUTPUT_DIR / filename
    if not filepath.exists():
        raise HTTPException(404, f"File not found: {filename}")
    return FileResponse(str(filepath), filename=filename)


@app.get("/api/latest-files")
async def latest_files():
    """List the most recent output files."""
    files = sorted(OUTPUT_DIR.glob("*"), key=lambda f: f.stat().st_mtime, reverse=True)
    return [{"name": f.name, "size": f.stat().st_size, "url": f"/download/{f.name}"} for f in files[:20]]
