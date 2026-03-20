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

from sync_budget import update_budget_xlsx
from sync_model import update_financial_model
from sync_slides import update_pitch_slides

app = FastAPI(title="Rishca Sync Service", version="1.0.0")

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
        "status": "healthy",
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
    Updates Budget XLSX → Financial Model XLSX → Pitch Slides PPTX.
    Returns paths to generated files.
    """
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
    slides_template = TEMPLATES_DIR / "Rishca_OS_Financial_Pitch_Slides.pptx"
    model_for_slides = results["files"].get("model", str(model_template))
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
