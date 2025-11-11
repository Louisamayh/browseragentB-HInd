#!/usr/bin/env python3
"""
CallM_BH API Server
Web interface for UK business lookup and contact enrichment
"""

import os
import sys
import asyncio
import uuid
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Set environment variables before importing phase modules
os.environ.setdefault("GOOGLE_API_KEY", os.getenv("GOOGLE_API_KEY", ""))
os.environ.setdefault("PARTIAL_EVERY", "20")

# Import common utilities (these don't read INPUT_CSV at import time)
from common import read_rows, sniff_dialect_and_header

# NOTE: phase1_discovery and phase2_contacts are imported inside functions
# to ensure environment variables are set before they read INPUT_CSV

# ============================================================================
# Configuration
# ============================================================================

UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("output")
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================================
# Data Models
# ============================================================================

@dataclass
class JobStatus:
    job_id: str
    status: str  # "pending", "running", "completed", "failed", "stopped"
    phase: str  # "phase1", "phase2", "completed"
    progress: float  # 0.0 to 1.0
    current_row: int
    total_rows: int
    message: str
    input_file: str
    output_file_phase1: Optional[str] = None
    output_file_phase2: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    error: Optional[str] = None

class StartJobRequest(BaseModel):
    skip_phase1: bool = False
    skip_phase2: bool = False

class StopJobRequest(BaseModel):
    job_id: str

# ============================================================================
# Global State
# ============================================================================

jobs: Dict[str, JobStatus] = {}
current_job_id: Optional[str] = None
stop_requested = False

# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(title="CallM_BH API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Helper Functions
# ============================================================================

def get_row_count(file_path: str) -> int:
    """Count total rows in CSV file (excluding header)"""
    try:
        rows = read_rows(file_path)
        return max(0, len(rows) - 1)  # Exclude header
    except Exception:
        return 0

async def run_phase1(job_id: str, input_file: str):
    """Run Phase 1: Discovery"""
    global stop_requested

    job = jobs[job_id]
    job.phase = "phase1"
    job.status = "running"
    job.message = "Running Phase 1: Discovering company information..."
    job.updated_at = datetime.now().isoformat()

    try:
        # Set environment variables for phase 1
        os.environ["INPUT_CSV"] = input_file
        output_file = str((OUTPUT_DIR / f"{job_id}_phase1_output.csv").absolute())
        os.environ["OUTPUT_CSV"] = output_file
        os.environ["PARTIAL_CSV"] = str((OUTPUT_DIR / f"{job_id}_phase1_partial.csv").absolute())

        # Import phase1 module AFTER setting environment variables
        from phase1_discovery import main as phase1_main

        # Run phase 1
        await phase1_main()

        if stop_requested:
            job.status = "stopped"
            job.message = "Job stopped by user during Phase 1"
            return False

        job.output_file_phase1 = output_file
        job.message = "Phase 1 completed successfully"
        return True

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Phase 1 failed: {str(e)}"
        return False

async def run_phase2(job_id: str):
    """Run Phase 2: Contact Enrichment"""
    global stop_requested

    job = jobs[job_id]
    job.phase = "phase2"
    job.status = "running"
    job.message = "Running Phase 2: Finding contacts and LinkedIn profiles..."
    job.updated_at = datetime.now().isoformat()

    try:
        # Phase 2 uses Phase 1 output as input
        if not job.output_file_phase1 or not Path(job.output_file_phase1).exists():
            raise Exception("Phase 1 output not found. Run Phase 1 first.")

        # Set environment variables for phase 2
        os.environ["INPUT_CSV"] = job.output_file_phase1
        output_file = str((OUTPUT_DIR / f"{job_id}_phase2_output.csv").absolute())
        os.environ["OUTPUT_CSV"] = output_file
        os.environ["PARTIAL_CSV_PHASE2"] = str((OUTPUT_DIR / f"{job_id}_phase2_partial.csv").absolute())

        # Import phase2 module AFTER setting environment variables
        from phase2_contacts import main as phase2_main

        # Run phase 2
        await phase2_main()

        if stop_requested:
            job.status = "stopped"
            job.message = "Job stopped by user during Phase 2"
            return False

        job.output_file_phase2 = output_file
        job.message = "Phase 2 completed successfully"
        return True

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Phase 2 failed: {str(e)}"
        return False

async def run_job(job_id: str, skip_phase1: bool = False, skip_phase2: bool = False):
    """Run the complete job (Phase 1 + Phase 2)"""
    global stop_requested, current_job_id

    stop_requested = False
    current_job_id = job_id
    job = jobs[job_id]

    try:
        # Phase 1: Discovery
        if not skip_phase1:
            success = await run_phase1(job_id, job.input_file)
            if not success:
                return

        # Phase 2: Contacts
        if not skip_phase2:
            success = await run_phase2(job_id)
            if not success:
                return

        # Job completed
        job.status = "completed"
        job.phase = "completed"
        job.progress = 1.0
        job.message = "All phases completed successfully!"
        job.updated_at = datetime.now().isoformat()

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Job failed: {str(e)}"
        job.updated_at = datetime.now().isoformat()
    finally:
        current_job_id = None

# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "CallM_BH API"}

@app.post("/api/upload")
async def upload_file(file: UploadFile):
    """Upload input CSV file"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    # Generate unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{file.filename}"
    file_path = UPLOAD_DIR / filename

    # Save file
    try:
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        # Validate CSV
        try:
            rows = read_rows(str(file_path))
            if len(rows) < 2:  # At least header + 1 row
                raise Exception("CSV must have at least one data row")

            # Check for required columns (ADDRESS, POSTCODE)
            header = rows[0]
            header_lower = [col.lower().strip() for col in header]

            has_address = any('address' in h for h in header_lower)
            has_postcode = any('postcode' in h or 'post_code' in h for h in header_lower)

            if not has_address or not has_postcode:
                raise Exception("CSV must have 'ADDRESS' and 'POSTCODE' columns")

        except Exception as e:
            file_path.unlink()  # Delete invalid file
            raise HTTPException(status_code=400, detail=f"Invalid CSV: {str(e)}")

        return {
            "filename": filename,
            "path": str(file_path.absolute()),
            "size": len(content),
            "rows": len(rows) - 1  # Exclude header
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

@app.post("/api/jobs/start")
async def start_job(request: StartJobRequest, background_tasks: BackgroundTasks, file_path: str):
    """Start a new job"""

    # Validate input file exists
    if not Path(file_path).exists():
        raise HTTPException(status_code=404, detail="Input file not found")

    # Create job
    job_id = str(uuid.uuid4())
    row_count = get_row_count(file_path)

    job = JobStatus(
        job_id=job_id,
        status="pending",
        phase="phase1" if not request.skip_phase1 else "phase2",
        progress=0.0,
        current_row=0,
        total_rows=row_count,
        message="Job created, starting...",
        input_file=file_path,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )

    jobs[job_id] = job

    # Start job in background
    background_tasks.add_task(
        run_job,
        job_id,
        request.skip_phase1,
        request.skip_phase2
    )

    return {"job_id": job_id}

@app.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Get job status"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    # Update progress by checking partial files
    if job.status == "running":
        if job.phase == "phase1":
            partial_file = OUTPUT_DIR / f"{job_id}_phase1_partial.csv"
            if partial_file.exists():
                try:
                    partial_rows = get_row_count(str(partial_file))
                    job.current_row = partial_rows
                    if job.total_rows > 0:
                        job.progress = min(0.45, partial_rows / job.total_rows * 0.5)  # Phase 1 = 0-50%
                except Exception:
                    pass

        elif job.phase == "phase2":
            partial_file = OUTPUT_DIR / f"{job_id}_phase2_partial.csv"
            if partial_file.exists():
                try:
                    partial_rows = get_row_count(str(partial_file))
                    job.current_row = partial_rows
                    if job.total_rows > 0:
                        job.progress = min(0.95, 0.5 + (partial_rows / job.total_rows * 0.5))  # Phase 2 = 50-100%
                except Exception:
                    pass

    return asdict(job)

@app.post("/api/jobs/{job_id}/stop")
async def stop_job(job_id: str):
    """Stop a running job"""
    global stop_requested

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    if job.status != "running":
        raise HTTPException(status_code=400, detail="Job is not running")

    stop_requested = True
    job.status = "stopped"
    job.message = "Stopping job..."
    job.updated_at = datetime.now().isoformat()

    return {"message": "Stop request sent"}

@app.get("/api/jobs/{job_id}/download/{phase}")
async def download_output(job_id: str, phase: str):
    """Download output file for a specific phase"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    if phase == "phase1":
        file_path = job.output_file_phase1
    elif phase == "phase2":
        file_path = job.output_file_phase2
    else:
        raise HTTPException(status_code=400, detail="Invalid phase. Use 'phase1' or 'phase2'")

    if not file_path or not Path(file_path).exists():
        # Try partial file
        partial_file = OUTPUT_DIR / f"{job_id}_{phase}_partial.csv"
        if partial_file.exists():
            file_path = str(partial_file)
        else:
            raise HTTPException(status_code=404, detail=f"Output file for {phase} not found")

    return FileResponse(
        file_path,
        media_type="text/csv",
        filename=f"{job_id}_{phase}_output.csv"
    )

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs"""
    return {"jobs": [asdict(job) for job in jobs.values()]}

@app.post("/api/shutdown")
async def shutdown():
    """Shutdown the server"""
    print("\n🛑 Shutdown requested via API")
    os._exit(0)

# ============================================================================
# Static Files & Root
# ============================================================================

# Serve static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    """Serve the main HTML page"""
    static_dir = Path("static")
    index_file = static_dir / "index.html"

    if not index_file.exists():
        return JSONResponse({
            "message": "CallM_BH API Server",
            "docs": "/docs",
            "health": "/api/health"
        })

    return FileResponse(str(index_file))

# ============================================================================
# Main
# ============================================================================

def main():
    """Start the API server"""
    print("=" * 60)
    print("🚀 CallM_BH API Server")
    print("=" * 60)
    print(f"📁 Upload directory: {UPLOAD_DIR.absolute()}")
    print(f"📁 Output directory: {OUTPUT_DIR.absolute()}")
    print(f"🌐 Server starting at: http://localhost:8000")
    print("=" * 60)

    # Check for .env file
    env_file = Path(".env")
    if not env_file.exists():
        print("\n⚠️  WARNING: .env file not found!")
        print("   Please create a .env file with your GOOGLE_API_KEY")
        print("   Example: GOOGLE_API_KEY=your_key_here")
        print()

    # Check for static directory
    static_dir = Path("static")
    if not static_dir.exists():
        print("\n⚠️  WARNING: static directory not found!")
        print("   Web interface will not be available")
        print("   API endpoints will still work at /api/*")
        print()

    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n\n❌ Server error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
