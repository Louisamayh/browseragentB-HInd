#!/usr/bin/env python3 
import os
import sys
import argparse
import asyncio
from pathlib import Path

from dotenv import load_dotenv

def parse_args():
    p = argparse.ArgumentParser(
        description="Run Phase 1 (discovery) then Phase 2 (contacts) sequentially."
    )
    # Files
    p.add_argument("--input", default="input.csv",
                   help="Phase 1 input CSV (default: input.csv)")
    p.add_argument("--core-output", default="output.core.csv",
                   help="Phase 1 output CSV / Phase 2 input (default: output.core.csv)")
    p.add_argument("--final-output", default="output.with_contacts.csv",
                   help="Phase 2 output CSV (default: output.with_contacts.csv)")

    # Shared knobs
    p.add_argument("--max-steps", type=int, default=120,
                   help="Agent max steps per run (default: 120)")
    p.add_argument("--row-retries", type=int, default=5,
                   help="Full lookup attempts per row (default: 5)")
    p.add_argument("--retry-start-sleep", type=float, default=1.0,
                   help="Initial retry delay seconds (default: 1.0)")
    p.add_argument("--retry-backoff-base", type=float, default=1.8,
                   help="Retry exponential backoff base (default: 1.8)")
    p.add_argument("--retry-max-sleep", type=float, default=12.0,
                   help="Max retry delay seconds (default: 12.0)")

    # Optional: run only a phase
    p.add_argument("--skip-phase1", action="store_true",
                   help="Skip Phase 1 (use existing core output)")
    p.add_argument("--skip-phase2", action="store_true",
                   help="Skip Phase 2 (only run discovery)")

    return p.parse_args()

def set_env_for_phases(args):
    """
    Set env vars BEFORE importing the phase modules so they pick up overrides.
    """
    # Files
    os.environ["INPUT_CSV"] = args.input
    os.environ["OUTPUT_CSV"] = args.core_output
    os.environ["INPUT_CSV_PHASE2"] = args.core_output
    os.environ["OUTPUT_CSV_PHASE2"] = args.final_output

    # Knobs (shared)
    os.environ["MAX_STEPS"] = str(args.max_steps)
    os.environ["ROW_RETRIES"] = str(args.row_retries)
    os.environ["RETRY_START_SLEEP"] = str(args.retry_start_sleep)
    os.environ["RETRY_BACKOFF_BASE"] = str(args.retry_backoff_base)
    os.environ["RETRY_MAX_SLEEP"] = str(args.retry_max_sleep)

async def run_all(args):
    # Phase 1
    if not args.skip_phase1:
        print("🚀 Running Phase 1 (discovery)...")
        try:
            from phase1_discovery import main as phase1_main  # import AFTER env set
        except Exception as e:
            print(f"❌ Failed to import phase1_discovery: {e}")
            sys.exit(1)

        await phase1_main()

        core_path = Path(args.core_output)
        if not core_path.exists():
            print(f"❌ Expected Phase 1 output missing: {core_path}")
            sys.exit(1)
        print(f"✅ Phase 1 complete. Output: {core_path}")
    else:
        print("⏭️ Skipping Phase 1 as requested.")

    # Phase 2
    if not args.skip_phase2:
        print("👤 Running Phase 2 (contacts)...")
        try:
            from phase2_contacts import main as phase2_main  # import AFTER env set
        except Exception as e:
            print(f"❌ Failed to import phase2_contacts: {e}")
            sys.exit(1)

        # Sanity check the input for phase 2
        core_path = Path(args.core_output)
        if not core_path.exists():
            print(f"❌ Phase 2 needs {core_path} (from Phase 1).")
            sys.exit(1)

        await phase2_main()
        final_path = Path(args.final_output)
        if not final_path.exists():
            print(f"❌ Expected Phase 2 output missing: {final_path}")
            sys.exit(1)
        print(f"✅ Phase 2 complete. Output: {final_path}")
    else:
        print("⏭️ Skipping Phase 2 as requested.")

def main():
    load_dotenv(override=True)

    if not os.getenv("GOOGLE_API_KEY"):
        print("❌ GOOGLE_API_KEY not found. Add it to your environment or .env file.")
        sys.exit(1)

    args = parse_args()
    set_env_for_phases(args)

    try:
        asyncio.run(run_all(args))
    except KeyboardInterrupt:
        print("\n🛑 Interrupted.")
        sys.exit(130)

if __name__ == "__main__":
    main()
