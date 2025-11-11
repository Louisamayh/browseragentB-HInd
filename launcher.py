#!/usr/bin/env python3
"""
CallM_BH Launcher
Launches the web server and opens the browser
"""

import os
import sys
import time
import webbrowser
import subprocess
from pathlib import Path

def check_venv():
    """Check if virtual environment exists"""
    venv_path = Path("venv")
    if not venv_path.exists():
        venv_path = Path(".venv")

    if not venv_path.exists():
        print("❌ Virtual environment not found!")
        print("   Please run SETUP.sh (Mac) or SETUP.bat (Windows) first")
        input("\nPress Enter to exit...")
        sys.exit(1)

    return venv_path

def check_env_file():
    """Check if .env file exists"""
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found!")
        print("   Please create a .env file with your GOOGLE_API_KEY")
        print("   Example: GOOGLE_API_KEY=your_key_here")
        input("\nPress Enter to exit...")
        sys.exit(1)

def start_server():
    """Start the API server"""
    venv_path = check_venv()

    # Determine Python executable path
    if sys.platform == "win32":
        python_exe = venv_path / "Scripts" / "python.exe"
        activate_script = venv_path / "Scripts" / "activate.bat"
    else:
        python_exe = venv_path / "bin" / "python"
        activate_script = venv_path / "bin" / "activate"

    if not python_exe.exists():
        print(f"❌ Python not found in virtual environment: {python_exe}")
        input("\nPress Enter to exit...")
        sys.exit(1)

    # Start server
    print("=" * 60)
    print("🚀 Starting CallM_BH...")
    print("=" * 60)
    print()

    server_script = Path("api_server.py")
    if not server_script.exists():
        print("❌ api_server.py not found!")
        input("\nPress Enter to exit...")
        sys.exit(1)

    # Open browser after a short delay
    print("🌐 Opening browser at http://localhost:8000")
    time.sleep(2)
    webbrowser.open("http://localhost:8000")

    # Start server (this will block)
    try:
        subprocess.run([str(python_exe), str(server_script)])
    except KeyboardInterrupt:
        print("\n\n🛑 CallM_BH stopped")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        input("\nPress Enter to exit...")
        sys.exit(1)

def main():
    """Main launcher function"""
    # Change to script directory
    script_dir = Path(__file__).parent.absolute()
    os.chdir(script_dir)

    # Check requirements
    check_env_file()

    # Start server
    start_server()

if __name__ == "__main__":
    main()
