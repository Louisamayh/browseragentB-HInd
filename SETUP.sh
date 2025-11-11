#!/bin/bash
# CallM_BH Setup Script for Mac/Linux
# Run this once to install CallM_BH

echo "=========================================="
echo "🔧 CallM_BH Setup"
echo "=========================================="
echo ""

# Check Python
echo "📋 Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed!"
    echo "Please install Python 3.11 or higher from python.org"
    exit 1
fi

python3 --version
echo "✅ Python found"

# Create virtual environment
echo ""
echo "📦 Creating virtual environment..."
if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Recreating..."
    rm -rf venv
fi

python3 -m venv venv
echo "✅ Virtual environment created"

# Activate and install dependencies
echo ""
echo "📥 Installing dependencies..."
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Check for .env file
echo ""
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found"
    read -p "Do you want to enter your Google API Key now? (y/n): " create_env
    if [ "$create_env" = "y" ] || [ "$create_env" = "Y" ]; then
        read -p "Enter your Google API Key: " api_key
        echo "GOOGLE_API_KEY=$api_key" > .env
        echo "✅ .env file created"
    else
        echo "ℹ️  You can create .env later with your GOOGLE_API_KEY"
    fi
else
    echo "✅ .env file found"
fi

# Create desktop shortcut (alias to .app)
echo ""
echo "🖥️  Creating desktop shortcut..."
DESKTOP="$HOME/Desktop"
APP_PATH="$(pwd)/CallM_BH.app"

if [ -d "$APP_PATH" ]; then
    # Create an alias (symlink) on the desktop
    if [ -L "$DESKTOP/CallM_BH.app" ]; then
        rm "$DESKTOP/CallM_BH.app"
    fi
    ln -s "$APP_PATH" "$DESKTOP/CallM_BH.app"

    if [ -L "$DESKTOP/CallM_BH.app" ]; then
        echo "✅ Desktop shortcut created"
    else
        echo "⚠️  Could not create desktop shortcut"
        echo "    You can manually drag CallM_BH.app to your desktop"
    fi
else
    echo "⚠️  CallM_BH.app not found"
fi

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "📱 To start CallM_BH:"
echo "   1. Double-click the CallM_BH icon on your desktop"
echo "   2. Or double-click: CallM_BH.app (in this folder)"
echo "   3. Or run: ./CallM_BH.bat"
echo "=========================================="
