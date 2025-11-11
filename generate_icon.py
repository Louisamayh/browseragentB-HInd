#!/usr/bin/env python3
"""
Generate CallM_BH app icons for Mac and Windows
Creates a target/bullseye icon representing precision targeting
"""

import os
import sys
from pathlib import Path

try:
    from PIL import Image, ImageDraw
except ImportError:
    print("❌ Pillow not installed. Installing...")
    os.system(f"{sys.executable} -m pip install Pillow")
    from PIL import Image, ImageDraw

def create_icon(size=1024):
    """Create a target/bullseye icon representing CallM_BH precision"""
    # Create image with transparent background
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    center = size // 2

    # Color palette - purple/blue gradient tones
    colors = [
        (102, 126, 234, 255),   # Outer ring - light purple
        (118, 75, 162, 255),     # Second ring - medium purple
        (67, 56, 202, 255),      # Third ring - dark purple
        (99, 102, 241, 255),     # Center - vibrant blue-purple
    ]

    # Draw concentric circles (target/bullseye)
    ring_sizes = [
        (center * 0.95, colors[0]),  # Outermost ring
        (center * 0.75, colors[1]),  # Second ring
        (center * 0.55, colors[2]),  # Third ring
        (center * 0.35, colors[3]),  # Center bullseye
    ]

    for ring_size, color in ring_sizes:
        x1 = center - ring_size
        y1 = center - ring_size
        x2 = center + ring_size
        y2 = center + ring_size
        draw.ellipse([x1, y1, x2, y2], fill=color)

    # Add white center dot for target precision
    center_size = center * 0.15
    x1 = center - center_size
    y1 = center - center_size
    x2 = center + center_size
    y2 = center + center_size
    draw.ellipse([x1, y1, x2, y2], fill=(255, 255, 255, 255))

    # Add subtle highlight for depth
    highlight_offset = size * 0.15
    highlight_size = center * 0.6
    x1 = center - highlight_offset - highlight_size
    y1 = center - highlight_offset - highlight_size
    x2 = center - highlight_offset + highlight_size
    y2 = center - highlight_offset + highlight_size
    draw.ellipse([x1, y1, x2, y2], fill=(255, 255, 255, 30))

    return img

def save_png_icon(img, path):
    """Save as PNG"""
    img.save(path, 'PNG')
    print(f"✅ Created: {path}")

def save_icns_icon(img, path):
    """Save as macOS .icns file"""
    sizes = [16, 32, 64, 128, 256, 512, 1024]
    iconset_path = Path(str(path).replace('.icns', '.iconset'))

    # Create iconset directory
    iconset_path.mkdir(exist_ok=True)

    # Generate all required sizes
    for sz in sizes:
        resized = img.resize((sz, sz), Image.Resampling.LANCZOS)
        resized.save(iconset_path / f"icon_{sz}x{sz}.png")

        # Also create @2x versions for retina
        if sz <= 512:
            resized_2x = img.resize((sz * 2, sz * 2), Image.Resampling.LANCZOS)
            resized_2x.save(iconset_path / f"icon_{sz}x{sz}@2x.png")

    # Convert iconset to icns using iconutil (Mac only)
    if sys.platform == 'darwin':
        os.system(f'iconutil -c icns "{iconset_path}" -o "{path}"')
        # Clean up iconset directory
        import shutil
        shutil.rmtree(iconset_path)
        print(f"✅ Created: {path}")
    else:
        print(f"⚠️  Skipping .icns creation (requires macOS)")

def save_ico_icon(img, path):
    """Save as Windows .ico file"""
    sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    img.save(path, format='ICO', sizes=sizes)
    print(f"✅ Created: {path}")

def main():
    print("=" * 60)
    print("🎯 CallM_BH Icon Generator")
    print("=" * 60)
    print()

    # Create icon
    print("📐 Generating icon...")
    icon = create_icon(1024)

    # Save in different formats
    print("\n💾 Saving icons...")

    # PNG (universal)
    save_png_icon(icon, "icon.png")

    # macOS .icns
    save_icns_icon(icon, "icon.icns")

    # Windows .ico
    save_ico_icon(icon, "icon.ico")

    print()
    print("=" * 60)
    print("✅ Icon generation complete!")
    print("=" * 60)
    print()
    print("📁 Generated files:")
    print("   • icon.png   - Universal PNG icon")
    print("   • icon.icns  - macOS app icon")
    print("   • icon.ico   - Windows app icon")
    print()

if __name__ == "__main__":
    main()
