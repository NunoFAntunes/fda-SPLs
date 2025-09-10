#!/usr/bin/env python3
import os
import random
import shutil
from pathlib import Path
import glob

def main():
    base_dir = Path(r"C:\code\fda-SPLs\data\extracted")
    
    # Source directories
    rx_parts = [
        "dm_spl_release_human_rx_part1",
        "dm_spl_release_human_rx_part2", 
        "dm_spl_release_human_rx_part3",
        "dm_spl_release_human_rx_part4",
        "dm_spl_release_human_rx_part5"
    ]
    
    # Destination directory
    subsection_dir = base_dir / "subsection"
    subsection_dir.mkdir(exist_ok=True)
    
    print("Collecting all XML files...")
    all_xml_files = []
    
    for part in rx_parts:
        part_dir = base_dir / part
        if part_dir.exists():
            xml_pattern = str(part_dir / "**" / "*.xml")
            xml_files = glob.glob(xml_pattern, recursive=True)
            all_xml_files.extend(xml_files)
            print(f"Found {len(xml_files)} XML files in {part}")
    
    total_files = len(all_xml_files)
    print(f"Total XML files found: {total_files}")
    
    if total_files < 200:
        print(f"Warning: Only {total_files} files available, but 200 requested")
        num_to_select = total_files
    else:
        num_to_select = 200
    
    print(f"Randomly selecting {num_to_select} files...")
    selected_files = random.sample(all_xml_files, num_to_select)
    
    print("Copying files to subsection directory...")
    copied_count = 0
    
    for file_path in selected_files:
        file_path = Path(file_path)
        
        # Create a unique filename to avoid conflicts
        dest_filename = f"{file_path.parent.name}_{file_path.name}"
        dest_path = subsection_dir / dest_filename
        
        # If filename already exists, add a counter
        counter = 1
        original_dest_path = dest_path
        while dest_path.exists():
            stem = original_dest_path.stem
            suffix = original_dest_path.suffix
            dest_path = subsection_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        
        try:
            shutil.copy2(file_path, dest_path)
            copied_count += 1
            if copied_count % 50 == 0:
                print(f"Copied {copied_count}/{num_to_select} files...")
        except Exception as e:
            print(f"Error copying {file_path}: {e}")
    
    print(f"Successfully copied {copied_count} XML files to {subsection_dir}")

if __name__ == "__main__":
    main()