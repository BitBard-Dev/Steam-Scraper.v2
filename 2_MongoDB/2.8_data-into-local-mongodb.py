## Step 8: Upload Game JSON data to a Locally Hosted MongoDB Collection
# Step 8.1: Split JSON File into Smaller Chunks (<16MB Each)
# MongoDB will thrown an error if uploaded files are >~16MB. 1000 entries/chunk worked, leading to 118x chunks total.
import json
import os

# File paths
# Note: "PATH" in INPUT_FILE/OUTPUT_DIR are the file path to the saved 
# ??? Correction: ensure file name is correct from Step XXX, 
# v30.only_games.josn is depricated & was only used to demonstrate proficiency for Python course while the (72hr) API query completed
INPUT_FILE = "C:\\PATH\\v30.only_games.json" 
OUTPUT_DIR = "C:\\PATH\\JSON_CHUNKS_GAME\\"
CHUNK_SIZE = 1000  # Adjust based on document size

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def split_json(file_path, chunk_size):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)  # Load entire JSON

    # Check if data is a dictionary instead of a list (verifies that Step 4 worked properly)
    if isinstance(data, dict):
        print("Warning: JSON file is a dictionary, extracting documents from values...")
        # Flatten the dictionary values into a list
        documents = []
        for key, value in data.items():
            if isinstance(value, list):  # Ensure we only extract lists
                documents.extend(value)
        
        data = documents  # Replace data with extracted documents list

    # Split the JSON safely
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        output_file = os.path.join(OUTPUT_DIR, f"chunk_{i//chunk_size}.json")

        with open(output_file, "w", encoding="utf-8") as f_out:
            json.dump(chunk, f_out, indent=4, ensure_ascii=False) # Ensure ascii=False to allow Unicode text

        print(f"Created {output_file}")

split_json(INPUT_FILE, CHUNK_SIZE)

print(f"Finished splitting \n{INPUT_FILE}")

# Step 8.2: Batch Import Chunked JSON Files Using Windows PowerShell (Could be done manually, but WAY slower...)
# POWERSHELL: Import all chunked JSON files into locally hosted MongoDB server
"""Get-ChildItem -Path "C:{PATH}" -Filter *.json | 
ForEach-Object { mongoimport --db v30_steam_games --collection v30_only_games --file $_.FullName --jsonArray }
"""