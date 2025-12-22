## Step 5: Clean steam_games_filtered.json to Remove Unnecessary Key-Value Pairs
import json

# File paths
INPUT_JSON = "steam_games_filtered.json"  # Your original JSON file
OUTPUT_JSON = "steam_games_cleaned.json"  # Cleaned JSON file

# Attributes to KEEP
keep_keys = {
    "type", "name", "steam_appid", "is_free", "about_the_game", "supported_languages",
    "developers", "publishers", "price_overview", "categories", "genres", "recommendations",
    "release_date", "content_descriptors", "ratings"
}

def clean_steam_json(input_file, output_file):
    """Loads, filters, and saves the cleaned JSON data."""
    try:
        # Load existing JSON
        with open(input_file, "r", encoding="utf-8") as f:
            games_data = json.load(f)
        
        # Keep only relevant attributes
        cleaned_data = [{k: v for k, v in game.items() if k in keep_keys} for game in games_data]

        # Save cleaned JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, indent=4, ensure_ascii=False)

        print(f"Successfully cleaned {len(cleaned_data)} game records. Saved to {output_file}")

    except Exception as e:
        print(f"Error processing JSON: {e}")

# Run the cleaning function
clean_steam_json(INPUT_JSON, OUTPUT_JSON)

## Step 6: Clean the supported_languages Values for Follow-On Analysis
import json
import re

# File paths
INPUT_JSON = "steam_games_cleaned.json"  # Step 5's cleaned JSON file
OUTPUT_JSON = "steam_games_cleaned_languages.json"  # JSON with cleaned languages

def clean_languages(lang_string):
    """Cleans the supported_languages string and categorizes full audio vs. interface/subtitles."""
    if not lang_string:
        return {"full_audio_languages": [], "interface_languages": []}

    # Extract full audio languages (inside <strong> tags)
    full_audio_matches = re.findall(r"<strong>(.*?)</strong>", lang_string)

    # Remove HTML tags & split by commas
    lang_string = re.sub(r"<.*?>", "", lang_string)  # Remove <strong>, <br>, etc.
    languages = [lang.strip() for lang in lang_string.split(",")]

    # Separate full audio and interface languages
    full_audio_languages = set(full_audio_matches)  # Convert to set to avoid duplicates
    interface_languages = [lang for lang in languages if lang not in full_audio_languages]

    return {
        "full_audio_languages": sorted(list(full_audio_languages)),  # Sort for consistency
        "interface_languages": sorted(interface_languages)  # Sort for consistency
    }

def clean_json_languages(input_file, output_file):
    """Processes JSON and cleans 'supported_languages'."""
    try:
        # Load existing JSON
        with open(input_file, "r", encoding="utf-8") as f:
            games_data = json.load(f)

        # Process each game's supported_languages
        for game in games_data:
            if "supported_languages" in game:
                game["supported_languages"] = clean_languages(game["supported_languages"])

        # Save updated JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(games_data, f, indent=4, ensure_ascii=False)

        print(f"Successfully cleaned and categorized languages for {len(games_data)} games.")
        print(f"Cleaned data saved to {output_file}")

    except Exception as e:
        print(f"Error processing JSON: {e}")

# Run the cleaning function
clean_json_languages(INPUT_JSON, OUTPUT_JSON)