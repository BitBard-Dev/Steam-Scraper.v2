## Step 7: Query SteamSpy to Retrieve Tags Data & Add it to steam_games_cleaned_languages.json

# Step 7.1: Create .csv file of current games for tracking purposes
# Should this step exist??? Should use the original steam_valid_apps_unique.csv to ensure consistency???
import json
import pandas as pd

# File paths
INPUT_JSON = "steam_games_cleaned_languages.json"  # Source JSON
OUTPUT_CSV = "steamspy_games.csv"  # List of games to query

def extract_games_for_steamspy_csv():
    """Extracts steam_appid and name from JSON and saves to CSV."""
    try:
        # Load JSON data
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            games_data = json.load(f)

        # Convert to DataFrame
        df = pd.DataFrame(games_data, columns=["steam_appid", "name"])

        # Save to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Extracted {len(df)} games. Saved to {OUTPUT_CSV}")

    except Exception as e:
        print(f"⚠️ Error processing JSON: {e}")

# Run function
extract_games_for_steamspy_csv()
# Step 7.2: Query SteamSpy API, Save Data, and Ensure All Games Were Queried
# Run in a separate Jupyter Notebook
import requests
import json
import pandas as pd
import time
import os

# API URL
STEAMSPY_API_URL = "https://steamspy.com/api.php?request=appdetails&appid={}"

# File paths
INPUT_CSV = "steamspy_games.csv"  # List of games to query
PROCESSED_CSV = "steamspy_processed_games.csv"  # Track completed queries
OUTPUT_JSON = "steam_games_allclean_tags.json"  # Save cleaned API responses

# SteamSpy Rate Limits
REQUEST_INTERVAL = 1  # 1 request per second

# Keys to extract from API response
STEAMSPY_KEYS = ["positive", "negative", "owners", "median_forever", "median_2weeks", "ccu", "tags"]

def load_processed_games():
    """Loads processed game appids from CSV to avoid duplicate queries."""
    if os.path.exists(PROCESSED_CSV):
        try:
            df = pd.read_csv(PROCESSED_CSV)
            return set(df["steam_appid"])
        except Exception as e:
            print(f"⚠️ Error reading processed games CSV: {e}")
    return set()

def save_processed_game(appid, name):
    """Logs processed games to CSV."""
    df = pd.DataFrame([[appid, name]], columns=["steam_appid", "name"])
    if not os.path.exists(PROCESSED_CSV):
        df.to_csv(PROCESSED_CSV, index=False)
    else:
        df.to_csv(PROCESSED_CSV, mode="a", index=False, header=False)

def fetch_steamspy_data(appid):
    """Fetches SteamSpy data for a given appid."""
    url = STEAMSPY_API_URL.format(appid)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ Error {response.status_code} for appid {appid}")
    except requests.RequestException as e:
        print(f"⚠️ Network error fetching {appid}: {e}")
    return None

def save_to_json(data, filename):
    """Appends new SteamSpy data to JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        existing_appids = {entry["steam_appid"] for entry in existing_data}
        new_entries = [entry for entry in data if entry["steam_appid"] not in existing_appids]

        existing_data.extend(new_entries)

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)

        print(f"✅ {len(new_entries)} new games added to {filename}")

    except Exception as e:
        print(f"⚠️ Error saving JSON: {e}")

def query_steamspy():
    """Queries SteamSpy API for all games listed in steamspy_games.csv."""
    games_df = pd.read_csv(INPUT_CSV)
    processed_games = load_processed_games()

    new_data = []
    for index, row in games_df.iterrows():
        appid, name = row["steam_appid"], row["name"]

        if appid in processed_games:
            print(f"⏭️ Skipping already processed appid: {appid}")
            continue

        print(f"🔍 Fetching data for {appid} - {name}")
        game_data = fetch_steamspy_data(appid)

        if game_data:
            filtered_data = {key: game_data.get(key, None) for key in STEAMSPY_KEYS}
            filtered_data["steam_appid"] = appid  # Ensure appid is included
            new_data.append(filtered_data)

            # Log processed game
            save_processed_game(appid, name)

        # Respect API rate limit
        time.sleep(REQUEST_INTERVAL)

    # Save all new data
    if new_data:
        save_to_json(new_data, OUTPUT_JSON)
    print("✅ SteamSpy data collection complete.")

# Run the function
query_steamspy()

# Ensure All Steam Apps Were Processed by Comparing steamspy_games.csv and steamspy_processed_games.csv
# File paths
VALID_APPS_CSV = "steamspy_games.csv"  # Apps that should have been processed
PROCESSED_APPS_CSV = "steamspy_processed_games.csv"  # Apps that were actually processed
MISSING_APPS_CSV = "steamspy_missing_apps.csv"  # (Optional) Output file for missing apps

# Run the comparison
find_missing_apps()

## Step 7.3: Ensure SteamSpy JSON data is incorporated into Steam AppDetails API JSON
#???

## Step 8: Upload Game JSON data to a Locally Hosted MongoDB Collection
# Step 8.1: Split JSON File into Smaller Chunks (<16MB Each)
# MongoDB will thrown an error if uploaded files are >~16MB. 1000 entries/chunk worked, leading to 118x chunks total.
import json
import os

# File paths
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
        print("⚠️ Warning: JSON file is a dictionary, extracting documents from values...")
        # Flatten the dictionary values into a list
        documents = []
        for key, value in data.items():
            if isinstance(value, list):  # Ensure we only extract lists
                documents.extend(value)
        
        data = documents  # Replace data with extracted documents list

    # 🔹 Now split the JSON safely
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        output_file = os.path.join(OUTPUT_DIR, f"chunk_{i//chunk_size}.json")

        with open(output_file, "w", encoding="utf-8") as f_out:
            json.dump(chunk, f_out, indent=4, ensure_ascii=False) # Ensure ascii=False to allow Unicode text

        print(f"✅ Created {output_file}")

split_json(INPUT_FILE, CHUNK_SIZE)

print(F"Finished splitting \n{INPUT_FILE}")

# Step 8.2: Batch Import Chunked JSON Files Using Windows PowerShell (Could be done manually, but WAY slower...)
# POWERSHELL: Import all chunked JSON files into locally hosted MongoDB server
"""Get-ChildItem -Path "C:{PATH}" -Filter *.json | 
ForEach-Object { mongoimport --db v30_steam_games --collection v30_only_games --file $_.FullName --jsonArray }
"""

## Step 9: Create MongoDB Tables & Insert Data

# Step 9.1: Install Necessary Libraries
# pip install pymongo
# !pip install pymongo
# import MongoClient
from pymongo import MongoClient
# import Pretty Print for easier viewing of data witin Jupyter Notebook
from pprint import pprint
# import SQLite
import sqlite3
# import Decimal128
from bson.decimal128 import Decimal128
# import datetime
from datetime import datetime

# Step 9.2: Connect to MongoDB Collection
# Connect to Atlas MongoDB Cluster0
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
# Define target database & collection
db = client.PROJECT_PHASE_2
coll = db.STEAM_GAMES_NOSTEAMSPY

# Step 9.3: Check for Unique steam_appid Values
from collections import Counter

# Extract all steam_appid values
appid_list = [game["steam_appid"] for game in coll.find({}, {"steam_appid": 1})]

# Count occurrences of each steam_appid
duplicate_appids = [appid for appid, count in Counter(appid_list).items() if count > 1]

print(f"⚠️ Duplicate steam_appids found: {duplicate_appids}")

# Step 9.4: Delete Duplicate Documents in MongoDB Collection
from pymongo import MongoClient

# Find all duplicates
pipeline = [
    {"$group": {"_id": "$steam_appid", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
    {"$match": {"count": {"$gt": 1}}}
]

duplicates = list(coll.aggregate(pipeline))

print(f"Found {len(duplicates)} duplicate steam_appid values.")

# Remove all but one document for each duplicate
for duplicate in duplicates:
    duplicate_ids = duplicate["docs"]  # List of _id values for the duplicate docs
    ids_to_delete = duplicate_ids[1:]  # Keep the first one, delete the rest

    delete_result = coll.delete_many({"_id": {"$in": ids_to_delete}})
    print(f"Deleted {delete_result.deleted_count} duplicates for steam_appid: {duplicate['_id']}")

print("✅ Duplicate cleanup complete!")

# Step 9.5: Re-Verify No Duplicates in MongoDB 
# Simply repeat Step 9.3

# Step 9.6: Create overall_info Table & Insert Data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('MITCHELL_PHASE2.sqlite')
cur = conn.cursor()

# define projection
projection_overall = {
    "steam_appid": 1,
    "type": 1,
    "name": 1,
    "release_date.date": 1,
    "release_date.coming_soon": 1,
    "about_the_game": 1,
    "developers": 1,
    "publishers": 1
}
all_games = coll.find({"type": "game"}, projection_overall)

# delete table if it exists (avoids manual delete of local .sql file)
cur.execute('DROP TABLE IF EXISTS overall_info')

# create "overall_info" table
cur.execute("""
    CREATE TABLE overall_info(
        steam_appid INTEGER PRIMARY KEY,
        type TEXT,
        name TEXT,
        release_date DATE,
        coming_soon BOOLEAN,
        about_the_game TEXT,
        developers TEXT,
        publishers TEXT
    )
""")
conn.commit()

# insert pulled data into MITCHELL_PHASE2.sqlite
for game in all_games:
    
    # Define complex column data here
    release_date_obj = game.get("release_date", {})
    raw_date = release_date_obj.get("date", None)
    coming_soon = bool(release_date_obj.get("coming_soon", False))
    developers = ", ".join(game.get("developers", [])) if isinstance(game.get("developers"), list) else "N/A"
    publishers = ", ".join(game.get("publishers", [])) if isinstance(game.get("publishers"), list) else "N/A"

    if isinstance(raw_date, str):
        try:
            formatted_date = datetime.strptime(raw_date, "%b %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            formatted_date = None
    else:
        formatted_date = None

    cur.execute("""
        INSERT INTO overall_info(
            steam_appid,
            type,
            name,
            release_date,
            coming_soon,
            about_the_game,
            developers,
            publishers)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        game.get("steam_appid", 0), # steam_appid # 0 will throw an error if steam_appid != UNIQUE
        game.get("type", "N/A"), # type
        game.get("name", "N/A"), # name
        formatted_date, # release_date
        coming_soon, # coming_soon
        game.get("about_the_game", "N/A"), # about_the_game
        developers, # developers
        publishers # publishers
    ))
conn.commit()

# Count rows in .sqlite file
cur.execute("SELECT COUNT(steam_appid) FROM overall_info")
total_entries_overall = cur.fetchone()[0]
print(f"There are {total_entries_overall} rows in the overall_info table.")

# Fetch and display first record
cur.execute("SELECT * FROM overall_info LIMIT 1")
first_game = cur.fetchone()
columns = ["steam_appid", "type", "name", "release_date", "coming_soon", "about_the_game", "developers", "publishers"]

if first_game:
    game_dict = dict(zip(columns, first_game))
    pprint(game_dict)
else:
    print("No records found in table")

# Close all connections
cur.close()
conn.close()

# Step 9.7: Create "pricing" table & insert data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('MITCHELL_PHASE2.sqlite')
cur = conn.cursor()

# define projection
projection_pricing = {
    "steam_appid": 1,
    "is_free": 1,
    "price_overview.currency": 1,
    "price_overview.discount_percent": 1,
    "price_overview.initial": 1,
    "price_overview.final": 1
}
all_games = coll.find({"type": "game"}, projection_pricing)

# delete table if it exists (avoids manual delete of local .sql file)
cur.execute('DROP TABLE IF EXISTS pricing_info')

# create "pricing_info" table
cur.execute("""
    CREATE TABLE pricing_info(
        steam_appid INTEGER PRIMARY KEY,
        is_free BOOLEAN,
        currency TEXT,
        discount_percent INTEGER,
        initial_price INTEGER,
        final_price INTEGER
    )
""")
conn.commit()

# insert pulled data into MITCHELL_PHASE2.sqlite
for game in all_games:
    
    # Define column data here
    steam_appid = game.get("steam_appid", 0)
    is_free = bool(game.get("is_free", False))
    price_overview = game.get("price_overview", {}) #dictionary within data
    currency = price_overview.get("currency", "N/A") 
    discount_percent = price_overview.get("discount_percent", 0)
    initial_price = price_overview.get("initial", None)
    final_price = price_overview.get("final", None)

    cur.execute("""
        INSERT INTO pricing_info(
            steam_appid,
            is_free,
            currency,
            discount_percent,
            initial_price,
            final_price)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        steam_appid,
        is_free,
        currency,
        discount_percent,
        initial_price,
        final_price
    ))
conn.commit()

# Count rows in .sqlite file
cur.execute("SELECT COUNT(steam_appid) FROM pricing_info")
total_entries_pricing = cur.fetchone()[0]
print(f"There are {total_entries_pricing} rows in the pricing_info table.")

# Fetch and display first record
cur.execute("SELECT * FROM pricing_info LIMIT 1")
first_game = cur.fetchone()
columns = ["steam_appid", "is_free", "currency", "discount_percent", "initial_price", "final_price"]

if first_game:
    game_dict = dict(zip(columns, first_game))
    pprint(game_dict)
else:
    print("No records found in table")

# Close all connections
cur.close()
conn.close()

# Step 9.8: Create "game_type" table & insert data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('MITCHELL_PHASE2.sqlite')
cur = conn.cursor()

# define projection
projection_gametype = {
    "steam_appid": 1,
    "categories": 1,
    "genres": 1,
    "recommendations": 1
}
all_games = coll.find({"type": "game"}, projection_gametype)

# delete table if it exists (avoids manual delete of local .sql file)
cur.execute('DROP TABLE IF EXISTS gametype_info')

# create "gametype_info" table
cur.execute("""
    CREATE TABLE gametype_info(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        steam_appid INTEGER,
        category_id INTEGER,
        category_description TEXT,
        genre_id INTEGER,
        genre_description TEXT,
        total_recommendations INTEGER
    )
""")
conn.commit()

# insert pulled data into MITCHELL_PHASE2.sqlite
for game in all_games:    
    # Define column data here
    steam_appid = int(game.get("steam_appid", 0))
    categories = game.get("categories", []) # list within data
    genres = game.get("genres", []) # list within data
    recommendations = game.get("recommendations", {}) #dictionary within data
    total_recommendations = recommendations.get("total", None) if recommendations else None
    
    if not categories:
        categories = [{"id": None, "description": None}]
    if not genres:
        genres = [{"id": None, "description": None}]

    for category in categories:
        category_id = category.get("id", 0)
        category_description = category.get("description", None)

        for genre in genres:
            genre_id = genre.get("id", 0)
            genre_description = genre.get("description", None)
        
        if category_id and genre_id:
            cur.execute("""
                INSERT INTO gametype_info(
                    steam_appid,
                    category_id,
                    category_description,
                    genre_id,
                    genre_description,
                    total_recommendations)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                steam_appid,
                category_id,
                category_description,
                genre_id,
                genre_description,
                total_recommendations
            ))
conn.commit()

# Count rows in .sqlite file
cur.execute("SELECT COUNT(id) FROM gametype_info")
total_entries_gametype = cur.fetchone()[0]
print(f"There are {total_entries_gametype} rows in the gametype_info table.")

# Count total distinct games in .sqlite file
cur.execute("SELECT COUNT(DISTINCT steam_appid) FROM gametype_info")
unique_games = cur.fetchone()[0]
print(f"There are {unique_games} unique games in the gametype_info table.\nGames lost: {96039 - unique_games}")

# Fetch and display first record
cur.execute("SELECT * FROM gametype_info WHERE steam_appid = 400")
portal_game = cur.fetchone()
columns = ["id", "steam_appid", "category_id", "category_description", "genre_id", "genre_description", "total_recommendations"]

if portal_game:
    game_dict = dict(zip(columns, portal_game))
    pprint(game_dict)
else:
    print("No records found in table")

# Close all connections
cur.close()
conn.close()

# Step 9.9: Create "language" table & insert data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('MITCHELL_PHASE2.sqlite')
cur = conn.cursor()

# define projection
projection_language = {
    "steam_appid": 1,
    "supported_languages.interface_languages": 1
}

all_games = coll.find({"type": "game"}, projection_language)

# delete table if it exists (avoids manual delete of local .sql file)
cur.execute('DROP TABLE IF EXISTS language_info')

# create "language_info" table
cur.execute("""
    CREATE TABLE language_info(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        steam_appid INTEGER,
        interface_languages TEXT
    )
""")
conn.commit()

# insert pulled data into MITCHELL_PHASE2.sqlite
for game in all_games:
    # Define column data here
    steam_appid = int(game.get("steam_appid", 0))
    supported_languages = game.get("supported_languages", {}) # dictionary within data
    interface_support = supported_languages.get("interface_languages", [])

    if not interface_support:
        continue

    for language in interface_support:
        cur.execute("""
            INSERT INTO language_info(
                steam_appid,
                interface_languages)
            VALUES (?, ?)
        """, (
            steam_appid,
            language
        ))
conn.commit()

# Count rows in .sqlite file
cur.execute("SELECT COUNT(id) FROM language_info")
total_entries_language = cur.fetchone()[0]
print(f"There are {total_entries_language} rows in the language_info table.")

# Count total distinct games in .sqlite file
cur.execute("SELECT COUNT(DISTINCT steam_appid) FROM language_info")
unique_games = cur.fetchone()[0]
print(f"There are {unique_games} unique games in the language_info table.\nGames lost: {96039 - unique_games}")

# Fetch and display first record
cur.execute("SELECT * FROM language_info WHERE steam_appid = 400")
portal_game = cur.fetchall()
columns = ["id", "steam_appid", "interface_languages", "audio_languages"]

if portal_game:
    for row in portal_game:
        game_dict = dict(zip(columns, row))
        pprint(game_dict)
else:
    print("No records found in table")

# Close all connections
cur.close()
conn.close()

# Step 9.10: Create "content_rating" table & insert data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('MITCHELL_PHASE2.sqlite')
cur = conn.cursor()

# define projection
projection_content = {
    "steam_appid": 1,
    "ratings": 1
}

all_games = coll.find({"type": "game"}, projection_content)

# delete table if it exists (avoids manual delete of local .sql file)
cur.execute('DROP TABLE IF EXISTS content_info')

# create "content_info" table
cur.execute("""
    CREATE TABLE content_info(
        steam_appid INTEGER PRIMARY KEY,
        esrb_rating TEXT,
        esrb_descriptors TEXT,
        pegi_rating TEXT
    )
""")
conn.commit()

# insert pulled data into MITCHELL_PHASE2.sqlite
for game in all_games:
    # Define column data here
    steam_appid = int(game.get("steam_appid", 0))
    ratings = game.get("ratings") or {}
    esrb = ratings.get("esrb") or {}
    rating = esrb.get("rating", None)
    descriptors = esrb.get("descriptors", None)
    pegi = ratings.get("pegi") or {}
    pegi_rating = pegi.get("rating", None)

    cur.execute("""
        INSERT INTO content_info(
            steam_appid,
            esrb_rating,
            esrb_descriptors,
            pegi_rating)
        VALUES (?, ?, ?, ?)
    """, (
        steam_appid,
        rating,
        descriptors,
        pegi_rating
    ))
conn.commit()

# Count rows in .sqlite file
cur.execute("SELECT COUNT(steam_appid) FROM content_info")
total_entries_content = cur.fetchone()[0]
print(f"There are {total_entries_content} rows in the content_info table.")

# Fetch and display first record
cur.execute("SELECT * FROM content_info WHERE steam_appid = 11480")
example_game = cur.fetchall()
columns = ["steam_appid", "esrb_rating", "esrb_descriptors", "pegi_rating"]

if example_game:
    for row in example_game:
        game_dict = dict(zip(columns, row))
        pprint(game_dict)
else:
    print("No records found in table")

# Close all connections
cur.close()
conn.close()

# Step 10: Clean data in .sqlite file
# Step 10.1: Clean language table by splitting tuples that are list-like strings (\n separated)
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load the languages table
df = pd.read_sql("SELECT * FROM languages", conn)

# Identify rows with multiple languages
df["supported_language"] = df["supported_language"].str.split("\n")  # Split on newlines
df = df.explode("supported_language")  # Expand each language into a new row

# Remove leading/trailing spaces
df["supported_language"] = df["supported_language"].str.strip()

# Save cleaned data back to SQLite (overwrite table)
df.to_sql("languages", conn, if_exists="replace", index=False)

# Close connection
conn.close()

# Step 10.2: Incrementally (AKA manually...) filter out valid language Entries and Update Tuples with Valid Languages
# SQL
"""SELECT * FROM languages 
WHERE supported_language NOT IN (
'English', 'French', 'German', 'Italian', 'Spanish - Spain', 'Simplified Chinese', 'Traditional Chinese', 'Korean', 'Russian', 'Dutch', 
'Finnish', 'Danish', 'Japanese', 'Norwegian', 'Polish', 'Portuguese - Portugal', 'Portuguese - Brazil', 'Romanian', 'Thai', 'Czech',
'Swedish', 'Bulgarian', 'Greek', 'Hungarian', 'Spanish - Latin America','Turkish', 'Ukrainian', 'Vietnamese', 'Indonesian', 'Spanish',
'Slovakian', 'Arabic', 'Catalan', 'Irish', 'Welsh', 'Persian', 'Filipino', 'Serbian', 'Slovak', 'Lithuanian', 
'Hebrew', 'Slovenian', 'Uyghur', 'Hindi', 'Bengali', 'Belarusian', 'Croatian', 'Latvian', 'Malay', 'Uzbek', 
'Basque', 'Icelandic', 'Azerbaijani', 'Albanian', 'Armenian', 'Afrikaans', 'Bosnian', 'Galician', 'Georgian', 'Gujarati',
'Kazakh', 'Kannada', 'Khmer', 'Kyrgyz', 'Macedonian', 'Marathi', 'Mongolian', 'Punjabi (Gurmukhi)', 'Punjabi (Shahmukhi)', 'Sinhalese',
'Tajik', 'Tamil', 'Telugu', 'Turkmen', 'Urdu', 'Estonian', 'Amharic', 'Assamese', 'Cherokee', 'Konkani',
'Dari', 'Hausa', 'Igbo', 'Yoruba', 'Luxembourgish', 'Malayalam', 'Maltese', 'Maori', 'Nepali', 'Odia', 
"K'iche'", 'Kinyarwanda', 'Quechua', 'Sotho', 'Sindhi', 'Sorani', 'Swahili', 'Tigrinya', 'Tswana', 'Tatar', 
'Wolof', 'Valencian', 'Xhosa', 'Zulu', 'Scots'
);

# SQL EXAMPLE
UPDATE languages
SET supported_language = 'English'
WHERE supported_language = 'İngilizce';

# SQL find number of distinct supported_language entries
SELECT COUNT(DISTINCT supported_language) FROM languages
"""

# Step 10.3: Check Tags Table for any null values and number of unique tags
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load the tags table
df = pd.read_sql("SELECT * FROM tags", conn)

# Check for null values
null_counts = df.isna().sum()
print("Total NULL values per column:")
print(null_counts)

# List number of unique tags
unique_tags_count = df["tag"].nunique()
print(f"Total unique tags: {unique_tags_count}")

# Close connection
conn.close()

# Step 10.4: Convert Foreign Currency Prices to USD
# Conversion rates manually pulled in FEB2025
import sqlite3

# Define conversion rates
conversion_rates = {
    "AUD": 0.63071128, "BRL": 0.173124, "CAD": 0.69539685, "CHF": 1.1358261, "CNY": 0.13823784,
    "EUR": 1.0835647, "GBP": 1.291715, "HKD": 0.12869107, "IDR": 0.000061366586, "JPY": 0.0067617847,
    "KRW": 0.0006902661, "MXN": 0.049244841, "MYR": 0.22646349, "PEN": 0.27332175, "PHP": 0.017409431,
    "PLN": 0.25967779, "RUB": 0.011263144, "SGD": 0.75125888, "VND": 0.000039197307, "ZAR": 0.054938251
}

# Connect to SQLite file
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
cursor = conn.cursor()

# Fetch all currency prices
cursor.execute("SELECT steam_appid, currency, price_final FROM overall WHERE price_final IS NOT NULL")
rows = cursor.fetchall()

# Track missing currency codes
missing_currencies = set()
updated_rows = 0  # Counter for debugging

# Convert prices to USD
for steam_appid, currency, price_final in rows:
    if currency in conversion_rates and price_final > 0:  # Ignore free games (price_final = 0)
        # Convert from cents to dollars, apply conversion rate, round to 2 decimals, convert back to cents
        usd_price = round((price_final / 100) * conversion_rates[currency], 2)  # USD price in dollars
        usd_price_cents = int(usd_price * 100)  # Convert back to cents

        # ✅ Corrected Update Statement (Updates both `price_final` and `currency`)
        cursor.execute(
            "UPDATE overall SET price_final = ?, currency = 'USD' WHERE steam_appid = ?",
            (usd_price_cents, steam_appid)
        )
        updated_rows += 1
    else:
        missing_currencies.add(currency)  # Track missing currencies

# Commit the changes
conn.commit()
conn.close()

# Debugging output
print(f"✅ {updated_rows} rows updated to USD!")
if missing_currencies:
    print(f"⚠️ Missing conversion rates for: {missing_currencies}")

# Step 10.5: Using SQL, Create new "revenue" Column with rea/float values for sales revenue calculation later
"""# SQL
ALTER TABLE overall ADD COLUMN price_usd REAL;

UPDATE overall SET price_usd = price_final / 100.0;

SELECT steam_appid, price_final, price_usd FROM overall LIMIT 10;
"""

# Step 10.6: Export Cleaned Data as 3 Separate .csv files (overall, tags, and languages)
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Save `overall` table
df_overall = pd.read_sql("SELECT * FROM overall", conn)
df_overall.to_csv("overall_cleaned.csv", index=False)
print("✅ Saved overall_cleaned.csv")

# Save `tags` table
df_tags = pd.read_sql("SELECT * FROM tags", conn)
df_tags.to_csv("tags_cleaned.csv", index=False)
print("✅ Saved tags_cleaned.csv")

# Save `languages` table
df_languages = pd.read_sql("SELECT * FROM languages", conn)
df_languages.to_csv("languages_cleaned.csv", index=False)
print("✅ Saved languages_cleaned.csv")

# Close connection
conn.close()

## Step 11: Data Analysis & Visualization
# Step 11.1: Word Cloud of tags
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load all tags from the `tags` table
df_tags = pd.read_sql("SELECT tag FROM tags", conn)

# Convert all tags into a single string
tag_text = " ".join(df_tags["tag"])

# Generate the word cloud
wordcloud = WordCloud(width=800, height=400, background_color="black", colormap="coolwarm").generate(tag_text)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")  # Hide axes
plt.title("Tags Word Cloud: Steam Marketplace")
plt.savefig('wordcloud_tags.png')
plt.show()

# Close database connection
conn.close()

# Step 11.2: Most Popular tags
# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load game titles from `overall` table
df_tags = pd.read_sql("SELECT tag FROM tags", conn)

# count # of each tag
tag_counts = df_tags["tag"].value_counts()

# Convert df for better readability
df_tag_counts = tag_counts.reset_index()
df_tag_counts.columns = ["tag", "count"]

# sort & display top 50 results
df_top_50 = df_tag_counts.head(25)
display(df_top_50)

conn.close()

# Step 11.3: Least Popular tags
# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Load game titles from `overall` table
df_tags = pd.read_sql("SELECT tag FROM tags", conn)
# count # of each tag
tag_counts = df_tags["tag"].value_counts()
# Convert df for better readability
df_tag_counts = tag_counts.reset_index()
df_tag_counts.columns = ["tag", "count"]
# sort & display top 50 results
df_top_50 = df_tag_counts.tail(25)
display(df_top_50)
conn.close()

# Step 11.4: Word Cloud of title words
from wordcloud import STOPWORDS

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load game titles from `overall` table
df_titles = pd.read_sql("SELECT name FROM overall", conn)

# Convert titles into a single string
title_text = " ".join(df_titles["name"])

# Define stopwords (common words to ignore)
stopwords = set(STOPWORDS)
stopwords.update(["Playtest", "Game"])  # Add any unwanted words

# Generate word cloud
# Why such a massive indent???
wordcloud_titles = WordCloud(width=800, height=400, background_color="black", colormap="coolwarm",
                             stopwords=stopwords).generate(title_text)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud_titles, interpolation="bilinear")
plt.axis("off")
plt.title("Titles Word Cloud: Steam Marketplace")
plt.savefig('wordcloud_titles.png')
plt.show()

# Close database connection
conn.close()

# Step 11.5: Most popular title words
import sqlite3
import pandas as pd
from wordcloud import STOPWORDS

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Load game titles from the `overall` table
df_titles = pd.read_sql("SELECT name FROM overall", conn)

# Split titles into individual words and flatten into a single list
words_series = df_titles["name"].str.split(expand=True).stack()

# Count occurrences of each word
word_counts = words_series.value_counts()

# Convert to DataFrame for better readability
df_word_counts = word_counts.reset_index()
df_word_counts.columns = ["word", "count"]  # Rename columns

# Define custom stopwords (add any unwanted words)
custom_stopwords = set(STOPWORDS)
custom_stopwords.update(["The", "-", "Playtest", "A", "&", "Of", ":", "Collector's", "To", "In", "Edition", "I"])

# Filter out stopwords
df_filtered = df_word_counts[~df_word_counts["word"].isin(custom_stopwords)]

# Display the top 50 most common words
display(df_filtered.head(25))

# Close connection
conn.close()

# Step 11.6: Most profitable games based only on product sale estimates (ranges)
# Step 11.6.1: Add "revenue" column in SQLite based on "owners" and "price_usd"
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Execute multiple SQL statements
conn.executescript("""
    ALTER TABLE overall ADD COLUMN revenue REAL;

    UPDATE overall 
    SET revenue = price_usd * owners;
""")

# Fetch and display results in Pandas
df = pd.read_sql("SELECT steam_appid, name, owners, price_usd, revenue FROM overall LIMIT 10;", conn)
conn.close()
# Show the results
display(df)

# Step 11.6.2: Display top 50 most profitable games based on Sales
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
df_top_revenue = pd.read_sql("""
    SELECT steam_appid, name, owners, price_usd, revenue 
    FROM overall 
    ORDER BY revenue DESC
    LIMIT 50;
""", conn)
conn.close()

# Convert 'revenue' to numeric type to avoid TypeError
df_top_revenue["revenue"] = pd.to_numeric(df_top_revenue["revenue"], errors="coerce")
# Convert revenue to $1,000,000 increments
df_top_revenue["revenue"] = df_top_revenue["revenue"] / 1_000_000

# Plot Horizontal Bar Graph of top sellers
plt.figure(figsize=(12, 10))
ax = df_top_revenue.plot.barh(x='name', y='revenue', legend=False, color="blue", figsize=(12, 10))

# Format x-axis in 1,000,000 increments
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x)}M'))

# Adjust title & labels
plt.xlabel("Revenue (Million USD)", fontsize=12)
plt.ylabel("Game Title", fontsize=12)
plt.title("Top 50 Highest Revenue Games on Steam", fontsize=14)

# Adjust Game title label size
plt.subplots_adjust(left=0.3)
plt.yticks(fontsize=8)

# Save figure locally
plt.savefig('barh_revenue.png', bbox_inches="tight", dpi=300)
# Display figure
plt.show()

# Step 11.7: 10 most expensive games on Steam
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Fetch and display results in Pandas
df_top_price = pd.read_sql("SELECT steam_appid, name, price_usd FROM overall ORDER BY price_usd DESC;", conn)
conn.close()
# Show the results
display(df_top_price.head(10))

# Step 11.8: Histogram of game prices
# Query limited to <$200 due to only 9 games being over that price
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
df_game_price = pd.read_sql("SELECT steam_appid, name, price_usd, is_free FROM overall WHERE price_usd <= 200 AND price_usd > 0 AND is_free = 0;", conn)

ax=df_game_price.hist(column='price_usd', bins=200, grid=False)
# Define label, title, and axis ranges
plt.xlabel("Game Price")
plt.title("Steam Game Price Distribution")
plt.axis('tight')
#plt.axis([0, 10, 0, 20]);
# Save figure locally
plt.savefig('historgram_prices_0-200.png')
# Display figure
plt.show()

# Step 11.9: Histogram of game prices ($0-$50 Only)
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
df_game_price = pd.read_sql("SELECT steam_appid, name, price_usd, is_free FROM overall WHERE price_usd <= 50 AND price_usd > 0 AND is_free = 0;", conn)

ax=df_game_price.hist(column='price_usd', bins=50, grid=False)
# Define label, title, and axis ranges
plt.xlabel("Game Price")
plt.title("Steam Game Price Distribution")
plt.axis('tight')
#plt.axis([0, 10, 0, 20]);
# Save figure locally
plt.savefig('historgram_prices_0-50.png')
# Display figure
plt.show()

# Print Mean, Median, Standard Deviation
print(df_pos_ratio["positive_ratio"].describe())

# Step 11.10: Average/Median Game Price (Filtering out free games)
import sqlite3
import pandas as pd

conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
df_game_price = pd.read_sql("SELECT price_usd FROM overall WHERE is_free = 0;", conn)

print(df_game_price.mean())
print(df_game_price.median())
conn.close()

# Step 11.11: Player Positive Ratings (Percentage) Histogram
# Do not include games with 0 reviews, avoiding new games and those with few reviews
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
df_pos_ratio = pd.read_sql("""
    SELECT positive_ratio, positive, negative
    FROM overall 
    WHERE negative > 0 
    ORDER BY positive_ratio DESC;
""", conn)
conn.close()

# Handle null values
df_pos_ratio = df_pos_ratio.dropna(subset=["positive_ratio"])

# Convert positive rating fraction to percentage
df_pos_ratio["positive_ratio"] = df_pos_ratio["positive_ratio"] * 100

ax = df_pos_ratio.hist(column='positive_ratio', bins=100, grid=False)
# Define label, title, and axis ranges
plt.xlabel("Positive Ratings (Percentage)")
plt.title("Steam Game Positive Ratings Distribution")
plt.axis('tight')
#plt.axis([0, 10, 0, 20]);
# Save figure locally
plt.savefig('historgram_ratings_percentage.png')
# Display figure
plt.show()

# Print Mean, Median
print("Positive Ratio Mean:", round(df_pos_ratio["positive_ratio"].mean(), 2))
print("Positive Ratio Median:", round(df_pos_ratio["positive_ratio"].median(), 2))

# Step 11.12: 25 Highest-Rated Games on Steam
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Fetch and display results in Pandas
df_top_price = pd.read_sql("""
    SELECT steam_appid, name, positive, negative, positive_ratio
    FROM overall 
    WHERE positive > 3000 
    ORDER BY positive_ratio DESC;
""", conn)
conn.close()
# Show the results
display(df_top_price.head(25))

# Step 11.13: 25 Most-Loved Games on Steam (by # of Positive Reviews)
import sqlite3
import pandas as pd

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Fetch and display results in Pandas
df_top_price = pd.read_sql("""
    SELECT steam_appid, name, positive, negative, positive_ratio
    FROM overall 
    WHERE negative > 0 
    ORDER BY positive DESC;
""", conn)
conn.close()
# Show the results
display(df_top_price.head(25))

# Step 11.14: Language Support Through the Years
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")
# Fetch and display results in Pandas
df_lang_time = pd.read_sql("""
    SELECT o.steam_appid, o.name, o.release_date, o.coming_soon, l.supported_language
    FROM overall AS o
    INNER JOIN languages AS l ON o.steam_appid = l.steam_appid
    WHERE release_date IS NOT NULL
    AND coming_soon = 0
    AND release_date < '2025-01-01'
    AND release_date >= '2010-01-01'
""", conn)
conn.close()

# Convert release_date to release_year
df_lang_time["release_year"] = pd.to_datetime(df_lang_time["release_date"]).dt.year

# Group by release_year and supported_language, then count games
df_lang_time = df_lang_time.groupby(["release_year", "supported_language"]).size().reset_index(name="game_count")

# Pivot so that each language becomes a separate column
df_pivot = df_lang_time.pivot(index="release_year", columns="supported_language", values="game_count").fillna(0)

# Ensure numeric data for plotting
df_pivot = df_pivot.apply(pd.to_numeric, errors="coerce")

# Plot line chart
plt.figure(figsize=(12, 6))
df_pivot.plot(kind="line", figsize=(12, 6), linewidth=2)

# Labels and title
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Games", fontsize=12)
plt.title("Steam Language Support Over Time", fontsize=14)
plt.legend(title="Language", bbox_to_anchor=(1.05, 1), loc="upper left")  # Move legend outside
plt.grid(True)

# Save and show
plt.savefig("line_language.png", bbox_inches="tight", dpi=300)
plt.show()

# Step 11.15a: Non-EFRIGS Language Support Over the Years
# EFRIGS = English, French, Russian, Italian, German, Spanish, the most common localization languages across media
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Fetch data, excluding EFRIGS languages
df_lang_time = pd.read_sql("""
    SELECT o.release_date, l.supported_language
    FROM overall AS o
    INNER JOIN languages AS l ON o.steam_appid = l.steam_appid
    WHERE o.release_date IS NOT NULL
    AND o.coming_soon = 0
    AND o.release_date < '2025-01-01'
    AND o.release_date >= '2010-01-01'
    AND l.supported_language NOT IN ('English', 'French', 'Italian', 'German', 'Spanish - Spain', 'Spanish - Latin America');
""", conn)
conn.close()

# Convert release_date to release_year
df_lang_time["release_year"] = pd.to_datetime(df_lang_time["release_date"]).dt.year

# Group by release_year and supported_language, then count games
df_lang_time = df_lang_time.groupby(["release_year", "supported_language"]).size().reset_index(name="game_count")

# Only keep the top 15 uncommon languages
top_languages = df_lang_time["supported_language"].value_counts().head(15).index
df_lang_time = df_lang_time[df_lang_time["supported_language"].isin(top_languages)]

# Pivot so that each language becomes a separate column
df_pivot = df_lang_time.pivot(index="release_year", columns="supported_language", values="game_count").fillna(0)

# Ensure numeric data for plotting
df_pivot = df_pivot.apply(pd.to_numeric, errors="coerce")

# ✅ Use Seaborn color palette for distinct colors
custom_colors = sns.color_palette("Set1", n_colors=len(df_pivot.columns))

# Plot line chart
plt.figure(figsize=(12, 6))
ax = df_pivot.plot(kind="line", figsize=(12, 6), linewidth=2, color=custom_colors)
                   
# Labels and title
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Games", fontsize=12)
plt.title("Steam Uncommon Language Support Over Time", fontsize=14)

# ✅ Limit the legend to only the top 15 uncommon languages
handles, labels = ax.get_legend_handles_labels()
filtered_handles = [h for h, l in zip(handles, labels) if l in top_languages]
filtered_labels = [l for l in labels if l in top_languages]
plt.legend(filtered_handles, filtered_labels, title="Top 15 Languages", bbox_to_anchor=(1.05, 1), loc="upper left")

plt.grid(True)

# Save and show
plt.savefig("line_uncommon_language.png", bbox_inches="tight", dpi=300)
plt.show()

# Step 11.15b: Again, but with the label on the line ends (crowded, but offers a different view)
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to SQLite
conn = sqlite3.connect("steam_analysis_cleaned.sqlite")

# Fetch data, excluding common languages
df_lang_time = pd.read_sql("""
    SELECT o.release_date, l.supported_language
    FROM overall AS o
    INNER JOIN languages AS l ON o.steam_appid = l.steam_appid
    WHERE o.release_date IS NOT NULL
    AND o.coming_soon = 0
    AND o.release_date < '2025-01-01'
    AND o.release_date >= '2010-01-01'
    AND l.supported_language NOT IN ('English', 'French', 'Italian', 'German', 'Spanish - Spain', 'Spanish - Latin America');
""", conn)
conn.close()

# Convert release_date to release_year
df_lang_time["release_year"] = pd.to_datetime(df_lang_time["release_date"]).dt.year

# Group by release_year and supported_language, then count games
df_lang_time = df_lang_time.groupby(["release_year", "supported_language"]).size().reset_index(name="game_count")

# ✅ Only keep the top 15 most common uncommon languages
top_languages = df_lang_time["supported_language"].value_counts().head(15).index
df_lang_time = df_lang_time[df_lang_time["supported_language"].isin(top_languages)]

# Pivot so that each language becomes a separate column
df_pivot = df_lang_time.pivot(index="release_year", columns="supported_language", values="game_count").fillna(0)

# Ensure numeric data for plotting
df_pivot = df_pivot.apply(pd.to_numeric, errors="coerce")

# ✅ Plot each language separately
plt.figure(figsize=(12, 6))

for language in df_pivot.columns:
    plt.plot(df_pivot.index, df_pivot[language], linewidth=2, label=language)

    # **Place text label at the end of the line (2024)**
    plt.text(
        x=df_pivot.index.max(),  # Rightmost x-position (latest year)
        y=df_pivot[language].iloc[-1],  # Last y-value of the line
        s=language,  # Text to display
        fontsize=10,
        verticalalignment="center"
    )

# Labels and title
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Games", fontsize=12)
plt.title("Steam Uncommon Language Support Over Time", fontsize=14)

# ✅ Remove legend (labels are now on the lines)
plt.grid(True)
plt.savefig("line_uncommon_language_pinned_labels.png", bbox_inches="tight", dpi=300)
plt.show()
