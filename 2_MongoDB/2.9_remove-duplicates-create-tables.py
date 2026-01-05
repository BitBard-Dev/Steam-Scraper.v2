## Step 9: Clean data & create MongoDB Tables

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
# Step 7.3 yields this name of NOSTEAMSPY. Unsure why I originally did this??? 
    # ??? STEAMSPY data incoporation must not have been originally documented... Have to redo now...
coll = db.STEAM_GAMES_NOSTEAMSPY

# Step 9.3: Check for Unique steam_appid Values
from collections import Counter

# Extract all steam_appid values
appid_list = [game["steam_appid"] for game in coll.find({}, {"steam_appid": 1})]

# Count occurrences of each steam_appid
duplicate_appids = [appid for appid, count in Counter(appid_list).items() if count > 1]

print(f"Duplicate steam_appids found: {duplicate_appids}")

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

print("Duplicate cleanup complete!")

# Step 9.5: Re-Verify No Duplicates in MongoDB 
# Simply repeat Step 9.3

# Step 9.6: Create overall_info Table & Insert Data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('PROJECT_PHASE2.sqlite')
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

# insert pulled data into PROJECT_PHASE2.sqlite
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
conn = sqlite3.connect('PROJECT_PHASE2.sqlite')
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

# insert pulled data into PROJECT_PHASE2.sqlite
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
conn = sqlite3.connect('PROJECT_PHASE2.sqlite')
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

# insert pulled data into PROJECT_PHASE2.sqlite file
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
print(f"There are {unique_games} unique games in the gametype_info table.\nGames lost: {96039 - unique_games}") # ??? 96039 == magic number. Verify total games from previous steps. Create global variable to track unique games

# Fetch and display first record
cur.execute("SELECT * FROM gametype_info WHERE steam_appid = 400") # ??? pseudo magic number. Portal has appid 400. This step originally done to satisfy assignment requirments.
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

# Step 9.9: Create "language_info" table & insert data
# connect to the SQLite database & define cursor
conn = sqlite3.connect('PROJECT_PHASE2.sqlite')
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

# insert pulled data into PROJECT_PHASE2.sqlite
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
conn = sqlite3.connect('PROJECT_PHASE2.sqlite')
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

# insert pulled data into PROJECT_PHASE2.sqlite
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