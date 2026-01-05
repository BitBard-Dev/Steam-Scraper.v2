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

# Step 10.2: Incrementally (AKA manually...) filter out valid language entries and update tuples with valid languages
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

        # Corrected Update Statement (Updates both `price_final` and `currency`)
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
print(f"{updated_rows} rows updated to USD!")
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


# ??? Improvement: save each table to CSV after updating it instead of at the end. Saves memory.
# ??? Saved to CSV to satisfy assignment requirement (and backup), not necessary for further steps.

# Save `overall` table
df_overall = pd.read_sql("SELECT * FROM overall", conn)
df_overall.to_csv("overall_cleaned.csv", index=False)
print("Saved overall_cleaned.csv")

# Save `tags` table
df_tags = pd.read_sql("SELECT * FROM tags", conn)
df_tags.to_csv("tags_cleaned.csv", index=False)
print("Saved tags_cleaned.csv")

# Save `languages` table
df_languages = pd.read_sql("SELECT * FROM languages", conn)
df_languages.to_csv("languages_cleaned.csv", index=False)
print("Saved languages_cleaned.csv")

# Close connection
conn.close()