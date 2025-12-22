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

# Use Seaborn color palette for distinct colors
custom_colors = sns.color_palette("Set1", n_colors=len(df_pivot.columns))

# Plot line chart
plt.figure(figsize=(12, 6))
ax = df_pivot.plot(kind="line", figsize=(12, 6), linewidth=2, color=custom_colors)
                   
# Labels and title
plt.xlabel("Year", fontsize=12)
plt.ylabel("Number of Games", fontsize=12)
plt.title("Steam Uncommon Language Support Over Time", fontsize=14)

# Limit the legend to only the top 15 uncommon languages
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

# Only keep the top 15 most common uncommon languages
top_languages = df_lang_time["supported_language"].value_counts().head(15).index
df_lang_time = df_lang_time[df_lang_time["supported_language"].isin(top_languages)]

# Pivot so that each language becomes a separate column
df_pivot = df_lang_time.pivot(index="release_year", columns="supported_language", values="game_count").fillna(0)

# Ensure numeric data for plotting
df_pivot = df_pivot.apply(pd.to_numeric, errors="coerce")

# Plot each language separately
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

# Remove legend (labels are now on the lines)
plt.grid(True)
plt.savefig("line_uncommon_language_pinned_labels.png", bbox_inches="tight", dpi=300)
plt.show()
