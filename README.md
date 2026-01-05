# Steam-Scraper.v2.wip

#### WIP: This project is Work in Progress and does not reflect a fully functional codebase.

## Purpose
This project collects, stores, cleans, and analyzes game data from the __Steam Marketplace__ and __SteamSpy__ using public APIs provided by [Valve Corporation](https://store.steampowered.com) and [SteamSpy](https://steamspy.com).
The goal is to support market analysis and/or reveal interesting trends in the dominant PC (and possibly entire) games market. 
- Note: Data on DLC, music, demos, etc. purposefully not collected/analyzed
  
The original project was submitted in 3 phases as the semester-long project for a university-level _Programming Fundamentals with Python_ course (taken Spring 2025). The current version refactors the original Jupyter Notebooks into modular .py scripts.
  
  #### Original Project Phases:
  1. Acquire a __JSON dataset__ from the internet (APIs/web scraping), then write to a local __MongoDB__ collection.
  2. Extract data from __MongoDB__ and insert into a local __SQLite__ database.
  3. Clean data and save as a __CSV__, then conduct __data analysis and visualization__.

There are several unnecessary, required assignment steps which will be eliminated. Additionally, this program is currently semi-manual as it requires considerable babysitting and manual handling of file names and backups. This will be addressed, allowing for easier UX through automation and customization.

## Methodology
All Steam applications (games, music, DLC, etc.) are referenced by a unique __appid__ or __steam_appid__.

The current steps to the project are as follow:

1. __Query ISteam API to pull all valid (not null) appids (CSV) _--DEPRICATED___
   - ISteam API is depricated, replaced by IStoreService/GetAppList sometime between FEB2025 and DEC2025.
     - Update to follow using the new IStoreService API
   - Output only contains "appid" and "name" with no supporting details. We can't yet filter by "type" (games) yet.
   - Duplicates are removed
2. __Query AppDetails API to pull details (JSON), saving details for only "game" appids__
   - Due to Steam API rate limiting (200/5minutes) and the massive Steam catalogue (~172,000 unique apps), querying took ~72 hours!
     - Steam may allow higher query rates with an API key, but I was unable to receive one despite having a valid, active personal account.
   - Storage may become an issue as the final, uncleaned JSON file is ~1.6GB. With backups, the folder baloons to ~10GB!
     - After keeping only 15 keys (removing links, etc.) the final JSON file drops to ~350MB.
   - Supported language data is cleaned, removing HTML tags ```<strong> </strong>``` within strings.
3. __Query SteamSpy to pull data (JSON) on reviews, owners, concurrent players, and tags__
   - Ensure SteamSpy data is combined with AppDetails JSON.
     - This step not originally documented in Jupyter Notebooks, requiring reconstruction of this step.
4. __Upload collected game data into local MongoDB collection__
   - JSON files needed to be split into chunks (<16MB) for input. Anything larger threw an error in MongoDB.
     - Splitting done through Python, then batch uploaded to MongoDB using Windows PowerShell
5. __Clean DB and create tables (SQLite)__
   - Remove entries with duplicate "steam_appid" value
   - _Database Schema:_
     - overall_info, 
     - pricing, 
     - game_type, 
     - language_info,
     - content_rating
6. __Data post-processing/cleaning (SQLite)__
   - Incrementally (AKA manually) filter out valid language entries and update tuples with valid languages using SQL commands
     - Many languages strings were listed in foreign languages (e.g. İngilizce). These were replaced by the English translation and new languages were added to a list (105 total).
   - Check for null "tag" values
   - Convert currency to USD
   - Create "revenue" column in overall table using SQL commands
7.  __Analyze data using Matplotlib plots__
   - Top 50 Highest Revenue Games on Steam
   - Steam Game Price Distribution (All)
   - Steam Game Price Distribution ($0-50)
   - Steam Game Positive Ratings Distribution
   - Steam Language Support Over Time
   - Steam Uncommon Language Support Over Time (Remove EFRIGS support from chart)

## Disclaimer
- Original code was written with the help of a custom GPT assistant provided by the course professor. However, it was used as a tool and specifically instructed to only edit/critique instead of providing any code outright.
  - Current code refactoring (.ipynb to .py) is being done without LLM assistance.
- Though the program includes error handling, I cannot guarantee full stability or safety. Use at your own risk.
  - See [LICENSE](docs/LICENSE.md) for full disclaimer