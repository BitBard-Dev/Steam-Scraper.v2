
## ??? Ensure all global variables are defined in the local function to eliminate global calls & reduce memory/increase speed 

# 1_API-Pulls
    # 1.1-1.2
# STEAMKEY = __________
APP_LIST_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?key=STEAMKEY&format=json"
VALID_APPS_CSV = "steam_valid_apps.csv"
VALID_APPS_UNIQUE_CSV = "steam_valid_apps_unique.csv"
    # 1.3-1.4
APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={}"
PROCESSED_APPS_CSV = "processed_apps.csv"  # "Running tally" for successfully processed apps
GAMES_ONLY_JSON = "steam_games_only.json"  # Output JSON for games only
STEAM_BATCH_SIZE = 200  # Max requests per 5 minutes (Steam API limit)
STEAM_REQUEST_INTERVAL = 305  # Wait time after batch completion (5 minutes + buffer)
MISSING_APPS_CSV = "missing_apps.csv"  # Output file for any missing apps
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}  # Spoofs as a regular user to reduce risk of blocking.
# ??? Retest if this header is still necessary with new API path.




