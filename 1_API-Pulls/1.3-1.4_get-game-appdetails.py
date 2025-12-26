from global_variables import VALID_APPS_UNIQUE_CSV, PROCESSED_APPS_CSV, APP_DETAILS_URL, GAMES_ONLY_JSON, MISSING_APPS_CSV, HEADERS

## Step 3: Query Steam AppDetails API to Pull Details for Each Steam App ID, pulling only "game" details
    ## Save  details to JSON (queried from API as JSON), log processed apps in processed_apps.csv

# Steam API rate limits. Lack of API key leads to these approximate rate limits.
# ??? Improvement: implement python "logging" module with STEAM_BATCH_SIZE of 1 and REQUST_INTERVAL of 1.5. Better with UI.

def fetch_app_detail(app_id):
    """Fetch details for a single app with retry handling."""
    url = APP_DETAILS_URL.format(app_id)
    attempts = 3  # Max attempts for failures

    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 429:  # Too Many Requests
                print(f"429 Too Many Requests for {app_id}. Waiting 30 seconds...")
                time.sleep(30)
                continue  # Retry

            if response.status_code != 200:
                print(f"Error {response.status_code} fetching {app_id}: {response.text}")
                return {"appid": app_id, "error": f"HTTP {response.status_code}"}

            data = response.json()

            if not data or str(app_id) not in data:
                print(f"Warning: No valid data received for {app_id}")
                return {"appid": app_id, "error": "No valid data"}

            # Extract only "data" part
            app_data = data[str(app_id)]
            if not app_data.get("success"):
                print(f"Invalid app ID: {app_id} (No success flag)")
                return {"appid": app_id, "error": "Invalid App ID"}

            # Extract actual game data
            game_info = app_data["data"]

            # Ensure it's a game before returning appdetails
            if game_info.get("type") == "game":
                return game_info
            else:
                print(f"Skipping non-game app: {app_id} (Type: {game_info.get('type')})")
                return {"appid": app_id, "error": f"Not a game (Type: {game_info.get('type')} )"}

        except requests.RequestException as e:
            print(f"Network error while fetching {app_id}: {e}")
            return {"appid": app_id, "error": "Network error"}

    print(f"Repeated failures for {app_id}. Skipping.")
    return {"appid": app_id, "error": "Too Many Requests - Skipped"}

def load_valid_apps():
    """Loads valid app IDs from the CSV file."""
    df = pd.read_csv(VALID_APPS_UNIQUE_CSV)
    return df["appid"].tolist()

def load_existing_json():
    """Loads existing JSON data and extracts already processed app IDs."""
    processed_app_ids = set()

    # Check processed CSV to ensure no duplicates occur
    if os.path.exists(PROCESSED_CSV):
        try:
            df = pd.read_csv(PROCESSED_CSV, dtype={"appid": str})
            processed_app_ids.update(df["appid"].astype(int).tolist())  # Merge both sets
        except Exception as e:
            print(f"Error reading processed_apps.csv: {e}")

    try:
        with open(GAMES_ONLY_JSON, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            if not isinstance(existing_data, list): #Ensure it's a list
                existing_data = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []  # Now using a list instead of a dictionary

    # Extract processed app IDs from JSON
    for app in existing_data:
        if isinstance(app, dict):
            processed_app_ids.update({app["steam_appid"]})


    return existing_data, processed_app_ids

def save_data_to_json(new_data, filename):
    """Safely appends new game data to JSON file without duplicates."""
    try:
        # Load existing JSON file
        with open(filename, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            if not isinstance(existing_data, list): # Ensure it's a list, not a dict
                existing_data = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []  # Start fresh if file doesn't exist

    # Extract existing app IDs to avoid duplicates
    existing_app_ids = {app["steam_appid"] for app in existing_data if isinstance(app, dict)}

    # Append only new unique games
    new_entries = [app for app in new_data if app.get("steam_appid") not in existing_app_ids]
    existing_data.extend(new_entries)

    # Save updated JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

    print(f"{len(new_data)} new games added to {filename}")

def save_processed_apps_to_csv(processed_apps):
    """Logs ALL processed apps, including non-games and errors???, to avoid re-querying"""
    df = pd.DataFrame(processed_apps, columns=["appid", "name", "status"])
    
    if not os.path.exists(PROCESSED_CSV):
        df.to_csv(PROCESSED_CSV, index=False)  # Create new file
    else:
        df.to_csv(PROCESSED_CSV, mode="a", index=False, header=False)  # Append without column headers ???NECESSARY???

    print(f"Processed {len(processed_apps)} apps. Logged to {PROCESSED_CSV}")

def count_processed_apps():
    """Counts total processed apps from processed_apps.csf"""
    if os.path.exists(PROCESSED_CSV):
        df = pd.read_csv(PROCESSED_CSV)
        return len(df) # Count total rows in processed_apps.csv
    return 0 # Returns 0 if file doesn't exist or no apps processed yet

# run in main.py ???
def main():
    """Fetches Steam app details sequentially, logging processed app IDs and names."""
    all_valid_app_ids = load_valid_apps()
    existing_data, processed_app_ids = load_existing_json()
    unprocessed_app_ids = [appid for appid in all_valid_app_ids if appid not in processed_app_ids]

    if not unprocessed_app_ids:
        print("All app IDs have been processed. Exiting loop.")
        return

    while unprocessed_app_ids:
        batch = unprocessed_app_ids[:STEAM_BATCH_SIZE]  # Query up to STEAM_BATCH_SIZE IDs
        unprocessed_app_ids = unprocessed_app_ids[STEAM_BATCH_SIZE:]  # Remove processed batch from unprocessed list

        print(f"\nProcessing {len(batch)} new app IDs...")

        new_games = []  # Store only game-type entries
        processed_entries = [] #Store successfully procesed appids & names

        for app_id in batch:
            result = fetch_app_detail(app_id)

            # Log all apps, including non-games and errors
            if "error" not in result and result.get("type") == "game":
                new_games.append(result)
                processed_entries.append((app_id, result["name"], "Success")) # Save game info
            else:
                processed_entries.append((app_id, "", result.get("error", "Unknown Error"))) # Log non-games & errors

        save_processed_apps_to_csv(processed_entries) # Save all processed apps (games & non-games)

        print(f"Finished fetching {len(batch)} apps.")

        # Save only games to JSON
        if new_games:
            save_data_to_json(new_games, GAMES_ONLY_JSON) 

        # Get running total of processed apps
        total_processed_apps = count_processed_apps()

        # Calculate percentage completion
        percentage_complete = (total_processed_apps *100) / 172803

        # Print summary
        print(f"\n📊 Total games in JSON file: {len(load_existing_json()[0])}")
        print(f"Total apps processed: {total_processed_apps}")
        print(f"Percentage complete: {percentage_complete:.2f}%")
        
        # Wait before next batch
        print("\n⏳ Waiting 5 minutes before the next batch...")
        time.sleep(STEAM_REQUEST_INTERVAL)

# run in main.py ???
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print(f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    main()  # Run sequentially

    end_time = datetime.datetime.now()
    print(f"Script finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    duration = end_time - start_time
    print(f"Total execution time: {str(duration)}")

## Step 4: Verify All Steam Apps Were Processed by comparing steam_valid_apps_unique.csv and processed_apps.csv

def find_missing_apps():
    """Compares two CSV files and finds unprocessed app IDs."""
    # Load both CSVs
    valid_apps = pd.read_csv(VALID_APPS_UNIQUE_CSV, usecols=["appid"])
    processed_apps = pd.read_csv(PROCESSED_APPS_CSV, usecols=["appid"])

    # Convert to sets for fast comparison
    valid_app_ids = set(valid_apps["appid"])
    processed_app_ids = set(processed_apps["appid"])

    # Find missing app IDs
    missing_app_ids = valid_app_ids - processed_app_ids

    if missing_app_ids:
        print(f"{len(missing_app_ids)} apps were not processed!")
        
        # Create a DataFrame for missing apps
        missing_apps_df = valid_apps[valid_apps["appid"].isin(missing_app_ids)]
        
        # Save to CSV (Optional: Reprocess missing apps later)
        missing_apps_df.to_csv(MISSING_APPS_CSV, index=False)
        print(f"Missing apps saved to {MISSING_APPS_CSV} for reprocessing.")
    else:
        print("All apps have been successfully processed.")

# Run the comparison
find_missing_apps()