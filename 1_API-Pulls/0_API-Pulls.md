## Phase 1: Collect details on all games in the Steam Marketplace catalogue by querying: ISteam SteamAppDetails, and SteamSpy APIs

## NOTE: 
- I was unable to get an API key for this project despite multiple requests using my active, personal account. This likely led to the rate limitations when querying the AppDetails API
- The original querying for this was done February 2025.
- The ISteamApps method GetAppList/v2 has since been depricated. I've yet to alter the code to use the current IStoreService/GetAppList method
- The AppDetails API URL does not seem to work anymore as queries to known appids return null. (Need to review the Steamworks Web API documentation @ https://partner.steamgames.com/doc/webapi)


- Step 1: Query the ISteam API to Determine Total Number of Valid Steam Apps
- Step 2: Remove Duplicate Steam App IDs from the list of Valid Steam Apps
- Step 3: Query Steam API to Pull All AppDetails for Each Steam App ID
- Step 4: Pull Game AppDetails from the steam_apps_grouped JSON File
    - Step 4a: Remove Documents with Duplicate App ID


Improvements/Fixes:
- Ensure file name variables are unique or consistent across functions/steps


