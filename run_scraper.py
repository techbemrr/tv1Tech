import gspread
from google.oauth2.service_account import Credentials
import os
import json

def get_gspread_client():
    # Define the required scopes
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 1. TRY ENVIRONMENT VARIABLE FIRST (Best for GitHub Actions/Heroku/Docker)
    # Set this in your environment as a stringified JSON
    creds_json = os.getenv("GOOGLE_SHEETS_CREDS_JSON")
    
    if creds_json:
        # Load from memory to avoid disk "PermissionError"
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        # 2. FALLBACK TO FILE (Ensure path is absolute and has read/write permissions)
        # Use 'r' prefix to handle Windows backslashes correctly
        path = r"C:\path\to\your\service_account.json" 
        try:
            creds = Credentials.from_service_account_file(path, scopes=scopes)
        except PermissionError as e:
            print(f"CRITICAL: OS blocked access to {path}. Check if file is open in another app.")
            raise e

    return gspread.authorize(creds)

# Execution logic with proper error handling
try:
    gc = get_gspread_client()
    # Replace with your actual Sheet ID or Name
    sh = gc.open_by_key("YOUR_SHEET_ID_HERE") 
    print("Successfully connected!")
except Exception as e:
    print(f"Failed to resolve: {e}")
