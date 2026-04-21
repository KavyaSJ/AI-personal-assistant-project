import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Gmail and Calendar scopes
# GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
# CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar"]
SCOPES = ["https://www.googleapis.com/auth/gmail.modify",
          "https://www.googleapis.com/auth/calendar"]

def get_gmail_service():
    """
    Returns an authenticated Gmail API service (v1).
    Handles token loading, refreshing, and login if needed.
    """
    creds = None

    # 1. Load token.json if it exists
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # 2. If no valid credentials, refresh or login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # 3. Refresh expired token automatically
            creds.refresh(Request())
        else:
            # 4. Login flow (opens browser)
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save token for future use
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    # 5. Return Gmail service
    service = build("gmail", "v1", credentials=creds)
    return service


def get_calendar_service():
    """
    Returns an authenticated Google Calendar API service (v3).
    Handles token loading, refreshing, and login if needed.
    """
    creds = None

    # 1. Load token.json if it exists
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # 2. If no valid credentials, refresh or login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # 3. Refresh expired token automatically
            creds.refresh(Request())
        else:
            # 4. Login flow (opens browser)
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save token for future use
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    # 5. Return Calendar service
    service = build("calendar", "v3", credentials=creds)
    return service