from auth import get_gmail_service, get_calendar_service
from datetime import datetime, timezone

# 1. Get Gmail service and print user's email
gmail_service = get_gmail_service()

profile = gmail_service.users().getProfile(userId="me").execute()
print("Email address:", profile.get("emailAddress"))


# 2. Get Calendar service and print next 3 events
calendar_service = get_calendar_service()

now = datetime.now(timezone.utc).isoformat()

events_result = calendar_service.events().list(
    calendarId="primary",
    timeMin=now,
    maxResults=3,
    singleEvents=True,
    orderBy="startTime"
).execute()

events = events_result.get("items", [])

print("\nNext 3 events:")

if not events:
    print("No upcoming events found.")
else:
    for event in events:
        start = event["start"].get("dateTime", event["start"].get("date"))
        print(f"- {event.get('summary', 'No Title')} at {start}")