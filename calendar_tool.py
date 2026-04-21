from auth import get_calendar_service
from datetime import datetime, timedelta

CALENDAR_TIMEZONE = "America/Toronto"


# -----------------------------
# Helper: convert "HH:MM" → minutes
# -----------------------------
def time_to_minutes(time_str):
    hours, minutes = map(int, time_str.split(":"))
    return hours * 60 + minutes


# -----------------------------
# Helper: normalize Calendar event payload
# -----------------------------
def _normalize_event(event):
    start = event.get("start", {})
    end = event.get("end", {})
    start_value = start.get("dateTime", start.get("date", ""))
    end_value = end.get("dateTime", end.get("date", ""))

    all_day = "T" not in start_value
    date_value = start_value[:10] if start_value else ""
    start_time = start_value[11:16] if "T" in start_value else "00:00"
    end_time = end_value[11:16] if "T" in end_value else "23:59"

    return {
        "id": event.get("id", ""),
        "title": event.get("summary", "No Title"),
        "date": date_value,
        "start_time": start_time,
        "end_time": end_time,
        "location": event.get("location", ""),
        "description": event.get("description", ""),
        "all_day": all_day,
        "html_link": event.get("htmlLink", ""),
    }


# -----------------------------
# 1. Get all events on a date
# -----------------------------
def get_events_on_date(date_str):
    """
    Fetch all calendar events on a given date.
    Returns list of dicts: {title, start_time, end_time}
    """
    service = get_calendar_service()

    start_of_day = datetime.strptime(date_str, "%Y-%m-%d")
    end_of_day = start_of_day + timedelta(days=1)

    events_result = service.events().list(
        calendarId="primary",
        timeMin=start_of_day.isoformat() + "Z",
        timeMax=end_of_day.isoformat() + "Z",
        singleEvents=True,
        orderBy="startTime"
    ).execute()

    events = events_result.get("items", [])
    result = []

    for event in events:
        normalized = _normalize_event(event)
        result.append({
            "title": normalized["title"],
            "start_time": normalized["start_time"],
            "end_time": normalized["end_time"],
        })

    return result


# -----------------------------
# 1b. Get events in a date range
# -----------------------------
def get_events_between(start_date_str, end_date_str, max_results=100):
    """
    Fetch calendar events between two inclusive dates.
    Returns normalized event dicts.
    """
    service = get_calendar_service()

    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)

    events_result = service.events().list(
        calendarId="primary",
        timeMin=start_dt.isoformat() + "Z",
        timeMax=end_dt.isoformat() + "Z",
        singleEvents=True,
        orderBy="startTime",
        maxResults=max_results,
    ).execute()

    events = events_result.get("items", [])
    return [_normalize_event(event) for event in events]


# -----------------------------
# 1c. Find matching events by title and date
# -----------------------------
def find_matching_events(title="", date_str="", start_date_str="", end_date_str="", max_results=100):
    title = (title or "").strip().lower()
    start_date_str = start_date_str or date_str
    end_date_str = end_date_str or date_str

    if not start_date_str or not end_date_str:
        return []

    matches = []
    for event in get_events_between(start_date_str, end_date_str, max_results=max_results):
        if title and title not in (event.get("title", "").strip().lower()):
            continue
        if date_str and event.get("date") != date_str:
            continue
        matches.append(event)

    return matches


# -----------------------------
# 2. Check for time conflicts
# -----------------------------
def check_conflict(date_str, new_start_str, new_end_str):
    """
    Check if a new event overlaps with existing events.
    Returns dict with conflict info and suggestion.
    """
    events = get_events_on_date(date_str)

    new_start = time_to_minutes(new_start_str)
    new_end = time_to_minutes(new_end_str)

    for event in events:
        existing_start = time_to_minutes(event["start_time"])
        existing_end = time_to_minutes(event["end_time"])

        overlap = new_start < existing_end and new_end > existing_start

        if overlap:
            suggested_start = existing_end

            if suggested_start % 15 != 0:
                suggested_start += (15 - suggested_start % 15)

            hours = suggested_start // 60
            minutes = suggested_start % 60

            suggested_time = f"{hours:02d}:{minutes:02d}"

            return {
                "conflict": True,
                "clashing_event": event["title"],
                "suggested_time": suggested_time
            }

    return {
        "conflict": False,
        "clashing_event": None,
        "suggested_time": None
    }


# -----------------------------
# 3. Create a calendar event
# -----------------------------
def create_event(title, date_str, start_time_str, end_time_str,
                 location="", description="", reminders_minutes=[]):
    """
    Create a Google Calendar event.
    Returns the event URL.
    """
    service = get_calendar_service()

    start_dt = f"{date_str}T{start_time_str}:00"
    end_dt = f"{date_str}T{end_time_str}:00"

    reminders = [{"method": "popup", "minutes": m} for m in reminders_minutes]

    event_body = {
        "summary": title,
        "location": location,
        "description": description,
        "start": {
            "dateTime": start_dt,
            "timeZone": CALENDAR_TIMEZONE,
        },
        "end": {
            "dateTime": end_dt,
            "timeZone": CALENDAR_TIMEZONE,
        },
        "reminders": {
            "useDefault": False,
            "overrides": reminders,
        },
    }

    event = service.events().insert(
        calendarId="primary",
        body=event_body,
    ).execute()

    return event.get("htmlLink")


# -----------------------------
# 4. Delete one event by ID
# -----------------------------
def delete_event_by_id(event_id):
    service = get_calendar_service()
    service.events().delete(calendarId="primary", eventId=event_id).execute()
    return True


# -----------------------------
# 5. Delete matching events by title and date
# -----------------------------
def delete_matching_events(title="", date_str="", start_date_str="", end_date_str=""):
    matches = find_matching_events(
        title=title,
        date_str=date_str,
        start_date_str=start_date_str,
        end_date_str=end_date_str,
    )

    deleted = []
    for event in matches:
        delete_event_by_id(event.get("id", ""))
        deleted.append(event)

    return deleted
