import os
import re
import time
import tempfile
import hashlib
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import agent
import calendar_tool
import email_flow
import email_tool
import memory
import pdf_tool
import planner
import settings_store


# Initialize all session state keys used in the app.
def init_session_state():
    defaults = {
        "messages": [],
        "history": [],
        "check_interval": 30,
        "pending_approval": None,
        "pending_thread_id": None,
        "pending_events": [],
        "pending_event_after_send": None,
        "email_results": [],
        "trigger_email_check": False,
        "auto_email_check_enabled": True,
        "last_email_check_at": 0.0,
        "precomputed_email_drafts": {},
        "uploaded_file_name": None,
        "uploaded_file_bytes": None,
        "trigger_file_process": False,
        "last_upload_signature": None,
        "pending_course_outline": None,
        "pending_invitation": None,
        "pending_outbound_email": None,
        "last_uploaded_text": "",
        "last_uploaded_tables": [],
        "last_parsed_document": None,
        "llm_choice": agent.get_default_model_choice(),
        "pending_draft_revision_count": 0,
        "show_settings": False,
        "settings_notice": "",
        "_awaiting_recipient_email": False,
        "_pending_email_request": {},
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# Add a message to the chat history.
def add_message(role, content):
    if isinstance(content, str) and content.strip():
        st.session_state["messages"].append({"role": role, "content": content})


# Remove one email result after it has been handled.
def remove_email_result(thread_id):
    st.session_state["email_results"] = [
        item for item in st.session_state["email_results"]
        if item.get("thread_id") != thread_id
    ]

    st.session_state["precomputed_email_drafts"].pop(thread_id, None)


# Stores a prepared draft in session memory only. It is not saved to Gmail until the user asks for it.
def set_precomputed_email_draft(thread_id, draft_text, draft_kind):
    if not thread_id or not draft_text:
        return

    st.session_state["precomputed_email_drafts"][thread_id] = {
        "draft_text": draft_text,
        "draft_kind": draft_kind,
        "prepared_at": time.time(),
    }

    for item in st.session_state.get("email_results", []):
        if item.get("thread_id") == thread_id:
            item["precomputed_draft_text"] = draft_text
            item["precomputed_draft_kind"] = draft_kind
            break


# Returns a prepared in-memory draft for one email, if available.
def get_precomputed_email_draft(thread_id):
    session_value = st.session_state.get("precomputed_email_drafts", {}).get(thread_id)
    if session_value:
        return session_value

    for item in st.session_state.get("email_results", []):
        if item.get("thread_id") == thread_id and item.get("precomputed_draft_text"):
            return {
                "draft_text": item.get("precomputed_draft_text", ""),
                "draft_kind": item.get("precomputed_draft_kind", "reply"),
                "prepared_at": 0,
            }

    return None


# Schedules a browser refresh while the Streamlit page is open.
def install_auto_refresh(interval_seconds):
    interval_ms = max(15000, int(interval_seconds * 1000))
    refresh_html = f"""
    <script>
    const refreshAfterMs = {interval_ms};
    if (!window.parent.__assistantAutoRefreshConfigured) {{
        window.parent.__assistantAutoRefreshConfigured = true;
        window.parent.setTimeout(function() {{
            window.parent.location.reload();
        }}, refreshAfterMs);
    }}
    </script>
    """
    components.html(refresh_html, height=0)


# Runs an email check automatically when the selected interval has elapsed.
def maybe_run_scheduled_email_check():
    if not st.session_state.get("auto_email_check_enabled", True):
        return

    interval_minutes = int(st.session_state.get("check_interval", 30) or 30)
    interval_seconds = interval_minutes * 60
    install_auto_refresh(interval_seconds)

    now_ts = time.time()
    last_ts = float(st.session_state.get("last_email_check_at", 0) or 0)

    if last_ts <= 0 or (now_ts - last_ts) >= interval_seconds:
        run_email_check_action(auto=True)
        st.session_state["last_email_check_at"] = now_ts


# Prepares low-risk email drafts in memory only so the user can create the real Gmail draft on demand.
def apply_email_preparation_plan():
    plan_state = planner.build_email_planner_state(
        email_results=st.session_state.get("email_results", []),
        pending_approval=st.session_state.get("pending_approval"),
        pending_outbound_email=st.session_state.get("pending_outbound_email"),
        pending_events=st.session_state.get("pending_events", []),
    )
    actions = planner.build_email_action_plan(plan_state)

    for action in actions:
        thread_id = action.get("thread_id", "")
        if not thread_id or get_precomputed_email_draft(thread_id):
            continue

        matching_item = None
        for item in st.session_state.get("email_results", []):
            if item.get("thread_id") == thread_id:
                matching_item = item
                break

        if not matching_item:
            continue

        thread_text = matching_item.get("thread_text", "")
        if not thread_text:
            continue

        try:
            if action.get("action") == "prepare_rsvp_draft":
                draft_text = agent.draft_rsvp(thread_text, attending=True)
                set_precomputed_email_draft(thread_id, draft_text, "rsvp")
            elif action.get("action") == "prepare_reply_draft":
                draft_text = agent.draft_response(thread_text, "")
                set_precomputed_email_draft(thread_id, draft_text, "reply")
        except Exception:
            continue


# Clears editor widget state so refreshed extracted data shows correctly.
def clear_review_editor_state():
    keys_to_clear = [
        "course_sessions_editor",
        "course_assignments_editor",
        "course_name_input",
        "course_days_input",
        "course_start_time_input",
        "course_end_time_input",
        "course_term_start_input",
        "course_term_end_input",
        "course_location_input",
        "invite_title_input",
        "invite_date_input",
        "invite_start_input",
        "invite_end_input",
        "invite_location_input",
        "invite_host_input",
        "invite_rsvp_required_input",
        "invite_rsvp_deadline_input",
        "invite_description_input",
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]


# Adds an event to the pending list only if it is not already saved or queued.
def queue_pending_event(event_dict):
    title = (event_dict or {}).get("title", "")
    date = (event_dict or {}).get("date", "")

    if title and date and memory.event_exists(title, date):
        add_message("assistant", f"The event '{title}' on {date} is already saved in memory.")
        return

    for existing in st.session_state["pending_events"]:
        if (
            existing.get("title") == title
            and existing.get("date") == date
            and existing.get("start_time") == event_dict.get("start_time")
        ):
            add_message("assistant", f"The event '{title}' is already waiting for calendar approval.")
            return

    st.session_state["pending_events"].append(event_dict)


# Builds a short message about parsing quality for uploaded documents.
def describe_parsing_quality(parsed_document):
    quality = (parsed_document or {}).get("quality", {}) or {}
    notes = []

    if quality.get("weak_text_extraction"):
        notes.append("text extraction was weak")

    if quality.get("ocr_used"):
        notes.append("OCR fallback added extra text")
    elif quality.get("ocr_attempted") and not quality.get("ocr_used") and quality.get("ocr_status") not in ["not_attempted", "not_applicable"]:
        notes.append(f"OCR status: {quality.get('ocr_status')}")

    if quality.get("table_count"):
        notes.append(f"{quality.get('table_count')} table(s) found")

    if not notes:
        return ""

    return "Parsing notes: " + ", ".join(notes) + "."


# Builds a readable label for the extraction provider used by the hybrid pipeline.
def describe_extraction_provider(meta):
    meta = meta or {}
    provider = meta.get("provider") or meta.get("backend") or ((meta.get("result") or {}).get("extraction_meta", {}) or {}).get("provider", "")

    if provider == "local_qwen":
        model_name = (
            meta.get("result", {}).get("extraction_meta", {}).get("model")
            or os.getenv("LOCAL_LLM_MODEL", "")
        )
        if model_name:
            return f"the local structured LLM backend ({model_name})"
        return "the local structured LLM backend"

    if provider == "gemini":
        model_name = meta.get("result", {}).get("extraction_meta", {}).get("model") or os.getenv("GEMINI_MODEL_NAME", "")
        if model_name:
            return f"the Gemini backend ({model_name})"
        return "the Gemini backend"

    return "the deterministic parser"


# Returns a short user-facing note when routing fell back from the selected backend.
def consume_last_route_notice():
    route_info = agent.get_last_route_info(reset=True)
    if not route_info:
        return ""

    if route_info.get("fallback_used"):
        used_model = route_info.get("used_model", "")
        used_provider = route_info.get("used_provider", "")
        if used_model:
            return f"I used {used_provider} ({used_model}) because the preferred backend was unavailable or returned unusable output."
        if used_provider:
            return f"I used {used_provider} because the preferred backend was unavailable or returned unusable output."

    return ""


# Loads persisted app settings.
def load_app_settings():
    return settings_store.load_settings()


# Applies persisted settings to the runtime agent configuration.
def apply_runtime_settings(app_settings):
    active_api_key = settings_store.get_active_api_key(app_settings)
    selected_local_model = (app_settings.get("selected_local_model") or os.getenv("LOCAL_LLM_MODEL", "") or "").strip()
    cloud_models = settings_store.get_cloud_models(app_settings)
    agent.configure_runtime(
        api_key=active_api_key,
        local_model=selected_local_model,
        cloud_models=cloud_models,
        debug=bool(app_settings.get("debug_tools_enabled", False)),
    )


# Builds a markdown bullet list from a list of short lines.
def format_bullet_message(title, items):
    items = [str(item).strip() for item in (items or []) if str(item).strip()]
    if not items:
        return title
    bullet_lines = "\n".join([f"- {item}" for item in items])
    return f"{title}\n\n{bullet_lines}"


# Masks a secret for display.
def mask_secret(value):
    value = str(value or "").strip()
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "*" * max(4, len(value) - 8) + value[-4:]


# Sets the pending draft state after a new draft is created.
def set_pending_draft_state(draft_text, thread_id=None, pending_event=None):
    st.session_state["pending_approval"] = draft_text
    st.session_state["pending_thread_id"] = thread_id
    st.session_state["pending_event_after_send"] = pending_event
    st.session_state["pending_draft_revision_count"] = 0


# Clears the current pending draft state.
def clear_pending_draft_state():
    st.session_state["pending_approval"] = None
    st.session_state["pending_thread_id"] = None
    st.session_state["pending_event_after_send"] = None
    st.session_state["pending_draft_revision_count"] = 0


# Guesses the MIME type for an uploaded file.
def guess_mime_type(file_name):
    lower_name = (file_name or "").lower()
    if lower_name.endswith(".pdf"):
        return "application/pdf"
    if lower_name.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return "application/octet-stream"


# Converts a subject into a reply subject.
def make_reply_subject(subject):
    subject = (subject or "").strip()

    if not subject:
        return "Re:"

    if subject.lower().startswith("re:"):
        return subject

    return "Re: " + subject


# Builds the References header value for a reply.
def build_references(existing_references, message_id):
    existing_references = (existing_references or "").strip()
    message_id = (message_id or "").strip()

    if existing_references and message_id:
        if message_id in existing_references:
            return existing_references
        return existing_references + " " + message_id

    if existing_references:
        return existing_references

    if message_id:
        return message_id

    return ""


# Converts common time formats to HH:MM.
def parse_single_time(time_text):
    if not time_text:
        return None

    cleaned = str(time_text).strip()
    cleaned = cleaned.replace("a.m.", " AM").replace("p.m.", " PM")
    cleaned = cleaned.replace("a.m", " AM").replace("p.m", " PM")
    cleaned = cleaned.replace("am", " AM").replace("pm", " PM")
    cleaned = cleaned.replace(".", ":")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    formats = [
        "%H:%M",
        "%I:%M %p",
        "%I %p",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            pass

    match = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", cleaned)
    if match:
        return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}"

    return None


# Adds minutes to an HH:MM time while staying in the same day.
def add_minutes_to_time(time_str, minutes):
    if not time_str:
        return ""

    try:
        start_dt = datetime.strptime(time_str, "%H:%M")
        end_dt = start_dt + timedelta(minutes=minutes)
        if end_dt.day != start_dt.day:
            return "23:59"
        return end_dt.strftime("%H:%M")
    except ValueError:
        return ""


# Subtracts minutes from an HH:MM time while staying within the same day.
def subtract_minutes_from_time(time_str, minutes):
    if not time_str:
        return ""

    try:
        current_dt = datetime.strptime(time_str, "%H:%M")
        shifted = current_dt - timedelta(minutes=minutes)
        floor_dt = current_dt.replace(hour=0, minute=0)
        if shifted < floor_dt:
            shifted = floor_dt
        return shifted.strftime("%H:%M")
    except ValueError:
        return ""


# Converts raw time text into start_time and end_time in HH:MM format.
def parse_time_range(raw_time):
    if not raw_time:
        return "", ""

    text = str(raw_time).strip()
    text = text.replace("–", "-")
    separators = [" - ", "-", " to ", " until ", " through "]

    for sep in separators:
        if sep in text:
            left, right = text.split(sep, 1)
            start_time = parse_single_time(left.strip())
            end_time = parse_single_time(right.strip())
            if start_time and end_time:
                return start_time, end_time

    single_time = parse_single_time(text)
    if single_time:
        return single_time, add_minutes_to_time(single_time, 60)

    return "", ""


# Converts common date formats to YYYY-MM-DD.
def parse_date_to_iso(date_text):
    if not date_text:
        return ""

    cleaned = str(date_text).strip()
    cleaned = cleaned.replace("Sept ", "Sep ")
    cleaned = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", cleaned, flags=re.IGNORECASE)

    formats = [
        "%Y-%m-%d",
        "%A, %B %d, %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%A %B %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%B %d",
        "%b %d",
        "%m/%d",
        "%d/%m",
    ]

    current_year = datetime.now().year

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            if "%Y" not in fmt:
                dt = dt.replace(year=current_year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    parsed = pdf_tool._parse_date_string(cleaned, default_year=current_year)
    return parsed or ""


# Validates a basic email address.
def is_valid_email(email_text):
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (email_text or "").strip()))


# Strips helper markers from edited UI text.
def strip_inferred_marker(text):
    if text is None:
        return ""
    return re.sub(r"\s*\(inferred\)\s*", "", str(text), flags=re.IGNORECASE).strip()


# Turns a deadline time into a compact calendar marker window.
def deadline_event_window(due_time):
    due_time = parse_single_time(due_time) or due_time or "23:59"
    start_time = subtract_minutes_from_time(due_time, 1)
    if not start_time:
        start_time = due_time
    return start_time, due_time


# Returns True when an event should use the deadline conflict strategy.
def is_deadline_like_event(event_dict):
    event_dict = event_dict or {}
    title = (event_dict.get("title") or "").strip().lower()
    event_kind = (event_dict.get("event_kind") or "").strip().lower()
    description = (event_dict.get("description") or "").strip().lower()
    if event_dict.get("deadline_mode"):
        return True
    if event_kind in ["deadline", "assignment", "project", "assessment", "exam"]:
        return True
    keywords = ["assignment", "project", "deadline", "due", "midterm", "final exam", "quiz", "term project"]
    return any(keyword in title or keyword in description for keyword in keywords)


# Finds a non-conflicting one-minute slot before a due time for deadline-like items.
def resolve_deadline_slot(date, due_time, max_backshift_minutes=180):
    due_time = parse_single_time(due_time) or due_time or "23:59"
    end_time = due_time
    start_time, end_time = deadline_event_window(end_time)
    last_conflict = None

    for shift_count in range(max_backshift_minutes + 1):
        conflict_info = calendar_tool.check_conflict(date, start_time, end_time)
        if not conflict_info.get("conflict"):
            return {
                "start_time": start_time,
                "end_time": end_time,
                "shifted_minutes": shift_count,
                "conflict": False,
            }

        last_conflict = conflict_info
        end_time = subtract_minutes_from_time(end_time, 1)
        start_time = subtract_minutes_from_time(end_time, 1)
        if not end_time:
            break

    return {
        "start_time": "",
        "end_time": "",
        "shifted_minutes": max_backshift_minutes,
        "conflict": True,
        "conflict_info": last_conflict or {},
    }


# Converts one event_details object into the event format used by the app.
def normalize_event_details(
    event_details,
    thread_id="",
    thread_text="",
    source="email",
    response_required=False,
    response_type="none",
):
    if not event_details or not isinstance(event_details, dict):
        return None

    title = (event_details.get("title") or "").strip()
    raw_date = (event_details.get("date") or "").strip()
    date = parse_date_to_iso(raw_date)
    location = (event_details.get("location") or "").strip()
    raw_time = (event_details.get("time") or "").strip()

    if not title and not date and not raw_time and not location:
        return None

    start_time, end_time = parse_time_range(raw_time)

    return {
        "title": title,
        "date": date,
        "start_time": start_time,
        "end_time": end_time,
        "location": location,
        "description": "",
        "reminders_minutes": [1440, 120],
        "thread_id": thread_id,
        "thread_text": thread_text,
        "source": source,
        "response_required": response_required,
        "response_type": response_type,
        "raw_time": raw_time,
        "raw_date": raw_date,
    }


# Extracts events from a document and normalizes them for the app.
def extract_document_events(text, doc_type):
    raw_events = agent.extract_events_from_document(text, doc_type)
    normalized_events = []

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        title = (event.get("title") or "").strip()
        raw_date = (event.get("date") or "").strip()
        date = parse_date_to_iso(raw_date)
        start_time = (event.get("start_time") or "").strip()
        end_time = (event.get("end_time") or "").strip()
        location = (event.get("location") or "").strip()
        description = (event.get("description") or "").strip()
        reminders_minutes = event.get("reminders_minutes", [1440, 120])

        if not title and not date:
            continue

        if start_time and not end_time:
            end_time = add_minutes_to_time(start_time, 60)

        normalized_events.append({
            "title": title,
            "date": date,
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "description": description,
            "reminders_minutes": reminders_minutes,
            "source": "document",
            "response_required": False,
            "response_type": "none",
            "raw_time": f"{start_time} - {end_time}" if start_time or end_time else "",
            "raw_date": raw_date,
        })

    return normalized_events


# Gets reply details for the latest message in a thread so Gmail drafts stay threaded correctly.
def get_reply_details(thread_id):
    messages = email_tool.get_thread_messages(thread_id, last_n=5)

    if not messages:
        return None

    last_message = messages[-1]

    to_value = last_message.get("reply_to_header") or last_message.get("from_header") or ""
    subject = make_reply_subject(last_message.get("subject", ""))
    message_id = last_message.get("message_id", "")
    references = build_references(last_message.get("references", ""), message_id)

    return {
        "to": to_value,
        "subject": subject,
        "in_reply_to": message_id,
        "references": references,
    }


# Updates an already-created Gmail draft so the saved Gmail version matches the on-screen draft.
def sync_pending_draft_to_gmail(thread_id, updated_draft_text):
    if not thread_id:
        return False, "I could not update the Gmail draft because no thread is linked to it."

    draft_id = memory.get_draft_id(thread_id)

    if not draft_id:
        return False, "I could not update the Gmail draft because no saved draft ID was found for this thread."

    try:
        if not email_tool.draft_exists(draft_id):
            return False, "I could not update the Gmail draft because it no longer exists in Gmail."

        reply_details = get_reply_details(thread_id)

        if not reply_details:
            return False, "I could not update the Gmail draft because the reply details were unavailable."

        updated_draft_id = email_tool.update_draft(
            draft_id=draft_id,
            to=reply_details["to"],
            subject=reply_details["subject"],
            body=updated_draft_text,
            thread_id=thread_id,
            in_reply_to=reply_details["in_reply_to"],
            references=reply_details["references"],
        )

        if updated_draft_id:
            memory.save_draft_id(thread_id, updated_draft_id)

        return True, "I updated the draft based on your feedback."

    except Exception as e:
        return False, f"I could not update the Gmail draft. Error: {str(e)}"


# Refines the pending draft and keeps the Gmail draft synchronized with the UI version.
def apply_feedback_to_pending_draft(feedback_text):
    current_draft = st.session_state.get("pending_approval")
    thread_id = st.session_state.get("pending_thread_id")
    revision_count = int(st.session_state.get("pending_draft_revision_count", 0) or 0)

    if current_draft is None:
        return "There is no pending draft to update."

    revised_draft = agent.refine_draft(current_draft, feedback_text, revision_count=revision_count)
    route_notice = consume_last_route_notice()

    if thread_id:
        success, message = sync_pending_draft_to_gmail(thread_id, revised_draft)

        if success:
            st.session_state["pending_approval"] = revised_draft
            st.session_state["pending_draft_revision_count"] = revision_count + 1
            if route_notice:
                return message + " " + route_notice

        return message

    st.session_state["pending_approval"] = revised_draft
    st.session_state["pending_draft_revision_count"] = revision_count + 1
    if route_notice:
        return "I updated the pending draft with your feedback. " + route_notice
    return "I updated the pending draft with your feedback."


# Creates a threaded Gmail draft and saves its ID in memory.
def create_threaded_draft(thread_id, draft_text):
    reply_details = get_reply_details(thread_id)

    if not reply_details:
        return None

    draft_id = email_tool.create_draft(
        to=reply_details["to"],
        subject=reply_details["subject"],
        body=draft_text,
        thread_id=thread_id,
        in_reply_to=reply_details["in_reply_to"],
        references=reply_details["references"],
    )

    memory.save_draft_id(thread_id, draft_id)
    return draft_id


# Runs the email check and stores the actionable results.
def run_email_check_action(auto=False):
    summaries, classifications = email_flow.run_email_check()

    if summaries:
        add_message("assistant", format_bullet_message("Email check results:", summaries))
    elif not auto:
        add_message("assistant", "No new candidate emails were found.")

    existing_ids = {item.get("thread_id") for item in st.session_state["email_results"]}
    for item in classifications:
        if item.get("thread_id") not in existing_ids:
            st.session_state["email_results"].append(item)


# Processes an uploaded PDF or DOCX file.
def process_uploaded_file():
    file_name = st.session_state.get("uploaded_file_name")
    file_bytes = st.session_state.get("uploaded_file_bytes")

    if not file_name or file_bytes is None:
        return

    tmp_path = None

    try:
        suffix = os.path.splitext(file_name)[1].lower()
        if suffix not in [".pdf", ".docx"]:
            add_message("assistant", f"Unsupported file type for '{file_name}'. Please upload a PDF or DOCX file.")
            return

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(file_bytes)
            tmp_path = tmp_file.name

        if suffix == ".pdf":
            parsed_document = pdf_tool.parse_pdf_document(tmp_path)
        else:
            parsed_document = pdf_tool.parse_docx_document(tmp_path)

        text = parsed_document.get("raw_text", "")
        tables = parsed_document.get("tables", [])

        if not text.strip():
            parsing_note = describe_parsing_quality(parsed_document)
            extra = f" {parsing_note}" if parsing_note else ""
            add_message("assistant", f"I could not extract readable text from '{file_name}'.{extra}")
            return

        st.session_state["last_uploaded_text"] = text
        st.session_state["last_uploaded_tables"] = tables
        st.session_state["last_parsed_document"] = parsed_document

        doc_type = pdf_tool.detect_document_type(text)
        parsing_note = describe_parsing_quality(parsed_document)
        parsing_suffix = f" {parsing_note}" if parsing_note else ""

        if doc_type == "course_outline":
            deterministic_outline = pdf_tool.extract_course_outline(
                text,
                tables=tables,
                parsed_document=parsed_document,
            )
            extraction = agent.extract_course_outline_hybrid(
                text,
                tables=tables,
                parsed_document=parsed_document,
                deterministic_result=deterministic_outline,
                return_meta=True,
                file_path=tmp_path,
                file_name=file_name,
                mime_type=guess_mime_type(file_name),
            )
            outline = extraction.get("result", deterministic_outline)
            st.session_state["pending_course_outline"] = outline
            st.session_state["pending_invitation"] = None
            clear_review_editor_state()

            provider_text = describe_extraction_provider(extraction)
            status = extraction.get("status", "success")
            detail = extraction.get("message", "")
            detail_suffix = f" {detail}" if detail else ""
            if status == "no_change":
                add_message(
                    "assistant",
                    f"I detected a course outline in '{file_name}'. I reviewed it with {provider_text}, but the visible result stayed close to the deterministic extraction. Review the extracted course details below.{parsing_suffix}{detail_suffix}",
                )
            else:
                add_message(
                    "assistant",
                    f"I detected a course outline in '{file_name}' and processed it with {provider_text}. Review the extracted course details below.{parsing_suffix}{detail_suffix}",
                )
            return

        if doc_type == "invitation":
            deterministic_invitation = pdf_tool.extract_invitation_details(
                text,
                parsed_document=parsed_document,
                tables=tables,
            )
            extraction = agent.extract_invitation_hybrid(
                text,
                tables=tables,
                parsed_document=parsed_document,
                deterministic_result=deterministic_invitation,
                return_meta=True,
                file_path=tmp_path,
                file_name=file_name,
                mime_type=guess_mime_type(file_name),
            )
            invitation = extraction.get("result", deterministic_invitation)
            st.session_state["pending_invitation"] = invitation
            st.session_state["pending_course_outline"] = None
            clear_review_editor_state()

            provider_text = describe_extraction_provider(extraction)
            status = extraction.get("status", "success")
            detail = extraction.get("message", "")
            detail_suffix = f" {detail}" if detail else ""
            if status == "no_change":
                add_message(
                    "assistant",
                    f"I detected an invitation in '{file_name}'. I reviewed it with {provider_text}, but the visible result stayed close to the deterministic extraction. Review the extracted event below.{parsing_suffix}{detail_suffix}",
                )
            else:
                add_message(
                    "assistant",
                    f"I detected an invitation in '{file_name}' and processed it with {provider_text}. Review the extracted event below.{parsing_suffix}{detail_suffix}",
                )
            return

        events = extract_document_events(text, doc_type)
        for event in events:
            queue_pending_event(event)

        if events:
            add_message(
                "assistant",
                f"Uploaded '{file_name}' detected as '{doc_type}'. Found {len(events)} event(s) to review.{parsing_suffix}",
            )
        else:
            add_message(
                "assistant",
                f"Uploaded '{file_name}' detected as '{doc_type}'. No clear event was extracted.{parsing_suffix}",
            )

    except Exception as e:
        add_message("assistant", f"I could not process '{file_name}'. Error: {str(e)}")

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

        st.session_state["uploaded_file_name"] = None
        st.session_state["uploaded_file_bytes"] = None
        st.session_state["trigger_file_process"] = False


# Builds a plain text block from recent Gmail thread metadata.
def _build_threads_metadata_text():
    threads = email_tool.get_recent_threads(max_results=20)
    blocks = []

    for item in threads:
        headers = item.get("headers", {}) or {}
        block = [
            f"From: {item.get('sender', '')}",
            f"To: {headers.get('To', '')}",
            f"Reply-To: {headers.get('Reply-To', '')}",
            f"Subject: {item.get('subject', '')}",
        ]
        blocks.append("\n".join(block))

    return "\n\n---\n\n".join(blocks)


# Creates a new outbound email draft and stores it in session state.
def _draft_and_queue_outbound_email(recipient_name, recipient_email, subject_hint, body_notes):
    drafted = agent.draft_new_email(
        recipient_name=recipient_name,
        recipient_email=recipient_email,
        subject=subject_hint,
        body_notes=body_notes,
        conversation_history=st.session_state.get("history", []),
    )
    route_notice = consume_last_route_notice()

    st.session_state["pending_outbound_email"] = {
        "to": recipient_email,
        "subject": drafted.get("subject", subject_hint or ""),
        "body": drafted.get("body", body_notes or ""),
        "recipient_name": recipient_name,
    }

    message = f"I prepared a new outbound email draft for {recipient_name or recipient_email}. Review it below."
    if route_notice:
        message += " " + route_notice
    add_message("assistant", message)


# Handles a chat message that asks for a new outbound email.
def _handle_email_request_from_chat(text, classification):
    recipient_name = (classification.get("recipient_name") or "").strip()
    subject_hint = (classification.get("subject_hint") or "").strip()
    body_notes = (classification.get("email_body_notes") or text or "").strip()

    if not recipient_name:
        add_message("assistant", "Please include the recipient's name so I can draft the email correctly.")
        return ""

    try:
        threads_text = _build_threads_metadata_text()
    except Exception:
        threads_text = ""

    match = agent.find_contact_in_threads(recipient_name, threads_text)
    email_address = (match.get("email") or "").strip()
    display_name = (match.get("display_name") or recipient_name).strip() or recipient_name

    if not email_address:
        st.session_state["_awaiting_recipient_email"] = True
        st.session_state["_pending_email_request"] = {
            "recipient_name": recipient_name,
            "subject_hint": subject_hint,
            "body_notes": body_notes,
        }
        add_message("assistant", f"I could not find an email address for {recipient_name}. Please type the email address.")
        return ""

    _draft_and_queue_outbound_email(display_name, email_address, subject_hint, body_notes)
    return ""


# Resumes outbound email drafting after the user supplies an email address.
def _finish_outbound_email_with_address(email_address):
    pending_request = dict(st.session_state.get("_pending_email_request") or {})
    st.session_state["_awaiting_recipient_email"] = False

    email_address = (email_address or "").strip()
    if not is_valid_email(email_address):
        st.session_state["_awaiting_recipient_email"] = True
        add_message("assistant", "That does not look like a valid email address. Please type a full email address like name@example.com.")
        return ""

    recipient_name = pending_request.get("recipient_name", "")
    subject_hint = pending_request.get("subject_hint", "")
    body_notes = pending_request.get("body_notes", "")

    st.session_state["_pending_email_request"] = {}
    _draft_and_queue_outbound_email(recipient_name, email_address, subject_hint, body_notes)
    return ""


# Handles a pasted event from chat and queues it for review.
def _handle_event_paste_from_chat(classification):
    item_kind = (classification.get("item_kind") or "appointment").strip().lower() or "appointment"
    title = (classification.get("event_title") or classification.get("item_summary") or "").strip()
    date = parse_date_to_iso(classification.get("event_date") or "")
    location = (classification.get("event_location") or "").strip()
    event_time = (classification.get("event_time") or "").strip()
    event_end_time = (classification.get("event_end_time") or "").strip()
    start_time, end_time = parse_time_range(event_time)

    if event_end_time and not end_time:
        parsed_end = parse_single_time(event_end_time)
        if parsed_end:
            end_time = parsed_end

    if not start_time and date and item_kind == "task":
        start_time = "09:00"
        end_time = "09:30"
    elif start_time and not end_time:
        default_minutes = 30 if item_kind == "task" else 60
        end_time = add_minutes_to_time(start_time, default_minutes)

    event_dict = {
        "title": title or ("Task" if item_kind == "task" else "New event"),
        "date": date,
        "start_time": start_time,
        "end_time": end_time,
        "location": location,
        "description": f"Pasted from chat ({item_kind})",
        "reminders_minutes": [1440, 120],
        "source": "chat",
        "event_kind": item_kind,
        "deadline_mode": False,
        "response_required": False,
        "response_type": "none",
        "raw_time": event_time,
        "raw_date": classification.get("event_date", ""),
    }

    if not event_dict["date"]:
        add_message("assistant", "I found calendar details, but the date is still unclear. Please include the date more explicitly.")
        return ""

    queue_pending_event(event_dict)
    detected_label = item_kind.capitalize()
    add_message(
        "assistant",
        f"{detected_label} detected. I extracted '{event_dict['title']}' on {event_dict['date']} and queued it for calendar review.",
    )
    return ""


# Converts the result of st.data_editor into a list of dict records.
def _records_from_editor(editor_value):
    if isinstance(editor_value, pd.DataFrame):
        return editor_value.fillna("").to_dict(orient="records")

    if hasattr(editor_value, "to_dict"):
        try:
            df = pd.DataFrame(editor_value)
            return df.fillna("").to_dict(orient="records")
        except Exception:
            pass

    if isinstance(editor_value, list):
        records = []
        for item in editor_value:
            if isinstance(item, dict):
                records.append({k: "" if pd.isna(v) else v for k, v in item.items()})
        return records

    return []


# Converts edited session rows back into normalized records.
def _normalize_session_rows_from_editor(editor_value, course_defaults):
    normalized = []
    for row in _records_from_editor(editor_value):
        title = strip_inferred_marker(row.get("title", ""))
        date = parse_date_to_iso(strip_inferred_marker(row.get("date", "")))
        start_time = parse_single_time(strip_inferred_marker(row.get("start_time", ""))) or course_defaults.get("start_time", "")
        end_time = parse_single_time(strip_inferred_marker(row.get("end_time", ""))) or course_defaults.get("end_time", "")
        description = strip_inferred_marker(row.get("description", ""))
        session_number_raw = strip_inferred_marker(row.get("session_number", ""))
        try:
            session_number = int(float(session_number_raw)) if str(session_number_raw).strip() else 0
        except ValueError:
            session_number = 0

        date_inferred_raw = row.get("date_inferred", False)
        date_inferred_value = str(date_inferred_raw).strip().lower()
        date_inferred = date_inferred_value in ["true", "1", "yes"] or date_inferred_raw is True

        if not title or not date or not start_time or not end_time:
            continue

        normalized.append({
            "session_number": session_number,
            "date": date,
            "date_inferred": date_inferred,
            "start_time": start_time,
            "end_time": end_time,
            "title": title,
            "description": description,
            "reminders_minutes": [60],
        })

    return normalized


# Converts edited assignment rows back into normalized records.
def _normalize_assignment_rows_from_editor(editor_value):
    normalized = []
    for row in _records_from_editor(editor_value):
        title = strip_inferred_marker(row.get("title", ""))
        due_date = parse_date_to_iso(strip_inferred_marker(row.get("due_date", "")))
        raw_due_time = strip_inferred_marker(row.get("due_time", ""))
        due_time = parse_single_time(raw_due_time) or ("23:59" if due_date else "")
        description = strip_inferred_marker(row.get("description", ""))

        due_time_inferred_raw = row.get("due_time_inferred", False)
        due_time_inferred_value = str(due_time_inferred_raw).strip().lower()
        due_time_inferred = due_time_inferred_value in ["true", "1", "yes"] or due_time_inferred_raw is True
        if not raw_due_time and due_date:
            due_time_inferred = True

        if not title or not due_date:
            continue

        normalized.append({
            "title": title,
            "due_date": due_date,
            "due_time": due_time,
            "due_time_inferred": due_time_inferred,
            "description": description,
            "reminders_minutes": [10080, 1440],
        })

    return normalized


# Builds session rows for AI re-check without dropping useful partial rows.
def _preserve_session_rows_for_recheck(editor_value, course_defaults):
    preserved = []
    for row in _records_from_editor(editor_value):
        title = strip_inferred_marker(row.get("title", ""))
        date_text = strip_inferred_marker(row.get("date", ""))
        description = strip_inferred_marker(row.get("description", ""))
        session_number_raw = strip_inferred_marker(row.get("session_number", ""))
        start_time_text = strip_inferred_marker(row.get("start_time", ""))
        end_time_text = strip_inferred_marker(row.get("end_time", ""))

        normalized_date = parse_date_to_iso(date_text) if date_text else ""
        normalized_start = parse_single_time(start_time_text) or start_time_text or course_defaults.get("start_time", "")
        normalized_end = parse_single_time(end_time_text) or end_time_text or course_defaults.get("end_time", "")

        try:
            session_number = int(float(session_number_raw)) if str(session_number_raw).strip() else 0
        except ValueError:
            session_number = 0

        date_inferred_raw = row.get("date_inferred", False)
        date_inferred_value = str(date_inferred_raw).strip().lower()
        date_inferred = date_inferred_value in ["true", "1", "yes"] or date_inferred_raw is True

        has_any_content = any([
            title,
            date_text,
            start_time_text,
            end_time_text,
            description,
            str(session_number_raw).strip(),
        ])
        if not has_any_content:
            continue

        preserved.append({
            "session_number": session_number,
            "date": normalized_date,
            "date_inferred": date_inferred,
            "start_time": normalized_start,
            "end_time": normalized_end,
            "title": title,
            "description": description,
            "reminders_minutes": [60],
        })

    return preserved


# Builds assignment rows for AI re-check without dropping useful partial rows.
def _preserve_assignment_rows_for_recheck(editor_value):
    preserved = []
    for row in _records_from_editor(editor_value):
        title = strip_inferred_marker(row.get("title", ""))
        due_date_text = strip_inferred_marker(row.get("due_date", ""))
        raw_due_time = strip_inferred_marker(row.get("due_time", ""))
        description = strip_inferred_marker(row.get("description", ""))

        due_date = parse_date_to_iso(due_date_text) if due_date_text else ""
        due_time = parse_single_time(raw_due_time) or raw_due_time

        due_time_inferred_raw = row.get("due_time_inferred", False)
        due_time_inferred_value = str(due_time_inferred_raw).strip().lower()
        due_time_inferred = due_time_inferred_value in ["true", "1", "yes"] or due_time_inferred_raw is True

        has_any_content = any([title, due_date_text, raw_due_time, description])
        if not has_any_content:
            continue

        preserved.append({
            "title": title,
            "due_date": due_date,
            "due_time": due_time,
            "due_time_inferred": due_time_inferred,
            "description": description,
            "reminders_minutes": [10080, 1440],
        })

    return preserved


# Builds the current edited course outline so AI re-evaluation uses the live UI values.
def _build_current_course_outline_for_recheck(original_outline, course_name, days_of_week_text, start_time, end_time, term_start, term_end, location, edited_sessions, edited_assignments):
    current_outline = dict(original_outline or {})
    current_outline["course_name"] = course_name.strip()
    current_outline["days_of_week"] = pdf_tool._normalize_weekday_tokens([part.strip() for part in days_of_week_text.split(",") if part.strip()])
    current_outline["start_time"] = parse_single_time(start_time) or start_time.strip()
    current_outline["end_time"] = parse_single_time(end_time) or end_time.strip()
    current_outline["term_start_date"] = parse_date_to_iso(term_start)
    current_outline["term_end_date"] = parse_date_to_iso(term_end)
    current_outline["location"] = location.strip()
    current_outline["sessions"] = _preserve_session_rows_for_recheck(edited_sessions, current_outline)
    current_outline["assignments"] = _preserve_assignment_rows_for_recheck(edited_assignments)
    current_outline["extraction_contexts"] = dict((original_outline or {}).get("extraction_contexts", {}))
    return current_outline


# Builds the current edited invitation so AI re-evaluation uses the live UI values.
def _build_current_invitation_for_recheck(invite_title, invite_date, invite_start, invite_end, invite_location, invite_host, invite_rsvp_required, invite_rsvp_deadline, invite_description, original_invitation):
    current_invitation = dict(original_invitation or {})
    current_invitation["event_title"] = invite_title.strip()
    current_invitation["event_date"] = parse_date_to_iso(invite_date)
    current_invitation["event_time"] = parse_single_time(invite_start) or invite_start.strip()
    current_invitation["event_end_time"] = parse_single_time(invite_end) or invite_end.strip()
    current_invitation["location"] = invite_location.strip()
    current_invitation["host"] = invite_host.strip()
    current_invitation["rsvp_required"] = bool(invite_rsvp_required)
    current_invitation["rsvp_deadline"] = parse_date_to_iso(invite_rsvp_deadline) if str(invite_rsvp_deadline).strip() else ""
    current_invitation["description"] = invite_description.strip()
    return current_invitation


# Adds course sessions and assessments to the calendar using the edited values from the UI.
def _approve_course_outline(edited_outline, edited_sessions, edited_assignments):
    added_count = 0
    skipped_count = 0

    for session in edited_sessions:
        title = session.get("title", "")
        date = session.get("date", "")
        start_time = session.get("start_time", "")
        end_time = session.get("end_time", "")
        description = session.get("description", "")

        if memory.event_exists(title, date):
            skipped_count += 1
            continue

        conflict_info = calendar_tool.check_conflict(date, start_time, end_time)
        if conflict_info.get("conflict"):
            add_message(
                "assistant",
                f"Skipped class '{title}' on {date} because it conflicts with '{conflict_info.get('clashing_event')}'.",
            )
            skipped_count += 1
            continue

        calendar_tool.create_event(
            title=title,
            date_str=date,
            start_time_str=start_time,
            end_time_str=end_time,
            location=edited_outline.get("location", ""),
            description=description,
            reminders_minutes=[60],
        )
        memory.save_event(title, date)
        added_count += 1

    for assignment in edited_assignments:
        title = assignment.get("title", "")
        date = assignment.get("due_date", "")
        due_time = assignment.get("due_time", "") or "23:59"

        if memory.event_exists(title, date):
            skipped_count += 1
            continue

        slot_info = resolve_deadline_slot(date, due_time)
        if slot_info.get("conflict"):
            conflict_info = slot_info.get("conflict_info", {})
            add_message(
                "assistant",
                f"Skipped deadline '{title}' on {date} because all nearby deadline slots were occupied by '{conflict_info.get('clashing_event', 'another event')}'.",
            )
            skipped_count += 1
            continue

        start_time = slot_info.get("start_time", "")
        end_time = slot_info.get("end_time", "")
        description = assignment.get("description", "")
        if due_time:
            description = (description + "\n" if description else "") + f"Due by {due_time}"

        calendar_tool.create_event(
            title=title,
            date_str=date,
            start_time_str=start_time,
            end_time_str=end_time,
            location=edited_outline.get("location", ""),
            description=description,
            reminders_minutes=[10080, 1440],
        )
        memory.save_event(title, date)
        added_count += 1

    add_message("assistant", f"Course outline approved. Added {added_count} calendar item(s) and skipped {skipped_count} duplicate or conflicting item(s).")
    st.session_state["pending_course_outline"] = None
    clear_review_editor_state()


# Builds a calendar event dict from the pending invitation data.
def _build_invitation_event_dict(invitation):
    event_date = parse_date_to_iso(invitation.get("event_date", ""))
    start_time = parse_single_time(invitation.get("event_time", "")) or ""
    end_time = parse_single_time(invitation.get("event_end_time", "")) or ""

    if start_time and not end_time:
        end_time = add_minutes_to_time(start_time, 60)

    description_lines = []
    if invitation.get("host"):
        description_lines.append(f"Host: {invitation.get('host')}")
    if invitation.get("description"):
        description_lines.append(invitation.get("description"))
    if invitation.get("rsvp_required") and invitation.get("rsvp_deadline"):
        description_lines.append(f"RSVP by {invitation.get('rsvp_deadline')}")

    return {
        "title": invitation.get("event_title", "") or "Invitation event",
        "date": event_date,
        "start_time": start_time,
        "end_time": end_time,
        "location": invitation.get("location", ""),
        "description": "\n".join(description_lines),
        "reminders_minutes": [1440, 120],
    }


# Builds an editable event dict from the calendar review form values.
def _edited_pending_event(event, prefix):
    edited = dict(event or {})
    title = st.text_input("Title", value=event.get("title", ""), key=f"{prefix}_title")
    date = st.text_input("Date", value=event.get("date", ""), key=f"{prefix}_date")

    col1, col2 = st.columns(2)
    with col1:
        start_time = st.text_input("Start time", value=event.get("start_time", ""), key=f"{prefix}_start")
    with col2:
        end_time = st.text_input("End time", value=event.get("end_time", ""), key=f"{prefix}_end")

    location = st.text_input("Location", value=event.get("location", ""), key=f"{prefix}_location")
    description = st.text_area("Description", value=event.get("description", ""), key=f"{prefix}_description", height=80)

    edited["title"] = title.strip()
    edited["date"] = parse_date_to_iso(date) or date.strip()
    edited["start_time"] = parse_single_time(start_time) or start_time.strip()
    edited["end_time"] = parse_single_time(end_time) or end_time.strip()
    edited["location"] = location.strip()
    edited["description"] = description.strip()

    if edited.get("start_time") and not edited.get("end_time"):
        edited["end_time"] = add_minutes_to_time(edited.get("start_time"), 60)

    return edited


# Handles normal chat input.
def handle_user_message(text):
    if st.session_state["pending_approval"]:
        return apply_feedback_to_pending_draft(text)

    if st.session_state.get("_awaiting_recipient_email"):
        return _finish_outbound_email_with_address(text)

    classification = agent.classify_chat_input(text)
    intent = classification.get("intent", "general")

    if intent == "email_request":
        return _handle_email_request_from_chat(text, classification)

    if intent == "calendar_item_paste":
        return _handle_event_paste_from_chat(classification)

    return "Use 'Check Emails Now' to review email actions, upload a PDF or DOCX to process a document, or ask me to draft a new email."


# Sends the currently pending Gmail draft.
def approve_draft():
    thread_id = st.session_state["pending_thread_id"]

    if not thread_id:
        add_message("assistant", "No thread is linked to the pending draft.")
        return

    draft_id = memory.get_draft_id(thread_id)

    if not draft_id:
        add_message("assistant", "I could not find the draft ID for this thread.")
        return

    try:
        if not email_tool.draft_exists(draft_id):
            add_message("assistant", "I could not send the draft because it no longer exists in Gmail.")
            return

        email_tool.send_draft(draft_id)
        add_message("assistant", "Draft sent successfully.")

    except Exception as e:
        add_message("assistant", f"I could not send the draft. Error: {str(e)}")
        return

    if st.session_state["pending_event_after_send"]:
        queue_pending_event(st.session_state["pending_event_after_send"])
        add_message("assistant", "The related event is now ready for calendar review.")

    clear_pending_draft_state()


# Adds one event to Google Calendar after checking for conflicts.
def add_event_to_calendar(event_dict):
    title = event_dict.get("title", "")
    date = parse_date_to_iso(event_dict.get("date", ""))
    start_time = event_dict.get("start_time", "")
    end_time = event_dict.get("end_time", "")
    location = event_dict.get("location", "")
    description = event_dict.get("description", "")
    reminders = event_dict.get("reminders_minutes", [])

    if start_time and not end_time:
        end_time = add_minutes_to_time(start_time, 60)

    if is_deadline_like_event(event_dict) and date:
        due_time = end_time or start_time or "23:59"
        slot_info = resolve_deadline_slot(date, due_time)
        if slot_info.get("conflict"):
            conflict_info = slot_info.get("conflict_info", {})
            add_message(
                "assistant",
                f"Cannot schedule deadline '{title}' because all nearby deadline slots were occupied by '{conflict_info.get('clashing_event', 'another event')}'.",
            )
            return
        start_time = slot_info.get("start_time", start_time)
        end_time = slot_info.get("end_time", end_time)

    if not title or not date or not start_time or not end_time:
        add_message(
            "assistant",
            f"I could not add '{title or 'this event'}' because the date or time is incomplete.",
        )
        return

    if memory.event_exists(title, date):
        add_message("assistant", f"The event '{title}' on {date} already exists in memory.")
        return

    if not is_deadline_like_event(event_dict):
        conflict_info = calendar_tool.check_conflict(date, start_time, end_time)
        if conflict_info["conflict"]:
            add_message(
                "assistant",
                f"Cannot schedule '{title}' because it conflicts with '{conflict_info['clashing_event']}'. Suggested alternative start time: {conflict_info['suggested_time']}",
            )
            return

    calendar_tool.create_event(
        title=title,
        date_str=date,
        start_time_str=start_time,
        end_time_str=end_time,
        location=location,
        description=description,
        reminders_minutes=reminders,
    )

    memory.save_event(title, date)
    add_message("assistant", f"Event '{title}' was added to your calendar successfully.")

    st.session_state["pending_events"] = [
        item for item in st.session_state["pending_events"]
        if not (
            item.get("title") == title
            and item.get("date") == date
            and item.get("start_time") == start_time
        )
    ]


st.set_page_config(layout="wide", page_title="AI Personal Assistant")
st.title("AI Personal Assistant")

init_session_state()
app_settings = load_app_settings()
apply_runtime_settings(app_settings)


with st.sidebar:
    st.header("Controls")

    if st.button("Settings"):
        st.session_state["show_settings"] = not st.session_state.get("show_settings", False)

    if st.session_state.get("show_settings", False):
        with st.container(border=True):
            st.markdown("**Settings**")

            debug_enabled = st.checkbox(
                "Debugging tools",
                value=bool(app_settings.get("debug_tools_enabled", False)),
                help="Turns debug tools on or off, including the Clear Memory button.",
                key="settings_debug_toggle",
            )
            if debug_enabled != bool(app_settings.get("debug_tools_enabled", False)):
                app_settings["debug_tools_enabled"] = bool(debug_enabled)
                settings_store.save_settings(app_settings)
                apply_runtime_settings(app_settings)
                st.rerun()

            st.markdown("**Local LLMs**")
            installed_local_models = settings_store.get_all_local_models(app_settings)
            if not installed_local_models:
                st.caption("No Ollama local models detected yet.")
            local_model_options = installed_local_models or [app_settings.get("selected_local_model") or os.getenv("LOCAL_LLM_MODEL", "qwen2.5:7b-instruct")]
            current_local_model = app_settings.get("selected_local_model") or local_model_options[0]
            selected_local_model = st.selectbox(
                "Installed local models",
                options=local_model_options,
                index=local_model_options.index(current_local_model) if current_local_model in local_model_options else 0,
                key="settings_local_model_select",
            )
            if selected_local_model and selected_local_model != app_settings.get("selected_local_model"):
                app_settings["selected_local_model"] = selected_local_model
                app_settings = settings_store.upsert_local_model(app_settings, selected_local_model)
                settings_store.save_settings(app_settings)
                apply_runtime_settings(app_settings)

            new_local_model = st.text_input("Add new local model", key="settings_new_local_model", placeholder="Example: llama3.2 or qwen2.5:14b")
            if st.button("Download with Ollama", key="settings_download_local_model"):
                app_settings = settings_store.upsert_local_model(app_settings, new_local_model)
                settings_store.save_settings(app_settings)
                launch_result = settings_store.launch_ollama_pull(new_local_model)
                if launch_result.get("success"):
                    st.success(launch_result.get("message", "Started local model download."))
                else:
                    st.error(launch_result.get("message", "Could not start local model download."))

            st.markdown("**API**")
            api_labels = settings_store.get_api_labels(app_settings)
            current_api_label = app_settings.get("active_api_label", "Environment GEMINI_API_KEY")
            if current_api_label not in api_labels:
                current_api_label = "Environment GEMINI_API_KEY"
            selected_api_label = st.selectbox(
                "Current API source",
                options=api_labels,
                index=api_labels.index(current_api_label),
                key="settings_api_label_select",
            )
            if selected_api_label != app_settings.get("active_api_label"):
                app_settings["active_api_label"] = selected_api_label
                settings_store.save_settings(app_settings)
                apply_runtime_settings(app_settings)
                st.rerun()

            masked_api_value = settings_store.get_masked_api_value(app_settings)
            if masked_api_value:
                st.caption(f"Active key: {masked_api_value}")
            else:
                st.caption("No active API key detected.")

            new_api_label = st.text_input("Add API label", key="settings_new_api_label", placeholder="Example: My Gemini Key")
            new_api_key = st.text_input("Add API key", key="settings_new_api_key", type="password")
            if st.button("Save API key", key="settings_save_api_key"):
                app_settings = settings_store.add_api_entry(app_settings, new_api_label, new_api_key)
                settings_store.save_settings(app_settings)
                apply_runtime_settings(app_settings)
                st.success("API key saved.")
                st.rerun()

            st.markdown("**Cloud AI models**")
            for model_name in settings_store.get_cloud_models(app_settings):
                st.caption(f"- {model_name}")
            new_cloud_model = st.text_input("Add new cloud model", key="settings_new_cloud_model", placeholder="Example: gemini-2.0-flash")
            if st.button("Add cloud model", key="settings_add_cloud_model"):
                app_settings = settings_store.add_cloud_model(app_settings, new_cloud_model)
                settings_store.save_settings(app_settings)
                apply_runtime_settings(app_settings)
                st.success(f"Added cloud model '{new_cloud_model}'.")
                st.rerun()

    llm_options = agent.get_model_selection_options(
        cloud_models=settings_store.get_cloud_models(app_settings),
        local_models=settings_store.get_all_local_models(app_settings) or [app_settings.get("selected_local_model") or os.getenv("LOCAL_LLM_MODEL", "qwen2.5:7b-instruct")],
    )
    current_choice = st.session_state.get("llm_choice", agent.get_default_model_choice())
    if current_choice not in llm_options:
        current_choice = agent.get_default_model_choice()
        if current_choice not in llm_options:
            current_choice = llm_options[0]
    llm_choice = st.selectbox(
        "LLM / extraction model",
        options=llm_options,
        index=llm_options.index(current_choice),
    )
    st.session_state["llm_choice"] = llm_choice
    agent.set_runtime_model_choice(llm_choice)

    if st.button("Check Emails Now"):
        st.session_state["trigger_email_check"] = True

    uploaded_file = st.file_uploader("Upload a PDF or Word doc", type=["pdf", "docx"])
    if uploaded_file is not None:
        uploaded_bytes = uploaded_file.getvalue()
        signature = f"{uploaded_file.name}_{uploaded_file.size}_{hashlib.md5(uploaded_bytes).hexdigest()}"
        if signature != st.session_state["last_upload_signature"]:
            st.session_state["uploaded_file_name"] = uploaded_file.name
            st.session_state["uploaded_file_bytes"] = uploaded_bytes
            st.session_state["trigger_file_process"] = True
            st.session_state["last_upload_signature"] = signature

    interval_options = [15, 30, 60]
    current_interval = int(st.session_state.get("check_interval", 30) or 30)
    if current_interval not in interval_options:
        current_interval = 30
    interval = st.selectbox(
        "Email check interval (minutes)",
        options=interval_options,
        index=interval_options.index(current_interval),
    )
    st.session_state["check_interval"] = interval

    auto_check_enabled = st.checkbox(
        "Automatic email checks while this page is open",
        value=bool(st.session_state.get("auto_email_check_enabled", True)),
        help="When enabled, the page refreshes at the selected interval and checks for new emails automatically.",
    )
    st.session_state["auto_email_check_enabled"] = auto_check_enabled

    if bool(app_settings.get("debug_tools_enabled", False)):
        if st.button("Clear Memory"):
            if os.path.exists("agent_memory.json"):
                os.remove("agent_memory.json")
            add_message("assistant", "Agent memory cleared.")

maybe_run_scheduled_email_check()

if st.session_state["trigger_email_check"]:
    run_email_check_action(auto=False)
    st.session_state["trigger_email_check"] = False
    st.session_state["last_email_check_at"] = time.time()

if st.session_state["trigger_file_process"]:
    process_uploaded_file()

apply_email_preparation_plan()

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


if st.session_state["email_results"]:
    st.subheader("Email actions")

    for item in list(st.session_state["email_results"]):
        thread_id = item.get("thread_id", "")
        thread_text = item.get("thread_text", "")
        sender = item.get("sender", "Unknown")
        subject = item.get("subject", "")
        category = item.get("category", "business").title()
        summary = item.get("summary", "")
        response_required = item.get("response_required", False)
        response_type = item.get("response_type", "none")
        confidence = item.get("confidence", "unknown")

        event = normalize_event_details(
            item.get("event_details"),
            thread_id=thread_id,
            thread_text=thread_text,
            source="email",
            response_required=response_required,
            response_type=response_type,
        )

        with st.expander(f"[{category}] {summary or subject or 'Email'}"):
            st.markdown(f"**From:** {sender}")
            st.markdown(f"**Subject:** {subject}")
            st.markdown(f"**Confidence:** {confidence}")
            st.markdown(f"**Reply needed:** {response_required}")

            if summary:
                st.markdown(f"**Summary:** {summary}")

            if event:
                st.markdown("**Detected event:**")
                st.markdown(f"- Title: {event.get('title', '')}")
                st.markdown(f"- Date: {event.get('date', '')}")
                st.markdown(f"- Time: {event.get('start_time', '')} to {event.get('end_time', '')}")
                st.markdown(f"- Location: {event.get('location', '')}")

            if response_type != "rsvp":
                prepared_draft = get_precomputed_email_draft(thread_id)
                if prepared_draft and prepared_draft.get("draft_text"):
                    st.caption("A draft has been prepared in memory for this email. Gmail will only be updated if you click Create Reply Draft.")

                user_notes = st.text_area(
                    "Notes for the reply",
                    key=f"reply_notes_{thread_id}",
                    placeholder="Example: thank them, confirm tomorrow works, keep it brief",
                )

                action_columns = st.columns(3 if event else 2)

                with action_columns[0]:
                    if st.button("Create Reply Draft", key=f"create_reply_{thread_id}"):
                        if user_notes.strip():
                            draft_text = agent.draft_response(thread_text, user_notes)
                        elif prepared_draft and prepared_draft.get("draft_text"):
                            draft_text = prepared_draft.get("draft_text", "")
                        else:
                            draft_text = agent.draft_response(thread_text, "")

                        draft_id = create_threaded_draft(thread_id, draft_text)
                        route_notice = consume_last_route_notice()

                        if draft_id:
                            memory.save_decision(thread_id, "yes")
                            set_pending_draft_state(draft_text, thread_id=thread_id, pending_event=None)
                            message = "I created a draft reply for your approval."
                            if route_notice:
                                message += " " + route_notice
                            add_message("assistant", message)
                            remove_email_result(thread_id)
                        else:
                            add_message("assistant", "I could not create a threaded Gmail draft for this email.")

                        st.rerun()

                if event:
                    with action_columns[1]:
                        if st.button("Queue Event for Calendar", key=f"queue_event_with_reply_{thread_id}"):
                            result = email_flow.handle_event_decision(
                                thread_id=thread_id,
                                thread_text=thread_text,
                                event_details=event,
                                attending=True,
                                response_required=False,
                            )
                            queue_pending_event(result.get("event_details", event))
                            add_message("assistant", result.get("message", "Event queued for calendar review."))
                            remove_email_result(thread_id)
                            st.rerun()

                    dismiss_column = action_columns[2]
                else:
                    dismiss_column = action_columns[1]

                with dismiss_column:
                    if st.button("Ignore This Email", key=f"ignore_reply_{thread_id}"):
                        message = email_flow.handle_reply_request(
                            thread_id=thread_id,
                            thread_text=thread_text,
                            user_choice="no",
                            user_notes="",
                        )
                        add_message("assistant", message)
                        remove_email_result(thread_id)
                        st.rerun()

            elif event and response_type == "rsvp":
                prepared_draft = get_precomputed_email_draft(thread_id)
                if prepared_draft and prepared_draft.get("draft_text"):
                    st.caption("An RSVP draft has been prepared in memory. Gmail will only be updated if you click Attend and Draft RSVP.")

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("Attend and Draft RSVP", key=f"attend_{thread_id}"):
                        if prepared_draft and prepared_draft.get("draft_text"):
                            draft_text = prepared_draft.get("draft_text", "")
                            pending_event = event
                        else:
                            result = email_flow.handle_event_decision(
                                thread_id=thread_id,
                                thread_text=thread_text,
                                event_details=event,
                                attending=True,
                                response_required=True,
                            )
                            draft_text = result.get("draft_text")
                            pending_event = result.get("event_details", event)

                        if not draft_text:
                            add_message("assistant", "I could not create the RSVP draft.")
                        else:
                            draft_id = create_threaded_draft(thread_id, draft_text)

                            if not draft_id:
                                add_message("assistant", "I could not create a threaded Gmail draft for this RSVP.")
                            else:
                                memory.save_decision(thread_id, "yes")
                                set_pending_draft_state(draft_text, thread_id=thread_id, pending_event=pending_event)
                                route_notice = consume_last_route_notice()
                                message = "I created an RSVP draft for your approval."
                                if route_notice:
                                    message += " " + route_notice
                                add_message("assistant", message)
                                remove_email_result(thread_id)

                        st.rerun()

                with col2:
                    if st.button("Decline Invitation", key=f"decline_{thread_id}"):
                        result = email_flow.handle_event_decision(
                            thread_id=thread_id,
                            thread_text=thread_text,
                            event_details=event,
                            attending=False,
                            response_required=True,
                        )
                        add_message("assistant", result.get("message", "Invitation declined."))
                        remove_email_result(thread_id)
                        st.rerun()

            elif event and response_type != "rsvp":
                col1, col2 = st.columns(2)

                with col1:
                    if st.button("Queue Event for Calendar", key=f"queue_event_{thread_id}"):
                        result = email_flow.handle_event_decision(
                            thread_id=thread_id,
                            thread_text=thread_text,
                            event_details=event,
                            attending=True,
                            response_required=False,
                        )
                        queue_pending_event(result.get("event_details", event))
                        add_message("assistant", result.get("message", "Event queued for calendar review."))
                        remove_email_result(thread_id)
                        st.rerun()

                with col2:
                    if st.button("Dismiss", key=f"dismiss_event_{thread_id}"):
                        memory.save_decision(thread_id, "no")
                        add_message("assistant", "OK, I will not ask about this event again.")
                        remove_email_result(thread_id)
                        st.rerun()

            else:
                if st.button("Dismiss", key=f"dismiss_{thread_id}"):
                    memory.save_decision(thread_id, "no")
                    add_message("assistant", "OK, I will not ask about this email again.")
                    remove_email_result(thread_id)
                    st.rerun()


if st.session_state["pending_approval"] is not None:
    st.subheader("Draft approval")
    st.info(st.session_state["pending_approval"])

    feedback = st.text_area(
        "Request changes before sending",
        key="draft_feedback_box",
        placeholder="Example: make it warmer, shorter, and more formal",
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Approve and Send Now"):
            approve_draft()
            st.rerun()

    with col2:
        if st.button("Apply Changes"):
            if feedback.strip():
                message = apply_feedback_to_pending_draft(feedback)
                add_message("assistant", message)
                st.rerun()


if st.session_state.get("pending_outbound_email"):
    st.subheader("Outbound email draft")
    draft = st.session_state["pending_outbound_email"]

    outbound_to = st.text_input("To", value=draft.get("to", ""), key="outbound_to_input")
    outbound_subject = st.text_input("Subject", value=draft.get("subject", ""), key="outbound_subject_input")
    outbound_body = st.text_area("Message", value=draft.get("body", ""), key="outbound_body_input", height=220)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Send outbound email"):
            if not outbound_to.strip() or not outbound_subject.strip() or not outbound_body.strip():
                add_message("assistant", "Please complete the To, Subject, and Message fields before sending.")
            elif not is_valid_email(outbound_to.strip()):
                add_message("assistant", "Please enter a valid recipient email address.")
            else:
                try:
                    draft_id = email_tool.create_draft(
                        to=outbound_to.strip(),
                        subject=outbound_subject.strip(),
                        body=outbound_body,
                    )
                    email_tool.send_draft(draft_id)
                    add_message("assistant", f"Your email to {outbound_to.strip()} was sent successfully.")
                    st.session_state["pending_outbound_email"] = None
                except Exception as e:
                    add_message("assistant", f"I could not send the outbound email. Error: {str(e)}")
            st.rerun()

    with col2:
        if st.button("Discard outbound draft"):
            st.session_state["pending_outbound_email"] = None
            add_message("assistant", "The outbound email draft was discarded.")
            st.rerun()


if st.session_state.get("pending_course_outline"):
    st.subheader("Course outline review")
    outline = st.session_state["pending_course_outline"]

    course_name = st.text_input("Course name", value=outline.get("course_name", ""), key="course_name_input")
    days_of_week_text = st.text_input(
        "Days of week",
        value=", ".join(outline.get("days_of_week", [])),
        key="course_days_input",
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        start_time = st.text_input("Class start time", value=outline.get("start_time", ""), key="course_start_time_input")
    with col2:
        end_time = st.text_input("Class end time", value=outline.get("end_time", ""), key="course_end_time_input")
    with col3:
        location = st.text_input("Location", value=outline.get("location", ""), key="course_location_input")

    col4, col5 = st.columns(2)
    with col4:
        term_start = st.text_input("Term start date", value=outline.get("term_start_date", ""), key="course_term_start_input")
    with col5:
        term_end = st.text_input("Term end date", value=outline.get("term_end_date", ""), key="course_term_end_input")

    sessions_df = pd.DataFrame([{k: v for k, v in row.items() if k != "reminders_minutes"} for row in outline.get("sessions", [])])
    if sessions_df.empty:
        sessions_df = pd.DataFrame(columns=["session_number", "date", "date_inferred", "start_time", "end_time", "title", "description"])

    assignments_df = pd.DataFrame([{k: v for k, v in row.items() if k != "reminders_minutes"} for row in outline.get("assignments", [])])
    if assignments_df.empty:
        assignments_df = pd.DataFrame(columns=["title", "due_date", "due_time", "due_time_inferred", "description"])

    st.markdown("**Sessions**")
    edited_sessions = st.data_editor(sessions_df, key="course_sessions_editor", num_rows="dynamic", width="stretch")

    st.markdown("**Assignments and assessments**")
    edited_assignments = st.data_editor(assignments_df, key="course_assignments_editor", num_rows="dynamic", width="stretch")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Approve course outline"):
            edited_outline = {
                "course_name": course_name.strip(),
                "days_of_week": pdf_tool._normalize_weekday_tokens([part.strip() for part in days_of_week_text.split(",") if part.strip()]),
                "start_time": parse_single_time(start_time) or start_time.strip(),
                "end_time": parse_single_time(end_time) or end_time.strip(),
                "term_start_date": parse_date_to_iso(term_start),
                "term_end_date": parse_date_to_iso(term_end),
                "location": location.strip(),
            }
            normalized_sessions = _normalize_session_rows_from_editor(edited_sessions, edited_outline)
            normalized_assignments = _normalize_assignment_rows_from_editor(edited_assignments)
            _approve_course_outline(edited_outline, normalized_sessions, normalized_assignments)
            st.rerun()

    with col2:
        if st.button("Re-evaluate with AI", key="recheck_course_outline"):
            current_outline = _build_current_course_outline_for_recheck(
                outline,
                course_name,
                days_of_week_text,
                start_time,
                end_time,
                term_start,
                term_end,
                location,
                edited_sessions,
                edited_assignments,
            )
            verification = agent.verify_course_extraction(
                st.session_state.get("last_uploaded_text", ""),
                current_outline,
                current_outline.get("extraction_contexts", {}),
                extracted_tables=st.session_state.get("last_uploaded_tables", []),
                parsed_document=st.session_state.get("last_parsed_document"),
                return_meta=True,
            )
            st.session_state["pending_course_outline"] = verification.get("result", current_outline)
            clear_review_editor_state()

            status = verification.get("status", "error")
            detail = verification.get("message", "")
            provider_text = describe_extraction_provider(verification)
            if status == "success":
                add_message("assistant", f"I re-evaluated the course outline with {provider_text} and applied the updated result. Please review the refreshed fields.")
            elif status == "no_change":
                add_message("assistant", f"{provider_text.capitalize()} reviewed the course outline but did not find supported visible changes beyond the current values.")
            else:
                fallback_message = f"{provider_text.capitalize()} did not return usable structured updates, so I kept the current course outline values."
                if detail:
                    fallback_message += f" Details: {detail}"
                add_message("assistant", fallback_message)
            st.rerun()

    with col3:
        if st.button("Dismiss course outline"):
            st.session_state["pending_course_outline"] = None
            clear_review_editor_state()
            add_message("assistant", "The course outline review was dismissed.")
            st.rerun()


if st.session_state.get("pending_invitation"):
    st.subheader("Invitation review")
    invitation = st.session_state["pending_invitation"]

    invite_title = st.text_input("Event title", value=invitation.get("event_title", ""), key="invite_title_input")

    col1, col2, col3 = st.columns(3)
    with col1:
        invite_date = st.text_input("Event date", value=invitation.get("event_date", ""), key="invite_date_input")
    with col2:
        invite_start = st.text_input("Start time", value=invitation.get("event_time", ""), key="invite_start_input")
    with col3:
        invite_end = st.text_input("End time", value=invitation.get("event_end_time", ""), key="invite_end_input")

    invite_location = st.text_input("Location", value=invitation.get("location", ""), key="invite_location_input")
    invite_host = st.text_input("Host", value=invitation.get("host", ""), key="invite_host_input")
    invite_rsvp_required = st.checkbox("RSVP required", value=bool(invitation.get("rsvp_required", False)), key="invite_rsvp_required_input")
    invite_rsvp_deadline = st.text_input("RSVP deadline", value=invitation.get("rsvp_deadline", ""), key="invite_rsvp_deadline_input")
    invite_description = st.text_area("Description", value=invitation.get("description", ""), key="invite_description_input", height=120)

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Approve invitation"):
            edited_invitation = {
                "event_title": invite_title.strip(),
                "event_date": parse_date_to_iso(invite_date),
                "event_time": parse_single_time(invite_start) or invite_start.strip(),
                "event_end_time": parse_single_time(invite_end) or invite_end.strip(),
                "location": invite_location.strip(),
                "host": invite_host.strip(),
                "rsvp_required": bool(invite_rsvp_required),
                "rsvp_deadline": parse_date_to_iso(invite_rsvp_deadline) if invite_rsvp_deadline.strip() else "",
                "description": invite_description.strip(),
            }

            event_dict = _build_invitation_event_dict(edited_invitation)
            if not event_dict.get("date") or not event_dict.get("start_time"):
                add_message("assistant", "The invitation still needs a clear date and start time before it can be added to the calendar.")
            else:
                if memory.event_exists(event_dict["title"], event_dict["date"]):
                    add_message("assistant", f"The event '{event_dict['title']}' on {event_dict['date']} already exists in memory.")
                else:
                    conflict_info = calendar_tool.check_conflict(event_dict["date"], event_dict["start_time"], event_dict["end_time"])
                    if conflict_info.get("conflict"):
                        add_message(
                            "assistant",
                            f"Cannot schedule '{event_dict['title']}' because it conflicts with '{conflict_info.get('clashing_event')}'. Suggested alternative start time: {conflict_info.get('suggested_time')}",
                        )
                    else:
                        calendar_tool.create_event(
                            title=event_dict["title"],
                            date_str=event_dict["date"],
                            start_time_str=event_dict["start_time"],
                            end_time_str=event_dict["end_time"],
                            location=event_dict.get("location", ""),
                            description=event_dict.get("description", ""),
                            reminders_minutes=[1440, 120],
                        )
                        memory.save_event(event_dict["title"], event_dict["date"])
                        add_message("assistant", f"Invitation event '{event_dict['title']}' was added to your calendar.")
                        st.session_state["pending_invitation"] = None
                        clear_review_editor_state()
            st.rerun()

    with col2:
        if st.button("Re-evaluate invitation with AI"):
            current_invitation = _build_current_invitation_for_recheck(
                invite_title,
                invite_date,
                invite_start,
                invite_end,
                invite_location,
                invite_host,
                invite_rsvp_required,
                invite_rsvp_deadline,
                invite_description,
                invitation,
            )
            verification = agent.verify_invitation_extraction(
                st.session_state.get("last_uploaded_text", ""),
                current_invitation,
                extracted_tables=st.session_state.get("last_uploaded_tables", []),
                parsed_document=st.session_state.get("last_parsed_document"),
                return_meta=True,
            )
            st.session_state["pending_invitation"] = verification.get("result", current_invitation)
            clear_review_editor_state()

            status = verification.get("status", "error")
            detail = verification.get("message", "")
            provider_text = describe_extraction_provider(verification)
            if status == "success":
                add_message("assistant", f"I re-evaluated the invitation with {provider_text} and applied the updated result. Please review the refreshed fields.")
            elif status == "no_change":
                add_message("assistant", f"{provider_text.capitalize()} reviewed the invitation but did not find supported visible changes beyond the current values.")
            else:
                fallback_message = f"{provider_text.capitalize()} did not return usable structured updates, so I kept the current invitation values."
                if detail:
                    fallback_message += f" Details: {detail}"
                add_message("assistant", fallback_message)
            st.rerun()

    with col3:
        if st.button("Dismiss invitation"):
            st.session_state["pending_invitation"] = None
            clear_review_editor_state()
            add_message("assistant", "The invitation review was dismissed.")
            st.rerun()


if st.session_state["pending_events"]:
    st.subheader("Calendar review")

    for i, event in enumerate(list(st.session_state["pending_events"])):
        title = event.get("title", "")
        raw_time = event.get("raw_time", "")
        raw_date = event.get("raw_date", "")

        with st.expander(title or f"Event {i + 1}"):
            if raw_date and raw_date != event.get("date", ""):
                st.markdown(f"**Original date text:** {raw_date}")
            if raw_time and ((not event.get("start_time")) or (not event.get("end_time"))):
                st.markdown(f"**Raw time text:** {raw_time}")

            edited_event = _edited_pending_event(event, f"pending_event_{i}")

            col1, col2 = st.columns(2)

            with col1:
                if st.button(f"Add '{edited_event.get('title') or 'this event'}' to Calendar", key=f"add_event_{i}"):
                    add_event_to_calendar(edited_event)
                    st.rerun()

            with col2:
                if st.button(f"Remove '{edited_event.get('title') or 'this event'}'", key=f"remove_event_{i}"):
                    if event in st.session_state["pending_events"]:
                        st.session_state["pending_events"].remove(event)
                    add_message("assistant", f"Removed '{edited_event.get('title') or 'this event'}' from pending calendar actions.")
                    st.rerun()


user_input = st.chat_input("Type a message...")
if user_input:
    add_message("user", user_input)
    response = handle_user_message(user_input)
    if isinstance(response, str) and response.strip():
        add_message("assistant", response)
    st.rerun()
