# email_tool.py

import base64
from email.mime.text import MIMEText
from email.utils import parseaddr

from googleapiclient.errors import HttpError

from auth import get_gmail_service

# email_tool.py
# talks to Gmail. It can fetch email threads, create draft replies,
# update drafts, send them, and check if a draft still exists.


# Gets one header value from a Gmail message payload.
def _get_header_value(payload, header_name):
    headers = payload.get("headers", [])

    for header in headers:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value", "")

    return ""


# Decodes Gmail base64url text safely.
def _decode_base64_text(data):
    if not data:
        return ""

    missing_padding = len(data) % 4
    if missing_padding:
        data += "=" * (4 - missing_padding)

    try:
        decoded_bytes = base64.urlsafe_b64decode(data.encode("utf-8"))
        return decoded_bytes.decode("utf-8", errors="replace")
    except Exception:
        return ""


# Extracts readable text from a Gmail payload.
def _get_message_body(payload):
    if not payload:
        return ""

    body_data = payload.get("body", {}).get("data")
    if body_data:
        return _decode_base64_text(body_data)

    parts = payload.get("parts", [])

    for part in parts:
        if part.get("mimeType") == "text/plain":
            data = part.get("body", {}).get("data")
            if data:
                return _decode_base64_text(data)

    for part in parts:
        nested_parts = part.get("parts", [])
        if nested_parts:
            text = _get_message_body(part)
            if text:
                return text

    for part in parts:
        if part.get("mimeType") == "text/html":
            data = part.get("body", {}).get("data")
            if data:
                return _decode_base64_text(data)

    return ""


# Extracts only the email address part from a header like "Alex <alex@email.com>".
def _extract_email_address(header_value):
    name, email_address = parseaddr(header_value or "")
    if email_address:
        return email_address
    return header_value or ""


# Makes sure the reply subject starts with Re:
def _make_reply_subject(subject):
    subject = (subject or "").strip()

    if not subject:
        return "Re:"

    if subject.lower().startswith("re:"):
        return subject

    return "Re: " + subject


# Builds the References header for a reply.
def _build_references(existing_references, message_id):
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


# Builds a raw Gmail message for draft create/update calls.
def _build_raw_message(to, subject, body, in_reply_to=None, references=None):
    message = MIMEText(body or "", "plain", "utf-8")
    message["To"] = to or ""
    message["Subject"] = subject or ""

    if in_reply_to:
        message["In-Reply-To"] = in_reply_to

    if references:
        message["References"] = references

    raw_message = base64.urlsafe_b64encode(
        message.as_bytes()
    ).decode("utf-8")

    return raw_message


# Fetches recent Gmail threads, excluding Trash, Spam, and non-primary categories.
def get_recent_threads(max_results=20):
    service = get_gmail_service()

    results = service.users().threads().list(
        userId="me",
        maxResults=max_results,
        q="in:inbox -category:promotions -category:social -category:updates -category:forums -in:spam -in:trash"
    ).execute()

    thread_list = results.get("threads", [])
    output = []

    for thread in thread_list:
        thread_id = thread.get("id", "")

        if not thread_id:
            continue

        thread_data = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="metadata",
            metadataHeaders=["From", "To", "Subject", "Reply-To", "Message-ID", "References", "In-Reply-To"]
        ).execute()

        messages = thread_data.get("messages", [])
        if not messages:
            continue

        latest_message = messages[-1]
        payload = latest_message.get("payload", {})

        sender = _get_header_value(payload, "From")
        subject = _get_header_value(payload, "Subject")
        snippet = latest_message.get("snippet", "")

        headers = {}
        for header in payload.get("headers", []):
            name = header.get("name", "")
            value = header.get("value", "")
            headers[name] = value

        output.append({
            "id": thread_id,
            "sender": sender,
            "subject": subject,
            "snippet": snippet,
            "headers": headers
        })

    return output


# Fetches the last N messages in a Gmail thread and decodes the body text.
# It also returns reply-thread headers needed to create a correct Gmail reply draft.
def get_thread_messages(thread_id, last_n=5):
    service = get_gmail_service()

    thread_data = service.users().threads().get(
        userId="me",
        id=thread_id,
        format="full"
    ).execute()

    messages = thread_data.get("messages", [])
    messages = messages[-last_n:]

    output = []

    for message in messages:
        payload = message.get("payload", {})

        from_header = _get_header_value(payload, "From")
        reply_to_header = _get_header_value(payload, "Reply-To")
        to_header = _get_header_value(payload, "To")
        subject = _get_header_value(payload, "Subject")
        rfc_message_id = _get_header_value(payload, "Message-ID")
        references = _get_header_value(payload, "References")
        in_reply_to = _get_header_value(payload, "In-Reply-To")
        body = _get_message_body(payload)

        reply_target_header = reply_to_header or from_header

        output.append({
            "from_address": _extract_email_address(from_header),
            "from_header": from_header,
            "reply_to_address": _extract_email_address(reply_target_header),
            "reply_to_header": reply_target_header,
            "to_address": _extract_email_address(to_header),
            "to_header": to_header,
            "subject": subject,
            "body": body,
            "message_id": rfc_message_id,
            "gmail_message_id": message.get("id", ""),
            "thread_id": message.get("threadId", ""),
            "references": references,
            "in_reply_to": in_reply_to
        })

    return output


# Creates a Gmail draft.
# If thread_id, in_reply_to, and references are provided, the draft is built as a proper threaded reply.
def create_draft(to, subject, body, thread_id=None, in_reply_to=None, references=None):
    service = get_gmail_service()

    raw_message = _build_raw_message(
        to=to,
        subject=subject,
        body=body,
        in_reply_to=in_reply_to,
        references=references
    )

    draft_body = {
        "message": {
            "raw": raw_message
        }
    }

    if thread_id:
        draft_body["message"]["threadId"] = thread_id

    draft = service.users().drafts().create(
        userId="me",
        body=draft_body
    ).execute()

    return draft.get("id")


# Updates an existing Gmail draft with new content.
# This replaces the saved draft body while keeping threading headers if provided.
def update_draft(draft_id, to, subject, body, thread_id=None, in_reply_to=None, references=None):
    service = get_gmail_service()

    raw_message = _build_raw_message(
        to=to,
        subject=subject,
        body=body,
        in_reply_to=in_reply_to,
        references=references
    )

    draft_body = {
        "id": draft_id,
        "message": {
            "raw": raw_message
        }
    }

    if thread_id:
        draft_body["message"]["threadId"] = thread_id

    updated_draft = service.users().drafts().update(
        userId="me",
        id=draft_id,
        body=draft_body
    ).execute()

    return updated_draft.get("id")


# Sends an existing Gmail draft using its draft ID.
def send_draft(draft_id):
    service = get_gmail_service()

    service.users().drafts().send(
        userId="me",
        body={"id": draft_id}
    ).execute()


# Returns True if a draft still exists and False if it was deleted or already sent.
def draft_exists(draft_id):
    service = get_gmail_service()

    try:
        service.users().drafts().get(
            userId="me",
            id=draft_id
        ).execute()
        return True
    except HttpError as error:
        if error.resp.status == 404:
            return False
        raise