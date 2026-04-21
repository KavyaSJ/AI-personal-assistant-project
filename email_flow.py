# email_flow.py

import rule_filter
import email_tool
import agent
import memory


# Cleans a sender value so it looks nicer in summaries.
def _clean_sender_name(sender):
    if not sender:
        return "Unknown"

    sender = sender.strip()

    if "<" in sender and ">" in sender:
        name_part = sender.split("<")[0].strip().strip('"')
        email_part = sender.split("<")[1].replace(">", "").strip()

        if name_part:
            return name_part

        return email_part

    return sender


# Builds one text block from thread messages for the AI.
def _build_thread_text(messages):
    parts = []

    for message in messages:
        from_text = message.get("from_header") or message.get("from_address", "")
        subject = message.get("subject", "")
        body = message.get("body", "")

        block = (
            f"From: {from_text}\n"
            f"Subject: {subject}\n"
            f"Body:\n{body}"
        )

        parts.append(block)

    return "\n\n====================\n\n".join(parts)


# Makes sure a reply subject starts with Re:
def _make_reply_subject(subject):
    subject = (subject or "").strip()

    if not subject:
        return "Re:"

    if subject.lower().startswith("re:"):
        return subject

    return "Re: " + subject


# Builds the References header value for a reply.
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


# Gets reply details from the last message in the thread.
def _get_reply_details(thread_id):
    messages = email_tool.get_thread_messages(thread_id, last_n=5)

    if not messages:
        return None

    last_message = messages[-1]

    to_value = last_message.get("reply_to_header") or last_message.get("from_header") or ""
    subject = _make_reply_subject(last_message.get("subject", ""))
    message_id = last_message.get("message_id", "")
    references = _build_references(last_message.get("references", ""), message_id)

    return {
        "to": to_value,
        "subject": subject,
        "in_reply_to": message_id,
        "references": references,
    }


# Checks recent emails, filters obvious junk, runs AI classification, and returns summaries plus raw results.
def run_email_check():
    summaries = []
    classifications = []
    candidate_items = []

    threads = email_tool.get_recent_threads(max_results=20)

    for thread in threads:
        thread_id = thread.get("id", "")
        sender = thread.get("sender", "")
        subject = thread.get("subject", "")
        headers = thread.get("headers", {})

        if not thread_id:
            continue

        if memory.is_processed(thread_id):
            continue

        filter_result = rule_filter.classify_email(sender, subject, headers)
        if filter_result != "candidate":
            memory.mark_processed(thread_id)
            continue

        messages = email_tool.get_thread_messages(thread_id, last_n=5)
        if not messages:
            memory.mark_processed(thread_id)
            continue

        thread_text = _build_thread_text(messages)
        candidate_items.append({
            "thread_id": thread_id,
            "thread_text": thread_text,
            "sender": sender,
            "subject": subject,
        })

    if candidate_items:
        ai_results = agent.classify_email_batch(candidate_items, chunk_size=4)
    else:
        ai_results = []

    if len(ai_results) != len(candidate_items):
        ai_results = []
        for item in candidate_items:
            single_result = agent.classify_email(item.get("thread_text", ""))
            single_result["thread_id"] = item.get("thread_id", "")
            ai_results.append(single_result)

    for item, ai_result in zip(candidate_items, ai_results):
        thread_id = item.get("thread_id", "")
        sender = item.get("sender", "")
        subject = item.get("subject", "")
        thread_text = item.get("thread_text", "")

        memory.mark_processed(thread_id)

        category = ai_result.get("category", "business").title()
        summary = ai_result.get("summary", "No summary available.")
        response_required = bool(ai_result.get("response_required", False))
        status_text = "reply needed" if response_required else "no reply needed"
        sender_name = _clean_sender_name(sender)

        summary_text = f"[{category}] From {sender_name}: {summary} ({status_text})"
        summaries.append(summary_text)

        result_item = dict(ai_result)
        result_item["thread_id"] = thread_id
        result_item["thread_text"] = thread_text
        result_item["sender"] = sender
        result_item["subject"] = subject
        classifications.append(result_item)

    return summaries, classifications


# Handles the user's yes or no decision about replying to an email.
def handle_reply_request(thread_id, thread_text, user_choice, user_notes=""):
    choice = (user_choice or "").strip().lower()

    if choice == "no":
        memory.save_decision(thread_id, "no")
        return "OK, I won't ask about this email again."

    if choice == "yes":
        draft_text = agent.draft_response(thread_text, user_notes)

        reply_details = _get_reply_details(thread_id)
        if not reply_details:
            return "I could not create a draft because I could not find the reply details."

        draft_id = email_tool.create_draft(
            to=reply_details["to"],
            subject=reply_details["subject"],
            body=draft_text,
            thread_id=thread_id,
            in_reply_to=reply_details["in_reply_to"],
            references=reply_details["references"],
        )

        memory.save_decision(thread_id, "yes")
        memory.save_draft_id(thread_id, draft_id)
        return draft_text

    return "Please choose 'yes' or 'no'."


# Handles the user's decision about attending an event in an email.
def handle_event_decision(thread_id, thread_text, event_details, attending, response_required):
    if attending is False:
        memory.save_decision(thread_id, "no")
        return {
            "message": "OK, I won't create an event or RSVP for this email."
        }

    memory.save_decision(thread_id, "yes")

    if attending is True and response_required is True:
        draft_text = agent.draft_rsvp(thread_text, attending=True)
        return {
            "message": "Here is your RSVP draft for approval.",
            "draft_text": draft_text,
            "event_details": event_details,
        }

    if attending is True and response_required is False:
        return {
            "message": "OK, this event can be added to your calendar.",
            "event_details": event_details,
        }

    return {
        "message": "No event action was taken.",
        "event_details": event_details,
    }
