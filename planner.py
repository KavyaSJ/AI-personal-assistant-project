# planner.py


def _has_event_details(item):
    event_details = (item or {}).get("event_details")
    return isinstance(event_details, dict) and any([
        str(event_details.get("title", "")).strip(),
        str(event_details.get("date", "")).strip(),
        str(event_details.get("time", "")).strip(),
        str(event_details.get("location", "")).strip(),
    ])


# Builds a lightweight planner state for the email workflow.
def build_email_planner_state(email_results=None, pending_approval=None, pending_outbound_email=None, pending_events=None):
    return {
        "email_results": list(email_results or []),
        "pending_approval": pending_approval,
        "pending_outbound_email": pending_outbound_email,
        "pending_events": list(pending_events or []),
    }


# Chooses low-risk preparation steps only.
# Important: this planner should never create a Gmail draft or send anything by itself.
def build_email_action_plan(state):
    state = dict(state or {})
    email_results = list(state.get("email_results") or [])

    if state.get("pending_approval") or state.get("pending_outbound_email"):
        return []

    if not email_results:
        return []

    actions = []

    for item in email_results:
        thread_id = item.get("thread_id", "")
        response_required = bool(item.get("response_required", False))
        response_type = str(item.get("response_type", "none") or "none").strip().lower()
        has_event = _has_event_details(item)
        already_prepared = bool((item or {}).get("precomputed_draft_text"))

        if not thread_id or already_prepared:
            continue

        if response_type == "rsvp" and has_event:
            actions.append({
                "action": "prepare_rsvp_draft",
                "thread_id": thread_id,
            })
            continue

        if response_required:
            actions.append({
                "action": "prepare_reply_draft",
                "thread_id": thread_id,
            })

    return actions
