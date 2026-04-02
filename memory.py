# memory.py

import json
import os

MEMORY_FILE = "agent_memory.json"


# Returns a fresh empty memory structure.
def _default_memory():
    return {
        "processed_emails": [],
        "created_events": [],
        "draft_ids": {},
        "user_decisions": {},
        "meta": {},
    }


# Reads agent_memory.json and returns its contents as a dictionary.
# If the file does not exist, returns the default empty structure.
def load_memory():
    if not os.path.exists(MEMORY_FILE):
        return _default_memory()

    with open(MEMORY_FILE, "r", encoding="utf-8") as file:
        mem = json.load(file)

    default_mem = _default_memory()

    for key in default_mem:
        if key not in mem:
            mem[key] = default_mem[key]

    return mem


# Writes the memory dictionary to agent_memory.json using indented formatting.
def save_memory(mem):
    with open(MEMORY_FILE, "w", encoding="utf-8") as file:
        json.dump(mem, file, indent=2)


# Adds a thread ID to processed_emails and saves the file.
def mark_processed(thread_id):
    mem = load_memory()

    if thread_id not in mem["processed_emails"]:
        mem["processed_emails"].append(thread_id)
        save_memory(mem)


# Returns True if a thread ID is already in processed_emails.
def is_processed(thread_id):
    mem = load_memory()
    return thread_id in mem["processed_emails"]


# Saves a draft ID for a given thread ID and writes the file.
def save_draft_id(thread_id, draft_id):
    mem = load_memory()
    mem["draft_ids"][thread_id] = draft_id
    save_memory(mem)


# Returns the saved draft ID for a thread, or None if not found.
def get_draft_id(thread_id):
    mem = load_memory()
    return mem["draft_ids"].get(thread_id)


# Saves the user's yes/no decision for a thread and writes the file.
def save_decision(thread_id, decision):
    mem = load_memory()
    mem["user_decisions"][thread_id] = decision
    save_memory(mem)


# Returns the saved decision for a thread: 'yes', 'no', or None.
def get_decision(thread_id):
    mem = load_memory()
    return mem["user_decisions"].get(thread_id)


# Returns True if an event with the same title and date already exists.
def event_exists(title, date):
    mem = load_memory()

    for event in mem["created_events"]:
        if event.get("title") == title and event.get("date") == date:
            return True

    return False


# Saves a new event with title and date if it does not already exist.
def save_event(title, date):
    mem = load_memory()

    exists = False
    for event in mem["created_events"]:
        if event.get("title") == title and event.get("date") == date:
            exists = True
            break

    if not exists:
        mem["created_events"].append({
            "title": title,
            "date": date,
        })
        save_memory(mem)


# Removes one saved event from memory if it exists.
def remove_event(title, date):
    mem = load_memory()
    original_count = len(mem["created_events"])
    mem["created_events"] = [
        event for event in mem["created_events"]
        if not (event.get("title") == title and event.get("date") == date)
    ]

    if len(mem["created_events"]) != original_count:
        save_memory(mem)
        return True

    return False


# Reads a persisted meta value.
def get_meta_value(key, default=None):
    mem = load_memory()
    return mem.get("meta", {}).get(key, default)


# Saves a persisted meta value.
def set_meta_value(key, value):
    mem = load_memory()
    if "meta" not in mem or not isinstance(mem["meta"], dict):
        mem["meta"] = {}
    mem["meta"][key] = value
    save_memory(mem)
