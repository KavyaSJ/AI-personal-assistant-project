import json
import os
import subprocess
import sys
from typing import Dict, List

SETTINGS_FILE = "assistant_settings.json"

DEFAULT_CLOUD_MODELS = [
    "gemini-3.1-flash-lite-preview",
    "gemini-3.0-flash",
    "gemini-2.5-flash",
]


def _default_settings() -> dict:
    return {
        "debug_tools_enabled": False,
        "local_models": [],
        "selected_local_model": "",
        "cloud_models": list(DEFAULT_CLOUD_MODELS),
        "api_entries": [],
        "active_api_label": "Environment GEMINI_API_KEY",
    }


def load_settings() -> dict:
    settings = _default_settings()
    if not os.path.exists(SETTINGS_FILE):
        return settings

    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as file:
            saved = json.load(file)
        if isinstance(saved, dict):
            for key, value in saved.items():
                settings[key] = value
    except Exception:
        return settings

    settings["cloud_models"] = get_cloud_models(settings)
    settings["local_models"] = get_saved_local_models(settings)
    return settings


def save_settings(settings: dict) -> None:
    clean = _default_settings()
    if isinstance(settings, dict):
        clean.update(settings)
    with open(SETTINGS_FILE, "w", encoding="utf-8") as file:
        json.dump(clean, file, indent=2)


def get_saved_local_models(settings: dict) -> List[str]:
    values = []
    for model_name in (settings or {}).get("local_models", []) or []:
        model_name = str(model_name).strip()
        if model_name and model_name not in values:
            values.append(model_name)
    return values


def get_cloud_models(settings: dict) -> List[str]:
    values = []
    for model_name in DEFAULT_CLOUD_MODELS + list((settings or {}).get("cloud_models", []) or []):
        model_name = str(model_name).strip()
        if model_name and model_name not in values:
            values.append(model_name)
    return values


def list_ollama_models() -> List[str]:
    try:
        completed = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception:
        return []

    output = completed.stdout or ""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return []

    models = []
    for line in lines[1:]:
        parts = line.split()
        if not parts:
            continue
        model_name = parts[0].strip()
        if model_name and model_name not in models:
            models.append(model_name)
    return models


def get_all_local_models(settings: dict) -> List[str]:
    values = []
    for model_name in get_saved_local_models(settings) + list_ollama_models():
        model_name = str(model_name).strip()
        if model_name and model_name not in values:
            values.append(model_name)
    return values


def upsert_local_model(settings: dict, model_name: str) -> dict:
    settings = dict(settings or _default_settings())
    model_name = str(model_name or "").strip()
    if not model_name:
        return settings
    models = get_saved_local_models(settings)
    if model_name not in models:
        models.append(model_name)
    settings["local_models"] = models
    if not settings.get("selected_local_model"):
        settings["selected_local_model"] = model_name
    return settings


def add_cloud_model(settings: dict, model_name: str) -> dict:
    settings = dict(settings or _default_settings())
    model_name = str(model_name or "").strip()
    if not model_name:
        return settings
    models = get_cloud_models(settings)
    if model_name not in models:
        models.append(model_name)
    settings["cloud_models"] = models
    return settings


def add_api_entry(settings: dict, label: str, key: str) -> dict:
    settings = dict(settings or _default_settings())
    label = str(label or "").strip()
    key = str(key or "").strip()
    if not label or not key:
        return settings

    entries = []
    seen = False
    for entry in settings.get("api_entries", []) or []:
        if not isinstance(entry, dict):
            continue
        current_label = str(entry.get("label", "")).strip()
        current_key = str(entry.get("key", "")).strip()
        if not current_label or not current_key:
            continue
        if current_label == label:
            entries.append({"label": label, "key": key})
            seen = True
        else:
            entries.append({"label": current_label, "key": current_key})

    if not seen:
        entries.append({"label": label, "key": key})

    settings["api_entries"] = entries
    settings["active_api_label"] = label
    return settings


def get_api_labels(settings: dict) -> List[str]:
    labels = ["Environment GEMINI_API_KEY"]
    for entry in (settings or {}).get("api_entries", []) or []:
        if isinstance(entry, dict):
            label = str(entry.get("label", "")).strip()
            if label and label not in labels:
                labels.append(label)
    return labels


def get_active_api_key(settings: dict) -> str:
    active_label = str((settings or {}).get("active_api_label", "Environment GEMINI_API_KEY")).strip() or "Environment GEMINI_API_KEY"
    if active_label == "Environment GEMINI_API_KEY":
        return os.getenv("GEMINI_API_KEY", "")

    for entry in (settings or {}).get("api_entries", []) or []:
        if isinstance(entry, dict) and str(entry.get("label", "")).strip() == active_label:
            return str(entry.get("key", "")).strip()
    return os.getenv("GEMINI_API_KEY", "")


def get_masked_api_value(settings: dict) -> str:
    key = get_active_api_key(settings)
    if not key:
        return ""
    if len(key) <= 8:
        return "*" * len(key)
    return key[:4] + "*" * max(4, len(key) - 8) + key[-4:]


def launch_ollama_pull(model_name: str) -> Dict[str, str]:
    model_name = str(model_name or "").strip()
    if not model_name:
        return {"success": False, "message": "Please enter a local model name."}

    try:
        if os.name == "nt":
            command_text = f'ollama pull {model_name}'
            subprocess.Popen(
                ["powershell", "-NoExit", "-Command", command_text],
                creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
            )
            return {
                "success": True,
                "message": f"Started downloading '{model_name}' in a new PowerShell window.",
            }

        subprocess.Popen(["ollama", "pull", model_name])
        return {
            "success": True,
            "message": f"Started downloading '{model_name}' with Ollama.",
        }
    except Exception as error:
        return {"success": False, "message": f"Could not start Ollama download. Error: {error}"}
