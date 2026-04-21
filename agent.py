
import json
import os

from dotenv import load_dotenv

import pdf_tool


load_dotenv()

DEFAULT_GEMINI_MODEL_NAME = (os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash") or "gemini-2.5-flash").strip()
GEMINI_FLASH_31_LITE_MODEL = (os.getenv("GEMINI_FLASH_31_LITE_MODEL", "gemini-3.1-flash-lite-preview") or "gemini-3.1-flash-lite-preview").strip()
GEMINI_FLASH_3_MODEL = (os.getenv("GEMINI_FLASH_3_MODEL", "gemini-3.0-flash") or "gemini-3.0-flash").strip()
GEMINI_FLASH_25_MODEL = (os.getenv("GEMINI_FLASH_25_MODEL", DEFAULT_GEMINI_MODEL_NAME) or DEFAULT_GEMINI_MODEL_NAME).strip()
DOCUMENT_LLM_BACKEND = (os.getenv("DOCUMENT_LLM_BACKEND", "auto") or "auto").strip().lower()
LOCAL_LLM_BASE_URL = (os.getenv("LOCAL_LLM_BASE_URL", "http://localhost:11434/v1") or "").strip()
LOCAL_LLM_MODEL = (os.getenv("LOCAL_LLM_MODEL", "qwen2.5:7b-instruct") or "").strip()
LOCAL_LLM_API_KEY = (os.getenv("LOCAL_LLM_API_KEY", "ollama") or "ollama").strip()
LOCAL_LLM_TIMEOUT_SECONDS = int((os.getenv("LOCAL_LLM_TIMEOUT_SECONDS", "45") or "45").strip())
LOCAL_LLM_MAX_RETRIES = int((os.getenv("LOCAL_LLM_MAX_RETRIES", "1") or "1").strip())
LOCAL_LLM_MAX_PROMPT_CHARS = int((os.getenv("LOCAL_LLM_MAX_PROMPT_CHARS", "16000") or "16000").strip())
LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS = int((os.getenv("LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS", "14000") or "14000").strip())
AGENT_DEBUG = (os.getenv("AGENT_DEBUG", "0") or "0").strip().lower() in ["1", "true", "yes"]
API_KEY = os.getenv("GEMINI_API_KEY")

MODEL_CHOICE_AUTO = "Auto"
MODEL_CHOICE_LOCAL_PREFIX = "Local: "
MODEL_CHOICE_CLOUD_PREFIX = "Cloud: "

_RUNTIME_MODEL_CHOICE = None
_RUNTIME_API_KEY = None
_RUNTIME_LOCAL_MODEL = None
_RUNTIME_CLOUD_MODELS = None
_RUNTIME_DEBUG = None
_CURRENT_GEMINI_API_KEY = None
_LAST_ROUTE_INFO = {
    "task": "",
    "requested_provider": "",
    "used_provider": "",
    "requested_model": "",
    "used_model": "",
    "fallback_used": False,
    "message": "",
}


try:
    from google import genai
    from google.genai import types as genai_types

    HAS_GEMINI_SDK = True
except Exception:
    genai = None
    genai_types = None
    HAS_GEMINI_SDK = False


try:
    from openai import OpenAI

    HAS_OPENAI = True
except Exception:
    OpenAI = None
    HAS_OPENAI = False


try:
    import instructor

    HAS_INSTRUCTOR = True
except Exception:
    instructor = None
    HAS_INSTRUCTOR = False


try:
    from pydantic import BaseModel, Field

    HAS_PYDANTIC = True
except Exception:
    BaseModel = object
    HAS_PYDANTIC = False

    def Field(default=None, default_factory=None):  # type: ignore
        if default_factory is not None:
            return default_factory()
        return default


if HAS_PYDANTIC:
    class SessionRowModel(BaseModel):
        session_number: int = 0
        date: str = ""
        date_inferred: bool = False
        start_time: str = ""
        end_time: str = ""
        title: str = ""
        description: str = ""
        reminders_minutes: list[int] = Field(default_factory=lambda: [60])


    class AssignmentRowModel(BaseModel):
        title: str = ""
        due_date: str = ""
        due_time: str = ""
        due_time_inferred: bool = False
        description: str = ""
        reminders_minutes: list[int] = Field(default_factory=lambda: [10080, 1440])


    class CourseMetadataModel(BaseModel):
        course_name: str = ""
        days_of_week: list[str] = Field(default_factory=list)
        start_time: str = ""
        end_time: str = ""
        term_start_date: str = ""
        term_end_date: str = ""
        location: str = ""


    class CourseItemsModel(BaseModel):
        sessions: list[SessionRowModel] = Field(default_factory=list)
        assignments: list[AssignmentRowModel] = Field(default_factory=list)


    class CourseOutlineModel(BaseModel):
        course_name: str = ""
        days_of_week: list[str] = Field(default_factory=list)
        start_time: str = ""
        end_time: str = ""
        term_start_date: str = ""
        term_end_date: str = ""
        location: str = ""
        sessions: list[SessionRowModel] = Field(default_factory=list)
        assignments: list[AssignmentRowModel] = Field(default_factory=list)


    class InvitationDetailsModel(BaseModel):
        event_title: str = ""
        event_date: str = ""
        event_time: str = ""
        event_end_time: str = ""
        location: str = ""
        host: str = ""
        rsvp_required: bool = False
        rsvp_deadline: str = ""
        description: str = ""


_gemini_client = None


# Returns a lazy Gemini client only when it is actually needed.
def _get_gemini_client():
    global _gemini_client
    global _CURRENT_GEMINI_API_KEY

    active_api_key = (_RUNTIME_API_KEY or API_KEY or "").strip()

    if _gemini_client is not None and _CURRENT_GEMINI_API_KEY == active_api_key:
        return _gemini_client

    if not active_api_key:
        raise ValueError("GEMINI_API_KEY was not found in your environment or runtime settings.")

    if not HAS_GEMINI_SDK:
        raise ValueError("google-genai is not installed.")

    _gemini_client = genai.Client(api_key=active_api_key)
    _CURRENT_GEMINI_API_KEY = active_api_key
    return _gemini_client

    if not API_KEY:
        raise ValueError("GEMINI_API_KEY was not found in your environment.")

    if not HAS_GEMINI_SDK:
        raise ValueError("google-genai is not installed.")

    _gemini_client = genai.Client(api_key=API_KEY)
    return _gemini_client


# Turns conversation history into a simple text block for the prompt.
def _format_history(conversation_history):
    if not conversation_history:
        return "No previous conversation history."

    lines = []

    for i, item in enumerate(conversation_history, start=1):
        if isinstance(item, dict):
            role = item.get("role", "message")
            content = item.get("content", item.get("text", str(item)))
            lines.append(f"{i}. {role}: {content}")
        else:
            lines.append(f"{i}. {str(item)}")

    return "\n".join(lines)


# Calls Gemini with a system prompt and user prompt, then returns plain text.
def _generate_text(system_prompt, user_prompt, temperature=0.0, json_mode=False, model_name=None):
    client = _get_gemini_client()
    model_name = model_name or _model_name_for_provider("gemini")

    config = genai_types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=temperature,
    )

    if json_mode:
        config.response_mime_type = "application/json"

    response = client.models.generate_content(
        model=model_name,
        contents=user_prompt,
        config=config,
    )

    text = getattr(response, "text", "")

    if text is None:
        return ""

    return text.strip()


# Tries to parse JSON directly, and if that fails, extracts JSON from the response text.
# Works for both JSON objects and JSON arrays.
def _parse_json_response(text):
    try:
        return json.loads(text)
    except Exception:
        pass

    start_obj = text.find("{")
    end_obj = text.rfind("}")
    if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
        try:
            json_text = text[start_obj:end_obj + 1]
            return json.loads(json_text)
        except Exception:
            pass

    start_arr = text.find("[")
    end_arr = text.rfind("]")
    if start_arr != -1 and end_arr != -1 and end_arr > start_arr:
        try:
            json_text = text[start_arr:end_arr + 1]
            return json.loads(json_text)
        except Exception:
            pass

    raise ValueError("Could not parse JSON from model response.")


# Makes sure fallback dicts always have the requested keys.
def _merge_with_default_dict(result, default_dict):
    if not isinstance(result, dict):
        return dict(default_dict)

    output = dict(default_dict)
    for key in default_dict:
        if key in result:
            output[key] = result.get(key)
    return output


def _merge_nested_with_default(result, default_value):
    if isinstance(default_value, dict):
        output = {}
        safe_result = result if isinstance(result, dict) else {}
        for key, value in default_value.items():
            output[key] = _merge_nested_with_default(safe_result.get(key), value)
        for key, value in safe_result.items():
            if key not in output:
                output[key] = value
        return output

    if isinstance(default_value, list):
        if not isinstance(result, list):
            return list(default_value)
        return result

    if result is None:
        return default_value
    return result


def _trim_prompt_text(text, max_chars=32000):
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def _json_snapshot(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _verification_meta(result, status, message="", raw_response="", provider="deterministic", backend="deterministic"):
    return {
        "result": result,
        "status": status,
        "message": message,
        "raw_response": raw_response,
        "provider": provider,
        "backend": backend,
    }


def _course_outline_visible_payload(data):
    data = data if isinstance(data, dict) else {}
    return {
        "course_name": data.get("course_name", ""),
        "days_of_week": data.get("days_of_week", []),
        "start_time": data.get("start_time", ""),
        "end_time": data.get("end_time", ""),
        "term_start_date": data.get("term_start_date", ""),
        "term_end_date": data.get("term_end_date", ""),
        "location": data.get("location", ""),
        "sessions": data.get("sessions", []),
        "assignments": data.get("assignments", []),
    }


def _invitation_visible_payload(data):
    data = data if isinstance(data, dict) else {}
    return {
        "event_title": data.get("event_title", ""),
        "event_date": data.get("event_date", ""),
        "event_time": data.get("event_time", ""),
        "event_end_time": data.get("event_end_time", ""),
        "location": data.get("location", ""),
        "host": data.get("host", ""),
        "rsvp_required": data.get("rsvp_required", False),
        "rsvp_deadline": data.get("rsvp_deadline", ""),
        "description": data.get("description", ""),
    }


def _safe_model_dump(value):
    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    if hasattr(value, "model_dump"):
        return value.model_dump()

    if hasattr(value, "dict"):
        return value.dict()

    return {}


def _debug_enabled():
    if _RUNTIME_DEBUG is not None:
        return bool(_RUNTIME_DEBUG)
    return AGENT_DEBUG


def _debug_log(message):
    if _debug_enabled():
        print(f"[agent] {message}")


def _dedupe_models(values):
    models = []
    for value in values or []:
        value = str(value or "").strip()
        if value and value not in models:
            models.append(value)
    return models


def _default_cloud_models():
    return _dedupe_models([
        DEFAULT_GEMINI_MODEL_NAME,
        GEMINI_FLASH_31_LITE_MODEL,
        GEMINI_FLASH_3_MODEL,
        GEMINI_FLASH_25_MODEL,
    ])


def _active_local_model():
    if isinstance(_RUNTIME_MODEL_CHOICE, dict) and _RUNTIME_MODEL_CHOICE.get("backend") == "local_qwen" and _RUNTIME_MODEL_CHOICE.get("model"):
        return str(_RUNTIME_MODEL_CHOICE.get("model") or "").strip()
    return (_RUNTIME_LOCAL_MODEL or LOCAL_LLM_MODEL or "qwen2.5:7b-instruct").strip()


def _active_cloud_models():
    runtime_models = _dedupe_models(_RUNTIME_CLOUD_MODELS or [])
    if runtime_models:
        return runtime_models
    return _default_cloud_models()


def get_model_selection_options(cloud_models=None, local_models=None):
    cloud_values = _dedupe_models(cloud_models or _active_cloud_models())
    local_values = _dedupe_models(local_models or [_active_local_model()])

    options = [MODEL_CHOICE_AUTO]
    options.extend([f"{MODEL_CHOICE_LOCAL_PREFIX}{model_name}" for model_name in local_values])
    options.extend([f"{MODEL_CHOICE_CLOUD_PREFIX}{model_name}" for model_name in cloud_values])
    return options


def get_default_model_choice():
    if DOCUMENT_LLM_BACKEND in ["local_qwen", "local", "openai", "ollama", "vllm"]:
        return f"{MODEL_CHOICE_LOCAL_PREFIX}{_active_local_model()}"
    if DOCUMENT_LLM_BACKEND == "gemini":
        return f"{MODEL_CHOICE_CLOUD_PREFIX}{DEFAULT_GEMINI_MODEL_NAME}"
    return MODEL_CHOICE_AUTO


def configure_runtime(api_key=None, local_model=None, cloud_models=None, debug=None):
    global _RUNTIME_API_KEY
    global _RUNTIME_LOCAL_MODEL
    global _RUNTIME_CLOUD_MODELS
    global _RUNTIME_DEBUG

    if api_key is not None:
        _RUNTIME_API_KEY = str(api_key or "").strip()
    if local_model is not None:
        _RUNTIME_LOCAL_MODEL = str(local_model or "").strip()
    if cloud_models is not None:
        _RUNTIME_CLOUD_MODELS = _dedupe_models(cloud_models)
    if debug is not None:
        _RUNTIME_DEBUG = bool(debug)


def set_runtime_api_key(api_key):
    configure_runtime(api_key=api_key)


def set_runtime_model_choice(choice):
    global _RUNTIME_MODEL_CHOICE

    choice = str(choice or "").strip()
    if not choice or choice == MODEL_CHOICE_AUTO:
        _RUNTIME_MODEL_CHOICE = {"raw": MODEL_CHOICE_AUTO, "backend": "auto", "model": ""}
        return

    if choice.startswith(MODEL_CHOICE_LOCAL_PREFIX):
        model_name = choice.replace(MODEL_CHOICE_LOCAL_PREFIX, "", 1).strip()
        _RUNTIME_MODEL_CHOICE = {"raw": choice, "backend": "local_qwen", "model": model_name or _active_local_model()}
        return

    if choice.startswith(MODEL_CHOICE_CLOUD_PREFIX):
        model_name = choice.replace(MODEL_CHOICE_CLOUD_PREFIX, "", 1).strip()
        _RUNTIME_MODEL_CHOICE = {"raw": choice, "backend": "gemini", "model": model_name or DEFAULT_GEMINI_MODEL_NAME}
        return

    legacy_mapping = {
        "Local Qwen2.5:7b-instruct": {"raw": choice, "backend": "local_qwen", "model": _active_local_model()},
        "Gemini Flash 3.1 Lite Preview": {"raw": choice, "backend": "gemini", "model": GEMINI_FLASH_31_LITE_MODEL},
        "Gemini Flash 3": {"raw": choice, "backend": "gemini", "model": GEMINI_FLASH_3_MODEL},
        "Gemini Flash 2.5 Flash": {"raw": choice, "backend": "gemini", "model": GEMINI_FLASH_25_MODEL},
    }
    _RUNTIME_MODEL_CHOICE = legacy_mapping.get(choice, {"raw": MODEL_CHOICE_AUTO, "backend": "auto", "model": ""})


def get_runtime_model_choice():
    choice = _RUNTIME_MODEL_CHOICE
    if isinstance(choice, dict) and choice.get("raw"):
        return choice.get("raw")
    return get_default_model_choice()


def _choice_config(choice=None):
    if isinstance(_RUNTIME_MODEL_CHOICE, dict) and choice is None:
        return {
            "backend": _RUNTIME_MODEL_CHOICE.get("backend", "auto"),
            "model": _RUNTIME_MODEL_CHOICE.get("model", ""),
        }

    choice = choice or get_runtime_model_choice()

    if choice == MODEL_CHOICE_AUTO:
        return {"backend": "auto", "model": ""}
    if choice.startswith(MODEL_CHOICE_LOCAL_PREFIX):
        return {"backend": "local_qwen", "model": choice.replace(MODEL_CHOICE_LOCAL_PREFIX, "", 1).strip() or _active_local_model()}
    if choice.startswith(MODEL_CHOICE_CLOUD_PREFIX):
        return {"backend": "gemini", "model": choice.replace(MODEL_CHOICE_CLOUD_PREFIX, "", 1).strip() or DEFAULT_GEMINI_MODEL_NAME}

    legacy_mapping = {
        "Local Qwen2.5:7b-instruct": {"backend": "local_qwen", "model": _active_local_model()},
        "Gemini Flash 3.1 Lite Preview": {"backend": "gemini", "model": GEMINI_FLASH_31_LITE_MODEL},
        "Gemini Flash 3": {"backend": "gemini", "model": GEMINI_FLASH_3_MODEL},
        "Gemini Flash 2.5 Flash": {"backend": "gemini", "model": GEMINI_FLASH_25_MODEL},
    }
    return legacy_mapping.get(choice, {"backend": "auto", "model": ""})


def _gemini_backend_available():
    return bool((_RUNTIME_API_KEY or API_KEY) and HAS_GEMINI_SDK)


def _local_backend_available():
    return bool(LOCAL_LLM_BASE_URL and _active_local_model() and HAS_OPENAI)


def _backend_available(provider_name):
    if provider_name == "local_qwen":
        return _local_backend_available()
    if provider_name == "gemini":
        return _gemini_backend_available()
    if provider_name == "deterministic":
        return True
    return False


def _provider_label(provider_name):
    if provider_name == "local_qwen":
        return f"local structured LLM ({_active_local_model()})"
    if provider_name == "gemini":
        return "Gemini"
    return "deterministic parser"


def _model_name_for_provider(provider_name, choice=None):
    choice_info = _choice_config(choice)

    if provider_name == "local_qwen":
        if choice_info.get("backend") == "local_qwen" and choice_info.get("model"):
            return choice_info.get("model")
        return _active_local_model()

    if provider_name == "gemini":
        if choice_info.get("backend") == "gemini" and choice_info.get("model"):
            return choice_info.get("model")
        cloud_models = _active_cloud_models()
        return cloud_models[0] if cloud_models else DEFAULT_GEMINI_MODEL_NAME

    return ""


def _get_cloud_model_candidates(requested_model=""):
    models = []
    if requested_model:
        models.append(requested_model)
    for model_name in _active_cloud_models():
        if model_name not in models:
            models.append(model_name)
    return models or [DEFAULT_GEMINI_MODEL_NAME]


def _route_provider_sequence(task_name="general", escalation_level=0):
    choice_info = _choice_config()
    manual_backend = choice_info.get("backend", "auto")

    if manual_backend == "local_qwen":
        ordered = ["local_qwen", "gemini"]
    elif manual_backend == "gemini":
        ordered = ["gemini", "local_qwen"]
    else:
        ordered = ["gemini", "local_qwen"]

    available = [provider for provider in ordered if _backend_available(provider)]
    if available:
        return available

    return ["deterministic"]


def _set_last_route_info(task_name, requested_provider, used_provider, requested_model, used_model, fallback_used, message):
    global _LAST_ROUTE_INFO
    _LAST_ROUTE_INFO = {
        "task": task_name,
        "requested_provider": requested_provider,
        "used_provider": used_provider,
        "requested_model": requested_model,
        "used_model": used_model,
        "fallback_used": bool(fallback_used),
        "message": message or "",
    }


def get_last_route_info(reset=False):
    info = dict(_LAST_ROUTE_INFO)
    if reset:
        _set_last_route_info("", "", "", "", "", False, "")
    return info

def _openai_client():
    if not HAS_OPENAI:
        raise ValueError("openai is not installed.")
    return OpenAI(base_url=LOCAL_LLM_BASE_URL, api_key=LOCAL_LLM_API_KEY, timeout=LOCAL_LLM_TIMEOUT_SECONDS)


def _shrink_prompt_text(text, max_chars):
    text = text or ""
    if len(text) <= max_chars:
        return text

    marker = "\n\n[content trimmed for local model stability]\n\n"
    head_size = int(max_chars * 0.65)
    tail_size = max_chars - head_size - len(marker)
    if tail_size < 0:
        tail_size = 0
    return text[:head_size] + marker + text[-tail_size:]


def _call_local_text(system_prompt, user_prompt, temperature=0.0, max_chars=None):
    client = _openai_client()
    prompt_text = _shrink_prompt_text(user_prompt, max_chars or LOCAL_LLM_MAX_PROMPT_CHARS)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt_text},
    ]

    last_error = None
    for attempt in range(max(1, LOCAL_LLM_MAX_RETRIES)):
        try:
            _debug_log(f"local text request attempt={attempt + 1} chars={len(prompt_text)}")
            response = client.chat.completions.create(
                model=_active_local_model(),
                messages=messages,
                temperature=temperature,
                timeout=LOCAL_LLM_TIMEOUT_SECONDS,
            )
            content = (response.choices[0].message.content or "").strip()
            if content:
                return content
            last_error = ValueError("Local model returned empty text output.")
        except Exception as error:
            last_error = error
            _debug_log(f"local text request failed: {error}")

    raise last_error or ValueError("Local text generation failed.")


def _call_local_json(system_prompt, user_prompt, temperature=0.0, max_chars=None):
    client = _openai_client()
    prompt_text = _shrink_prompt_text(user_prompt, max_chars or LOCAL_LLM_MAX_PROMPT_CHARS)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt_text},
    ]

    last_error = None
    for attempt in range(max(1, LOCAL_LLM_MAX_RETRIES)):
        try:
            _debug_log(f"local json request attempt={attempt + 1} chars={len(prompt_text)}")
            response = client.chat.completions.create(
                model=_active_local_model(),
                messages=messages,
                temperature=temperature,
                response_format={"type": "json_object"},
                timeout=LOCAL_LLM_TIMEOUT_SECONDS,
            )
            content = (response.choices[0].message.content or "").strip()
            return _parse_json_response(content), content
        except Exception as first_error:
            last_error = first_error
            _debug_log(f"local structured-json request failed: {first_error}")
            try:
                response = client.chat.completions.create(
                    model=_active_local_model(),
                    messages=messages,
                    temperature=temperature,
                    timeout=LOCAL_LLM_TIMEOUT_SECONDS,
                )
                content = (response.choices[0].message.content or "").strip()
                return _parse_json_response(content), content
            except Exception as second_error:
                last_error = second_error
                _debug_log(f"local plain-json fallback failed: {second_error}")

    raise last_error or ValueError("Local JSON generation failed.")


def _call_local_structured(system_prompt, user_prompt, response_model=None, temperature=0.0, max_chars=None):
    prompt_text = _shrink_prompt_text(user_prompt, max_chars or LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS)

    if response_model is not None and HAS_INSTRUCTOR and HAS_PYDANTIC:
        try:
            _debug_log(f"local instructor request chars={len(prompt_text)}")
            client = instructor.from_openai(_openai_client(), mode=instructor.Mode.JSON)
            response = client.chat.completions.create(
                model=_active_local_model(),
                response_model=response_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_text},
                ],
                temperature=temperature,
                max_retries=1,
                timeout=LOCAL_LLM_TIMEOUT_SECONDS,
            )
            return _safe_model_dump(response), ""
        except Exception as error:
            _debug_log(f"local instructor fallback engaged: {error}")

    return _call_local_json(system_prompt, prompt_text, temperature=temperature, max_chars=max_chars or LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS)


def _call_gemini_text(system_prompt, user_prompt, temperature=0.0, model_name=None):
    response_text = _generate_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        json_mode=False,
        model_name=model_name or _model_name_for_provider("gemini"),
    )
    return response_text


def _call_gemini_json(system_prompt, user_prompt, temperature=0.0, model_name=None):
    response_text = _generate_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        json_mode=True,
        model_name=model_name or _model_name_for_provider("gemini"),
    )
    return _parse_json_response(response_text), response_text


def _guess_mime_type(file_path):
    lower_name = (file_path or "").lower()
    if lower_name.endswith(".pdf"):
        return "application/pdf"
    if lower_name.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return "application/octet-stream"


def _call_gemini_file_json(system_prompt, file_path, mime_type=None, temperature=0.0, model_name=None):
    client = _get_gemini_client()
    model_name = model_name or _model_name_for_provider("gemini")
    config = genai_types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=temperature,
        response_mime_type="application/json",
    )

    mime_type = mime_type or _guess_mime_type(file_path)
    prompt_text = "Use the attached document as the only source of truth. Return JSON only."
    last_error = None

    if hasattr(client, "files") and hasattr(client.files, "upload"):
        uploaded_file = None
        try:
            uploaded_file = client.files.upload(file=file_path)
            response = client.models.generate_content(
                model=model_name,
                contents=[uploaded_file, prompt_text],
                config=config,
            )
            text = (getattr(response, "text", "") or "").strip()
            return _parse_json_response(text), text
        except Exception as error:
            last_error = error
            _debug_log(f"gemini direct file upload failed: {error}")
        finally:
            try:
                if uploaded_file is not None and hasattr(client.files, "delete") and hasattr(uploaded_file, "name"):
                    client.files.delete(name=uploaded_file.name)
            except Exception:
                pass

    if genai_types is not None and hasattr(genai_types, "Part") and hasattr(genai_types.Part, "from_bytes"):
        try:
            with open(file_path, "rb") as handle:
                file_bytes = handle.read()
            file_part = genai_types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
            response = client.models.generate_content(
                model=model_name,
                contents=[file_part, prompt_text],
                config=config,
            )
            text = (getattr(response, "text", "") or "").strip()
            return _parse_json_response(text), text
        except Exception as error:
            last_error = error
            _debug_log(f"gemini bytes file input failed: {error}")

    raise last_error or ValueError("Gemini SDK file input is not available for this environment.")




def _attempt_gemini_json_with_model_fallback(system_prompt, user_prompt, temperature=0.0):
    last_error = None
    for gemini_model in _get_cloud_model_candidates(_model_name_for_provider("gemini")):
        try:
            result, raw = _call_gemini_json(system_prompt, user_prompt, temperature=temperature, model_name=gemini_model)
            return result, raw, gemini_model
        except Exception as error:
            last_error = error
            _debug_log(f"gemini json model={gemini_model} failed: {error}")
    raise last_error or ValueError("No Gemini model returned usable JSON.")


def _attempt_gemini_text_with_model_fallback(system_prompt, user_prompt, temperature=0.0):
    last_error = None
    for gemini_model in _get_cloud_model_candidates(_model_name_for_provider("gemini")):
        try:
            text_value = _call_gemini_text(system_prompt, user_prompt, temperature=temperature, model_name=gemini_model)
            return text_value, gemini_model
        except Exception as error:
            last_error = error
            _debug_log(f"gemini text model={gemini_model} failed: {error}")
    raise last_error or ValueError("No Gemini model returned usable text.")


def _attempt_gemini_file_json_with_model_fallback(system_prompt, file_path, mime_type=None, temperature=0.0):
    last_error = None
    for gemini_model in _get_cloud_model_candidates(_model_name_for_provider("gemini")):
        try:
            result, raw = _call_gemini_file_json(system_prompt, file_path, mime_type=mime_type, temperature=temperature, model_name=gemini_model)
            return result, raw, gemini_model
        except Exception as error:
            last_error = error
            _debug_log(f"gemini direct-file model={gemini_model} failed: {error}")
    raise last_error or ValueError("No Gemini model accepted the direct file input.")
def _run_routed_json_task(task_name, system_prompt, user_prompt, temperature=0.0, default_result=None, response_model=None, escalation_level=0, local_max_chars=None):
    providers = _route_provider_sequence(task_name=task_name, escalation_level=escalation_level)
    requested_provider = providers[0] if providers else "deterministic"
    requested_model = _model_name_for_provider(requested_provider)
    last_error = None

    for provider in providers:
        try:
            if provider == "local_qwen":
                if response_model is not None:
                    result, raw_response = _call_local_structured(system_prompt, user_prompt, response_model=response_model, temperature=temperature, max_chars=local_max_chars or LOCAL_LLM_MAX_PROMPT_CHARS)
                else:
                    result, raw_response = _call_local_json(system_prompt, user_prompt, temperature=temperature, max_chars=local_max_chars or LOCAL_LLM_MAX_PROMPT_CHARS)
                used_model = _active_local_model()
            elif provider == "gemini":
                result = None
                raw_response = ""
                used_model = ""
                gemini_error = None
                for gemini_model in _get_cloud_model_candidates(_model_name_for_provider("gemini")):
                    try:
                        result, raw_response = _call_gemini_json(system_prompt, user_prompt, temperature=temperature, model_name=gemini_model)
                        used_model = gemini_model
                        gemini_error = None
                        break
                    except Exception as error:
                        gemini_error = error
                        _debug_log(f"routed json task={task_name} gemini model={gemini_model} failed: {error}")
                if gemini_error is not None and used_model == "":
                    raise gemini_error
            else:
                continue

            fallback_used = provider != requested_provider or (provider == requested_provider and used_model and requested_model and used_model != requested_model)
            message = ""
            if fallback_used:
                message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output."
            _set_last_route_info(task_name, requested_provider, provider, requested_model, used_model, fallback_used, message)
            return result, raw_response
        except Exception as error:
            last_error = error
            _debug_log(f"routed json task={task_name} provider={provider} failed: {error}")

    _set_last_route_info(task_name, requested_provider, "", requested_model, "", False, str(last_error or "No provider returned a usable result."))
    if default_result is not None:
        return default_result, ""
    raise last_error or ValueError("No routed JSON provider succeeded.")


def _run_routed_text_task(task_name, system_prompt, user_prompt, temperature=0.0, default_text="", escalation_level=0, local_max_chars=None):
    providers = _route_provider_sequence(task_name=task_name, escalation_level=escalation_level)
    requested_provider = providers[0] if providers else "deterministic"
    requested_model = _model_name_for_provider(requested_provider)
    last_error = None

    for provider in providers:
        try:
            if provider == "local_qwen":
                text_value = _call_local_text(system_prompt, user_prompt, temperature=temperature, max_chars=local_max_chars or LOCAL_LLM_MAX_PROMPT_CHARS)
                used_model = _active_local_model()
            elif provider == "gemini":
                text_value = ""
                used_model = ""
                gemini_error = None
                for gemini_model in _get_cloud_model_candidates(_model_name_for_provider("gemini")):
                    try:
                        text_value = _call_gemini_text(system_prompt, user_prompt, temperature=temperature, model_name=gemini_model)
                        used_model = gemini_model
                        gemini_error = None
                        break
                    except Exception as error:
                        gemini_error = error
                        _debug_log(f"routed text task={task_name} gemini model={gemini_model} failed: {error}")
                if gemini_error is not None and used_model == "":
                    raise gemini_error
            else:
                continue

            fallback_used = provider != requested_provider or (provider == requested_provider and used_model and requested_model and used_model != requested_model)
            message = ""
            if fallback_used:
                message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output."
            _set_last_route_info(task_name, requested_provider, provider, requested_model, used_model, fallback_used, message)
            return text_value
        except Exception as error:
            last_error = error
            _debug_log(f"routed text task={task_name} provider={provider} failed: {error}")

    _set_last_route_info(task_name, requested_provider, "", requested_model, "", False, str(last_error or "No provider returned usable text."))
    if default_text:
        return default_text
    raise last_error or ValueError("No routed text provider succeeded.")


def _build_table_preview(tables, max_tables=5, max_rows=6):
    preview = []

    for table in (tables or [])[:max_tables]:
        page_number = table.get("page_number") if isinstance(table, dict) else None
        table_rows = table.get("rows", table) if isinstance(table, dict) else table
        rows = table_rows[:max_rows] if isinstance(table_rows, list) else []
        row_lines = []
        for row in rows:
            if isinstance(row, dict):
                row_text = " | ".join([str(value) for value in row.values() if str(value).strip()])
            else:
                row_text = " | ".join([str(cell) for cell in row if str(cell).strip()])
            if row_text:
                row_lines.append(row_text)

        if row_lines:
            prefix = f"Page {page_number}" if page_number else "Table"
            preview.append(prefix + ":\n" + "\n".join(row_lines))

    return "\n\n".join(preview)


def _build_blocks_preview(parsed_document, max_blocks=40):
    if not parsed_document:
        return ""

    lines = []
    for block in parsed_document.get("blocks", [])[:max_blocks]:
        text = (block.get("text") or "").strip()
        if not text:
            continue
        page_number = block.get("page_number", "")
        lines.append(f"[Page {page_number}] {text}")

    return "\n".join(lines)


def _build_document_evidence(document_text, tables=None, parsed_document=None, current_extracted=None, for_local=False):
    parse_quality = {}
    if parsed_document and isinstance(parsed_document, dict):
        parse_quality = parsed_document.get("quality", {}) or {}

    parts = [
        "Document raw text:",
        _trim_prompt_text(document_text, LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS if for_local else 18000),
    ]

    table_preview = _build_table_preview(tables or [], max_tables=6, max_rows=8)
    if table_preview:
        parts.extend([
            "",
            "Table preview:",
            _trim_prompt_text(table_preview, 2500 if for_local else 6000),
        ])

    block_preview = _build_blocks_preview(parsed_document, max_blocks=60)
    if block_preview:
        parts.extend([
            "",
            "Page and block preview:",
            _trim_prompt_text(block_preview, 3500 if for_local else 9000),
        ])

    if parse_quality:
        parts.extend([
            "",
            "Parsing quality metadata:",
            json.dumps(parse_quality, ensure_ascii=False, indent=2),
        ])

    if current_extracted is not None:
        parts.extend([
            "",
            "Current extracted data:",
            json.dumps(current_extracted, ensure_ascii=False, indent=2),
        ])

    return "\n".join(parts)


def _normalize_result_provider_meta(result, provider, status, message):
    result = dict(result or {})
    result["extraction_meta"] = {
        "provider": provider,
        "status": status,
        "message": message,
        "backend": provider,
        "model": LOCAL_LLM_MODEL if provider == "local_qwen" else (_model_name_for_provider("gemini") if provider == "gemini" else ""),
    }
    return result


def _merge_context_dicts(base_context, candidate_context):
    merged = dict(base_context or {})
    for key, value in (candidate_context or {}).items():
        if value:
            merged[key] = value
    return merged


def _merge_session_rows(base_rows, candidate_rows, conservative=False):
    base_rows = list(base_rows or [])
    candidate_rows = list(candidate_rows or [])
    merged = [dict(row) for row in base_rows]

    def row_key(row):
        session_number = row.get("session_number")
        title = (row.get("title") or "").strip().lower()
        date = row.get("date") or ""
        if session_number:
            return ("session", session_number)
        if title and date:
            return ("title_date", title, date)
        if title:
            return ("title", title)
        return None

    index = {}
    for position, row in enumerate(merged):
        key = row_key(row)
        if key is not None and key not in index:
            index[key] = position

    for row in candidate_rows:
        candidate = dict(row)
        key = row_key(candidate)
        if key is None or key not in index:
            merged.append(candidate)
            if key is not None and key not in index:
                index[key] = len(merged) - 1
            continue

        current = merged[index[key]]
        for field in ["date", "start_time", "end_time", "title", "description"]:
            current_value = current.get(field, "")
            candidate_value = candidate.get(field, "")
            if conservative:
                if (not current_value) and candidate_value:
                    current[field] = candidate_value
            else:
                if candidate_value:
                    current[field] = candidate_value

        if not current.get("session_number") and candidate.get("session_number"):
            current["session_number"] = candidate.get("session_number")

        if candidate.get("date_inferred") is False:
            current["date_inferred"] = False
        elif "date_inferred" not in current:
            current["date_inferred"] = bool(candidate.get("date_inferred", False))

    return merged


def _merge_assignment_rows(base_rows, candidate_rows, conservative=False):
    base_rows = list(base_rows or [])
    candidate_rows = list(candidate_rows or [])
    merged = [dict(row) for row in base_rows]

    def row_key(row):
        title = (row.get("title") or "").strip().lower()
        due_date = row.get("due_date") or ""
        due_time = row.get("due_time") or ""
        if title and due_date:
            return ("title_date", title, due_date)
        if title and due_time:
            return ("title_time", title, due_time)
        if title:
            return ("title", title)
        return None

    index = {}
    for position, row in enumerate(merged):
        key = row_key(row)
        if key is not None and key not in index:
            index[key] = position

    for row in candidate_rows:
        candidate = dict(row)
        key = row_key(candidate)
        if key is None or key not in index:
            merged.append(candidate)
            if key is not None and key not in index:
                index[key] = len(merged) - 1
            continue

        current = merged[index[key]]
        for field in ["due_date", "due_time", "title", "description"]:
            current_value = current.get(field, "")
            candidate_value = candidate.get(field, "")
            if conservative:
                if (not current_value) and candidate_value:
                    current[field] = candidate_value
            else:
                if candidate_value:
                    current[field] = candidate_value

        if candidate.get("due_time_inferred") is False:
            current["due_time_inferred"] = False
        elif "due_time_inferred" not in current:
            current["due_time_inferred"] = bool(candidate.get("due_time_inferred", False))

    return merged


def _merge_course_outline(base_result, candidate_result, conservative=False):
    base = pdf_tool.validate_course_outline_data(base_result or {})
    candidate = pdf_tool.validate_course_outline_data(candidate_result or {})
    merged = dict(base)

    for field in [
        "course_name",
        "days_of_week",
        "start_time",
        "end_time",
        "term_start_date",
        "term_end_date",
        "location",
    ]:
        base_value = base.get(field)
        candidate_value = candidate.get(field)
        if conservative:
            if (not base_value) and candidate_value:
                merged[field] = candidate_value
        else:
            if candidate_value:
                merged[field] = candidate_value

    merged["sessions"] = _merge_session_rows(base.get("sessions", []), candidate.get("sessions", []), conservative=conservative)
    merged["assignments"] = _merge_assignment_rows(base.get("assignments", []), candidate.get("assignments", []), conservative=conservative)
    merged["extraction_contexts"] = _merge_context_dicts(base.get("extraction_contexts", {}), candidate.get("extraction_contexts", {}))
    merged["parse_meta"] = candidate.get("parse_meta") or base.get("parse_meta", {})
    merged["extraction_meta"] = candidate.get("extraction_meta") or base.get("extraction_meta", {})
    merged["confidence"] = max(float(base.get("confidence", 0.0) or 0.0), float(candidate.get("confidence", 0.0) or 0.0))

    return pdf_tool.validate_course_outline_data(merged)


def _merge_invitation(base_result, candidate_result, conservative=False):
    base = pdf_tool.validate_invitation_details(base_result or {})
    candidate = pdf_tool.validate_invitation_details(candidate_result or {})
    merged = dict(base)

    for field in [
        "event_title",
        "event_date",
        "event_time",
        "event_end_time",
        "location",
        "host",
        "rsvp_deadline",
        "description",
    ]:
        base_value = base.get(field)
        candidate_value = candidate.get(field)
        if conservative:
            if (not base_value) and candidate_value:
                merged[field] = candidate_value
        else:
            if candidate_value:
                merged[field] = candidate_value

    if conservative:
        if not merged.get("rsvp_required") and candidate.get("rsvp_required"):
            merged["rsvp_required"] = True
    else:
        merged["rsvp_required"] = candidate.get("rsvp_required") if "rsvp_required" in candidate else base.get("rsvp_required", False)

    merged["parse_meta"] = candidate.get("parse_meta") or base.get("parse_meta", {})
    merged["extraction_meta"] = candidate.get("extraction_meta") or base.get("extraction_meta", {})
    merged["confidence"] = max(float(base.get("confidence", 0.0) or 0.0), float(candidate.get("confidence", 0.0) or 0.0))

    return pdf_tool.validate_invitation_details(merged)


def _course_metadata_prompt():
    return (
        "You extract only course metadata from a course outline or syllabus. "
        "Return JSON only. "
        "Do not invent facts. Leave unsupported fields blank. "
        "Normalize days_of_week into full weekday names. "
        "Normalize times into HH:MM 24-hour format when possible. "
        "Normalize dates into YYYY-MM-DD when possible. "
        "Fields: course_name, days_of_week, start_time, end_time, term_start_date, term_end_date, location."
    )


def _course_items_prompt():
    return (
        "You extract dated course sessions and assessments from a course outline or syllabus. "
        "Return JSON only. "
        "Do not invent facts. Keep unsupported fields blank. "
        "Normalize dates into YYYY-MM-DD when possible. "
        "Normalize times into HH:MM 24-hour format when possible. "
        "If a session number is visible, include it. "
        "If the document gives a term start date, term end date, and recurring meeting days but not explicit class dates, "
        "you may infer session dates by aligning the ordered module or session sequence to the recurring schedule inside that term window. "
        "If a due item appears under a specific module row and the module date can be inferred reliably, you may assign that module date as the due_date. "
        "If a due date exists but no due time is stated, leave due_time blank here and let downstream validation default it. "
        "Return keys: sessions and assignments. "
        "Each session item has: session_number, date, date_inferred, start_time, end_time, title, description, reminders_minutes. "
        "Each assignment item has: title, due_date, due_time, due_time_inferred, description, reminders_minutes."
    )

def _course_repair_prompt():
    return (
        "You are repairing a course outline extraction. "
        "Return JSON only. "
        "Use the document evidence, table preview, parsed blocks, and current extracted data. "
        "Fill blanks and correct clearly supported values. "
        "Preserve existing valid values unless the evidence strongly contradicts them. "
        "If the document gives a recurring class pattern and term window, you may keep or add inferred session dates that align with that pattern. "
        "If a due milestone appears under a dated or inferable module row, you may keep or add that due_date. "
        "Do not invent missing facts beyond what can be supported or safely inferred from the schedule pattern. "
        "Return the full course outline object with keys: course_name, days_of_week, start_time, end_time, term_start_date, term_end_date, location, sessions, assignments."
    )

def _invitation_prompt():
    return (
        "You extract structured invitation details. "
        "Return JSON only. "
        "Do not invent facts. Leave unsupported fields blank. "
        "Normalize dates into YYYY-MM-DD when possible. "
        "Normalize times into HH:MM 24-hour format when possible. "
        "Return keys: event_title, event_date, event_time, event_end_time, location, host, rsvp_required, rsvp_deadline, description."
    )


def _invitation_repair_prompt():
    return (
        "You are repairing invitation extraction. "
        "Return JSON only. "
        "Use the document evidence, table preview, and current extracted data. "
        "Fill blanks and correct clearly supported values without overwriting valid user edits recklessly. "
        "Do not invent missing facts. "
        "Return keys: event_title, event_date, event_time, event_end_time, location, host, rsvp_required, rsvp_deadline, description."
    )


def _run_course_outline_provider(provider, document_text, tables=None, parsed_document=None, file_path=None, mime_type=None):
    evidence = _build_document_evidence(document_text, tables=tables, parsed_document=parsed_document, for_local=(provider == "local_qwen"))

    try:
        if provider == "local_qwen":
            if not _local_backend_available():
                raise ValueError("Local OpenAI-compatible backend is not available.")

            metadata_model = CourseMetadataModel if HAS_PYDANTIC else None
            items_model = CourseItemsModel if HAS_PYDANTIC else None

            metadata_result, metadata_raw = _call_local_structured(
                _course_metadata_prompt(),
                evidence,
                response_model=metadata_model,
                temperature=0.0,
                max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS,
            )
            items_result, items_raw = _call_local_structured(
                _course_items_prompt(),
                evidence,
                response_model=items_model,
                temperature=0.0,
                max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS,
            )

            merged_result = {}
            if isinstance(metadata_result, dict):
                merged_result.update(metadata_result)
            if isinstance(items_result, dict):
                merged_result.update(items_result)

            merged_result = pdf_tool.validate_course_outline_data(merged_result)
            return {
                "success": True,
                "result": merged_result,
                "provider": provider,
                "message": f"Structured course extraction completed with {_provider_label(provider)}.",
                "raw_response": "\n\n".join([str(metadata_raw or ""), str(items_raw or "")]).strip(),
            }

        if provider == "gemini":
            direct_note = ""
            if file_path:
                try:
                    metadata_result, metadata_raw = _call_gemini_file_json(_course_metadata_prompt(), file_path, mime_type=mime_type, temperature=0.0)
                    items_result, items_raw = _call_gemini_file_json(_course_items_prompt(), file_path, mime_type=mime_type, temperature=0.0)
                    merged_result = {}
                    if isinstance(metadata_result, dict):
                        merged_result.update(metadata_result)
                    if isinstance(items_result, dict):
                        merged_result.update(items_result)
                    merged_result = pdf_tool.validate_course_outline_data(merged_result)
                    return {
                        "success": True,
                        "result": merged_result,
                        "provider": provider,
                        "message": "Structured course extraction completed with Gemini direct document input.",
                        "raw_response": "\n\n".join([str(metadata_raw or ""), str(items_raw or "")]).strip(),
                    }
                except Exception as direct_error:
                    direct_note = f" Gemini direct document input was unavailable, so parsed text fallback was used instead. Details: {direct_error}"

            metadata_result, metadata_raw = _call_gemini_json(_course_metadata_prompt(), evidence, temperature=0.0)
            items_result, items_raw = _call_gemini_json(_course_items_prompt(), evidence, temperature=0.0)

            merged_result = {}
            if isinstance(metadata_result, dict):
                merged_result.update(metadata_result)
            if isinstance(items_result, dict):
                merged_result.update(items_result)

            merged_result = pdf_tool.validate_course_outline_data(merged_result)
            return {
                "success": True,
                "result": merged_result,
                "provider": provider,
                "message": f"Structured course extraction completed with {_provider_label(provider)}.{direct_note}",
                "raw_response": "\n\n".join([str(metadata_raw or ""), str(items_raw or "")]).strip(),
            }
    except Exception as e:
        return {
            "success": False,
            "result": {},
            "provider": provider,
            "message": str(e),
            "raw_response": "",
        }

    return {
        "success": False,
        "result": {},
        "provider": provider,
        "message": "Unsupported provider.",
        "raw_response": "",
    }


def _run_course_repair_provider(provider, document_text, current_extracted, tables=None, parsed_document=None):
    evidence = _build_document_evidence(document_text, tables=tables, parsed_document=parsed_document, current_extracted=current_extracted, for_local=(provider == "local_qwen"))

    try:
        if provider == "local_qwen":
            if not _local_backend_available():
                raise ValueError("Local OpenAI-compatible backend is not available.")

            model = CourseOutlineModel if HAS_PYDANTIC else None
            result, raw = _call_local_structured(_course_repair_prompt(), evidence, response_model=model, temperature=0.0, max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS)
            return {
                "success": True,
                "result": pdf_tool.validate_course_outline_data(result),
                "provider": provider,
                "message": f"Course repair completed with {_provider_label(provider)}.",
                "raw_response": raw,
            }

        if provider == "gemini":
            result, raw, used_model = _attempt_gemini_json_with_model_fallback(_course_repair_prompt(), evidence, temperature=0.0)
            return {
                "success": True,
                "result": pdf_tool.validate_course_outline_data(result),
                "provider": provider,
                "message": f"Course repair completed with {_provider_label(provider)} using {used_model}.",
                "raw_response": raw,
            }
    except Exception as e:
        return {
            "success": False,
            "result": {},
            "provider": provider,
            "message": str(e),
            "raw_response": "",
        }

    return {
        "success": False,
        "result": {},
        "provider": provider,
        "message": "Unsupported provider.",
        "raw_response": "",
    }


def _run_invitation_provider(provider, document_text, tables=None, parsed_document=None, file_path=None, mime_type=None):
    evidence = _build_document_evidence(document_text, tables=tables, parsed_document=parsed_document, for_local=(provider == "local_qwen"))

    try:
        if provider == "local_qwen":
            if not _local_backend_available():
                raise ValueError("Local OpenAI-compatible backend is not available.")

            model = InvitationDetailsModel if HAS_PYDANTIC else None
            result, raw = _call_local_structured(_invitation_prompt(), evidence, response_model=model, temperature=0.0, max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS)
            return {
                "success": True,
                "result": pdf_tool.validate_invitation_details(result),
                "provider": provider,
                "message": f"Invitation extraction completed with {_provider_label(provider)}.",
                "raw_response": raw,
            }

        if provider == "gemini":
            direct_note = ""
            used_model = ""
            if file_path:
                try:
                    result, raw, used_model = _attempt_gemini_file_json_with_model_fallback(_invitation_prompt(), file_path, mime_type=mime_type, temperature=0.0)
                    return {
                        "success": True,
                        "result": pdf_tool.validate_invitation_details(result),
                        "provider": provider,
                        "message": f"Invitation extraction completed with Gemini direct document input using {used_model}.",
                        "raw_response": raw,
                    }
                except Exception as direct_error:
                    direct_note = f" Gemini direct document input was unavailable, so parsed text fallback was used instead. Details: {direct_error}"

            result, raw, used_model = _attempt_gemini_json_with_model_fallback(_invitation_prompt(), evidence, temperature=0.0)
            return {
                "success": True,
                "result": pdf_tool.validate_invitation_details(result),
                "provider": provider,
                "message": f"Invitation extraction completed with {_provider_label(provider)} using {used_model}.{direct_note}",
                "raw_response": raw,
            }
    except Exception as e:
        return {
            "success": False,
            "result": {},
            "provider": provider,
            "message": str(e),
            "raw_response": "",
        }

    return {
        "success": False,
        "result": {},
        "provider": provider,
        "message": "Unsupported provider.",
        "raw_response": "",
    }


def _run_invitation_repair_provider(provider, document_text, current_extracted, tables=None, parsed_document=None):
    evidence = _build_document_evidence(document_text, tables=tables, parsed_document=parsed_document, current_extracted=current_extracted, for_local=(provider == "local_qwen"))

    try:
        if provider == "local_qwen":
            if not _local_backend_available():
                raise ValueError("Local OpenAI-compatible backend is not available.")

            model = InvitationDetailsModel if HAS_PYDANTIC else None
            result, raw = _call_local_structured(_invitation_repair_prompt(), evidence, response_model=model, temperature=0.0, max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS)
            return {
                "success": True,
                "result": pdf_tool.validate_invitation_details(result),
                "provider": provider,
                "message": f"Invitation repair completed with {_provider_label(provider)}.",
                "raw_response": raw,
            }

        if provider == "gemini":
            result, raw, used_model = _attempt_gemini_json_with_model_fallback(_invitation_repair_prompt(), evidence, temperature=0.0)
            return {
                "success": True,
                "result": pdf_tool.validate_invitation_details(result),
                "provider": provider,
                "message": f"Invitation repair completed with {_provider_label(provider)} using {used_model}.",
                "raw_response": raw,
            }
    except Exception as e:
        return {
            "success": False,
            "result": {},
            "provider": provider,
            "message": str(e),
            "raw_response": "",
        }

    return {
        "success": False,
        "result": {},
        "provider": provider,
        "message": "Unsupported provider.",
        "raw_response": "",
    }


# Runs the hybrid two-pass course outline extraction flow.
def extract_course_outline_hybrid(document_text, tables=None, parsed_document=None, deterministic_result=None, return_meta=False, file_path=None, file_name="", mime_type=None):
    base_result = pdf_tool.validate_course_outline_data(
        deterministic_result or pdf_tool.extract_course_outline(
            document_text,
            tables=tables,
            parsed_document=parsed_document,
        )
    )

    raw_response = ""
    last_error = ""

    providers = _route_provider_sequence(task_name="document_extract")
    requested_provider = providers[0] if providers else "deterministic"

    for provider in providers:
        provider_result = _run_course_outline_provider(
            provider,
            document_text,
            tables=tables,
            parsed_document=parsed_document,
            file_path=file_path,
            mime_type=mime_type,
        )

        if not provider_result.get("success"):
            last_error = provider_result.get("message", "")
            continue

        merged_result = _merge_course_outline(base_result, provider_result.get("result", {}), conservative=True)
        merged_result["parse_meta"] = merged_result.get("parse_meta") or (parsed_document.get("quality", {}) if parsed_document else {})
        merged_result = _normalize_result_provider_meta(
            merged_result,
            provider,
            "success",
            provider_result.get("message", ""),
        )

        before = _course_outline_visible_payload(base_result)
        after = _course_outline_visible_payload(merged_result)
        status = "success"
        message = provider_result.get("message", "")
        if provider != requested_provider:
            message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. {message}".strip()
        if provider != requested_provider:
            message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. {message}".strip()
        if _json_snapshot(before) == _json_snapshot(after):
            status = "no_change"
            message = f"{_provider_label(provider)} returned no supported visible changes beyond the deterministic extraction."

        meta = _verification_meta(
            merged_result,
            status,
            message,
            provider_result.get("raw_response", ""),
            provider=provider,
            backend=provider,
        )
        return meta if return_meta else merged_result

    fallback_result = _normalize_result_provider_meta(
        base_result,
        "deterministic",
        "success" if base_result else "error",
        "Fell back to deterministic parsing because no LLM provider returned a usable structured result."
        + (f" Last provider issue: {last_error}" if last_error else ""),
    )
    meta = _verification_meta(
        fallback_result,
        "success" if base_result else "error",
        fallback_result["extraction_meta"]["message"],
        raw_response,
        provider="deterministic",
        backend="deterministic",
    )
    return meta if return_meta else fallback_result


# Runs the hybrid invitation extraction flow.
def extract_invitation_hybrid(document_text, tables=None, parsed_document=None, deterministic_result=None, return_meta=False, file_path=None, file_name="", mime_type=None):
    base_result = pdf_tool.validate_invitation_details(
        deterministic_result or pdf_tool.extract_invitation_details(
            document_text,
            parsed_document=parsed_document,
            tables=tables,
        )
    )

    raw_response = ""
    last_error = ""

    providers = _route_provider_sequence(task_name="document_extract")
    requested_provider = providers[0] if providers else "deterministic"

    for provider in providers:
        provider_result = _run_invitation_provider(
            provider,
            document_text,
            tables=tables,
            parsed_document=parsed_document,
            file_path=file_path,
            mime_type=mime_type,
        )

        if not provider_result.get("success"):
            last_error = provider_result.get("message", "")
            continue

        merged_result = _merge_invitation(base_result, provider_result.get("result", {}), conservative=False)
        merged_result["parse_meta"] = merged_result.get("parse_meta") or (parsed_document.get("quality", {}) if parsed_document else {})
        merged_result = _normalize_result_provider_meta(
            merged_result,
            provider,
            "success",
            provider_result.get("message", ""),
        )

        before = _invitation_visible_payload(base_result)
        after = _invitation_visible_payload(merged_result)
        status = "success"
        message = provider_result.get("message", "")
        if _json_snapshot(before) == _json_snapshot(after):
            status = "no_change"
            message = f"{_provider_label(provider)} returned no supported visible changes beyond the deterministic extraction."

        meta = _verification_meta(
            merged_result,
            status,
            message,
            provider_result.get("raw_response", ""),
            provider=provider,
            backend=provider,
        )
        return meta if return_meta else merged_result

    fallback_result = _normalize_result_provider_meta(
        base_result,
        "deterministic",
        "success" if base_result else "error",
        "Fell back to deterministic parsing because no LLM provider returned a usable structured result."
        + (f" Last provider issue: {last_error}" if last_error else ""),
    )
    meta = _verification_meta(
        fallback_result,
        "success" if base_result else "error",
        fallback_result["extraction_meta"]["message"],
        raw_response,
        provider="deterministic",
        backend="deterministic",
    )
    return meta if return_meta else fallback_result


# Verifies and corrects a deterministic or user-edited course outline extraction.
def verify_course_extraction(document_text, extracted_data, extraction_contexts, extracted_tables=None, parsed_document=None, return_meta=False):
    base_result = pdf_tool.validate_course_outline_data(extracted_data or {})
    base_result["extraction_contexts"] = _merge_context_dicts(base_result.get("extraction_contexts", {}), extraction_contexts or {})

    raw_response = ""
    last_error = ""

    providers = _route_provider_sequence(task_name="document_repair")
    requested_provider = providers[0] if providers else "deterministic"

    for provider in providers:
        provider_result = _run_course_repair_provider(
            provider,
            document_text,
            base_result,
            tables=extracted_tables,
            parsed_document=parsed_document,
        )

        if not provider_result.get("success"):
            last_error = provider_result.get("message", "")
            continue

        merged_result = _merge_course_outline(base_result, provider_result.get("result", {}), conservative=True)
        merged_result["extraction_contexts"] = _merge_context_dicts(merged_result.get("extraction_contexts", {}), extraction_contexts or {})
        merged_result["parse_meta"] = merged_result.get("parse_meta") or (parsed_document.get("quality", {}) if parsed_document else {})
        merged_result = _normalize_result_provider_meta(
            merged_result,
            provider,
            "success",
            provider_result.get("message", ""),
        )

        before = _course_outline_visible_payload(base_result)
        after = _course_outline_visible_payload(merged_result)
        if _json_snapshot(after) == _json_snapshot(before):
            meta = _verification_meta(
                merged_result,
                "no_change",
                (f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. " if provider != requested_provider else "") + f"{_provider_label(provider)} reviewed the course outline but did not find supported visible changes.",
                provider_result.get("raw_response", ""),
                provider=provider,
                backend=provider,
            )
            return meta if return_meta else merged_result

        repair_message = f"{_provider_label(provider)} returned supported updates for the course outline."
        if provider != requested_provider:
            repair_message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. " + repair_message
        meta = _verification_meta(
            merged_result,
            "success",
            repair_message,
            provider_result.get("raw_response", ""),
            provider=provider,
            backend=provider,
        )
        return meta if return_meta else merged_result

    fallback_result = _normalize_result_provider_meta(
        base_result,
        "deterministic",
        "no_change",
        "No structured LLM repair result was usable, so the current course outline values were preserved."
        + (f" Last provider issue: {last_error}" if last_error else ""),
    )
    meta = _verification_meta(
        fallback_result,
        "error" if last_error else "no_change",
        fallback_result["extraction_meta"]["message"],
        raw_response,
        provider="deterministic",
        backend="deterministic",
    )
    return meta if return_meta else fallback_result


# Verifies and corrects a deterministic or user-edited invitation extraction.
def verify_invitation_extraction(document_text, extracted_data, extracted_tables=None, parsed_document=None, return_meta=False):
    base_result = pdf_tool.validate_invitation_details(extracted_data or {})

    raw_response = ""
    last_error = ""

    providers = _route_provider_sequence(task_name="document_repair")
    requested_provider = providers[0] if providers else "deterministic"

    for provider in providers:
        provider_result = _run_invitation_repair_provider(
            provider,
            document_text,
            base_result,
            tables=extracted_tables,
            parsed_document=parsed_document,
        )

        if not provider_result.get("success"):
            last_error = provider_result.get("message", "")
            continue

        merged_result = _merge_invitation(base_result, provider_result.get("result", {}), conservative=True)
        merged_result["parse_meta"] = merged_result.get("parse_meta") or (parsed_document.get("quality", {}) if parsed_document else {})
        merged_result = _normalize_result_provider_meta(
            merged_result,
            provider,
            "success",
            provider_result.get("message", ""),
        )

        before = _invitation_visible_payload(base_result)
        after = _invitation_visible_payload(merged_result)
        if _json_snapshot(after) == _json_snapshot(before):
            meta = _verification_meta(
                merged_result,
                "no_change",
                (f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. " if provider != requested_provider else "") + f"{_provider_label(provider)} reviewed the invitation but did not find supported visible changes.",
                provider_result.get("raw_response", ""),
                provider=provider,
                backend=provider,
            )
            return meta if return_meta else merged_result

        repair_message = f"{_provider_label(provider)} returned supported updates for the invitation."
        if provider != requested_provider:
            repair_message = f"Used {_provider_label(provider)} because the preferred backend was unavailable or returned unusable output. " + repair_message
        meta = _verification_meta(
            merged_result,
            "success",
            repair_message,
            provider_result.get("raw_response", ""),
            provider=provider,
            backend=provider,
        )
        return meta if return_meta else merged_result

    fallback_result = _normalize_result_provider_meta(
        base_result,
        "deterministic",
        "no_change",
        "No structured LLM repair result was usable, so the current invitation values were preserved."
        + (f" Last provider issue: {last_error}" if last_error else ""),
    )
    meta = _verification_meta(
        fallback_result,
        "error" if last_error else "no_change",
        fallback_result["extraction_meta"]["message"],
        raw_response,
        provider="deterministic",
        backend="deterministic",
    )
    return meta if return_meta else fallback_result


# Plans the most likely assistant action for a free-form chat message.
def plan_chat_action(text: str, current_datetime_text: str = "") -> dict:
    default_result = {
        "intent": "general",
        "recipient_name": "",
        "recipient_email": "",
        "subject_hint": "",
        "email_body_notes": "",
        "items": [],
        "query_scope": "",
        "query_date": "",
        "delete_title": "",
        "delete_date": "",
        "delete_time": "",
        "notes": "",
        "confidence": "low",
    }

    if not text or not str(text).strip():
        return default_result

    current_datetime_text = (current_datetime_text or "").strip()

    system_prompt = (
        "You are the planning layer for a personal assistant app. "
        "The app can do these things: "
        "1) draft a new outbound email, "
        "2) add calendar items like appointments, reminders, calls, meetings, invitations, and tasks, "
        "3) answer schedule questions like today, tomorrow, or this week, "
        "4) delete calendar events by title and date when the user asks. "
        "Return ONLY a JSON object. "
        "Choose exactly one intent from: email_request, calendar_add, calendar_query, calendar_delete, general. "
        "Interpret natural language dates such as today, tomorrow, tonight, this week, next week, and specific dates using the provided current datetime. "
        "When the user provides a numbered or bulleted to-do list, use intent calendar_add and return one task item per list entry in items. "
        "When the user asks about schedule or calendar availability, use calendar_query. "
        "When the user asks to remove, delete, cancel, or clear an existing calendar item, use calendar_delete. "
        "When the user asks to email someone, draft a message, send a note, or write to someone, use email_request. "
        "For calendar_add, items must be a JSON array of objects. Each item object must contain exactly these keys: title, date, start_time, end_time, location, item_kind, description. "
        "item_kind must be one of task, appointment, invitation. "
        "Use YYYY-MM-DD for dates when possible. "
        "Use HH:MM in 24-hour format when possible. "
        "If a time is missing, use an empty string. "
        "If the meaning is unclear, choose the most likely supported intent only when the wording strongly implies it. Otherwise return general."
    )

    user_prompt = (
        f"Current datetime: {current_datetime_text or 'not provided'}\n\n"
        f"User message:\n{text}\n\n"
        "Return JSON with keys: intent, recipient_name, recipient_email, subject_hint, email_body_notes, items, query_scope, query_date, delete_title, delete_date, delete_time, notes, confidence."
    )

    try:
        result, _ = _run_routed_json_task(
            "chat_planning",
            system_prompt,
            user_prompt,
            temperature=0.0,
            default_result=default_result,
            local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
        )
        merged = _merge_with_default_dict(result, default_result)
        items = merged.get("items", [])
        if not isinstance(items, list):
            items = []
        clean_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            clean_items.append({
                "title": item.get("title", ""),
                "date": item.get("date", ""),
                "start_time": item.get("start_time", ""),
                "end_time": item.get("end_time", ""),
                "location": item.get("location", ""),
                "item_kind": item.get("item_kind", ""),
                "description": item.get("description", ""),
            })
        merged["items"] = clean_items
        return merged
    except Exception:
        return _fallback_chat_plan(text, current_datetime_text=current_datetime_text)


# Deterministic fallback for common chat assistant requests.
def _fallback_chat_plan(text: str, current_datetime_text: str = "") -> dict:
    text = str(text or "").strip()
    lowered = text.lower()
    default_result = {
        "intent": "general",
        "recipient_name": "",
        "recipient_email": "",
        "subject_hint": "",
        "email_body_notes": "",
        "items": [],
        "query_scope": "",
        "query_date": "",
        "delete_title": "",
        "delete_date": "",
        "delete_time": "",
        "notes": "",
        "confidence": "low",
    }

    if any(phrase in lowered for phrase in ["schedule for this week", "my schedule this week", "what is my schedule for this week", "calendar this week"]):
        result = dict(default_result)
        result["intent"] = "calendar_query"
        result["query_scope"] = "this_week"
        result["confidence"] = "medium"
        return result

    if any(phrase in lowered for phrase in ["schedule for today", "what is my schedule today", "my schedule today", "calendar today"]):
        result = dict(default_result)
        result["intent"] = "calendar_query"
        result["query_scope"] = "today"
        result["confidence"] = "medium"
        return result

    if any(phrase in lowered for phrase in ["schedule for tomorrow", "what is my schedule tomorrow", "my schedule tomorrow", "calendar tomorrow"]):
        result = dict(default_result)
        result["intent"] = "calendar_query"
        result["query_scope"] = "tomorrow"
        result["confidence"] = "medium"
        return result

    return default_result


# Classifies a free-form chat message into a small set of legacy app intents.
def classify_chat_input(text: str) -> dict:
    plan = plan_chat_action(text)
    default_result = {
        "intent": "general",
        "recipient_name": "",
        "subject_hint": "",
        "email_body_notes": "",
        "event_title": "",
        "event_date": "",
        "event_time": "",
        "event_end_time": "",
        "event_location": "",
        "item_kind": "",
        "item_summary": "",
        "confidence": plan.get("confidence", "low"),
    }

    if plan.get("intent") == "email_request":
        default_result.update({
            "intent": "email_request",
            "recipient_name": plan.get("recipient_name", ""),
            "subject_hint": plan.get("subject_hint", ""),
            "email_body_notes": plan.get("email_body_notes", text or ""),
        })
        return default_result

    items = plan.get("items", []) if isinstance(plan.get("items"), list) else []
    if plan.get("intent") == "calendar_add" and items:
        item = items[0] if isinstance(items[0], dict) else {}
        default_result.update({
            "intent": "calendar_item_paste",
            "event_title": item.get("title", ""),
            "event_date": item.get("date", ""),
            "event_time": item.get("start_time", ""),
            "event_end_time": item.get("end_time", ""),
            "event_location": item.get("location", ""),
            "item_kind": item.get("item_kind", ""),
            "item_summary": item.get("description", "") or item.get("title", ""),
        })
        return default_result

    return default_result


# Classifies multiple email threads in fewer LLM requests when possible.
def classify_email_batch(email_items, conversation_history=None, chunk_size=4):
    conversation_history = conversation_history or []
    email_items = list(email_items or [])

    if not email_items:
        return []

    results = []
    for start in range(0, len(email_items), max(1, chunk_size)):
        chunk = email_items[start:start + max(1, chunk_size)]
        default_chunk = []
        for item in chunk:
            default_chunk.append({
                "thread_id": item.get("thread_id", ""),
                "category": "business",
                "summary": "No summary available.",
                "response_required": False,
                "event_details": None,
                "response_type": "none",
                "confidence": "low",
            })

        system_prompt = (
            "You are an email classifier. Read each email thread and return ONLY a JSON array. "
            "Return one result object for each input thread in the same order. "
            "Each result object must contain: thread_id, category, summary, response_required, event_details, response_type, confidence. "
            "category must be one of personal, business, appointment, invitation. "
            "response_type must be rsvp or none. "
            "event_details must be null or an object with title, date, time, location."
        )

        chunk_payload = []
        for item in chunk:
            chunk_payload.append({
                "thread_id": item.get("thread_id", ""),
                "thread_text": item.get("thread_text", ""),
            })

        user_prompt = (
            f"Conversation history:\n{_format_history(conversation_history)}\n\n"
            "Email threads:\n"
            f"{json.dumps(chunk_payload, ensure_ascii=False, indent=2)}"
        )

        try:
            batch_result, _ = _run_routed_json_task(
                "email_classification",
                system_prompt,
                user_prompt,
                temperature=0.0,
                default_result=default_chunk,
                local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
            )

            if not isinstance(batch_result, list) or len(batch_result) != len(chunk):
                raise ValueError("Batched classification did not return the expected number of results.")

            normalized_chunk = []
            for index, item in enumerate(batch_result):
                current_default = default_chunk[index]
                if not isinstance(item, dict):
                    normalized_chunk.append(current_default)
                    continue
                normalized = dict(current_default)
                normalized.update({
                    "thread_id": item.get("thread_id", current_default["thread_id"]),
                    "category": item.get("category", current_default["category"]),
                    "summary": item.get("summary", current_default["summary"]),
                    "response_required": bool(item.get("response_required", current_default["response_required"])),
                    "event_details": item.get("event_details", current_default["event_details"]),
                    "response_type": item.get("response_type", current_default["response_type"]),
                    "confidence": item.get("confidence", current_default["confidence"]),
                })
                normalized_chunk.append(normalized)

            results.extend(normalized_chunk)
        except Exception:
            for item in chunk:
                single = classify_email(item.get("thread_text", ""), conversation_history=conversation_history)
                single["thread_id"] = item.get("thread_id", "")
                results.append(single)

    return results


# Classifies an email thread and returns structured JSON as a Python dict.
def classify_email(thread_text, conversation_history=None):
    conversation_history = conversation_history or []

    default_result = {
        "category": "business",
        "summary": "No summary available.",
        "response_required": False,
        "event_details": None,
        "response_type": "none",
        "confidence": "low",
    }

    system_prompt = (
        "You are an email classifier. "
        "Read the email thread and return ONLY a JSON object with no extra text. "
        "The JSON must have exactly these fields: "
        "category: one of 'personal', 'business', 'appointment', 'invitation'; "
        "summary: one sentence describing the email; "
        "response_required: true or false; "
        "event_details: an object with title, date, time, location, or null if no event; "
        "response_type: 'rsvp' if attending confirmation is needed, otherwise 'none'; "
        "confidence: 'high', 'medium', or 'low'."
    )

    user_prompt = (
        f"Conversation history:\n{_format_history(conversation_history)}\n\n"
        f"Email thread:\n{thread_text}"
    )

    result, _ = _run_routed_json_task(
        "email_classification",
        system_prompt,
        user_prompt,
        temperature=0.0,
        default_result=default_result,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )

    if not isinstance(result, dict):
        raise ValueError("Email classification did not return a JSON object.")

    return _merge_with_default_dict(result, default_result)


# Writes a professional email reply based on the thread and the user's notes.
def draft_response(thread_text, user_notes, conversation_history=None):
    conversation_history = conversation_history or []

    system_prompt = (
        "Write a professional email reply based on the user's notes. "
        "Return only the email body text. "
        "Do not include a subject line. "
        "Do not include extra explanation."
    )

    user_prompt = (
        f"Conversation history:\n{_format_history(conversation_history)}\n\n"
        f"Email thread:\n{thread_text}\n\n"
        f"User notes:\n{user_notes}"
    )

    return _run_routed_text_task(
        "draft_response",
        system_prompt,
        user_prompt,
        temperature=0.4,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )


# Writes a short RSVP email that either accepts or declines.
def draft_rsvp(thread_text, attending=True):
    system_prompt = (
        "Write a short, polite RSVP email. "
        "Return only the email body text. "
        "Do not include a subject line. "
        "Do not include extra explanation."
    )

    if attending:
        attendance_text = "The user is attending. Confirm attendance politely."
    else:
        attendance_text = "The user is not attending. Decline politely."

    user_prompt = (
        f"Email thread:\n{thread_text}\n\n"
        f"Instruction:\n{attendance_text}"
    )

    return _run_routed_text_task(
        "draft_rsvp",
        system_prompt,
        user_prompt,
        temperature=0.4,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )


# Revises an existing draft using the user's feedback.
def refine_draft(draft_text, user_feedback, conversation_history=None, revision_count=0):
    conversation_history = conversation_history or []

    system_prompt = (
        "You wrote this email draft. The user wants changes. "
        "Apply the user's feedback and return the revised draft as plain text only. "
        "Do not include extra explanation."
    )

    user_prompt = (
        f"Conversation history:\n{_format_history(conversation_history)}\n\n"
        f"Current draft:\n{draft_text}\n\n"
        f"User feedback:\n{user_feedback}"
    )

    return _run_routed_text_task(
        "draft_refinement",
        system_prompt,
        user_prompt,
        temperature=0.4,
        escalation_level=revision_count,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )


# Extracts calendar events from an uploaded document and returns a list of event dicts.
def extract_events_from_document(document_text, document_type="general", conversation_history=None):
    conversation_history = conversation_history or []

    system_prompt = (
        "You extract calendar events from uploaded documents. "
        "Return ONLY a JSON array with no extra text. "
        "Each item in the array must be an object with exactly these fields: "
        "title, date, start_time, end_time, location, description, reminders_minutes. "
        "Use date format YYYY-MM-DD when possible. "
        "Use time format HH:MM in 24-hour format when possible. "
        "If a time is missing, use an empty string. "
        "If a location is missing, use an empty string. "
        "If a description is missing, use an empty string. "
        "For reminders_minutes, return a list like [1440, 120]. "
        "If there are no clear events, return an empty JSON array []. "
        "If the document is a course outline, look for lectures, tutorials, labs, midterms, finals, quizzes, assignments, and deadlines. "
        "Extract multiple events if the document contains multiple dates."
    )

    user_prompt = (
        f"Conversation history:\n{_format_history(conversation_history)}\n\n"
        f"Document type: {document_type}\n\n"
        f"Document text:\n{document_text}"
    )

    result, _ = _run_routed_json_task(
        "document_events",
        system_prompt,
        user_prompt,
        temperature=0.0,
        default_result=[],
        local_max_chars=LOCAL_LLM_DOCUMENT_MAX_PROMPT_CHARS,
    )

    if isinstance(result, dict):
        result = [result]

    if isinstance(result, list):
        clean_events = []
        for item in result:
            if isinstance(item, dict):
                clean_events.append({
                    "title": item.get("title", ""),
                    "date": item.get("date", ""),
                    "start_time": item.get("start_time", ""),
                    "end_time": item.get("end_time", ""),
                    "location": item.get("location", ""),
                    "description": item.get("description", ""),
                    "reminders_minutes": item.get("reminders_minutes", [1440, 120]),
                })
        return clean_events

    return []


# Drafts a brand-new outbound email.
def draft_new_email(recipient_name, recipient_email, subject, body_notes, conversation_history=None):
    conversation_history = conversation_history or []

    default_subject = subject or (f"Message for {recipient_name}" if recipient_name else "")
    default_result = {
        "subject": default_subject,
        "body": body_notes or "",
    }

    system_prompt = (
        "You draft a new outbound email. "
        "Return ONLY a JSON object with keys subject and body. "
        "The body should be polished, professional, and ready to send. "
        "Do not include markdown. Do not include extra explanation."
    )

    user_prompt = (
        f"Conversation history:\n{_format_history(conversation_history)}\n\n"
        f"Recipient name: {recipient_name}\n"
        f"Recipient email: {recipient_email}\n"
        f"Subject hint: {subject}\n"
        f"Body notes: {body_notes}\n\n"
        "Return JSON with keys subject and body only."
    )

    result, _ = _run_routed_json_task(
        "draft_new_email",
        system_prompt,
        user_prompt,
        temperature=0.2,
        default_result=default_result,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )
    return _merge_with_default_dict(result, default_result)


# Finds the most likely contact match from recent Gmail thread metadata.
def find_contact_in_threads(name: str, threads_text: str) -> dict:
    default_result = {
        "email": "",
        "display_name": "",
    }

    if not name or not threads_text:
        return default_result

    system_prompt = (
        "You match a person's name against recent Gmail thread metadata. "
        "Return ONLY a JSON object with keys email and display_name. "
        "Choose the single best match only if the metadata strongly supports it. "
        "If there is no reliable match, return empty strings."
    )

    user_prompt = (
        f"Target name: {name}\n\n"
        "Recent thread metadata:\n"
        f"{threads_text}\n\n"
        "Return JSON with keys email and display_name only."
    )

    result, _ = _run_routed_json_task(
        "contact_match",
        system_prompt,
        user_prompt,
        temperature=0.0,
        default_result=default_result,
        local_max_chars=LOCAL_LLM_MAX_PROMPT_CHARS,
    )
    return _merge_with_default_dict(result, default_result)
