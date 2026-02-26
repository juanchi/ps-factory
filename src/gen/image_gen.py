import os
import base64
import requests
from typing import Tuple


class ImageGenError(RuntimeError):
    pass


def _jpeg_size(data: bytes) -> Tuple[int, int]:
    i = 2  # skip SOI
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        i += 2
        # standalone markers
        if marker in (0xD8, 0xD9):
            continue
        if i + 2 > len(data):
            break
        seg_len = int.from_bytes(data[i:i+2], "big")
        if seg_len < 2 or i + seg_len > len(data):
            break
        # SOF markers that contain size
        if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
            if i + 7 > len(data):
                break
            h = int.from_bytes(data[i+3:i+5], "big")
            w = int.from_bytes(data[i+5:i+7], "big")
            return w, h
        i += seg_len
    raise ImageGenError("Could not parse JPEG size")


def _image_size(data: bytes, mime: str) -> Tuple[int, int]:
    if mime == "image/png":
        if len(data) < 24 or data[:8] != b"\x89PNG\r\n\x1a\n":
            raise ImageGenError("Invalid PNG image")
        w = int.from_bytes(data[16:20], "big")
        h = int.from_bytes(data[20:24], "big")
        return w, h
    if mime in ("image/jpeg", "image/jpg"):
        return _jpeg_size(data)
    raise ImageGenError(f"Unsupported image mime: {mime}")


def validate_4_5(data: bytes, mime: str, tol: float = 0.01) -> Tuple[bool, int, int]:
    w, h = _image_size(data, mime)
    ratio = (w / h) if h else 0
    ok = abs(ratio - (4.0 / 5.0)) <= tol
    return ok, w, h


def _extract_inline_image(resp_json: dict) -> Tuple[bytes, str]:
    candidates = resp_json.get("candidates") or []
    for c in candidates:
        parts = (((c or {}).get("content") or {}).get("parts")) or []
        for p in parts:
            inline = p.get("inlineData") or p.get("inline_data")
            if inline and inline.get("data"):
                mime = inline.get("mimeType") or inline.get("mime_type") or "image/png"
                return base64.b64decode(inline["data"]), mime
    raise ImageGenError("Gemini response did not include inline image data")


def build_image_prompt_en(visual_prompt: str) -> str:
    return (
        "Create a high-quality social image. "
        "Aspect ratio must be 4:5 and target output resolution is 1080x1350 pixels. "
        "All visible text rendered inside the image must be in Spanish. "
        "Add one short, punchy headline in Spanish (max 6 words), high-contrast, easy to read on mobile. "
        "Place headline near the top with strong visual hierarchy. "
        "Do not include the phrase 'Bitcoin Anchor' as literal on-image text. "
        "Do not include watermarks or logos unless explicitly requested. "
        f"Creative direction: {visual_prompt}"
    )


def _normalize_model_name(model_name: str) -> str:
    m = (model_name or "").strip()
    return m.split("/", 1)[1] if m.startswith("models/") else m


def generate_image_gemini(*, visual_prompt: str, timeout_s: int = 90) -> Tuple[bytes, str, str]:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise ImageGenError("GEMINI_API_KEY missing")

    raw_model = os.getenv("GEMINI_IMAGE_MODEL", "gemini-3-pro-image-preview")
    model = _normalize_model_name(raw_model)
    if not model:
        raise ImageGenError("GEMINI_IMAGE_MODEL missing/invalid")

    use_aspect_ratio = os.getenv("GEMINI_USE_ASPECT_RATIO", "0").strip().lower() in {"1", "true", "yes", "on"}

    final_prompt = build_image_prompt_en(visual_prompt) + " Output must not be square."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    generation_config = {
        "responseModalities": ["TEXT", "IMAGE"],
    }
    if use_aspect_ratio:
        generation_config["imageConfig"] = {"aspectRatio": "4:5"}

    payload = {
        "contents": [{"parts": [{"text": final_prompt}]}],
        "generationConfig": generation_config,
    }

    r = requests.post(url, json=payload, timeout=timeout_s)
    if r.status_code >= 400:
        raise ImageGenError(f"Gemini error {r.status_code}: {r.text[:400]}")

    data = r.json()
    img_bytes, mime = _extract_inline_image(data)
    return img_bytes, mime, final_prompt


def generate_image(*, visual_prompt: str, timeout_s: int = 90) -> Tuple[bytes, str, str]:
    provider = os.getenv("IMAGE_PROVIDER", "gemini").strip().lower()
    if provider == "gemini":
        return generate_image_gemini(visual_prompt=visual_prompt, timeout_s=timeout_s)
    raise ImageGenError(f"Unsupported IMAGE_PROVIDER: {provider}")
