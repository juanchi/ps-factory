import os
import re
import base64
import logging
import requests
from typing import Tuple


logger = logging.getLogger(__name__)


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


def _sanitize_visual_prompt(visual_prompt: str) -> str:
    t = str(visual_prompt or "")
    # Prevent explicit on-image text instructions with forbidden label
    t = re.sub(r"(?i)bitcoin\s*anchor", "concepto de ancla monetaria", t)
    t = re.sub(r"(?i)texto\s+visible\s*:\s*[^.\n]+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def build_image_prompt_en(visual_prompt: str) -> str:
    vp = _sanitize_visual_prompt(visual_prompt)
    return (
        "Create a high-quality social image. "
        "Aspect ratio must be 4:5 and target output resolution is 1080x1350 pixels. "
        "All visible text rendered inside the image must be in Spanish. "
        "Add one short, punchy headline in Spanish (max 6 words), high-contrast, easy to read on mobile. "
        "Place headline near the top with strong visual hierarchy. "
        "NEVER render these literal strings inside the image: 'Bitcoin Anchor', 'Anchor Bitcoin'. "
        "Never include labels such as 'Bitcoin Anchor:' in subtitles or footers. "
        "Do not include watermarks or logos unless explicitly requested. "
        f"Creative direction: {vp}"
    )


def _normalize_model_name(model_name: str) -> str:
    m = (model_name or "").strip()
    return m.split("/", 1)[1] if m.startswith("models/") else m


def apply_carousel_index_badge(data: bytes, mime: str, idx: int, total: int) -> Tuple[bytes, str]:
    """Draw a visible slide order badge (e.g. 1/6) on top-right."""
    try:
        from io import BytesIO
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        logger.warning("Carousel badge skipped (Pillow missing): %s", e)
        return data, mime

    label = f"{max(1, int(idx))}/{max(1, int(total))}"
    with Image.open(BytesIO(data)).convert("RGBA") as im:
        w, h = im.size
        overlay = Image.new("RGBA", im.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        fs = max(22, int(min(w, h) * 0.038))
        try:
            font = ImageFont.truetype("DejaVuSans-Bold.ttf", fs)
        except Exception:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), label, font=font)
        tw, th = max(1, bbox[2]-bbox[0]), max(1, bbox[3]-bbox[1])
        margin = max(14, int(min(w, h) * 0.02))
        pad = max(10, int(fs * 0.35))

        x1 = margin
        y2 = h - margin
        x2 = x1 + tw + (pad * 2)
        y1 = y2 - th - (pad * 2)

        draw.rounded_rectangle((x1, y1, x2, y2), radius=max(10, int(fs * 0.35)), fill=(0, 0, 0, 170))
        draw.text((x1 + pad, y1 + pad), label, font=font, fill=(255, 255, 255, 235))

        out = Image.alpha_composite(im, overlay)
        buf = BytesIO()
        if mime == "image/png":
            out.save(buf, format="PNG")
            return buf.getvalue(), "image/png"
        out.convert("RGB").save(buf, format="JPEG", quality=92)
        return buf.getvalue(), "image/jpeg"


def _apply_watermark_if_enabled(data: bytes, mime: str) -> Tuple[bytes, str]:
    enabled = os.getenv("IMAGE_WATERMARK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return data, mime

    text = os.getenv("IMAGE_WATERMARK_TEXT", "Panamá Soberano").strip() or "Panamá Soberano"
    position = os.getenv("IMAGE_WATERMARK_POSITION", "center").strip().lower()
    opacity_raw = os.getenv("IMAGE_WATERMARK_OPACITY", "0.55").strip()
    try:
        opacity = max(0.05, min(1.0, float(opacity_raw)))
    except Exception:
        opacity = 0.35

    try:
        from io import BytesIO
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        logger.warning("Watermark enabled but Pillow is not available; skipping watermark. error=%s", e)
        return data, mime

    with Image.open(BytesIO(data)).convert("RGBA") as im:
        w, h = im.size
        overlay = Image.new("RGBA", im.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        scale_raw = os.getenv("IMAGE_WATERMARK_SCALE", "0.65").strip()
        try:
            wm_scale = max(0.3, min(1.5, float(scale_raw)))
        except Exception:
            wm_scale = 0.65
        base_size = max(14, int(min(w, h) * 0.045 * wm_scale))
        try:
            font = ImageFont.truetype("DejaVuSans-Bold.ttf", base_size)
        except Exception:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), text, font=font)
        tw = max(1, bbox[2] - bbox[0])
        th = max(1, bbox[3] - bbox[1])
        margin_raw = os.getenv("IMAGE_WATERMARK_MARGIN", "6").strip()
        try:
            margin = max(2, int(margin_raw))
        except Exception:
            margin = 6

        if position == "bottom_left":
            x, y = margin, h - th - margin
        elif position == "top_right":
            x, y = w - tw - margin, margin
        elif position == "top_left":
            x, y = margin, margin
        elif position == "center":
            x, y = (w - tw) // 2, (h - th) // 2
        else:
            x, y = w - tw - margin, h - th - margin

        logger.info("Applying watermark text='%s' position='%s' opacity=%.2f", text, position, opacity)

        # add subtle background plate to improve readability
        pad = max(8, int(base_size * 0.35))
        plate = (x - pad, y - pad, x + tw + pad, y + th + pad)
        draw.rounded_rectangle(plate, radius=max(8, int(base_size * 0.35)), fill=(0, 0, 0, int(255 * opacity * 0.35)))

        # soft shadow + text
        draw.text((x + 2, y + 2), text, font=font, fill=(0, 0, 0, int(255 * 0.75)))
        draw.text((x, y), text, font=font, fill=(255, 255, 255, int(255 * opacity)))

        out = Image.alpha_composite(im, overlay)
        buf = BytesIO()
        if mime == "image/png":
            out.save(buf, format="PNG")
            return buf.getvalue(), "image/png"

        out.convert("RGB").save(buf, format="JPEG", quality=92)
        return buf.getvalue(), "image/jpeg"



def detect_forbidden_text_in_image(data: bytes, mime: str, forbidden_tokens: list[str]) -> tuple[bool, str]:
    """OCR-check generated image for forbidden labels/tokens. Returns (hit, token)."""
    toks = [str(t or "").strip().lower() for t in (forbidden_tokens or []) if str(t or "").strip()]
    if not toks:
        return False, ""
    try:
        from io import BytesIO
        from PIL import Image
        import pytesseract
    except Exception as e:
        logger.warning("OCR check skipped (pytesseract/Pillow unavailable): %s", e)
        return False, ""

    try:
        with Image.open(BytesIO(data)) as im:
            text = pytesseract.image_to_string(im)
    except Exception as e:
        logger.warning("OCR check failed: %s", e)
        return False, ""

    low = (text or "").lower()
    for t in toks:
        if t and t in low:
            return True, t
    return False, ""



def detect_blank_or_letterbox_bands(data: bytes, mime: str) -> tuple[bool, str]:
    """Detect top/bottom banners or blank/letterbox-like zones that break full-bleed look."""
    try:
        from io import BytesIO
        from PIL import Image, ImageStat
    except Exception as e:
        logger.warning("Band check skipped (Pillow unavailable): %s", e)
        return False, ""

    try:
        with Image.open(BytesIO(data)).convert("RGB") as im:
            w, h = im.size
            band_h = max(24, int(h * 0.18))
            top = im.crop((0, 0, w, band_h))
            bottom = im.crop((0, h - band_h, w, h))

            def _stats(img):
                st = ImageStat.Stat(img)
                mean = sum(st.mean) / 3.0
                std = sum(st.stddev) / 3.0
                return mean, std

            t_mean, t_std = _stats(top)
            b_mean, b_std = _stats(bottom)

            # Heuristics:
            # - very low stddev => flat bar/panel
            # - very dark + low stddev => letterbox-style black band
            if t_std < 18:
                return True, f"top_flat:{t_mean:.1f}/{t_std:.1f}"
            if b_std < 18:
                return True, f"bottom_flat:{b_mean:.1f}/{b_std:.1f}"
            if t_mean < 35 and t_std < 24:
                return True, f"top_letterbox:{t_mean:.1f}/{t_std:.1f}"
            if b_mean < 35 and b_std < 24:
                return True, f"bottom_letterbox:{b_mean:.1f}/{b_std:.1f}"
    except Exception as e:
        logger.warning("Band check failed: %s", e)
        return False, ""

    return False, ""

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
    img_bytes, mime = _apply_watermark_if_enabled(img_bytes, mime)
    return img_bytes, mime, final_prompt


def generate_image(*, visual_prompt: str, timeout_s: int = 90) -> Tuple[bytes, str, str]:
    provider = os.getenv("IMAGE_PROVIDER", "gemini").strip().lower()
    if provider == "gemini":
        return generate_image_gemini(visual_prompt=visual_prompt, timeout_s=timeout_s)
    raise ImageGenError(f"Unsupported IMAGE_PROVIDER: {provider}")
