from __future__ import annotations
import random, re, subprocess
import uuid

from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)


# ── Single shared generator ───────────────────────────────────────────────────
_generator = DefaultFingerprintGenerator(
    header_options=HeaderGeneratorOptions(
        browsers=["chrome"],           # Chrome-only — Edge/Firefox have different CDP tells
        devices=["desktop"],
        locales=["en-US", "en-GB", "de-DE", "fr-FR", "es-ES"],
    ),
    screen_options=ScreenOptions(
        min_width=1280,
        max_width=2560,
        min_height=720,
        max_height=1440,
    ),
)

# ── Realistic plugin definitions (matches real Chrome exactly) ────────────────
_CHROME_PLUGINS = [
    {
        "name": "PDF Viewer",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mimeTypes": [
            {"type": "application/pdf", "suffixes": "pdf"},
            {"type": "text/pdf",        "suffixes": "pdf"},
        ],
    },
    {
        "name": "Chrome PDF Viewer",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mimeTypes": [
            {"type": "application/pdf", "suffixes": "pdf"},
            {"type": "text/pdf",        "suffixes": "pdf"},
        ],
    },
    {
        "name": "Chromium PDF Viewer",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mimeTypes": [
            {"type": "application/pdf", "suffixes": "pdf"},
            {"type": "text/pdf",        "suffixes": "pdf"},
        ],
    },
    {
        "name": "Microsoft Edge PDF Viewer",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mimeTypes": [
            {"type": "application/pdf", "suffixes": "pdf"},
            {"type": "text/pdf",        "suffixes": "pdf"},
        ],
    },
    {
        "name": "WebKit built-in PDF",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mimeTypes": [
            {"type": "application/pdf", "suffixes": "pdf"},
            {"type": "text/pdf",        "suffixes": "pdf"},
        ],
    },
]

def _make_windows_ua(major: str, full: str) -> str:
    """
    Build a Windows 10/11 Chrome UA that is consistent with Client Hints.
    Always Win32 platform — never Linux, never Mac.
    """
    return (
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{full} Safari/537.36"
    )


# ── Client Hints brand set ────────────────────────────────────────────────────
def _build_client_hints(major: str, full: str) -> dict:   # add full param
    return {
        "brands": [
            {"brand": "Google Chrome",  "version": major},
            {"brand": "Chromium",       "version": major},
            {"brand": "Not/A)Brand",    "version": "99"},
        ],
        "mobile":    False,
        "platform":  "Windows",
        # High-entropy values detectors actually request:
        "uaFullVersion":    full,
        "fullVersionList": [
            {"brand": "Google Chrome",  "version": full},
            {"brand": "Chromium",       "version": full},
            {"brand": "Not/A)Brand",    "version": "99.0.0.0"},
        ],
        "platformVersion": "10.0.0",
        "architecture":    "x86",
        "bitness":         "64",
        "model":           "",
        "wow64":           False,
    }

# ── Accept-Language ───────────────────────────────────────────────────────────
def _build_accept_language(languages: list[str]) -> str:
    parts = []
    for i, lang in enumerate(languages):
        if i == 0:
            parts.append(lang)
        else:
            q = round(1.0 - i * 0.1, 1)
            q = max(q, 0.1)
            parts.append(f"{lang};q={q}")
    return ",".join(parts)


# ── Viewport from screen ──────────────────────────────────────────────────────
def _viewport_from_screen(width: int, height: int) -> dict:
    taskbar = random.randint(40, 48)
    toolbar = random.randint(88, 104)
    outer_h = height - taskbar
    inner_h = outer_h - toolbar
    return {
        "outerWidth":  width,
        "outerHeight": outer_h,
        "innerWidth":  width,
        "innerHeight": max(inner_h, 400),
        "availWidth":  width,
        "availHeight": outer_h,
    }



def get_real_chrome_version() -> tuple[str, str]:
    cmds = [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["/usr/bin/google-chrome", "--version"],
    ]
    for cmd in cmds:
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=5).stdout
            # FIX: capture full 4-part version like 146.0.7680.164
            m = re.search(r"(\d+)\.(\d+\.\d+\.\d+)", out)
            if m:
                major = m.group(1)
                full  = m.group(1) + "." + m.group(2)
                print(f"[INFO] Real Chrome version: {full}")
                return major, full
        except Exception:
            continue
    return "146", "146.0.7680.164"  # update fallback to match your installed version


# ── Fingerprint generator ─────────────────────────────────────────────────────
def generate() -> dict:
    """
    Generate a fully consistent Windows Chrome fingerprint.
    Platform is always Win32 regardless of what the crawlee generator returns —
    this is the #1 OS-mismatch fix.
    """
    fp = _generator.generate()

    # Always Windows — override whatever crawlee generated
    major, full = get_real_chrome_version()
    ua       = _make_windows_ua(major, full)
    platform = "Win32"                          # FIX: was leaking Linux/Mac
    language  = fp.navigator.language or "en-US"
    languages = list(fp.navigator.languages or [language])

    if languages[0] != language:
        languages.insert(0, language)
    languages = languages[:4]

    sw  = fp.screen.width  or 1920
    sh  = fp.screen.height or 1080
    dpr = fp.screen.devicePixelRatio or 1.0

    viewport = _viewport_from_screen(sw, sh)

    raw_headers = dict(fp.headers) if fp.headers else {}
    raw_headers["Accept-Language"] = _build_accept_language(languages)
    raw_headers["User-Agent"]      = ua
    # Sec-CH-UA headers must match UA exactly
    raw_headers["Sec-CH-UA"]                  = (
        f'"Google Chrome";v="{major}", "Chromium";v="{major}", "Not/A)Brand";v="99"'
    )
    raw_headers["Sec-CH-UA-Mobile"]           = "?0"
    raw_headers["Sec-CH-UA-Platform"]         = '"Windows"'
    raw_headers["Sec-CH-UA-Platform-Version"] = '"10.0.0"'
    raw_headers["Sec-CH-UA-Arch"]             = '"x86"'
    raw_headers["Sec-CH-UA-Bitness"]          = '"64"'

    for bad in ("x-forwarded-for", "x-real-ip", "via", "forwarded"):
        raw_headers.pop(bad, None)

    return {
        "fingerprint_id": uuid.uuid4().hex,
        "user_agent":           ua,
        "platform":             platform,
        "language":             language,
        "languages":            languages,
        "hardware_concurrency": fp.navigator.hardwareConcurrency or random.choice([4, 8, 12, 16]),
        "device_memory":        fp.navigator.deviceMemory or random.choice([8, 16]),
        "max_touch_points":     0,              # desktop Windows = 0
        "vendor":               "Google Inc.",
        "product_sub":          "20030107",
        "user_agent_data":      _build_client_hints(major, full),
        "chrome_major":         major,
        "screen_width":         sw,
        "screen_height":        sh,
        "avail_width":          viewport["availWidth"],
        "avail_height":         viewport["availHeight"],
        "inner_width":          viewport["innerWidth"],
        "inner_height":         viewport["innerHeight"],
        "outer_width":          viewport["outerWidth"],
        "outer_height":         viewport["outerHeight"],
        "device_pixel_ratio":   dpr,
        "color_depth":          fp.screen.colorDepth or 24,
        "webgl_vendor":         fp.videoCard.vendor   if fp.videoCard else "Intel Inc.",
        "webgl_renderer":       fp.videoCard.renderer if fp.videoCard else "Intel Iris OpenGL Engine",
        "plugins":              _CHROME_PLUGINS,
        "headers":              raw_headers,
    }
