# scraper/helper.py (updated with NBRs ID tracking)
import requests
import json
import random
import os
from config import setting
from pathlib import Path
from scraper.logger import get_logger
from patchright.async_api import Page, Browser
from .fg_generator import generate
from datetime import datetime
import csv
import asyncio

log = get_logger()

def load_existing_nbrs_ids() -> set:
    """Load all existing NBRs IDs from companies.csv to avoid duplicates"""
    nbrs_ids = set()
    csv_file = Path(setting.AHU_COMPANIES_CSV)
    
    if not csv_file.exists():
        log.info("[HP] No existing companies CSV found. Starting fresh.")
        return nbrs_ids
    
    try:
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("nbrs_id"):
                    nbrs_ids.add(row["nbrs_id"])
    except Exception as e:
        log.error(f"[HP] Error loading existing NBRs IDs: {e}")
    
    return nbrs_ids

def load_proxies(file_path: str):
    proxies = []
    
    # Load current start number from state file
    state_file = Path(setting.STATE_FILE)
    if state_file.exists():
        with open(state_file, "r") as f:
            proxies_start_number = json.load(f).get("proxies_start_number", 0)
    else:
        proxies_start_number = 0

    with open(file_path, "r") as f:
        all_proxies = f.readlines()
        if not all_proxies:
            log.error("[HP] No proxies found in file")
            return []
            
        for i in range(setting.CONCURRENCY):
            index = (proxies_start_number + i) % len(all_proxies)
            parts = all_proxies[index].strip().split(":")
            if len(parts) == 4:
                proxies.append(parts)
        new_start = (proxies_start_number + setting.CONCURRENCY) % len(all_proxies)

    # Save updated start number
    with open(state_file, "w") as f:
        json.dump({"proxies_start_number": new_start}, f)

    log.info(f"[HP] Total {len(proxies)} proxies loaded.")
    return proxies
         
def get_timezone_from_ip(ip: str | None = None) -> str:
    try:
        url = f"http://ip-api.com/json/{ip}" if ip else "http://ip-api.com/json"
        data = requests.get(url, timeout=5).json()
        if data.get("status") == "success":
            tz = data.get("timezone", "UTC")
            log.info(f"[HP] Timezone: {tz}")
            return tz
    except Exception as e:
        log.warning(f"[HP] Could not fetch timezone: {e}")
    return "UTC"

def get_proxy_public_ip(ip: str, port: str, user: str, pwd: str) -> str:
    try:
        r = requests.get(
            "https://api.ipify.org",
            proxies={"https": f"http://{user}:{pwd}@{ip}:{port}"},
            timeout=8,
        )
        addr = r.text.strip()
        log.info(f"[HP] Proxy public IP: {addr}")
        return addr
    except Exception as e:
        log.warning(f"[HP] Could not get proxy public IP: {e}")
    return ip

# ── JS injection script ───────────────────────────────────────────────────────
def build_js_script(fingerprint: dict) -> str:
    """
    Spoof every fingerprint surface reachable from JS.
    """
    langs = json.dumps(fingerprint["languages"])
    plugins_json = json.dumps(fingerprint["plugins"])

    # Micro-noise values baked in at generation time — unique per session
    canvas_r = random.randint(-2, 2)
    canvas_g = random.randint(-2, 2)
    canvas_b = random.randint(-2, 2)
    audio_noise = random.uniform(-0.00003, 0.00003)

    return f"""
(function () {{
  'use strict';

  // ── Safe property override ────────────────────────────────────────────────
  const _ov = (obj, prop, val) => {{
    try {{
      Object.defineProperty(obj, prop, {{
        get: () => val,
        configurable: true,
        enumerable: true,
      }});
    }} catch (_) {{}}
  }};
  

  // ── navigator — core ──────────────────────────────────────────────────────
  _ov(navigator, 'platform',            '{fingerprint["platform"]}');
  _ov(navigator, 'hardwareConcurrency', {fingerprint["hardware_concurrency"]});
  _ov(navigator, 'deviceMemory',        {fingerprint["device_memory"]});
  _ov(navigator, 'maxTouchPoints',      {fingerprint["max_touch_points"]});
  _ov(navigator, 'vendor',              '{fingerprint["vendor"]}');
  _ov(navigator, 'productSub',          '{fingerprint["product_sub"]}');


  // ── navigator.languages ───────────────────────────────────────────────────
  _ov(navigator, 'language',  {json.dumps(fingerprint["language"])});
  _ov(navigator, 'languages', {langs});

  // ── navigator.plugins — full PluginArray mock ─────────────────────────────
  const _pluginDefs = {plugins_json};
  const _makeMime = (m, plugin) => {{
    const mime = Object.create(MimeType.prototype);
    _ov(mime, 'type',          m.type);
    _ov(mime, 'suffixes',      m.suffixes);
    _ov(mime, 'description',   m.description || '');
    _ov(mime, 'enabledPlugin', plugin);
    return mime;
  }};
  const _makePlugin = (def) => {{
    const plugin = Object.create(Plugin.prototype);
    _ov(plugin, 'name',        def.name);
    _ov(plugin, 'filename',    def.filename);
    _ov(plugin, 'description', def.description);
    const mimes = def.mimeTypes.map(m => _makeMime(m, plugin));
    mimes.forEach((m, i) => {{ plugin[i] = m; }});
    _ov(plugin, 'length', mimes.length);
    plugin[Symbol.iterator] = function* () {{ yield* mimes; }};
    return plugin;
  }};
  const _plugins = _pluginDefs.map(_makePlugin);
  const _pluginArray = Object.create(PluginArray.prototype);
  _plugins.forEach((p, i) => {{ _pluginArray[i] = p; }});
  _ov(_pluginArray, 'length', _plugins.length);
  _pluginArray[Symbol.iterator] = function* () {{ yield* _plugins; }};
  _pluginArray.item      = (i)    => _plugins[i] || null;
  _pluginArray.namedItem = (name) => _plugins.find(p => p.name === name) || null;
  _pluginArray.refresh   = () => {{}};
  _ov(navigator, 'plugins',   _pluginArray);
  _ov(navigator, 'mimeTypes', new MimeTypeArray());

  // ── screen ────────────────────────────────────────────────────────────────
  _ov(screen, 'width',       {fingerprint["screen_width"]});
  _ov(screen, 'height',      {fingerprint["screen_height"]});
  _ov(screen, 'availWidth',  {fingerprint["avail_width"]});
  _ov(screen, 'availHeight', {fingerprint["avail_height"]});
  _ov(screen, 'colorDepth',  {fingerprint["color_depth"]});
  _ov(screen, 'pixelDepth',  {fingerprint["color_depth"]});
  _ov(window, 'devicePixelRatio', {fingerprint["device_pixel_ratio"]});

  // ── viewport (non-zero — headless tell) ───────────────────────────────────
  _ov(window, 'innerWidth',  {fingerprint["inner_width"]});
  _ov(window, 'innerHeight', {fingerprint["inner_height"]});
  _ov(window, 'outerWidth',  {fingerprint["outer_width"]});
  _ov(window, 'outerHeight', {fingerprint["outer_height"]});

  // ── WebGL ─────────────────────────────────────────────────────────────────
  const _patchWebGL = (ctx) => {{
    if (!ctx) return;
    const _gp = ctx.prototype.getParameter;
    ctx.prototype.getParameter = function (p) {{
      if (p === 37445) return '{fingerprint["webgl_vendor"]}';
      if (p === 37446) return '{fingerprint["webgl_renderer"]}';
      return _gp.call(this, p);
    }};
  }};
  _patchWebGL(WebGLRenderingContext);
  if (typeof WebGL2RenderingContext !== 'undefined') _patchWebGL(WebGL2RenderingContext);

  // ── Canvas noise — overdraw method ────────────────────────────────────────
  const _addCanvasNoise = (ctx2d) => {{
    if (!ctx2d || ctx2d.__noised) return;
    ctx2d.__noised = true;
    const prev = ctx2d.globalAlpha;
    const prevOp = ctx2d.globalCompositeOperation;
    ctx2d.globalAlpha = 0.004;
    ctx2d.globalCompositeOperation = 'source-over';
    ctx2d.fillStyle = `rgb(${{128 + {canvas_r}}},${{128 + {canvas_g}}},${{128 + {canvas_b}}})`;
    ctx2d.fillRect(0, 0, 1, 1);
    ctx2d.globalAlpha = prev;
    ctx2d.globalCompositeOperation = prevOp;
    ctx2d.__noised = false;
  }};

  const _toDataURL = HTMLCanvasElement.prototype.toDataURL;
  HTMLCanvasElement.prototype.toDataURL = function (...args) {{
    if (this.width > 0 && this.height > 0) _addCanvasNoise(this.getContext('2d'));
    return _toDataURL.apply(this, args);
  }};

  const _toBlob = HTMLCanvasElement.prototype.toBlob;
  HTMLCanvasElement.prototype.toBlob = function (cb, ...args) {{
    if (this.width > 0 && this.height > 0) _addCanvasNoise(this.getContext('2d'));
    return _toBlob.call(this, cb, ...args);
  }};

  // OffscreenCanvas support
  if (typeof OffscreenCanvas !== 'undefined') {{
    const _ocBlob = OffscreenCanvas.prototype.convertToBlob;
    OffscreenCanvas.prototype.convertToBlob = function (...args) {{
      const ctx = this.getContext('2d');
      if (ctx && this.width > 0) {{
        const prev = ctx.globalAlpha;
        ctx.globalAlpha = 0.003;
        ctx.fillStyle = `rgb(${{128 + {canvas_r}}},${{128 + {canvas_g}}},${{128 + {canvas_b}}})`;
        ctx.fillRect(0, 0, 1, 1);
        ctx.globalAlpha = prev;
      }}
      return _ocBlob.apply(this, args);
    }};
  }}

  // ── Audio fingerprint noise ────────────────────────────────────────────────
  const _audioNoise = {audio_noise};

  const _getChannelData = AudioBuffer.prototype.getChannelData;
  AudioBuffer.prototype.getChannelData = function (...args) {{
    const data = _getChannelData.apply(this, args);
    if (data.length > 0) data[0] = Math.max(-1, Math.min(1, data[0] + _audioNoise));
    return data;
  }};

  const _copyFromChannel = AudioBuffer.prototype.copyFromChannel;
  AudioBuffer.prototype.copyFromChannel = function (dest, channelNum, ...rest) {{
    _copyFromChannel.call(this, dest, channelNum, ...rest);
    if (dest && dest.length > 0) dest[0] = Math.max(-1, Math.min(1, dest[0] + _audioNoise));
  }};

  if (typeof AudioContext !== 'undefined') {{
    const _createAnalyser = AudioContext.prototype.createAnalyser;
    AudioContext.prototype.createAnalyser = function () {{
      const node = _createAnalyser.apply(this, arguments);
      const _gffd = node.getFloatFrequencyData.bind(node);
      node.getFloatFrequencyData = function (arr) {{
        _gffd(arr);
        if (arr && arr.length > 0) arr[0] += _audioNoise * 1000;
      }};
      return node;
    }};
  }}

  // ── WebRTC — strip STUN/TURN to prevent IP leaks ─────────────────────────
  const _RTCPeer = window.RTCPeerConnection || window.webkitRTCPeerConnection;
  if (_RTCPeer) {{
    const _Orig = _RTCPeer;
    function _SafeRTC(config, ...rest) {{
      if (config && config.iceServers) config.iceServers = [];
      return new _Orig(config, ...rest);
    }}
    _SafeRTC.prototype = _Orig.prototype;
    Object.defineProperty(_SafeRTC, 'name', {{ value: 'RTCPeerConnection' }});
    window.RTCPeerConnection       = _SafeRTC;
    window.webkitRTCPeerConnection = _SafeRTC;
  }}

  // ── Permissions API — avoid "denied" tell ─────────────────────────────────
  if (navigator.permissions && navigator.permissions.query) {{
    const _origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (params) => {{
      const granted = ['geolocation', 'notifications', 'camera', 'microphone', 'clipboard-read', 'clipboard-write'];
      if (params && granted.includes(params.name)) {{
        return Promise.resolve({{ state: 'prompt', onchange: null }});
      }}
      return _origQuery(params);
    }};
  }}

  // ── window.chrome — complete mock ─────────────────────────────────────────
  window.chrome = {{
    app: {{
      InstallState: {{ DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' }},
      RunningState: {{ CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }},
      isInstalled: false,
      getDetails:      () => null,
      getIsInstalled:  () => false,
      runningState:    () => 'cannot_run',
    }},
    runtime: {{
      OnInstalledReason: {{
        CHROME_UPDATE: 'chrome_update', INSTALL: 'install',
        SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update',
      }},
      OnRestartRequiredReason: {{
        APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic',
      }},
      PlatformArch: {{
        ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64',
        X86_32: 'x86-32', X86_64: 'x86-64',
      }},
      PlatformNaclArch: {{
        ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64',
      }},
      PlatformOs: {{
        ANDROID: 'android', CROS: 'cros', LINUX: 'linux',
        MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win',
      }},
      RequestUpdateCheckStatus: {{
        NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available',
      }},
      id:          undefined,
      connect:     () => {{}},
      sendMessage: () => {{}},
    }},
    csi:       () => {{}},
    loadTimes: () => ({{
      requestTime:             performance.timeOrigin / 1000,
      startLoadTime:           performance.timeOrigin / 1000,
      commitLoadTime:          performance.timeOrigin / 1000 + 0.1,
      finishDocumentLoadTime:  performance.timeOrigin / 1000 + 0.3,
      finishLoadTime:          performance.timeOrigin / 1000 + 0.5,
      firstPaintTime:          performance.timeOrigin / 1000 + 0.2,
      firstPaintAfterLoadTime: 0,
      navigationType:          'Other',
      wasFetchedViaSpdy:       true,
      wasNpnNegotiated:        true,
      npnNegotiatedProtocol:   'h2',
      wasAlternateProtocolAvailable: false,
      connectionInfo:          'h2',
    }}),
  }};

  // ── Automation artefact cleanup ───────────────────────────────────────────
  const _badKeys = Object.keys(window).filter(k =>
    k.startsWith('cdc_')           ||
    k.startsWith('__webdriver')    ||
    k.startsWith('__driver')       ||
    k.startsWith('__selenium')     ||
    k.startsWith('__nightmare')    ||
    k.startsWith('__puppeteer')    ||
    k === '_Selenium_IDE_Recorder' ||
    k === '__lastWatirAlert'       ||
    k === '__lastWatirConfirm'     ||
    k === '__lastWatirPrompt'      ||
    k === 'domAutomation'          ||
    k === 'domAutomationController'
  );
  _badKeys.forEach(k => {{ try {{ delete window[k]; }} catch (_) {{}} }});

  // ── Error stack trace — hide patchright internals ─────────────────────────
  const _origPrepare = Error.prepareStackTrace;
  if (_origPrepare) {{
    Error.prepareStackTrace = (err, stack) => {{
      const filtered = stack.filter(f => {{
        const src = f.getFileName() || '';
        return !src.includes('patchright') && !src.includes('playwright');
      }});
      return _origPrepare(err, filtered);
    }};
  }}

}})();
"""

# ── WebRTC IP spoof script ──────────────────────────────────────────────────
def webrtc_ip_spoof_script(proxy_public_ip: str) -> str:
    return f"""
(() => {{
  const FAKE_IP = "{proxy_public_ip}";
  const OrigRTC = window.RTCPeerConnection;
  if (!OrigRTC) return;

  window.RTCPeerConnection = function (config, constraints) {{
    if (config && config.iceServers) config.iceServers = [];
    const pc = new OrigRTC(config, constraints);
    const _add = pc.addEventListener.bind(pc);
    pc.addEventListener = function (type, handler, ...rest) {{
      if (type !== 'icecandidate') return _add(type, handler, ...rest);
      _add(type, (event) => {{
        if (!event.candidate || !event.candidate.candidate) {{
          handler && handler(event);
          return;
        }}
        const spoofed = event.candidate.candidate.replace(
          /\\b(?:\\d{{1,3}}\\.?){{4}}\\b/g, FAKE_IP
        );
        const fakeCandidate = Object.create(event.candidate);
        Object.defineProperty(fakeCandidate, 'candidate', {{ get: () => spoofed }});
        const fakeEvent = Object.create(event);
        Object.defineProperty(fakeEvent, 'candidate', {{ get: () => fakeCandidate }});
        handler && handler(fakeEvent);
      }}, ...rest);
    }};
    return pc;
  }};
  Object.assign(window.RTCPeerConnection, OrigRTC);
  window.RTCPeerConnection.prototype = OrigRTC.prototype;
  Object.defineProperty(window.RTCPeerConnection, 'name', {{ value: 'RTCPeerConnection' }});
}})();
"""

# Create browser context
async def create_context(browser: Browser, proxy: list[str]):
    # Build browser context
    fingerprint = generate()
    script = build_js_script(fingerprint)

    ip, port, user, pwd = proxy
    timezone = get_timezone_from_ip(ip)
    proxy_public_ip = get_proxy_public_ip(ip, port, user, pwd)

    context_options = {
        "no_viewport": True,
        "user_agent": fingerprint["user_agent"],
        "timezone_id": timezone,
        "proxy": {
            "server": f"http://{ip}:{port}",
            "username": user,
            "password": pwd,
        },
        "extra_http_headers": {
            "Accept-Language": fingerprint["headers"]["Accept-Language"]
        }
    }
    context = await browser.new_context(**context_options)
    await context.add_init_script(script)
    await context.add_init_script(webrtc_ip_spoof_script(proxy_public_ip))

    return context

async def take_screenshot(page: Page, folder_path: str, screenshot_name: str) -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%I-%M-%S%p")
        os.makedirs(folder_path, exist_ok=True)
        path = os.path.join(str(folder_path), f"{screenshot_name}_{timestamp}.png")
        await page.screenshot(path=path, full_page=True, timeout=0)
        log.debug(f"[HELPER] Screenshot saved: {path}")
    except Exception as e:
        log.warning(f"[HELPER] Screenshot failed: {e}")

def get_completed_keywords() -> set:
    """Get set of keywords that have been fully processed (all pages scraped)"""
    completed_file = Path(setting.COMPLETED_KEYWORDS_FILE)
    if completed_file.exists():
        try:
            with open(completed_file, "r") as f:
                return set(json.load(f))
        except Exception as e:
            log.warning(f"[HP] Could not load completed keywords: {e}")
    return set()

def mark_keyword_complete(keyword: str):
    """Mark a keyword as fully processed"""
    completed_file = Path(setting.COMPLETED_KEYWORDS_FILE)
    completed = get_completed_keywords()
    completed.add(keyword)
    try:
        with open(completed_file, "w") as f:
            json.dump(list(completed), f)
        log.debug(f"[HP] Marked '{keyword}' as complete")
    except Exception as e:
        log.error(f"[HP] Failed to mark keyword complete: {e}")

def get_last_processed_keyword() -> str | None:
    """
    Get the last keyword that was processed (has data in CSV).
    Returns the keyword from the last row in companies.csv
    """
    csv_file = Path(setting.AHU_COMPANIES_CSV)
    if not csv_file.exists():
        log.info("[KEYWORDS] No existing companies CSV found. Starting from beginning.")
        return None

    last_keyword = None
    try:
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                # Get the keyword from the last row
                last_keyword = rows[-1].get("keyword")
                log.info(f"[KEYWORDS] Found last processed keyword in CSV: '{last_keyword}'")
            else:
                log.info("[KEYWORDS] CSV file is empty. Starting from beginning.")
    except Exception as e:
        log.error(f"[HELPER] Error reading CSV: {e}")
    
    return last_keyword

async def load_keywords(keyword_queue: asyncio.Queue, collected_nbrs_ids: set = None):
    """
    Load keywords, resuming from the last processed keyword.
    """
    # Get completed keywords (fully scraped with no more pages)
    completed_keywords = get_completed_keywords()
    
    # Get the last keyword that had any data extracted
    last_keyword = get_last_processed_keyword()
    
    log.info(f"[KEYWORDS] Completed keywords: {len(completed_keywords)}")
    if completed_keywords:
        log.debug(f"[KEYWORDS] Completed: {', '.join(list(completed_keywords)[:10])}...")

    csv_file = Path(setting.KEYWORDS_CSV_FILE)
    if not csv_file.exists():
        log.error(f"[KEYWORDS] Keywords file not found: {csv_file}")
        return
    
    # Determine if we need to resume
    resume_mode = last_keyword is not None
    found_resume_point = not resume_mode
    keywords_loaded = 0
    skipped_completed = 0
    skipped_until_resume = 0
    
    try:
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                kw = row["keywords"].strip()
                
                # Skip already completed keywords
                if kw in completed_keywords:
                    skipped_completed += 1
                    continue
                
                # Resume logic: find where we left off
                if resume_mode and not found_resume_point:
                    skipped_until_resume += 1
                    if kw == last_keyword:
                        found_resume_point = True
                        log.info(f"[KEYWORDS] Resuming from keyword: '{kw}'")
                        log.info(f"[KEYWORDS] Skipped {skipped_until_resume} keywords before resume point")
                    continue
                
                # Add keyword to queue
                await keyword_queue.put(kw)
                keywords_loaded += 1
        
        log.info(f"[KEYWORDS] Loaded {keywords_loaded} keywords into queue")
        log.info(f"[KEYWORDS] Skipped {skipped_completed} already completed keywords")
        
        if resume_mode and not found_resume_point:
            log.warning(f"[KEYWORDS] Resume keyword '{last_keyword}' not found. Starting from beginning.")
            # Clear and reload all non-completed keywords
            while not keyword_queue.empty():
                try:
                    keyword_queue.get_nowait()
                    keyword_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            # Reload all non-completed keywords
            with open(csv_file, newline="", encoding="utf-8") as f2:
                reader2 = csv.DictReader(f2)
                for row2 in reader2:
                    kw2 = row2["keywords"].strip()
                    if kw2 not in completed_keywords:
                        await keyword_queue.put(kw2)
                        keywords_loaded += 1
            log.info(f"[KEYWORDS] Reloaded {keywords_loaded} keywords from start")
            
    except Exception as e:
        log.error(f"[KEYWORDS] Error loading keywords: {e}")
        import traceback
        traceback.print_exc()
      
def append_to_csv(file_path: Path, rows: list):
    """Append rows to CSV, handling duplicates by NBRs ID"""
    if not rows:
        return
        
    write_header = not file_path.exists()

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "nbrs_id",
                "company_type",
                "company_name",
                "phone",
                "address",
                "keyword",
            ],
        )

        if write_header:
            writer.writeheader()

        writer.writerows(rows)

async def company_writer(company_queue: asyncio.Queue):
    """Write companies to CSV with buffering"""
    buffer = []
    csv_file = Path(setting.AHU_COMPANIES_CSV)

    while True:
        company = await company_queue.get()

        if company is None:  # shutdown signal
            break

        buffer.append(company)

        if len(buffer) >= setting.COMPANY_BUFFER_SIZE:
            append_to_csv(csv_file, buffer)
            log.debug(f"[WRITER] Flushed {len(buffer)} companies to CSV")
            buffer.clear()

        company_queue.task_done()

    # flush remaining
    if buffer:
        append_to_csv(csv_file, buffer)
        log.info(f"[WRITER] Final flush: {len(buffer)} companies written")