import requests, json
import random
from config import setting
from pathlib import Path
from scraper.logger import get_logger

log = get_logger()

def load_proxies(file_path: str):
    proxies = []
    
    # Load current start number from state file
    if Path(setting.STATE_FILE).exists():
        with open(setting.STATE_FILE, "r") as f:
            proxies_start_number = json.load(f).get("proxies_start_number", 0)
    else:
        proxies_start_number = 0

    with open(file_path, "r") as f:
        all_proxies = f.readlines()
        for i in range(setting.CONCURRENCY):
            index = (proxies_start_number + i) % len(all_proxies)
            parts = all_proxies[index].strip().split(":")
            if len(parts) == 4:
                proxies.append(parts)
        new_start = (proxies_start_number + setting.CONCURRENCY) % len(all_proxies)

    # Save updated start number
    with open(setting.STATE_FILE, "w") as f:
        json.dump({"proxies_start_number": new_start}, f)

    log.info(f"[HP]Total {len(proxies)} proxy loaded.")
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

    Fixes vs previous version:
    - Canvas noise via overdraw (not getImageData mutation) → no before/after delta
    - OffscreenCanvas patched
    - Audio: getChannelData + copyFromChannel + createAnalyser all patched
    - navigator.webdriver = false (belt-and-suspenders; CDP args do the real work)
    - All Sec-CH-UA / Client Hints headers consistent with Windows UA
    - Platform always Win32
    - window.chrome more complete
    - Automation artefact cleanup expanded
    """
    langs        = json.dumps(fingerprint["languages"])
    plugins_json = json.dumps(fingerprint["plugins"])
    ua_data_json = json.dumps(fingerprint["user_agent_data"])

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

  // ── Canvas noise — overdraw method (no getImageData delta) ───────────────
  //
  // FIX: Old approach read pixels out via getImageData, mutated them, wrote
  // back — detectors compare before/after and see the mutation delta.
  // New approach: overdraw a near-invisible 1×1 rect. No read-back, no delta.
  //
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

  // OffscreenCanvas — FIX: was completely unpatched before
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

  // ── Audio fingerprint noise — full surface ────────────────────────────────
  //
  // FIX: Previous version only patched getChannelData. Detectors also probe
  // copyFromChannel and AnalyserNode.getFloatFrequencyData.
  //
  const _audioNoise = {audio_noise};

  const _getChannelData = AudioBuffer.prototype.getChannelData;
  AudioBuffer.prototype.getChannelData = function (...args) {{
    const data = _getChannelData.apply(this, args);
    if (data.length > 0) data[0] = Math.max(-1, Math.min(1, data[0] + _audioNoise));
    return data;
  }};

  // FIX: patch copyFromChannel
  const _copyFromChannel = AudioBuffer.prototype.copyFromChannel;
  AudioBuffer.prototype.copyFromChannel = function (dest, channelNum, ...rest) {{
    _copyFromChannel.call(this, dest, channelNum, ...rest);
    if (dest && dest.length > 0) dest[0] = Math.max(-1, Math.min(1, dest[0] + _audioNoise));
  }};

  // FIX: patch AnalyserNode frequency data
  if (typeof AudioContext !== 'undefined') {{
    const _createAnalyser = AudioContext.prototype.createAnalyser;
    AudioContext.prototype.createAnalyser = function () {{
      const node = _createAnalyser.apply(this, arguments);
      const _gffd = node.getFloatFrequencyData.bind(node);
      node.getFloatFrequencyData = function (arr) {{
        _gffd(arr);
        if (arr && arr.length > 0) arr[0] += _audioNoise * 1000; // dB scale
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

# ── WebRTC IP spoof script (proxy mode only) ──────────────────────────────────
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
