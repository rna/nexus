import random

# A collection of realistic browser fingerprint components.
# In a production system, this would be much larger and more frequently updated.

WEBGL_VENDORS = {
    "apple": "Apple Inc.",
    "intel": "Intel Inc.",
    "nvidia": "NVIDIA Corporation",
    "amd": "Advanced Micro Devices, Inc.",
}

WEBGL_RENDERERS = {
    "apple": "Apple M1",
    "intel": "Intel(R) Iris(TM) Plus Graphics 640",
    "nvidia": "NVIDIA GeForce GTX 1080/PCIe/SSE2",
    "amd": "AMD Radeon Pro 5700 XT",
}

LANGUAGES = ["en-US", "en-GB", "en-CA", "en-AU"]
PLATFORMS = ["Win32", "MacIntel", "Linux x86_64"]

def get_fingerprint():
    """
    Generates a consistent, randomized browser fingerprint profile for a session.
    """
    gpu_vendor_key = random.choice(list(WEBGL_VENDORS.keys()))
    
    return {
        "webgl_vendor": WEBGL_VENDORS[gpu_vendor_key],
        "webgl_renderer": WEBGL_RENDERERS[gpu_vendor_key],
        "language": random.choice(LANGUAGES),
        "platform": random.choice(PLATFORMS),
        "hardware_concurrency": random.choice([4, 8, 16]),
    }

def get_override_script(fingerprint: dict) -> str:
    """
    Generates a JavaScript string to override navigator properties.
    Sets `navigator.webdriver` to false.
    """
    return f"""
        // Set webdriver to false
        Object.defineProperty(navigator, 'webdriver', {{
            get: () => false,
        }});

        // Override platform
        Object.defineProperty(navigator, 'platform', {{
            get: () => '{fingerprint["platform"]}',
        }});
        
        // Override hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {{
            get: () => {fingerprint["hardware_concurrency"]},
        }});

        // Override languages
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['{fingerprint["language"]}'],
        }});

        // Override WebGL renderer
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) {{ // UNMASKED_VENDOR_WEBGL
                return '{fingerprint["webgl_vendor"]}';
            }}
            if (parameter === 37446) {{ // UNMASKED_RENDERER_WEBGL
                return '{fingerprint["webgl_renderer"]}';
            }}
            return getParameter(parameter);
        }};
    """
