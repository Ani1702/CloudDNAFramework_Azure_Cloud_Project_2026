import json, time, os
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.core.credentials import AzureKeyCredential

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Attempt to load settings from possible locations
settings_candidates = [
    BASE_DIR / 'local.settings.json',
    BASE_DIR.parent / 'services' / 'decision' / 'local.settings.json',
    BASE_DIR.parent.parent / 'local.settings.json',
]

loaded_settings = False
for s_path in settings_candidates:
    if s_path.exists():
        with open(s_path) as f:
            os.environ.update(json.load(f).get('Values', {}))
        loaded_settings = True
        break

if not loaded_settings:
    print("Warning: local.settings.json not found in candidate paths.")

publisher = EventGridPublisherClient(
    os.environ["STREAM_A_TOPIC_ENDPOINT"], AzureKeyCredential(os.environ["STREAM_A_TOPIC_KEY"]))

fixture_path = BASE_DIR.parent / "tests" / "fixtures" / "stream_a_sample.json"
windows = json.load(open(fixture_path))
for w in windows:
    publisher.send(EventGridEvent(event_type="CloudDNA.StreamAWindow", data=w,
                                    subject=f"metrics/{w['app_id']}", data_version="1.0"))
    time.sleep(1)   # roughly simulate real cadence