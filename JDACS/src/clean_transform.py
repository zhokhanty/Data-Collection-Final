from datetime import datetime

def clean_event(event: dict) -> dict | None:
    props = event.get("properties", {})
    geom = event.get("geometry", {})

    if not geom or "coordinates" not in geom:
        return None

    try:
        return {
            "event_id": props.get("eventid"),
            "event_type": props.get("eventtype"),
            "country": props.get("country"),
            "severity": float(props.get("severity", 0)),
            "latitude": geom["coordinates"][1],
            "longitude": geom["coordinates"][0],
            "event_time": props.get("fromdate"),
            "ingestion_time": datetime.utcnow().isoformat()
        }
    except Exception:
        return None
