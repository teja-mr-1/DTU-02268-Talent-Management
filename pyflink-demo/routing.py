"""Maps a position type to the DCR graph that models its hiring process."""
import os


def _parse(raw: str) -> dict:
    """'engineer=123,sales=456' -> {'engineer': '123', 'sales': '456'}"""
    out = {}
    for pair in raw.split(","):
        if "=" in pair:
            position, graph_id = pair.split("=", 1)
            out[position.strip()] = graph_id.strip()
    return out


GRAPH_IDS = _parse(os.getenv("DCR_GRAPH_IDS", ""))  # per-position overrides
DEFAULT_GRAPH_ID = os.getenv("DCR_GRAPH_ID", "")    # used for every other position


def graph_id_for(position: str) -> str:
    return GRAPH_IDS.get(position, DEFAULT_GRAPH_ID)
