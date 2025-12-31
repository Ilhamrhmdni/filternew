from __future__ import annotations
from pathlib import Path
import streamlit.components.v1 as components

_RELEASE = True

if _RELEASE:
    _component_func = components.declare_component(
        "paste_streamer",
        path=str(Path(__file__).parent / "frontend" / "build"),
    )
else:
    # Dev mode (jalankan `npm run dev`)
    _component_func = components.declare_component(
        "paste_streamer",
        url="http://localhost:3001",
    )

def paste_streamer(
    *,
    height: int = 220,
    chunk_chars: int = 2_000_000,  # besar chunk (dalam karakter)
    ack_seq: int = -1,             # handshake ack dari Python
    key: str | None = None
):
    """
    Return payload dict dari frontend:
    - {type:"start", job_id, total_chars}
    - {type:"chunk", job_id, seq, data}
    - {type:"done", job_id}
    """
    return _component_func(
        height=height,
        chunk_chars=chunk_chars,
        ack_seq=ack_seq,
        key=key,
        default=None,
    )
