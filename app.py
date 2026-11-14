"""NEXO Music realtime group rehearsal sessions (no audio is transmitted).

The server is authoritative for the session clock. It never streams the
position every frame: it publishes an anchor (positionMs, serverTimestamp,
isPlaying) and every client estimates

    position = positionMs + (serverNow - serverTimestamp)   while playing.

The leader corrects drift with periodic "sync" transports. A small ticker
per session emits section_changed / entry_upcoming / entry_now; after a seek
past cues are never replayed, and seeking back re-arms them.
"""

import asyncio
import logging
import os
import re
import secrets
import time
from typing import Any

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

logger = logging.getLogger("nexo.rehearsal")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

CODE_TTL_SECONDS = int(os.getenv("REHEARSAL_CODE_TTL_SECONDS", str(2 * 60 * 60)))
SESSION_TTL_SECONDS = int(os.getenv("REHEARSAL_SESSION_TTL_SECONDS", str(6 * 60 * 60)))
TICK_SECONDS = float(os.getenv("REHEARSAL_TICK_SECONDS", "0.1"))
# Digits of the join code: 8 (100 million codes) so it cannot be guessed on a
# public server; the clients also accept the old 4-digit ones.
CODE_DIGITS = int(os.getenv("REHEARSAL_CODE_DIGITS", "8"))
# A public server keeps every session in memory: cap how many can be open.
MAX_SESSIONS = int(os.getenv("REHEARSAL_MAX_SESSIONS", "100"))
# An entry_now is only emitted within this window after its start, so a
# late tick or reconnection never fires stale cues.
ENTRY_GRACE_MS = 1500
# A leader sync that moves the clock more than this is treated as a seek.
SYNC_JUMP_MS = 1500

app = FastAPI(title="NEXO Music Group Rehearsal")


def now_ms() -> int:
    return int(time.time() * 1000)


def log(event: str, **fields: Any) -> None:
    details = " ".join(f"{k}={v}" for k, v in fields.items())
    logger.info("[NEXO_REHEARSAL] %s %s", event, details)


# --------------------------------------------------------------------------
# Payloads


class EntryIn(BaseModel):
    id: str | None = None
    instrumentId: str
    instrumentName: str = ""
    participantIds: list[str] = Field(default_factory=list)
    startMs: int
    sectionId: str | None = None
    sectionName: str = ""
    bar: int | None = None
    warningBars: int = 1
    warningMs: int | None = None
    note: str = ""
    enabled: bool = True


class SongIn(BaseModel):
    songId: str = ""
    title: str = ""
    bpm: float | None = None
    pulsesPerBar: int = 4
    firstBeatMs: int = 0
    durationMs: int | None = None
    sections: list[dict[str, Any]] = Field(default_factory=list)
    chords: list[dict[str, Any]] = Field(default_factory=list)
    lyrics: list[dict[str, Any]] = Field(default_factory=list)


class CreateIn(BaseModel):
    leaderName: str
    instrumentId: str = "lider"
    instrumentName: str = "Líder"
    deviceId: str = ""
    song: SongIn
    entries: list[EntryIn] = Field(default_factory=list)


class JoinIn(BaseModel):
    name: str
    instrumentId: str
    instrumentName: str
    deviceId: str = ""


class TransportIn(BaseModel):
    action: str
    positionMs: int | None = None


class EntriesIn(BaseModel):
    entries: list[EntryIn]


# --------------------------------------------------------------------------
# Cue tracking (mirrors EntryCueTracker in the app)


class CueTracker:
    def __init__(self) -> None:
        self.upcoming: set[str] = set()
        self.now: set[str] = set()

    def reset(self, entries: list[dict[str, Any]], position: int) -> None:
        """After a seek/stop/reconnect: everything already started counts as
        fired (no stale cues); everything later is armed again."""
        self.now = {e["id"] for e in entries if e["startMs"] < position}
        self.upcoming = {e["id"] for e in entries if e["startMs"] <= position}

    def update(self, entries: list[dict[str, Any]], position: int) -> tuple[list, list]:
        upcoming, due = [], []
        for e in entries:
            if not e.get("enabled", True):
                continue
            start = e["startMs"]
            warning = e.get("warningMs") or 0
            if e["id"] not in self.upcoming and start - warning <= position < start:
                self.upcoming.add(e["id"])
                upcoming.append(e)
            if e["id"] not in self.now and start <= position < start + ENTRY_GRACE_MS:
                self.now.add(e["id"])
                self.upcoming.add(e["id"])
                due.append(e)
        return upcoming, due


# --------------------------------------------------------------------------
# Session


class Session:
    def __init__(self, body: CreateIn) -> None:
        self.id = secrets.token_urlsafe(16)
        self.code = _new_code()
        self.code_expires = time.time() + CODE_TTL_SECONDS
        self.created = time.time()
        self.touched = time.time()
        self.song = body.song.model_dump()
        self.song["sections"] = sorted(self.song["sections"], key=lambda s: s.get("startMs", 0))
        self.state = "stopped"
        self.resume_state = "stopped"
        self.is_playing = False
        self.position_ms = 0
        self.server_ts = now_ms()
        self.seq = 0
        self.current_section: str | None = None
        self.participants: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, str] = {}
        self.sockets: dict[str, WebSocket] = {}
        self.tracker = CueTracker()
        self.ticker: asyncio.Task | None = None
        leader = self.add_participant(body.leaderName, body.instrumentId, body.instrumentName, body.deviceId, "leader")
        self.leader_id = leader["id"]
        self.entries: list[dict[str, Any]] = []
        self.set_entries(body.entries)

    # participants -----------------------------------------------------
    def add_participant(self, name: str, instrument_id: str, instrument_name: str, device_id: str, role: str) -> dict:
        pid = secrets.token_hex(8)
        token = secrets.token_urlsafe(24)
        participant = {
            "id": pid,
            "name": name.strip()[:40] or "Músico",
            "instrumentId": instrument_id,
            "instrumentName": instrument_name,
            "deviceId": device_id,
            "role": role,
            "connected": False,
            "lastSeen": now_ms(),
        }
        self.participants[pid] = participant
        self.tokens[token] = pid
        participant["_token"] = token
        return participant

    def participant_for(self, token: str | None) -> dict | None:
        pid = self.tokens.get(token or "")
        return self.participants.get(pid) if pid else None

    # clock --------------------------------------------------------------
    def position_at(self, t: int) -> int:
        pos = self.position_ms + (t - self.server_ts if self.is_playing else 0)
        duration = self.song.get("durationMs")
        if duration:
            pos = min(pos, duration)
        return max(0, pos)

    def clock(self) -> dict[str, Any]:
        return {
            "positionMs": self.position_ms,
            "serverTimestamp": self.server_ts,
            "isPlaying": self.is_playing,
            "state": self.state,
            "seq": self.seq,
            "currentSection": self.current_section,
        }

    def section_at(self, position: int) -> dict | None:
        current = None
        for section in self.song["sections"]:
            if section.get("startMs", 0) <= position:
                current = section
        return current

    # entries ------------------------------------------------------------
    def set_entries(self, entries: list[EntryIn]) -> None:
        bar_ms = None
        bpm = self.song.get("bpm")
        if bpm:
            bar_ms = 60000 / bpm * (self.song.get("pulsesPerBar") or 4)
        result = []
        for i, entry in enumerate(entries):
            data = entry.model_dump()
            data["id"] = data["id"] or f"e{i}-{secrets.token_hex(3)}"
            if data["warningMs"] is None:
                data["warningMs"] = int(bar_ms * max(1, data["warningBars"])) if bar_ms else 4000
            result.append(data)
        self.entries = sorted(result, key=lambda e: e["startMs"])
        self.tracker.reset(self.entries, self.position_at(now_ms()))

    # transport ----------------------------------------------------------
    def apply(self, action: str, position: int | None) -> str | None:
        """Applies a leader transport. Returns the event to broadcast, or
        None when ignored (a periodic sync that arrives after pause/stop
        must not move the stopped clock)."""
        now = now_ms()
        if action == "sync" and not self.is_playing:
            return None
        current = self.position_at(now)
        previous = self.state
        if action == "play":
            event = "session_resumed" if previous == "paused" else "session_started"
            target = current if position is None else position
            if previous == "stopped" or abs(target - current) > SYNC_JUMP_MS:
                self.tracker.reset(self.entries, target)
            self.position_ms, self.is_playing, self.state = target, True, "playing"
        elif action == "pause":
            event = "session_paused"
            self.position_ms = current if position is None else position
            self.is_playing, self.state = False, "paused"
        elif action == "stop":
            event = "session_stopped"
            self.position_ms, self.is_playing, self.state = 0, False, "stopped"
            self.tracker.reset(self.entries, 0)
        elif action == "seek":
            if position is None:
                raise HTTPException(status_code=422, detail="seek requiere positionMs")
            event = "seek"
            self.position_ms = max(0, position)
            self.tracker.reset(self.entries, self.position_ms)
        elif action == "sync":
            if position is None:
                raise HTTPException(status_code=422, detail="sync requiere positionMs")
            event = "position_sync"
            if abs(position - current) > SYNC_JUMP_MS:
                self.tracker.reset(self.entries, position)
            self.position_ms = position
        else:
            raise HTTPException(status_code=422, detail=f"Acción desconocida: {action}")
        self.server_ts = now
        self.seq += 1
        self.touched = time.time()
        return event

    # broadcast ----------------------------------------------------------
    async def broadcast(self, message: dict[str, Any]) -> None:
        message.setdefault("serverTime", now_ms())
        dead = []
        for pid, socket in list(self.sockets.items()):
            try:
                await socket.send_json(message)
            except Exception:  # noqa: BLE001 - closed socket
                dead.append(pid)
        for pid in dead:
            self.sockets.pop(pid, None)

    def public(self) -> dict[str, Any]:
        return {
            "sessionId": self.id,
            "code": self.code,
            "songId": self.song.get("songId", ""),
            "song": self.song,
            "leaderId": self.leader_id,
            **self.clock(),
            "participants": [_public_participant(p) for p in self.participants.values()],
            "entries": self.entries,
        }

    async def tick(self) -> None:
        try:
            while self.sockets:
                await asyncio.sleep(TICK_SECONDS)
                position = self.position_at(now_ms())
                section = self.section_at(position)
                name = section.get("name") if section else None
                if name != self.current_section:
                    self.current_section = name
                    await self.broadcast({"type": "section_changed", "section": section, **self.clock()})
                if not self.is_playing:
                    continue
                upcoming, due = self.tracker.update(self.entries, position)
                if upcoming:
                    await self.broadcast({"type": "entry_upcoming", "entries": upcoming, **self.clock()})
                if due:
                    log("entry_now", session=self.id[:6], entries=",".join(e["instrumentId"] for e in due))
                    await self.broadcast({"type": "entry_now", "entries": due, **self.clock()})
        finally:
            self.ticker = None


def _public_participant(p: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in p.items() if not k.startswith("_")}


_sessions: dict[str, Session] = {}


def _new_code() -> str:
    active = {s.code for s in _sessions.values()}
    while True:
        code = f"NEXO-{secrets.randbelow(10**CODE_DIGITS):0{CODE_DIGITS}d}"
        if code not in active:
            return code


def _cleanup() -> None:
    cutoff = time.time() - SESSION_TTL_SECONDS
    for sid in [sid for sid, s in _sessions.items() if s.touched < cutoff and not s.sockets]:
        _sessions.pop(sid, None)


def _resolve(id_or_code: str) -> Session:
    session = _sessions.get(id_or_code)
    if session is None and re.fullmatch(r"(?i)nexo-\d{4,8}", id_or_code):
        code = id_or_code.upper()
        session = next((s for s in _sessions.values() if s.code == code), None)
        if session is not None and time.time() > session.code_expires:
            raise HTTPException(status_code=410, detail="El código de la sesión expiró")
    if session is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada")
    return session


def _require(session: Session, token: str | None, leader: bool = False) -> dict:
    participant = session.participant_for(token)
    if participant is None:
        raise HTTPException(status_code=401, detail="Token inválido")
    if leader and participant["role"] != "leader":
        raise HTTPException(status_code=403, detail="Solo el líder controla el transporte")
    return participant


# --------------------------------------------------------------------------
# HTTP


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "service": "rehearsal", "sessions": len(_sessions)}


@app.post("/rehearsal/sessions", status_code=201)
def create_session(body: CreateIn) -> dict[str, Any]:
    _cleanup()
    if len(_sessions) >= MAX_SESSIONS:
        raise HTTPException(status_code=503, detail="El servidor está lleno, inténtalo en unos minutos")
    session = Session(body)
    _sessions[session.id] = session
    leader = session.participants[session.leader_id]
    log("session_created", session=session.id[:6], code=session.code, entries=len(session.entries))
    return {
        "sessionId": session.id,
        "code": session.code,
        "codeExpiresAt": int(session.code_expires * 1000),
        "participantId": leader["id"],
        "leaderToken": leader["_token"],
        "participantToken": leader["_token"],
        "snapshot": session.public(),
    }


@app.post("/rehearsal/sessions/{id_or_code}/join", status_code=201)
async def join_session(id_or_code: str, body: JoinIn) -> dict[str, Any]:
    session = _resolve(id_or_code)
    participant = session.add_participant(body.name, body.instrumentId, body.instrumentName, body.deviceId, "musician")
    session.touched = time.time()
    log("participant_joined", session=session.id[:6], name=participant["name"], instrument=body.instrumentId)
    await session.broadcast({"type": "participant_joined", "participant": _public_participant(participant), **session.clock()})
    return {
        "sessionId": session.id,
        "code": session.code,
        "participantId": participant["id"],
        "participantToken": participant["_token"],
        "snapshot": session.public(),
    }


@app.get("/rehearsal/sessions/{session_id}")
def get_session(session_id: str, x_rehearsal_token: str | None = Header(default=None)) -> dict[str, Any]:
    session = _resolve(session_id)
    _require(session, x_rehearsal_token)
    return {**session.public(), "serverTime": now_ms()}


@app.post("/rehearsal/sessions/{session_id}/transport")
async def transport(session_id: str, body: TransportIn, x_rehearsal_token: str | None = Header(default=None)) -> dict[str, Any]:
    session = _resolve(session_id)
    _require(session, x_rehearsal_token, leader=True)
    event = session.apply(body.action, body.positionMs)
    if event is None:
        return {"type": "ignored", **session.clock(), "serverTime": now_ms()}
    log(event, session=session.id[:6], positionMs=session.position_ms, playing=session.is_playing)
    await session.broadcast({"type": event, **session.clock()})
    return {"type": event, **session.clock(), "serverTime": now_ms()}


@app.post("/rehearsal/sessions/{session_id}/entries")
async def update_entries(session_id: str, body: EntriesIn, x_rehearsal_token: str | None = Header(default=None)) -> dict[str, Any]:
    session = _resolve(session_id)
    _require(session, x_rehearsal_token, leader=True)
    session.set_entries(body.entries)
    session.seq += 1
    await session.broadcast({"type": "entry_updated", "entries": session.entries, **session.clock()})
    return {"entries": session.entries, **session.clock()}


# --------------------------------------------------------------------------
# WebSocket


@app.websocket("/rehearsal/sessions/{session_id}/ws")
async def session_socket(websocket: WebSocket, session_id: str, token: str = "") -> None:
    session = _sessions.get(session_id)
    participant = session.participant_for(token) if session else None
    if session is None or participant is None:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    pid = participant["id"]
    previous = session.sockets.get(pid)
    session.sockets[pid] = websocket
    if previous is not None and previous is not websocket:
        try:
            await previous.close(code=4000)
        except Exception:  # noqa: BLE001
            pass
    participant["connected"] = True
    participant["lastSeen"] = now_ms()
    is_leader = pid == session.leader_id
    if is_leader and session.state == "leaderDisconnected":
        session.state = session.resume_state
        await session.broadcast({"type": "leader_reconnected", **session.clock()})
    await websocket.send_json({"type": "snapshot", "snapshot": session.public(), "serverTime": now_ms()})
    await session.broadcast({"type": "participant_joined", "participant": _public_participant(participant), **session.clock()})
    if session.ticker is None:
        session.ticker = asyncio.create_task(session.tick())
    log("socket_open", session=session.id[:6], name=participant["name"], role=participant["role"])
    try:
        while True:
            message = await websocket.receive_json()
            participant["lastSeen"] = now_ms()
            kind = message.get("type")
            if kind == "ping":
                await websocket.send_json({"type": "pong", "clientTime": message.get("clientTime"), "serverTime": now_ms()})
            elif kind == "transport":
                if not is_leader:
                    await websocket.send_json({"type": "error", "detail": "Solo el líder controla el transporte"})
                    continue
                try:
                    event = session.apply(message.get("action", ""), message.get("positionMs"))
                except HTTPException as error:
                    await websocket.send_json({"type": "error", "detail": error.detail})
                    continue
                if event is None:
                    continue
                if event != "position_sync":
                    log(event, session=session.id[:6], positionMs=session.position_ms, playing=session.is_playing)
                await session.broadcast({"type": event, **session.clock()})
            elif kind == "snapshot":
                await websocket.send_json({"type": "snapshot", "snapshot": session.public(), "serverTime": now_ms()})
    except WebSocketDisconnect:
        pass
    finally:
        if session.sockets.get(pid) is websocket:
            session.sockets.pop(pid, None)
            participant["connected"] = False
            participant["lastSeen"] = now_ms()
            log("socket_closed", session=session.id[:6], name=participant["name"], role=participant["role"])
            if is_leader and session.state != "leaderDisconnected":
                session.resume_state = session.state
                session.state = "leaderDisconnected"
                await session.broadcast({"type": "leader_disconnected", **session.clock()})
            await session.broadcast({"type": "participant_left", "participant": _public_participant(participant), **session.clock()})
