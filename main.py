"""Tic-tac-toe server.

One FastAPI app that:
  - serves the game page at /
  - runs rooms over WebSockets at /ws/{room}?pid={player_id}

The server is the only source of truth: clients send "move" requests,
the server validates them and broadcasts the new state to everyone in the room.
"""

import json
import re
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

BASE = Path(__file__).parent
app = FastAPI(title="Tic-tac-toe")

CODE_RE = re.compile(r"[A-Z0-9]{1,12}")
PID_RE = re.compile(r"[A-Za-z0-9]{8,64}")

WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),  # rows
    (0, 3, 6), (1, 4, 7), (2, 5, 8),  # columns
    (0, 4, 8), (2, 4, 6),             # diagonals
]


class Room:
    def __init__(self) -> None:
        self.sockets: dict[WebSocket, str] = {}   # socket -> player id
        self.seats: dict[str, str | None] = {"X": None, "O": None}
        self.scores = {"X": 0, "O": 0, "draw": 0}
        self.starter = "X"
        self.reset()

    # ---- game state -------------------------------------------------
    def reset(self) -> None:
        self.board = [""] * 9
        self.turn = self.starter
        self.winner: str | None = None   # "X", "O", "draw" or None
        self.line: list[int] | None = None

    def mark_of(self, pid: str) -> str:
        for mark, seat_pid in self.seats.items():
            if seat_pid == pid:
                return mark
        return "spectator"

    def both_seated(self) -> bool:
        return all(self.seats.values())

    def play(self, pid: str, index: int) -> bool:
        if self.winner or not self.both_seated():
            return False
        if self.mark_of(pid) != self.turn:
            return False
        if not 0 <= index < 9 or self.board[index]:
            return False

        self.board[index] = self.turn
        for a, b, c in WIN_LINES:
            if self.board[a] and self.board[a] == self.board[b] == self.board[c]:
                self.winner = self.turn
                self.line = [a, b, c]
                self.scores[self.turn] += 1
                return True
        if all(self.board):
            self.winner = "draw"
            self.scores["draw"] += 1
        else:
            self.turn = "O" if self.turn == "X" else "X"
        return True

    def rematch(self, pid: str) -> bool:
        if self.winner is None or self.mark_of(pid) == "spectator":
            return False
        self.starter = "O" if self.starter == "X" else "X"
        self.reset()
        return True

    # ---- seats and connections -------------------------------------
    def join(self, ws: WebSocket, pid: str) -> None:
        self.sockets[ws] = pid
        if self.mark_of(pid) == "spectator":
            for mark in ("X", "O"):
                if self.seats[mark] is None:
                    self.seats[mark] = pid
                    break

    def leave(self, ws: WebSocket) -> None:
        pid = self.sockets.pop(ws, None)
        if pid and pid not in self.sockets.values():
            for mark, seat_pid in self.seats.items():
                if seat_pid == pid:
                    self.seats[mark] = None

    async def broadcast(self) -> None:
        for ws, pid in list(self.sockets.items()):
            payload = {
                "type": "state",
                "board": self.board,
                "turn": self.turn,
                "winner": self.winner,
                "line": self.line,
                "you": self.mark_of(pid),
                "ready": self.both_seated(),
                "scores": self.scores,
            }
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                pass  # a dead socket is cleaned up in its own handler


rooms: dict[str, Room] = {}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE / "index.html")


@app.websocket("/ws/{code}")
async def game_socket(ws: WebSocket, code: str, pid: str = "") -> None:
    code = code.upper()
    if not CODE_RE.fullmatch(code) or not PID_RE.fullmatch(pid):
        await ws.close(code=1008)
        return

    await ws.accept()
    room = rooms.setdefault(code, Room())
    room.join(ws, pid)
    await room.broadcast()

    try:
        while True:
            try:
                msg = json.loads(await ws.receive_text())
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue

            changed = False
            if msg.get("type") == "move" and isinstance(msg.get("index"), int):
                changed = room.play(pid, msg["index"])
            elif msg.get("type") == "rematch":
                changed = room.rematch(pid)

            if changed:
                await room.broadcast()
    except WebSocketDisconnect:
        pass
    finally:
        room.leave(ws)
        if room.sockets:
            await room.broadcast()
        else:
            rooms.pop(code, None)