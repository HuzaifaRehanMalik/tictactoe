"""Tic-tac-toe server.

One FastAPI app that:
  - serves the game page at /
  - runs rooms over WebSockets at /ws/{room}?pid={player_id}[&solo=easy|medium|hard]

The server is the only source of truth: clients send "move" requests,
the server validates them and broadcasts the new state to everyone in the room.
A room created with ?solo=... gets a computer player in the "O" seat.
"""

import asyncio
import json
import random
import re
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

BASE = Path(__file__).parent
app = FastAPI(title="Tic-tac-toe")

CODE_RE = re.compile(r"[A-Z0-9]{1,12}")
PID_RE = re.compile(r"[A-Za-z0-9]{8,64}")

LEVELS = ("easy", "medium", "hard")
BOT_PID = "computer"
BOT_MARK = "O"

WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),  # rows
    (0, 3, 6), (1, 4, 7), (2, 5, 8),  # columns
    (0, 4, 8), (2, 4, 6),             # diagonals
]


# ---- computer player ------------------------------------------------
def winner_of(board: list[str]) -> str | None:
    for a, b, c in WIN_LINES:
        if board[a] and board[a] == board[b] == board[c]:
            return board[a]
    return None


@lru_cache(maxsize=None)
def minimax(board: tuple, turn: str, me: str) -> int:
    """Score for `me` with `turn` to move. Faster wins score higher.
    Cached, so even the empty board is solved once and then instant."""
    w = winner_of(board)
    if w:
        left = board.count("") + 1
        return left if w == me else -left
    if all(board):
        return 0
    other = "O" if turn == "X" else "X"
    scores = [
        minimax(board[:i] + (turn,) + board[i + 1:], other, me)
        for i in range(9) if not board[i]
    ]
    return max(scores) if turn == me else min(scores)


def pick_move(board: list[str], me: str, level: str) -> int:
    """easy: takes a win if it sees one, otherwise random.
    medium: also blocks, otherwise random (beatable with a fork).
    hard: perfect play, never loses."""
    board = list(board)
    other = "X" if me == "O" else "O"
    free = [i for i in range(9) if not board[i]]

    def finishing(mark: str) -> int | None:
        for i in free:
            board[i] = mark
            won = winner_of(board) == mark
            board[i] = ""
            if won:
                return i
        return None

    if level == "hard":
        scored = [
            (minimax(tuple(board[:i] + [me] + board[i + 1:]), other, me), i)
            for i in free
        ]
        top = max(s for s, _ in scored)
        return random.choice([i for s, i in scored if s == top])

    win = finishing(me)
    if win is not None:
        return win
    if level == "medium":
        block = finishing(other)
        if block is not None:
            return block
    return random.choice(free)


# ---- rooms ----------------------------------------------------------
class Room:
    def __init__(self) -> None:
        self.sockets: dict[WebSocket, str] = {}   # socket -> player id
        self.seats: dict[str, str | None] = {"X": None, "O": None}
        self.scores = {"X": 0, "O": 0, "draw": 0}
        self.starter = "X"
        self.bot: str | None = None               # difficulty, or None
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

    # ---- computer player --------------------------------------------
    def enable_bot(self, level: str) -> None:
        self.bot = level
        self.seats[BOT_MARK] = BOT_PID

    def bot_to_move(self) -> bool:
        return bool(self.bot) and not self.winner and self.both_seated() and self.turn == BOT_MARK

    async def bot_turn(self) -> None:
        if not self.bot_to_move():
            return
        await asyncio.sleep(0.6)  # a short pause so it feels like a person
        if self.bot_to_move():
            self.play(BOT_PID, pick_move(self.board, BOT_MARK, self.bot))
            await self.broadcast()

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
                "solo": self.bot is not None,
                "level": self.bot,
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
async def game_socket(ws: WebSocket, code: str, pid: str = "", solo: str = "") -> None:
    code = code.upper()
    if not CODE_RE.fullmatch(code) or not PID_RE.fullmatch(pid):
        await ws.close(code=1008)
        return

    await ws.accept()
    room = rooms.get(code)
    if room is None:
        room = rooms[code] = Room()
        if solo in LEVELS:           # only a brand-new room can become solo
            room.enable_bot(solo)
    room.join(ws, pid)
    await room.broadcast()
    await room.bot_turn()

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
                await room.bot_turn()
    except WebSocketDisconnect:
        pass
    finally:
        room.leave(ws)
        if room.sockets:
            await room.broadcast()
        else:
            rooms.pop(code, None)