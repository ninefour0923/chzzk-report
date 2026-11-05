#!/usr/bin/env python3
"""치지직 스트리머 리포트 수집기

스트리머 채널 링크를 등록하면 그 순간부터, 방송 중일 때 1분마다
시청자 수·카테고리·채팅 수·치즈 후원 금액을 기록하고 대시보드로 보여준다.

실행:  python app.py          (브라우저에서 http://localhost:8000)
점검:  python app.py --test <채널 링크 또는 채널 ID>
"""
import base64
import csv
import hmac
import io
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE)          # 서버에서는 영구 저장소 경로(예: /data)
DB_PATH = os.path.join(DATA_DIR, "data.db")
KST = timezone(timedelta(hours=9))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "127.0.0.1")           # 서버에서는 0.0.0.0
PASSWORD = os.environ.get("APP_PASSWORD", "")        # 설정하면 모든 화면에 로그인 필요
POLL_SEC = 60
ID_RE = re.compile(r"[0-9a-f]{32}", re.I)

LOCK = threading.Lock()
DB = None
LIVE = {}       # 채널ID -> {viewers, cat, title}
COUNTERS = {}   # 채널ID -> [채팅 수, 치즈 합계]
LISTENERS = {}  # 채널ID -> ChatListener


def log(*a):
    print(datetime.now(KST).strftime("%H:%M:%S"), *a, flush=True)


def init_db(path=DB_PATH):
    global DB
    DB = sqlite3.connect(path, check_same_thread=False)
    DB.execute("create table if not exists channels(id text primary key, name text, added integer)")
    DB.execute("create table if not exists minutes(ch text, t integer, viewers integer, cat text, "
               "chat integer, cheese integer, primary key(ch, t))")
    DB.commit()


# ---------- 치지직 접속 ----------
def get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def live_detail(cid):
    d = get_json(f"https://api.chzzk.naver.com/service/v3/channels/{cid}/live-detail")
    return d.get("content") or {}


def channel_name(cid):
    try:
        d = get_json(f"https://api.chzzk.naver.com/service/v1/channels/{cid}")
        return (d.get("content") or {}).get("channelName") or ""
    except Exception as e:
        log("채널 이름 조회 실패:", repr(e)[:100])
        return ""


# ---------- 채팅 집계 ----------
def add_count(cid, chat=0, cheese=0):
    with LOCK:
        c = COUNTERS.setdefault(cid, [0, 0])
        c[0] += chat
        c[1] += cheese


def take_counts(cid):
    with LOCK:
        c = COUNTERS.get(cid, [0, 0])
        COUNTERS[cid] = [0, 0]
        return c


def items_of(msg):
    b = msg.get("bdy")
    if isinstance(b, list):
        return b
    if isinstance(b, dict):
        return b.get("messageList") or []
    return []


def classify(item):
    """채팅 메시지 한 건을 ('chat', 1) / ('cheese', 금액) / ('skip', 0)로 나눈다."""
    ex = item.get("extras")
    if isinstance(ex, str):
        try:
            ex = json.loads(ex)
        except Exception:
            ex = {}
    ex = ex if isinstance(ex, dict) else {}
    code = item.get("msgTypeCode")
    if code == 10 or "payAmount" in ex:
        try:
            return "cheese", int(float(ex.get("payAmount", 0)))
        except Exception:
            return "cheese", 0
    if code in (None, 1):
        return "chat", 1
    return "skip", 0


class ChatListener(threading.Thread):
    def __init__(self, cid, chat_id):
        super().__init__(daemon=True)
        self.cid, self.chat_id = cid, chat_id
        self.stop_evt = threading.Event()

    def stop(self):
        self.stop_evt.set()

    def run(self):
        while not self.stop_evt.is_set():
            try:
                self.session()
            except Exception as e:
                log("채팅 연결 오류", self.cid[:6], repr(e)[:120])
            self.stop_evt.wait(5)

    def session(self):
        import websocket  # pip install websocket-client
        url = ("https://comm-api.game.naver.com/nng_main/v1/chats/access-token"
               f"?channelId={self.chat_id}&chatType=STREAMING")
        tok = get_json(url)["content"]["accessToken"]
        sid = sum(ord(c) for c in self.chat_id) % 9 + 1
        ws = websocket.create_connection(f"wss://kr-ss{sid}.chat.naver.com/chat",
                                         timeout=25, origin="https://chzzk.naver.com")
        try:
            ws.send(json.dumps({"ver": "3", "cmd": 100, "svcid": "game", "cid": self.chat_id,
                                "bdy": {"uid": None, "devType": 2001, "accTkn": tok, "auth": "READ"},
                                "tid": 1}))
            while not self.stop_evt.is_set():
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    ws.send(json.dumps({"ver": "3", "cmd": 0}))
                    continue
                if not raw:
                    break
                self.handle(ws, json.loads(raw))
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def handle(self, ws, msg):
        cmd = msg.get("cmd")
        if cmd == 0:
            ws.send(json.dumps({"ver": "3", "cmd": 10000}))
        elif cmd == 10100:
            log("채팅 연결됨", self.cid[:6])
        elif cmd in (93101, 93102):
            for it in items_of(msg):
                kind, n = classify(it)
                if kind == "chat":
                    add_count(self.cid, chat=1)
                elif kind == "cheese":
                    add_count(self.cid, cheese=n)


def ensure_listener(cid, chat_id):
    old = LISTENERS.get(cid)
    if old and old.chat_id == chat_id and old.is_alive():
        return
    if old:
        old.stop()
    lis = ChatListener(cid, chat_id)
    LISTENERS[cid] = lis
    lis.start()


def stop_listener(cid):
    old = LISTENERS.pop(cid, None)
    if old:
        old.stop()


# ---------- 1분마다 기록 ----------
def poll_channel(cid, now=None):
    try:
        d = live_detail(cid)
    except Exception as e:
        log(cid[:6], "방송 상태 조회 실패:", repr(e)[:100])
        return
    if d.get("status") == "OPEN":
        viewers = int(d.get("concurrentUserCount") or 0)
        cat = d.get("liveCategoryValue") or d.get("liveCategory") or "미분류"
        LIVE[cid] = {"viewers": viewers, "cat": cat, "title": d.get("liveTitle") or ""}
        if d.get("chatChannelId"):
            ensure_listener(cid, d["chatChannelId"])
        chat, cheese = take_counts(cid)
        t = int((now or time.time()) // 60 * 60)
        with LOCK:
            DB.execute(
                "insert into minutes values(?,?,?,?,?,?) on conflict(ch,t) do update set "
                "viewers=excluded.viewers, cat=excluded.cat, "
                "chat=chat+excluded.chat, cheese=cheese+excluded.cheese",
                (cid, t, viewers, cat, chat, cheese))
            DB.commit()
    else:
        LIVE.pop(cid, None)
        stop_listener(cid)
        take_counts(cid)


def poll_loop():
    pool = ThreadPoolExecutor(8)
    while True:
        started = time.time()
        with LOCK:
            ids = [r[0] for r in DB.execute("select id from channels")]
        list(pool.map(poll_channel, ids))
        time.sleep(max(1, POLL_SEC - (time.time() - started)))


# ---------- 일간/주간 화면용 데이터 ----------
def build_rows():
    """분 단위 기록을 '날짜 + 카테고리' 구간으로 묶는다(카테고리가 바뀌면 새 구간)."""
    with LOCK:
        recs = DB.execute("select ch,t,viewers,cat,chat,cheese from minutes order by ch,t").fetchall()
    rows, cur = [], None

    def flush():
        if cur:
            rows.append({"ch": cur["ch"], "date": cur["date"], "cat": cur["cat"],
                         "peak": max(cur["v"]), "avg": sum(cur["v"]) / len(cur["v"]),
                         "chat": cur["chat"], "cheese": cur["cheese"],
                         "hours": round(len(cur["v"]) / 60, 3)})

    for ch, t, v, cat, chat, cheese in recs:
        day = datetime.fromtimestamp(t, KST).strftime("%Y-%m-%d")
        if cur and cur["ch"] == ch and cur["date"] == day and cur["cat"] == cat and t - cur["last"] <= 300:
            cur["v"].append(v)
            cur["chat"] += chat
            cur["cheese"] += cheese
            cur["last"] = t
        else:
            flush()
            cur = {"ch": ch, "date": day, "cat": cat, "v": [v], "chat": chat, "cheese": cheese, "last": t}
    flush()
    return rows


def state():
    with LOCK:
        chs = [{"id": r[0], "name": r[1]} for r in DB.execute("select id,name from channels order by added")]
    return {"channels": chs, "rows": build_rows(), "live": dict(LIVE)}


# ---------- 웹 서버 ----------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8", extra=None):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _local(self):
        if self.path == "/healthz":
            return True
        if PASSWORD:
            h = self.headers.get("Authorization") or ""
            ok = False
            if h.startswith("Basic "):
                try:
                    given = base64.b64decode(h[6:]).decode("utf-8").split(":", 1)[-1]
                    ok = hmac.compare_digest(given.encode("utf-8"), PASSWORD.encode("utf-8"))
                except Exception:
                    ok = False
            if not ok:
                self._send(401, '{"error":"login required"}',
                           extra={"WWW-Authenticate": 'Basic realm="chzzk-report"'})
                return False
            return True
        host = (self.headers.get("Host") or "").split(":")[0]
        if host not in ("localhost", "127.0.0.1"):
            self._send(403, '{"error":"local only"}')
            return False
        return True

    def do_GET(self):
        if not self._local():
            return
        p = urlparse(self.path)
        if p.path in ("/", "/index.html"):
            with open(os.path.join(BASE, "dashboard.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif p.path == "/healthz":
            self._send(200, "ok", "text/plain; charset=utf-8")
        elif p.path == "/api/state":
            self._send(200, json.dumps(state(), ensure_ascii=False))
        elif p.path == "/api/export.csv":
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(["날짜", "카테고리", "최고 시청자", "평균 시청자", "채팅수", "치즈", "방송시간(시간)", "채널ID"])
            for r in build_rows():
                w.writerow([r["date"], r["cat"], r["peak"], round(r["avg"]), r["chat"], r["cheese"], r["hours"], r["ch"]])
            self._send(200, "﻿" + out.getvalue(), "text/csv; charset=utf-8",
                       {"Content-Disposition": "attachment; filename=chzzk-report.csv"})
        else:
            self._send(404, '{"error":"not found"}')

    def do_POST(self):
        if not self._local():
            return
        if urlparse(self.path).path != "/api/channels" or "application/json" not in (self.headers.get("Content-Type") or ""):
            self._send(404, '{"error":"not found"}')
            return
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        except Exception:
            self._send(400, '{"error":"bad json"}')
            return
        m = ID_RE.search(str(body.get("url", "")))
        if not m:
            self._send(400, '{"error":"채널 링크에서 32자리 채널 ID를 찾지 못했어요"}')
            return
        cid = m.group(0).lower()
        name = (body.get("name") or "").strip() or channel_name(cid) or cid[:6]
        with LOCK:
            DB.execute("insert or ignore into channels values(?,?,?)", (cid, name, int(time.time())))
            DB.commit()
        threading.Thread(target=poll_channel, args=(cid,), daemon=True).start()
        self._send(200, json.dumps({"id": cid, "name": name}, ensure_ascii=False))

    def do_DELETE(self):
        if not self._local():
            return
        m = re.fullmatch(r"/api/channels/([0-9a-f]{32})", urlparse(self.path).path)
        if not m:
            self._send(404, '{"error":"not found"}')
            return
        cid = m.group(1)
        with LOCK:
            DB.execute("delete from channels where id=?", (cid,))
            DB.execute("delete from minutes where ch=?", (cid,))
            DB.commit()
        stop_listener(cid)
        LIVE.pop(cid, None)
        self._send(200, '{"ok":true}')


# ---------- 점검 ----------
def self_test(arg):
    m = ID_RE.search(arg)
    cid = m.group(0).lower() if m else arg
    d = live_detail(cid)
    print("방송 상태:", d.get("status"), "| 시청자:", d.get("concurrentUserCount"),
          "| 카테고리:", d.get("liveCategoryValue"), "| 제목:", d.get("liveTitle"))
    if d.get("status") == "OPEN" and d.get("chatChannelId"):
        print("채팅창을 30초 동안 읽어 볼게요...")
        lis = ChatListener(cid, d["chatChannelId"])
        lis.start()
        time.sleep(30)
        lis.stop()
        chat, cheese = take_counts(cid)
        print(f"30초 동안 채팅 {chat}건, 치즈 후원 합계 {cheese}")
    else:
        print("방송 중이 아니라 채팅 점검은 건너뛰었어요. 방송 중인 채널로 다시 해 보세요.")


def main():
    if len(sys.argv) > 2 and sys.argv[1] == "--test":
        self_test(sys.argv[2])
        return
    if HOST not in ("127.0.0.1", "localhost") and not PASSWORD:
        sys.exit("외부에서 열리는 서버에는 APP_PASSWORD 환경변수로 비밀번호를 꼭 설정하세요.")
    os.makedirs(DATA_DIR, exist_ok=True)
    init_db()
    threading.Thread(target=poll_loop, daemon=True).start()
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    log(f"대시보드: http://localhost:{PORT}  (끄려면 Ctrl+C)" if HOST in ("127.0.0.1", "localhost")
        else f"서버 실행 중: 포트 {PORT}, 데이터 폴더 {DATA_DIR}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
