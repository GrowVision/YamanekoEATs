import os, json, re, math, datetime
from datetime import timedelta, timezone
from flask import Flask, request, abort
import csv, io, requests
import threading
import unicodedata

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, PostbackEvent,
    QuickReply, QuickReplyButton, PostbackAction,
    FlexSendMessage, BubbleContainer, BoxComponent, TextComponent, ButtonComponent,
    URIAction
)

# ====== 基本設定 ======
JST = timezone(timedelta(hours=9))
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    raise RuntimeError("LINE env missing")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
app = Flask(__name__)

# ====== ストア（仮） ======
# line_user_id は各店舗のLINEユーザーID（個別トークできるID）を入れてください
STORES = [
    {
        "store_id": "ST1",
        "name": "島料理 A",
        "profile": "港から車5分。石垣牛と島野菜。",
        "map_url": "https://goo.gl/maps/xxxxxxxx",
        "pickup_ok": True,
        "line_user_id": "UXXXXXXXXXXXXXXX"  # ←差し替え
    },
    {
        "store_id": "ST2",
        "name": "居酒屋 B",
        "profile": "地魚と泡盛。21:30 L.O.",
        "map_url": "https://goo.gl/maps/yyyyyyyyyyyyy",
        "pickup_ok": False,
        "line_user_id": "UYYYYYYYYYYYYYYY"  # ←差し替え
    },
]

STORE_BY_ID = {s["store_id"]: s for s in STORES}

# ====== ストア情報：スプレッドシート連携 ======
STORES_SHEET_CSV_URL = os.getenv("STORES_SHEET_CSV_URL")
STORES_RELOAD_TOKEN = os.getenv("STORES_RELOAD_TOKEN", "")

# 置換：pickup_ok を robust に解釈する
def _parse_bool(v):
    s = str(v).strip().lower()
    # True グループ
    if s in {"1","true","t","yes","y","on","ok","〇","○","可","はい","有","可能"}:
        return True
    # False グループ
    if s in {"0","false","f","no","n","off","ng","×","✕","✖","不可","いいえ","無",""}:
        return False
    # 不明は False 扱い（必要ならログに出す）
    return False

def _load_stores_from_csv(url: str):
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    f = io.StringIO(resp.text)
    reader = csv.DictReader(f)

    stores = []
    # 行番号を出すため enumerate（ヘッダ行は1行目なので実データは2行目から）
    for row_idx, row in enumerate(reader, start=2):
        sid = (row.get("store_id") or "").strip()
        name = (row.get("name") or "").strip()
        profile = (row.get("profile") or "").strip()
        map_url = (row.get("map_url") or "").strip()
        pickup_ok_raw = row.get("pickup_ok")
        pickup_ok = _parse_bool(pickup_ok_raw)
        line_user_id = (row.get("line_user_id") or "").strip()

        # 必須: store_id, name, line_user_id
        if not sid or not name or not line_user_id:
            print(f"[STORES][SKIP] row={row_idx} sid='{sid}' name='{name}' line_user_id='{line_user_id}'  ※必須列欠落")
            continue

        # ★ここがあなたのデバッグ行（行番号も出すようにした版）
        print(f"[STORES] row={row_idx} sid={sid} name={name} pickup_ok_raw={pickup_ok_raw!r} -> {pickup_ok}")

        stores.append({
            "store_id": sid,
            "name": name,
            "profile": profile,
            "map_url": map_url,
            "pickup_ok": pickup_ok,
            "line_user_id": line_user_id
        })
    return stores


def refresh_stores():
    """環境変数のCSV URLがあれば、STORES/STORE_BY_IDを上書き"""
    global STORES, STORE_BY_ID
    if not STORES_SHEET_CSV_URL:
        print("[STORES] STORES_SHEET_CSV_URL not set; using in-code STORES")
        return
    try:
        new_stores = _load_stores_from_csv(STORES_SHEET_CSV_URL)
        if new_stores:
            STORES = new_stores
            STORE_BY_ID = {s["store_id"]: s for s in STORES}
            print(f"[STORES] Loaded {len(STORES)} stores from sheet")
        else:
            print("[STORES] Sheet had no valid rows; keeping previous list")
    except Exception as e:
        print("[STORES] Failed to load sheet:", e)

# 起動時に一度ロード（環境変数があればシートで上書き）
refresh_stores()

# 手動リロード用（token一致時のみ）
@app.route("/admin/reload_stores")
def admin_reload_stores():
    token = request.args.get("token", "")
    if not STORES_RELOAD_TOKEN or token != STORES_RELOAD_TOKEN:
        return abort(403)
    refresh_stores()
    return "ok"

# 簡易プレビュー（任意）
@app.route("/admin/stores_preview")
def admin_stores_preview():
    return {"count": len(STORES), "stores": STORES[:5]}

# 追加ここから（/admin/stores_preview の直後）
@app.route("/admin/test_push")
def admin_test_push():
    token = request.args.get("token","")
    if token != STORES_RELOAD_TOKEN:
        return abort(403)
    uid = request.args.get("uid","").strip()
    txt = request.args.get("text","TEST: store push ok?")
    if not uid:
        return "uid missing", 400
    ok = safe_push(uid, TextSendMessage(txt), "TEST")
    return "sent" if ok else "failed"

@app.route("/admin/test_push_all")
def admin_test_push_all():
    token = request.args.get("token","")
    if token != STORES_RELOAD_TOKEN:
        return abort(403)
    sent = 0
    for s in STORES:
        if safe_push(s["line_user_id"], TextSendMessage(f"TEST to {s['name']}"), s["name"]):
            sent += 1
    return f"sent {sent}/{len(STORES)}"
# 追加ここまで



# ====== 簡易セッション／リクエスト保持（メモリ） ======
SESS = {}       # user_id -> {lang,time_iso,pax,pickup,hotel, req_id}
REQUESTS = {}   # req_id -> {user_id, deadline, wanted_iso, pax, pickup, hotel, candidates:set, closed:bool}
PENDING_BOOK = {}  # user_id -> {"req_id","store_id","step", "name"}

# ====== ユーティリティ ======
def now_jst():
    return datetime.datetime.now(JST)

def next_half_hour_slots(n=6):
    """18:00開始を基本に、かつ '今から45分後以降' を最低条件として30分刻みで n 個返す"""
    t = now_jst()

    # きょうの 18:00
    today_18 = t.replace(hour=18, minute=0, second=0, microsecond=0)

    # 今から45分後（送迎などの準備時間）
    min_time = t + timedelta(minutes=45)

    # 開始時刻は  max(18:00, 今+45分)
    start_candidate = max(today_18, min_time)

    # :00 / :30 に切り上げ
    add_min = (30 - (start_candidate.minute % 30)) % 30
    start = (start_candidate + timedelta(minutes=add_min)).replace(second=0, microsecond=0)

    # 30分刻みで n 個
    slots = [start + timedelta(minutes=30*i) for i in range(n)]
    return slots


def qreply(items):
    return QuickReply(items=[QuickReplyButton(action=a) for a in items])

def lang_text(lang, jp, en):
    return jp if lang == "jp" else en

def make_req_id():
    return "REQ-" + now_jst().strftime("%Y%m%d-%H%M%S")

# --- reply→失敗時はpushへフォールバック ---
def reply_or_push(user_id, reply_token, *messages):
    msg = list(messages)
    if len(msg) == 1:
        msg = msg[0]
    try:
        line_bot_api.reply_message(reply_token, msg)
    except Exception as e:
        try:
            if user_id:
                line_bot_api.push_message(user_id, msg)
                print("[FALLBACK] reply→push", e)
            else:
                print("[FALLBACK] reply failed (no user_id)", e)
        except Exception as e2:
            print("[FALLBACK] both failed", e, e2)

# 追加ここから（reply_or_pushの直後に置く）
def safe_push(uid, message, store_name=""):
    try:
        line_bot_api.push_message(uid, message)
        print(f"[PUSH OK] {store_name} {uid}")
        return True
    except LineBotApiError as e:
        detail = getattr(e, "error", None)
        print(f"[PUSH NG] {store_name} {uid} status={getattr(e,'status_code',None)} detail={detail}")
    except Exception as e:
        print(f"[PUSH NG] {store_name} {uid} err={e}")
    return False
# 追加ここまで

# ====== Flex: 候補カード ======
def candidate_bubble(store, lang="jp"):
    title = store["name"]
    body1 = store["profile"]
    map_url = store["map_url"]

    return BubbleContainer(
        body=BoxComponent(
            layout="vertical",
            contents=[
                TextComponent(text=title, weight="bold", size="lg", wrap=True),
                TextComponent(text=body1, size="sm", wrap=True, margin="md"),
            ],
        ),
        footer=BoxComponent(
            layout="vertical",
            spacing="sm",
            contents=[
                ButtonComponent(
                    style="primary",
                    action=URIAction(label=lang_text(lang, "Googleマップ", "Google Maps"), uri=map_url)
                ),
                ButtonComponent(
                    style="link",
                    action=PostbackAction(
                        label=lang_text(lang, "この店に予約申請", "Book this place"),
                        data=json.dumps({"type": "book", "store_id": store["store_id"]})
                    )
                )
            ]
        )
    )

def schedule_timeout_notice(req_id: str):
    """締切時点で候補0件ならユーザーへ『満席でした』を自動通知してクローズ"""
    def _notify():
        req = REQUESTS.get(req_id)
        if not req or req.get("closed"):
            return
        if len(req.get("candidates", set())) == 0:
            lang = SESS.get(req["user_id"], {}).get("lang", "jp")
            jp = "現在、すべての登録店舗が満席でした。時間や人数を変えて再度お試しください。"
            en = "All registered restaurants were full for your request. Please try another time or party size."
            try:
                line_bot_api.push_message(req["user_id"], TextSendMessage(lang_text(lang, jp, en)))
            except Exception as e:
                print("timeout notice failed:", e)
        req["closed"] = True

    def _arm_timer():
        req = REQUESTS.get(req_id)
        if not req or req.get("closed"):
            return
        delay = max(0, int((req["deadline"] - now_jst()).total_seconds()))
        threading.Timer(delay, _notify).start()

    _arm_timer()

# --- 15分前リマインド（ユーザー＆店舗）
def schedule_prearrival_reminder(req_id: str):
    """予約時刻の15分前に、ユーザーと店舗へ自動リマインド（多重実行防止つき）"""
    req = REQUESTS.get(req_id)
    if not req or not req.get("confirmed"):
        return
    if req.get("reminder_scheduled"):
        return
    req["reminder_scheduled"] = True  # 予約確定時に一度だけ

    def _send():
        r = REQUESTS.get(req_id)
        if not r or not r.get("confirmed"):
            return
        user_id = r["user_id"]
        st = STORE_BY_ID.get(r.get("store_id"))
        if not st:
            return
        tstr = datetime.datetime.fromisoformat(r["wanted_iso"]).astimezone(JST).strftime("%H:%M")
        lang = SESS.get(user_id, {}).get("lang", "jp")

        # ユーザーへ
        u_msg = lang_text(
            lang,
            f"【リマインド】ご予約の15分前です。\n店舗：{st['name']}\n時間：{tstr}／{r['pax']}名\nGoogleマップ：{st['map_url']}",
            f"[Reminder] Your table is in 15 minutes.\nRestaurant: {st['name']}\nTime: {tstr} / {r['pax']} people\nGoogle Maps: {st['map_url']}",
        )
        try:
            line_bot_api.push_message(user_id, TextSendMessage(u_msg))
        except Exception as e:
            print("reminder user push failed:", e)

        # 店舗へ
        s_msg = f"【リマインド】このあと15分でご予約（{tstr}／{r['pax']}名）です。"
        try:
            line_bot_api.push_message(st["line_user_id"], TextSendMessage(s_msg))
        except Exception as e:
            print("reminder store push failed:", e)

    # 予約時刻の15分前にタイマー
    wanted_dt = datetime.datetime.fromisoformat(req["wanted_iso"]).astimezone(JST)
    fire_at = wanted_dt - timedelta(minutes=15)
    delay = max(0, int((fire_at - now_jst()).total_seconds()))
    threading.Timer(delay, _send).start()


# ====== Webhook ======
# /webhook: すべてのHTTPメソッドを許可し、まずログを出す
@app.route("/webhook", methods=["GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH"], strict_slashes=False)
def webhook():
    # --- ログ（RenderのLogsに出ます）
    try:
        print("[WEBHOOK] method=", request.method, "path=", request.path)
        # LINEのVerifyが本当にPOSTを投げているか、ここで分かります
    except Exception:
        pass

    # --- 開発中は、どのメソッドでも 200 を返して先に進む ---
    if request.method != "POST":
        return "OK"  # GET/HEAD/OPTIONS でも 200

    # POSTの場合のみ、LINE SDKへ渡す
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        # 署名不一致でも 200 返し（Verify を通しやすくする）
        return "OK", 200

    return "OK"
# ★ここから追加：起動ワードのゆらぎ吸収ユーティリティ
def _norm(s: str) -> str:
    # 全角/半角・大文字小文字・前後空白を吸収
    return unicodedata.normalize("NFKC", (s or "")).strip().lower()

def is_start_trigger(text: str) -> bool:
    s = _norm(text)
    # 単体パターン
    if s in {
        "予約をはじめる",
        "予約する",
        "start reservation",
        "reserve",
        "予約/reserve",
        "予約する/reserve",
        "予約 / reserve",
        "予約する / reserve",
    }:
        return True
    # 日英併記や区切り文字の違いを許容
    if "予約" in s and ("reserve" in s or "reservation" in s):
        return True
    return False
# ★追加ここまで

# ====== 受付：テキスト ======
@handler.add(MessageEvent, message=TextMessage)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()

    # ★暫定：店舗登録
    m = re.match(r"^店舗登録(?:\s+|　)(.+)$", text)
    if m:
        store_name = m.group(1).strip() or "未入力"
        print(f"[STORE_REG] {store_name}: {user_id}")
        reply_or_push(user_id, event.reply_token,
            TextSendMessage(f"店舗登録OK：{store_name}\nこのIDを運営に送ってください：\n{user_id}")
        )
        return

    # --- 5+ の数値入力を待っている場合 ---
    if SESS.get(user_id, {}).get("await") == "pax_number":
        m = re.match(r"^\d{1,2}$", text)
        if not m:
            reply_or_push(user_id, event.reply_token, TextSendMessage("人数を数字で入力してください（例：6）"))
            return
        SESS[user_id]["pax"] = int(text)
        SESS[user_id].pop("await", None)
        # 次へ（送迎）
        ask_pickup(event.reply_token, SESS[user_id]["lang"], user_id)
        return

    # --- ホテル名入力を待っている場合（任意） → 入力後に確認画面へ ---
    if SESS.get(user_id, {}).get("await") == "hotel_name":
        SESS[user_id]["hotel"] = text
        SESS[user_id].pop("await", None)
        ask_confirm(event.reply_token, user_id)
        return

    # トリガー（既存のキーワードでOK）
    if is_start_trigger(text):
        SESS[user_id] = {}
        ask_lang(event.reply_token, user_id)
        return

        # 予約フロー：名前・電話
    if user_id in PENDING_BOOK:
        pb = PENDING_BOOK[user_id]
        if pb["step"] == "name":
            PENDING_BOOK[user_id]["name"] = text
            PENDING_BOOK[user_id]["step"] = "phone"
            reply_or_push(user_id, event.reply_token, TextSendMessage("電話番号を入力してください（例：07012345678）"))
            return
        elif pb["step"] == "phone":
            if not re.match(r"^0\d{9,10}$", text):
                reply_or_push(user_id, event.reply_token, TextSendMessage("電話番号の形式で入力してください（例：07012345678）"))
                return
            PENDING_BOOK[user_id]["phone"] = text
            # いきなり確定せず、最終確認へ
            ask_booking_confirm(event.reply_token, user_id)
            return



    # デフォルト
    reply_or_push(user_id, event.reply_token, TextSendMessage("下のリッチメニュー「予約 / Reserve」を押して開始してください。"))

# ====== 受付：ポストバック ======
@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    try:
        data = json.loads(event.postback.data or "{}")
    except Exception:
        data = {}

    if data.get("type") == "store_reply":
        req_id   = data.get("req_id")
        status   = data.get("status")
        store_id = data.get("store_id")
        store    = STORE_BY_ID.get(store_id)

        req = REQUESTS.get(req_id)
        if not req:
            return

        # ★ガード：このボタンは該当店舗のLINE IDからの操作だけ通す
        expected_uid = store.get("line_user_id") if store else None
        if expected_uid and event.source.user_id != expected_uid:
            # お客さん等が誤って押しても無視（必要なら軽い案内を返してもOK）
            try:
                line_bot_api.push_message(
                    event.source.user_id,
                    TextSendMessage("このボタンは店舗専用です。お客さま側には操作は不要です。")
                )
            except Exception:
                pass
            return

        # すでに締切 or クローズなら店舗に案内して終了
        if now_jst() > req["deadline"] or req.get("closed"):
            safe_push(event.source.user_id, TextSendMessage("受付は終了しました（すでにマッチング済みです）。"))
            return

        if status == "ok":
            # 同一店舗の重複OKは1回だけ
            if store_id in req["candidates"]:
                safe_push(event.source.user_id, TextSendMessage("すでに送信済みです。ありがとうございます。"))
                return

            # 受付
            req["candidates"].add(store_id)

            # 店舗へ受領メッセージ
            safe_push(event.source.user_id, TextSendMessage("ありがとうございます。お客様へご案内しました。"))

            # ユーザーへ候補カードを即時送信
            if store:
                lang = SESS.get(req["user_id"], {}).get("lang", "jp")
                bubble = candidate_bubble(store, lang)
                line_bot_api.push_message(
                    req["user_id"],
                    FlexSendMessage(alt_text="候補が届きました / New option available", contents=bubble)
                )

            # 3件そろったらクローズ
            if len(req["candidates"]) >= 3:
                req["closed"] = True

        # 「不可」は静かに無視
        return

        # ★ユーザー：「この店に予約申請」→ 氏名入力へ
    if data.get("type") == "book":
        # 直近のリクエストIDを取得
        req_id = SESS.get(user_id, {}).get("req_id")
        if not req_id:
            # 念のため直近のREQUESTSから拾う（古い候補でも動くように）
            for rid, r in reversed(list(REQUESTS.items())):
                if r["user_id"] == user_id:
                    req_id = rid
                    break
        store_id = data.get("store_id")
        PENDING_BOOK[user_id] = {"req_id": req_id, "store_id": store_id, "step": "name"}
        reply_or_push(user_id, event.reply_token, TextSendMessage("お名前を入力してください"))
        return



    # ここから通常フロー
    step = data.get("step")

    if step == "lang":
        v = data.get("v", "jp")
        SESS.setdefault(user_id, {})["lang"] = v
        ask_time(event.reply_token, v, user_id)
        return

    if step == "time":
        SESS.setdefault(user_id, {})["time_iso"] = data.get("iso")
        ask_pax(event.reply_token, SESS[user_id].get("lang", "jp"), user_id)
        return

    if step == "pax":
        SESS.setdefault(user_id, {})["pax"] = int(data.get("v", 2))
        ask_pickup(event.reply_token, SESS[user_id].get("lang", "jp"), user_id)
        return

    # 5+ の分岐（ボタン押下でテキスト入力待ちへ）
    if step == "pax5plus":
        SESS.setdefault(user_id, {})["await"] = "pax_number"
        reply_or_push(user_id, event.reply_token, TextSendMessage("人数を数字で入力してください（例：6）"))
        return

    if step == "pickup":
        need = data.get("need")  # true/false
        SESS.setdefault(user_id, {})["pickup"] = bool(need)
        if need:
            # 任意でホテル名（入れなくてもOKにする）
            SESS[user_id]["await"] = "hotel_name"
            txt = lang_text(
                SESS[user_id].get("lang","jp"),
                "ホテル名をご記入ください（任意）",
                "Please enter your hotel name (optional)"
            )
            reply_or_push(user_id, event.reply_token, TextSendMessage(txt))
        else:
            # 送迎不要ならここで照会前の確認画面へ
            ask_confirm(event.reply_token, user_id)
        return

    # 照会内容の最終確認 Yes/No（照会送信前）
    if step == "confirm":
        v = data.get("v", "no")
        if v == "yes":
            # 確定 → 店舗へ一斉照会を送る
            start_inquiry(event.reply_token, user_id)
        else:
            # いいえ → 入力をリセットして言語選択からやり直し
            SESS[user_id] = {}
            ask_lang(event.reply_token, user_id)
        return

    # ★予約確定の最終確認 Yes/No（店舗選択→氏名・電話入力後）
    if step == "book_confirm":
        v = data.get("v", "no")
        pb = PENDING_BOOK.get(user_id, {})
        req = REQUESTS.get(pb.get("req_id"))
        if v == "yes":
            # 多重確定のガード
            if req and req.get("confirmed"):
                reply_or_push(
                    user_id, event.reply_token,
                    TextSendMessage(
                        lang_text(SESS.get(user_id,{}).get("lang","jp"),
                                  "すでに予約は確定しています。", "Your booking is already confirmed.")
                    )
                )
                return
            finalize_booking(event.reply_token, user_id)
        else:
            # やり直し
            SESS[user_id] = {}
            PENDING_BOOK.pop(user_id, None)
            ask_lang(event.reply_token, user_id)
        return


# ====== 質問UI ======
def ask_lang(reply_token, user_id):
    actions = [
        PostbackAction(label="日本語",  data=json.dumps({"step":"lang","v":"jp"})),
        PostbackAction(label="English", data=json.dumps({"step":"lang","v":"en"})),
    ]
    reply_or_push(
        user_id, reply_token,
        TextSendMessage("言語を選んでください / Choose your language",
                        quick_reply=qreply(actions))
    )

def ask_time(reply_token, lang, user_id):
    slots = next_half_hour_slots(6)
    actions = []
    for s in slots:
        label = s.strftime("%H:%M")
        actions.append(PostbackAction(label=label, data=json.dumps({"step":"time","iso":s.isoformat()})))
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(lang_text(lang, "ご希望の時間を選んでください", "Choose your time"),
                        quick_reply=qreply(actions))
    )

def ask_pax(reply_token, lang, user_id):
    actions = [
        PostbackAction(label="1",  data=json.dumps({"step":"pax","v":1})),
        PostbackAction(label="2",  data=json.dumps({"step":"pax","v":2})),
        PostbackAction(label="3",  data=json.dumps({"step":"pax","v":3})),
        PostbackAction(label="4",  data=json.dumps({"step":"pax","v":4})),
        PostbackAction(label="5+", data=json.dumps({"step":"pax5plus"})),
    ]
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(lang_text(lang, "人数を選んでください", "Select number of people"),
                        quick_reply=qreply(actions))
    )

def ask_pickup(reply_token, lang, user_id):
    actions = [
        PostbackAction(label=lang_text(lang, "必要", "Need"), data=json.dumps({"step":"pickup","need":True})),
        PostbackAction(label=lang_text(lang, "不要", "No"),   data=json.dumps({"step":"pickup","need":False})),
    ]
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(lang_text(lang, "送迎は必要ですか？", "Need pickup service?"),
                        quick_reply=qreply(actions))
    )

def ask_confirm(reply_token, user_id):
    sess = SESS.get(user_id, {})
    lang = sess.get("lang", "jp")

    # 表示用テキストを整形
    t_str = "-"
    try:
        if sess.get("time_iso"):
            t_str = datetime.datetime.fromisoformat(sess["time_iso"]).astimezone(JST).strftime("%H:%M")
    except Exception:
        pass
    pax = sess.get("pax", "-")
    pick = "希望" if sess.get("pickup") else "不要"
    hotel = sess.get("hotel") or "-"

    jp  = f"この内容で照会します。\n時間：{t_str}\n人数：{pax}名\n送迎：{pick}（{hotel}）\nよろしいですか？"
    en  = f"Send inquiry with:\nTime: {t_str}\nParty: {pax}\nPickup: {'Need' if sess.get('pickup') else 'No'} ({hotel})\nProceed?"
    text = lang_text(lang, jp, en)

    actions = [
        PostbackAction(label=lang_text(lang, "はい", "Yes"),
                       data=json.dumps({"step": "confirm", "v": "yes"})),
        PostbackAction(label=lang_text(lang, "いいえ", "No"),
                       data=json.dumps({"step": "confirm", "v": "no"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(text, quick_reply=qreply(actions)))

def ask_confirm(reply_token, user_id):
    sess = SESS.get(user_id, {})
    lang = sess.get("lang", "jp")

    # 表示用テキストを整形
    t_str = "-"
    try:
        if sess.get("time_iso"):
            t_str = datetime.datetime.fromisoformat(sess["time_iso"]).astimezone(JST).strftime("%H:%M")
    except Exception:
        pass
    pax = sess.get("pax", "-")
    pick = "希望" if sess.get("pickup") else "不要"
    hotel = sess.get("hotel") or "-"

    jp  = f"この内容で照会します。\n時間：{t_str}\n人数：{pax}名\n送迎：{pick}（{hotel}）\nよろしいですか？"
    en  = f"Send inquiry with:\nTime: {t_str}\nParty: {pax}\nPickup: {'Need' if sess.get('pickup') else 'No'} ({hotel})\nProceed?"
    text = lang_text(lang, jp, en)

    actions = [
        PostbackAction(label=lang_text(lang, "はい", "Yes"),
                       data=json.dumps({"step": "confirm", "v": "yes"})),
        PostbackAction(label=lang_text(lang, "いいえ", "No"),
                       data=json.dumps({"step": "confirm", "v": "no"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(text, quick_reply=qreply(actions)))

# ★ここから新規追加：予約確定の最終確認（店舗を選んで氏名・電話を入れた後）
def ask_booking_confirm(reply_token, user_id):
    """店舗決定後、氏名・電話まで受け取った後の最終予約確認"""
    pb   = PENDING_BOOK.get(user_id, {})
    req  = REQUESTS.get(pb.get("req_id"))
    st   = STORE_BY_ID.get(pb.get("store_id"))
    lang = SESS.get(user_id, {}).get("lang", "jp")

    if not req or not st or not pb.get("name") or not pb.get("phone"):
        reply_or_push(user_id, reply_token, TextSendMessage(
            lang_text(lang, "情報を取得できませんでした。最初からやり直してください。", "Session not found. Please start over.")
        ))
        return

    t_str = datetime.datetime.fromisoformat(req["wanted_iso"]).astimezone(JST).strftime("%H:%M")
    pick  = "希望" if req["pickup"] else "不要"
    hotel = req.get("hotel") or "-"

    jp = (
        f"この内容で予約を確定します。\n"
        f"店舗：{st['name']}\n"
        f"時間：{t_str}\n"
        f"人数：{req['pax']}名\n"
        f"送迎：{pick}（{hotel}）\n"
        f"お名前：{pb['name']}\n"
        f"電話：{pb['phone']}\n"
        f"よろしいですか？"
    )
    en = (
        f"Confirm booking with:\n"
        f"Restaurant: {st['name']}\n"
        f"Time: {t_str}\n"
        f"Party: {req['pax']}\n"
        f"Pickup: {'Need' if req['pickup'] else 'No'} ({hotel})\n"
        f"Name: {pb['name']}\n"
        f"Phone: {pb['phone']}\n"
        f"Proceed?"
    )

    actions = [
        PostbackAction(label=lang_text(lang, "はい", "Yes"),
                       data=json.dumps({"step":"book_confirm", "v":"yes"})),
        PostbackAction(label=lang_text(lang, "いいえ", "No"),
                       data=json.dumps({"step":"book_confirm", "v":"no"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(lang_text(lang, jp, en), quick_reply=qreply(actions)))

# ====== 照会スタート → 店舗一斉送信 ======
def start_inquiry(reply_token, user_id):
    sess = SESS.get(user_id, {})
    lang = sess.get("lang", "jp")
    req_id = make_req_id()
    deadline = now_jst() + timedelta(minutes=10)  # 最大待ち時間 10分

    REQUESTS[req_id] = {
        "user_id": user_id,
        "deadline": deadline,
        "wanted_iso": sess.get("time_iso"),
        "pax": sess.get("pax"),
        "pickup": sess.get("pickup"),
        "hotel": sess.get("hotel", ""),
        "candidates": set(),
        "closed": False,
    }
    SESS[user_id]["req_id"] = req_id

    # ユーザーへ受付メッセージ
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(lang_text(lang,
            "照会中です。最大10分、候補が届き次第表示します。",
            "Request sent. We’ll show options as they reply (up to 10 min)."))
    )

    # 店舗へ一斉送信
    wanted = datetime.datetime.fromisoformat(sess["time_iso"]).astimezone(JST).strftime("%H:%M")
    pax = sess["pax"]
    pickup_label = "希望" if sess["pickup"] else "不要"
    hotel = sess.get("hotel") or "-"
    deadline_str = deadline.strftime("%H:%M")
    remain = int((deadline - now_jst()).total_seconds() // 60)
    foreign_hint = " ※外国人（英語）" if lang == "en" else ""

    for s in STORES:
        # 送迎が必要な依頼 かつ 店舗が送迎不可なら除外
        if bool(sess.get("pickup")) and not bool(s.get("pickup_ok", False)):
            continue

        # 誤送信防止（万一店舗LINE＝お客さまのIDだった場合）
        if s["line_user_id"] == user_id:
            continue

        text = (
            f"【照会】{wanted}／{pax}名／送迎：{pickup_label}（{hotel}）{foreign_hint}\n"
            f"⏰ 締切：{deadline_str}（あと{remain}分）\n"
            f"押すだけで返信👇"
        )
        actions = [
            PostbackAction(label="OK",  data=json.dumps(
                {"type":"store_reply","req_id":req_id,"store_id":s["store_id"],"status":"ok"})),
            PostbackAction(label="不可", data=json.dumps(
                {"type":"store_reply","req_id":req_id,"store_id":s["store_id"],"status":"no"})),
        ]
        safe_push(
            s["line_user_id"],
            TextSendMessage(text=text, quick_reply=qreply(actions)),
            s["name"]
        )

    # 10分経って候補0件なら自動通知
    schedule_timeout_notice(req_id)


# ====== 予約確定 ======
def finalize_booking(reply_token, user_id):
    pb = PENDING_BOOK.get(user_id)
    if not pb:
        line_bot_api.reply_message(reply_token, TextSendMessage("セッションが見つかりませんでした。最初からやり直してください。"))
        return

    req = REQUESTS.get(pb["req_id"])
    store = STORE_BY_ID.get(pb["store_id"])
    if not req or not store:
        line_bot_api.reply_message(reply_token, TextSendMessage("予約情報を取得できませんでした。最初からやり直してください。"))
        return

    # ★重要：多重確定のガード（LINEの再送・連打対策）
    if req.get("confirmed"):
        try:
            line_bot_api.reply_message(
                reply_token,
                TextSendMessage(lang_text(SESS.get(user_id,{}).get("lang","jp"),
                    "すでに予約は確定しています。", "Your booking is already confirmed."))
            )
        except Exception:
            pass
        return

    # まず確定印をつけて以降の重複を遮断
    req["confirmed"] = True
    req["store_id"]  = pb["store_id"]
    req["name"]      = pb["name"]
    req["phone"]     = pb["phone"]
    req["closed"]    = True  # 以降の店舗OKは無視

    wanted_dt = datetime.datetime.fromisoformat(req["wanted_iso"]).astimezone(JST)
    tstr = wanted_dt.strftime("%H:%M")
    pickup_label = "希望" if req.get("pickup") else "不要"
    hotel = req.get("hotel") or "-"
    lang_code = SESS.get(user_id, {}).get("lang", "jp")
    foreign_hint = "\n※外国人のお客様（英語）" if lang_code == "en" else ""

    # --- 店舗へ確定連絡（REQなど不要情報は出さない） ---
    store_msg = (
        f"【予約確定】\n"
        f"お名前：{pb['name']}\n"
        f"電話：{pb['phone']}\n"
        f"時間：{tstr}／{req['pax']}名\n"
        f"送迎：{pickup_label}（{hotel}）"
        f"{foreign_hint}"
    )
    try:
        line_bot_api.push_message(store["line_user_id"], TextSendMessage(store_msg))
    except Exception as e:
        print("push confirm to store failed:", e)

    # --- ユーザーへ確定案内 + キャンセル注意 ---
    user_msg = lang_text(
        lang_code,
        f"ご予約が確定しました。\n"
        f"店舗：{store['name']}\n"
        f"時間：{tstr}／{req['pax']}名\n"
        f"送迎：{pickup_label}（{hotel}）\n"
        f"キャンセルは必ずお電話でお願いします。\n"
        f"Googleマップ：{store['map_url']}",
        f"Your booking is confirmed.\n"
        f"Restaurant: {store['name']}\n"
        f"Time: {tstr} / {req['pax']} people\n"
        f"Pickup: {'Need' if req.get('pickup') else 'No'} ({hotel})\n"
        f"For cancellation, please call the restaurant.\n"
        f"Google Maps: {store['map_url']}"
    )
    try:
        line_bot_api.reply_message(reply_token, TextSendMessage(user_msg))
    except Exception as e:
        # 返信失敗時のみpush（成功時は二重送信しない）
        try:
            line_bot_api.push_message(user_id, TextSendMessage(user_msg))
            print("[FALLBACK] confirm reply→push:", e)
        except Exception as e2:
            print("[FALLBACK] confirm both failed:", e, e2)

    # --- 15分前リマインドをセット（多重防止つき） ---
    schedule_prearrival_reminder(pb["req_id"])

    # 後片付け
    PENDING_BOOK.pop(user_id, None)




