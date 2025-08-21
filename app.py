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
        "pickup_point": "",             # 任意（使っているなら残す）
        "instagram_url": "",            # ★追加（空でもOK）
        "line_user_id": "UXXXXXXXXXXXXXXX"
    },
    {
        "store_id": "ST2",
        "name": "居酒屋 B",
        "profile": "地魚と泡盛。21:30 L.O.",
        "map_url": "https://goo.gl/maps/yyyyyyyyyyyyy",
        "pickup_ok": False,
        "pickup_point": "",             # 任意
        "instagram_url": "",            # ★追加
        "line_user_id": "UYYYYYYYYYYYYYYY"
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
    for row in reader:
        sid         = (row.get("store_id") or "").strip()
        name        = (row.get("name") or "").strip()
        profile     = (row.get("profile") or "").strip()
        map_url     = (row.get("map_url") or "").strip()
        pickup_ok   = _parse_bool(row.get("pickup_ok"))
        # ★ここでInstagram列を読む（無ければ空文字）
        instagram_url = (row.get("instagram_url") or "").strip()
        # （すでに運用しているなら pickup_point もここで読む想定）
        pickup_point  = (row.get("pickup_point") or "").strip()
        line_user_id  = (row.get("line_user_id") or "").strip()

        # 必須: store_id, name, line_user_id
        if not sid or not name or not line_user_id:
            continue

        # デバッグログ（任意）
        try:
            print(f"[STORES] load: sid={sid} name={name} pickup_ok_raw={row.get('pickup_ok')} -> {pickup_ok} ig={instagram_url[:40]}")
        except Exception:
            pass

        stores.append({
            "store_id": sid,
            "name": name,
            "profile": profile,
            "map_url": map_url,
            "pickup_ok": pickup_ok,
            "pickup_point": pickup_point,       # 既に使っている場合は残す
            "instagram_url": instagram_url,     # ★追加
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

def next_half_hour_slots(count: int = 6, must_be_after: datetime.datetime | None = None):
    """
    18:00〜22:00 の間で 30分刻みの候補を返す。
    かつ 'must_be_after'（例: 現在+45分）以降を最低条件にする。
    """
    now = now_jst()

    # きょうの 18:00 と 22:00（JST）
    start_of_window = now.replace(hour=18, minute=0, second=0, microsecond=0)
    end_of_window   = now.replace(hour=22, minute=0, second=0, microsecond=0)

    # “今+45分”などの条件と、18:00 を比較して遅い方から開始
    min_start = must_be_after or (now + timedelta(minutes=45))
    start_candidate = max(start_of_window, min_start)

    # :00 / :30 に **切り上げ**
    add_min = (30 - (start_candidate.minute % 30)) % 30
    first = (start_candidate + timedelta(minutes=add_min)).replace(second=0, microsecond=0)

    # 30分刻みで count 個。ただし 22:00 を**超えない**よう制限
    slots = []
    cur = first
    while len(slots) < count and cur <= end_of_window:
        slots.append(cur)
        cur = cur + timedelta(minutes=30)

    return slots


def qreply(items):
    return QuickReply(items=[QuickReplyButton(action=a) for a in items])

def lang_text(lang, jp, en):
    return jp if lang == "jp" else en

def bi(jp: str, en: str) -> str:
    """日本語 + 英語を1通にまとめる（改行区切り）"""
    return f"{jp}\n{en}"


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

def service_window_state(now: datetime.datetime | None = None) -> str:
    """
    受付時間の状態を返す:
      - "before16" … 16:00 前（受付前）
      - "inside"   … 16:00〜22:00（受付中）
      - "after22"  … 22:00 以降（受付終了）
    """
    now = now or now_jst()
    now = now.astimezone(JST)
    start = now.replace(hour=16, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if now < start:
        return "before16"
    if now >= end:
        return "after22"
    return "inside"


# --- phone helpers (ADD just below reply_or_push) ---
def _clean_phone(s: str) -> str:
    # スペース・ハイフン・括弧などを除去（+ と数字だけ残す）
    return re.sub(r"[^\d\+]", "", (s or "").strip())

def _valid_phone(s: str, lang: str) -> bool:
    s = _clean_phone(s)
    if lang == "en":
        # 国番号つき（+から始まり 6〜15桁）
        return bool(re.match(r"^\+\d{6,15}$", s))
    else:
        # 国内携帯/固定（0 で始まり 10〜11桁）
        return bool(re.match(r"^0\d{9,10}$", s))


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
    title   = store.get("name", "")
    body1   = store.get("profile", "")
    map_url = store.get("map_url", "")
    ig_url  = (store.get("instagram_url") or "").strip()  # ← シートに無くてもOK（空ならボタン非表示）

    # --- フッタボタンを配列で組み立て（後から条件で差し込む） ---
    footer_buttons = [
        ButtonComponent(
            style="primary",
            action=URIAction(
                label=lang_text(lang, "Googleマップ", "Google Maps"),
                uri=map_url or "https://maps.google.com"  # map_urlが空でも落ちないように保険
            )
        )
    ]

    # Instagram が設定されている店だけ Instagram ボタンを追加
    if ig_url:
        footer_buttons.append(
            ButtonComponent(
                style="secondary",
                action=URIAction(
                    # ブランド名として英語固定でOK。日本語にしたいなら lang_text に変えてください。
                    label="Instagram",
                    uri=ig_url
                )
            )
        )

    # 予約申請ボタン（従来どおり）
    footer_buttons.append(
        ButtonComponent(
            style="link",
            action=PostbackAction(
                label=lang_text(lang, "この店に予約申請", "Book this place"),
                data=json.dumps({"type": "book", "store_id": store.get("store_id")})
            )
        )
    )

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
            contents=footer_buttons
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

# --- 15分前リマインド（ユーザー＆店舗） ← ここを置き換え
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

        # 表示用
        wanted_dt = datetime.datetime.fromisoformat(r["wanted_iso"]).astimezone(JST)
        tstr  = wanted_dt.strftime("%H:%M")
        pax   = r["pax"]
        hotel = r.get("hotel") or "-"
        lang  = SESS.get(user_id, {}).get("lang", "jp")
        pickup = bool(r.get("pickup"))

        # 強い警告（送迎あり/なし・日英で分岐）
        jp_warn_pick = (
            "⚠️ 必ず時間までに『集合場所』へお越しください。\n"
            "⏰ 遅れる場合は “予約時間の15分前まで” に必ずお店へお電話を！\n"
            "🚫 連絡なしの遅刻・不着は『予約キャンセル』になります。"
        )
        jp_warn_nopick = (
            "⚠️ 必ず『予約時間までにご来店』ください。\n"
            "⏰ 遅れる場合は “予約時間の15分前まで” に必ずお店へお電話を！\n"
            "🚫 連絡なしの遅刻は『予約キャンセル』になります。"
        )
        en_warn_pick = (
            "⚠️ Please be at the PICKUP POINT ON TIME.\n"
            "⏰ If you will be late, CALL the restaurant at least 15 minutes before your time.\n"
            "🚫 No-show or late without notice will be CANCELLED."
        )
        en_warn_nopick = (
            "⚠️ Please arrive at the RESTAURANT ON TIME.\n"
            "⏰ If you will be late, CALL the restaurant at least 15 minutes before your time.\n"
            "🚫 No-show or late without notice will be CANCELLED."
        )

        # ユーザーへ（言語別・送迎明記・強調警告つき）
        if lang == "jp":
            user_msg = (
                "【リマインド】このあと15分でご予約です。\n"
                f"店舗：{st['name']}\n"
                f"時間：{tstr}／{pax}名\n"
                f"送迎：{'希望' if pickup else '不要'}（{hotel}）\n"
                f"Googleマップ：{st['map_url']}\n\n" +
                (jp_warn_pick if pickup else jp_warn_nopick)
            )
        else:
            user_msg = (
                "[Reminder] Your table is in 15 minutes.\n"
                f"Restaurant: {st['name']}\n"
                f"Time: {tstr} / {pax} people\n"
                f"Pickup: {'Need' if pickup else 'No'} ({hotel})\n"
                f"Google Maps: {st['map_url']}\n\n" +
                (en_warn_pick if pickup else en_warn_nopick)
            )
        try:
            line_bot_api.push_message(user_id, TextSendMessage(user_msg))
        except Exception as e:
            print("reminder user push failed:", e)

        # 店舗へ（誰の予約か分かる詳細＋外国人フラグ）
        store_msg = (
            "【15分前リマインド】\n"
            f"お名前：{r.get('name','-')}\n"
            f"電話：{r.get('phone','-')}\n"
            f"時間：{tstr}／{pax}名\n"
            f"送迎：{'希望' if pickup else '不要'}（{hotel}）"
        )
        if lang == "en":
            store_msg += "\n※外国人のお客様（英語）"
        try:
            line_bot_api.push_message(st["line_user_id"], TextSendMessage(store_msg))
        except Exception as e:
            print("reminder store push failed:", e)

    # 予約時刻の15分前にタイマー
    wanted_dt = datetime.datetime.fromisoformat(req["wanted_iso"]).astimezone(JST)
    fire_at = wanted_dt - timedelta(minutes=15)
    delay = max(0, int((fire_at - now_jst()).total_seconds()))
    threading.Timer(delay, _send).start()


# ====== Webhook ======
# /webhook: すべてのHTTPメソッドを許可し、まずログを出す
@app.route(
    "/webhook",
    methods=["GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH"],
    strict_slashes=False
)
def webhook():
    # --- ログ（RenderのLogsに出ます）
    try:
        print("[WEBHOOK] method=", request.method, "path=", request.path)
    except Exception:
        pass

    # --- POST 以外は 200 返して終了（LINEの疎通確認対策）
    if request.method != "POST":
        return "OK"

    # POST のみ LINE SDK で処理
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        # 署名不一致でも 200 返し（Verify を通しやすくする）
        return "OK", 200

    return "OK"


# Web サーバ時刻の確認用（JST とウィンドウ判定を可視化）
@app.route("/admin/timecheck")
def admin_timecheck():
    t = now_jst()
    return {
        "now_jst": t.isoformat(),
        "service_state": service_window_state(t),
        "note": "JST基準。state=before16/inside/after22"
    }


# ★ここから追加：起動ワードのゆらぎ吸収ユーティリティ
def _norm(s: str) -> str:
    # 全角/半角・大文字小文字・前後空白を吸収
    return unicodedata.normalize("NFKC", (s or "")).strip().lower()

def is_start_trigger(text: str) -> bool:
    s = _norm(text)
    if s in {"予約をはじめる","予約する","予約をする","start reservation","reserve",
             "予約/reserve","予約する/reserve","予約 / reserve","予約する / reserve"}:
        return True
    if "予約" in s and ("reserve" in s or "reservation" in s):
        return True
    return False


    
    # 日英併記や区切り文字の違いを許容
    if "予約" in s and ("reserve" in s or "reservation" in s):
        return True
    return False
# ★追加ここまで

@handler.add(MessageEvent, message=TextMessage)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()

    # ★暫定：店舗登録
    m = re.match(r"^店舗登録(?:\s+|　)(.+)$", text)
    if m:
        store_name = m.group(1).strip() or "未入力"
        print(f"[STORE_REG] {store_name}: {user_id}")
        reply_or_push(
            user_id, event.reply_token,
            TextSendMessage(f"店舗登録OK：{store_name}\nこのIDを運営に送ってください：\n{user_id}")
        )
        return

    # 5+ の数値入力待ち
    if SESS.get(user_id, {}).get("await") == "pax_number":
        m = re.match(r"^\d{1,2}$", text)
        if not m:
            reply_or_push(user_id, event.reply_token, TextSendMessage("人数を数字で入力してください（例：6）"))
            return
        SESS[user_id]["pax"] = int(text)
        SESS[user_id].pop("await", None)
        ask_pickup(event.reply_token, SESS[user_id]["lang"], user_id)
        return

    # ホテル名入力待ち（任意）→ 入力後に照会前の確認へ
    if SESS.get(user_id, {}).get("await") == "hotel_name":
        SESS[user_id]["hotel"] = text
        SESS[user_id].pop("await", None)
                # ★編集モードなら解除して確認へ
        if SESS.get(user_id, {}).get("edit_mode") == "hotel":
            SESS[user_id].pop("edit_mode", None)
            ask_confirm(event.reply_token, user_id)
            return

        ask_confirm(event.reply_token, user_id)
        return

# 起動ワード（常に最初からやり直し）
    if is_start_trigger(text):
        SESS[user_id] = {}
        PENDING_BOOK.pop(user_id, None)  # ★追加：途中までの予約入力も破棄
        ask_lang(event.reply_token, user_id)
        return


    # 予約フロー：氏名→電話→編集
    if user_id in PENDING_BOOK:
        pb   = PENDING_BOOK[user_id]
        lang = SESS.get(user_id, {}).get("lang", "jp")

        # --- 1) 氏名入力直後：電話を促す ---
        if pb["step"] == "name":
            PENDING_BOOK[user_id]["name"] = (text or "").strip()
            PENDING_BOOK[user_id]["step"] = "phone"
            msg = (
                "電話番号を入力してください（例：07012345678）"
                if lang == "jp"
                else "Please enter your phone number with country code (e.g., +81 7012345678)."
            )
            reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
            return

        # --- 2) 電話番号の入力・検証 ---
        elif pb["step"] == "phone":
            t = (text or "").strip()
            if not _valid_phone(t, lang):
                msg = (
                    "電話番号の形式で入力してください（例：07012345678）"
                    if lang == "jp"
                    else "Please enter a valid number (e.g., +81 7012345678)."
                )
                reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
                return
            PENDING_BOOK[user_id]["phone"] = _clean_phone(t)
            PENDING_BOOK[user_id]["step"]  = "idle"
            ask_booking_confirm(event.reply_token, user_id)
            return

        # --- 3) 編集：氏名のみ修正 ---
        elif pb["step"] == "edit_name":
            PENDING_BOOK[user_id]["name"] = (text or "").strip()
            PENDING_BOOK[user_id]["step"] = "idle"
            ask_booking_confirm(event.reply_token, user_id)
            return

        # --- 4) 編集：電話のみ修正 ---
        elif pb["step"] == "edit_phone":
            t = (text or "").strip()
            if not _valid_phone(t, lang):
                msg = (
                    "電話番号の形式で入力してください（例：07012345678）"
                    if lang == "jp"
                    else "Please enter a valid number (e.g., +81 7012345678)."
                )
                reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
                return
            PENDING_BOOK[user_id]["phone"] = _clean_phone(t)
            PENDING_BOOK[user_id]["step"]  = "idle"
            ask_booking_confirm(event.reply_token, user_id)
            return

            
            t = (text or "").strip()
            if not _valid_phone(t, lang):
                if lang == "en":
                    reply_or_push(
                        user_id, event.reply_token,
                        TextSendMessage("Please enter a valid phone number with country code (e.g., +81 7012345678).")
                    )
                else:
                    reply_or_push(
                        user_id, event.reply_token,
                        TextSendMessage("電話番号の形式で入力してください（例：07012345678）")
                    )
                return
            PENDING_BOOK[user_id]["phone"] = _clean_phone(t)
            # 氏名・電話まで揃ったので最終予約確認へ
            ask_booking_confirm(event.reply_token, user_id)
            return

    # デフォルト応答
    reply_or_push(
        user_id, event.reply_token,
        TextSendMessage("下のリッチメニュー「予約 / Reserve」を押して開始してください。")
    )

# ====== 受付：ポストバック ======
@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    try:
        data = json.loads(event.postback.data or "{}")
    except Exception:
        data = {}

    # --- 店舗側からの回答（OK/不可）
    if data.get("type") == "store_reply":
        req_id   = data.get("req_id")
        status   = data.get("status")
        store_id = data.get("store_id")
        store    = STORE_BY_ID.get(store_id)
        req      = REQUESTS.get(req_id)
        if not req:
            return

        # このボタンは該当店舗のLINE IDのみ有効
        expected_uid = store.get("line_user_id") if store else None
        if expected_uid and event.source.user_id != expected_uid:
            # 店舗以外が押したら無視
            return

        # 受付終了 or クローズ
        if now_jst() > req["deadline"] or req.get("closed"):
            safe_push(event.source.user_id, TextSendMessage("受付は終了しました（すでにマッチング済みです）。"))
            return

        if status == "ok":
            # 同一店舗の重複は1回だけ
            if store_id in req["candidates"]:
                safe_push(event.source.user_id, TextSendMessage("すでに送信済みです。ありがとうございます。"))
                return

            req["candidates"].add(store_id)
            # 店舗へ受領メッセージ
            safe_push(event.source.user_id, TextSendMessage("ありがとうございます。お客様へご案内しました。"))

            # ユーザーへ候補カード
            if store:
                lang = SESS.get(req["user_id"], {}).get("lang", "jp")
                bubble = candidate_bubble(store, lang)
                line_bot_api.push_message(
                    req["user_id"],
                    FlexSendMessage(alt_text="候補が届きました / New option available", contents=bubble)
                )

            # 3件集まったらクローズ
            if len(req["candidates"]) >= 3:
                req["closed"] = True
        # 「不可」は静かに無視
        return

    # --- ユーザー：「この店に予約申請」→ 氏名入力へ
    if data.get("type") == "book":
        # 直近のリクエストIDを取得（なければ直近のREQUESTSから拾う）
        req_id = SESS.get(user_id, {}).get("req_id")
        if not req_id:
            for rid, r in reversed(list(REQUESTS.items())):
                if r["user_id"] == user_id:
                    req_id = rid
                    break

        store_id = data.get("store_id")
        PENDING_BOOK[user_id] = {"req_id": req_id, "store_id": store_id, "step": "name"}

        lang = SESS.get(user_id, {}).get("lang", "jp")
        msg = ("お名前を入力してください（フルネーム）"
               if lang == "jp"
               else "Please enter your full name (alphabet).")
        reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
        return

    # --- 通常のステップ処理 ---
    step = data.get("step")

    if step == "lang":
        v = data.get("v", "jp")
        SESS.setdefault(user_id, {})["lang"] = v

        # 受付時間チェック（日本語＋英語の両方を1通で案内）
        state = service_window_state()
        if state == "before16":
            jp = "ただいま準備中のため、予約受付は16:00からです。16:00以降にお試しください。"
            en = "We're preparing for service. Reservations open at 16:00. Please try again after 16:00."
            reply_or_push(user_id, event.reply_token, TextSendMessage(bi(jp, en)))
            return
        if state == "after22":
            jp = "本日の予約受付は終了しました。22:00以降は、明日以降の日時でご予約ください。"
            en = "Today's reservation window has closed. After 22:00, please book for tomorrow or a later date."
            reply_or_push(user_id, event.reply_token, TextSendMessage(bi(jp, en)))
            return

        # 受付中 → 時間選択へ（18:00〜22:00、かつ今から45分以降のみ）
        ask_time(event.reply_token, v, user_id)
        return

    # === ここから追記：時間 → 人数 → 送迎（3引数版） ===

    # ① 時間が選ばれた
    if step == "time":
        iso = data.get("iso")
        if iso:
            SESS.setdefault(user_id, {})["time_iso"] = iso
                    # ★編集モードなら連鎖質問せず、確認画面に戻す
        if SESS.get(user_id, {}).get("edit_mode") == "time":
            SESS[user_id].pop("edit_mode", None)
            ask_confirm(event.reply_token, user_id)
            return

        # 言語はセッションから（無ければJP）
        lang = SESS.get(user_id, {}).get("lang", "jp")
        ask_pax(event.reply_token, lang, user_id)
        return

    # ② 人数が選ばれた（1〜4名 or 5名以上）
    if step == "pax":
        v = data.get("v")
        lang = SESS.get(user_id, {}).get("lang", "jp")

        # 5名以上は手入力へ誘導（旧UI互換で "5plus"/"5+" どちらでもOK）
        if v in ("5plus", "5+"):
            SESS.setdefault(user_id, {})["await"] = "pax_number"
            reply_or_push(
                user_id, event.reply_token,
                TextSendMessage(lang_text(
                    lang,
                    "人数を数字で入力してください（例：6）",
                    "Please enter the number of people (e.g., 6)."
                ))
            )
            return

        # 1〜4を数値として保持（失敗時はデフォルト2）
        try:
            SESS.setdefault(user_id, {})["pax"] = int(v)
        except Exception:
            SESS.setdefault(user_id, {})["pax"] = 2
                    # ★編集モードなら確認画面へ戻す
        if SESS.get(user_id, {}).get("edit_mode") == "pax":
            SESS[user_id].pop("edit_mode", None)
            ask_confirm(event.reply_token, user_id)
            return


        ask_pickup(event.reply_token, lang, user_id)
        return

   # ③ 送迎の要否が選ばれた
    if step == "pickup":
        need = (data.get("v") == "yes") or (data.get("need") is True)
        sess = SESS.setdefault(user_id, {})
        sess["pickup"] = bool(need)

        lang = sess.get("lang", "jp")
        if need:
            # ★送迎あり：通常どおりホテル名を聞く
            sess["await"] = "hotel_name"
            msg = "ホテル名をご記入ください。" if lang == "jp" else "Please enter your hotel name."
            reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
        else:
            # 送迎なし：ホテル消去。編集モードなら即確認へ
            sess["hotel"] = ""
            if sess.get("edit_mode") == "pickup":
                sess.pop("edit_mode", None)
                ask_confirm(event.reply_token, user_id)
            else:
                ask_confirm(event.reply_token, user_id)
        return


    # === 追記ここまで ===




    # 照会内容の最終確認（照会送信前）
    if step == "confirm":
        v = data.get("v", "no")
        if v == "yes":
            start_inquiry(event.reply_token, user_id)
        else:
            SESS[user_id] = {}
            ask_lang(event.reply_token, user_id)
        return
        # ★追記：照会前の編集メニュー表示
    if step == "edit_request_menu":
        ask_edit_request_menu(event.reply_token, user_id)
        return

    # ★追記：どの項目を直すか
    if step == "edit_request":
        target = data.get("target")
        lang = SESS.get(user_id, {}).get("lang", "jp")
        sess = SESS.setdefault(user_id, {})

        if target == "time":
            sess["edit_mode"] = "time"
            ask_time(event.reply_token, lang, user_id)
            return
        if target == "pax":
            sess["edit_mode"] = "pax"
            ask_pax(event.reply_token, lang, user_id)
            return
        if target == "pickup":
            sess["edit_mode"] = "pickup"
            ask_pickup(event.reply_token, lang, user_id)
            return
        if target == "hotel":
            sess["edit_mode"] = "hotel"
            sess["await"] = "hotel_name"
            reply_or_push(user_id, event.reply_token,
                          TextSendMessage(lang_text(lang, "ホテル名をご記入ください。", "Please enter your hotel name.")))
            return
        # back
        ask_confirm(event.reply_token, user_id)
        return


    # 予約確定の最終確認（店舗選択→氏名・電話入力後）
    if step == "book_confirm":
        v = data.get("v", "no")
        pb = PENDING_BOOK.get(user_id, {})
        req = REQUESTS.get(pb.get("req_id"))
        if v == "yes":
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
            SESS[user_id] = {}
            PENDING_BOOK.pop(user_id, None)
            ask_lang(event.reply_token, user_id)
        return
        
        # ★氏名/電話どちらを直すかのメニュー表示
    if step == "edit_personal_menu":
        ask_edit_personal_menu(event.reply_token, user_id)
        return

    # ★氏名/電話のどちらを編集するか選択 → 入力待ちへ
    if step == "edit_personal":
        target = data.get("target")
        lang = SESS.get(user_id, {}).get("lang", "jp")
        if target == "name":
            PENDING_BOOK.setdefault(user_id, {})["step"] = "edit_name"
            msg = "正しいお名前を入力してください。" if lang == "jp" else "Please enter your full name."
            reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
            return
        if target == "phone":
            PENDING_BOOK.setdefault(user_id, {})["step"] = "edit_phone"
            msg = ("電話番号を入力してください（例：07012345678）"
                   if lang == "jp"
                   else "Please enter your phone number with country code (e.g., +81 7012345678).")
            reply_or_push(user_id, event.reply_token, TextSendMessage(msg))
            return
        # 修正なし → 確認に戻す
        ask_booking_confirm(event.reply_token, user_id)
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
    
# （ここは def ask_lang(...) の直後に置く）
def ask_time(reply_token, lang, user_id):
    """
    16:00 受付開始 / 予約時間帯 18:00–22:00 に合わせて
    スロットを提示。受け付けは言語別メッセージでガイド。
    """
    # ← ここを修正： service_window_state() の返り値に合わせて正しく分岐
    state = service_window_state()  # "before16" / "inside" / "after22"

    if state == "before16":
        jp = "ただいま準備中のため、予約受付は16:00からです。16:00以降にお試しください。"
        en = "We're preparing for service. Reservations open at 16:00. Please try again after 16:00."
        reply_or_push(user_id, reply_token, TextSendMessage(lang_text(lang, jp, en)))
        return

    if state == "after22":
        jp = "本日の予約受付は終了しました。22:00以降は、明日以降の日時でご予約ください。"
        en = "Today's booking window has closed. After 22:00, please book for tomorrow or a later date."
        reply_or_push(user_id, reply_token, TextSendMessage(lang_text(lang, jp, en)))
        return

    # ここに来たら "inside"（受付中）なので、時間スロットを提示
    slots = next_half_hour_slots(
        count=8,
        must_be_after=now_jst() + timedelta(minutes=45)
    )
    actions = []
    for s in slots:
        label = s.strftime("%H:%M")
        actions.append(PostbackAction(
            label=label,
            data=json.dumps({"step": "time", "iso": s.isoformat()})
        ))
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(
            lang_text(lang, "ご希望の時間を選んでください", "Choose your time"),
            quick_reply=qreply(actions)
        )
    )


def ask_pax(reply_token, lang, user_id):
    """人数を聞く（1〜4はボタン、5名以上は手入力へ誘導）"""
    # クイックリプライ（1〜4名 + 5名以上）
    actions = [
        PostbackAction(label=lang_text(lang, "1名", "1"),
                       data=json.dumps({"step": "pax", "v": 1})),
        PostbackAction(label=lang_text(lang, "2名", "2"),
                       data=json.dumps({"step": "pax", "v": 2})),
        PostbackAction(label=lang_text(lang, "3名", "3"),
                       data=json.dumps({"step": "pax", "v": 3})),
        PostbackAction(label=lang_text(lang, "4名", "4"),
                       data=json.dumps({"step": "pax", "v": 4})),
        PostbackAction(label=lang_text(lang, "5名以上", "5+"),
                       data=json.dumps({"step": "pax", "v": "5plus"})),
    ]
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(
            lang_text(lang, "人数を選んでください", "How many people?"),
            quick_reply=qreply(actions)
        )
    )

def ask_pickup(reply_token, lang, user_id):
    """送迎の要否を聞く（Yes/No）。このあとホテル名の任意入力へ"""
    actions = [
        PostbackAction(label=lang_text(lang, "希望", "Need"),
                       data=json.dumps({"step": "pickup", "v": "yes"})),
        PostbackAction(label=lang_text(lang, "不要", "No"),
                       data=json.dumps({"step": "pickup", "v": "no"})),
    ]
    reply_or_push(
        user_id, reply_token,
        TextSendMessage(
            lang_text(lang, "送迎は必要ですか？", "Do you need pickup?"),
            quick_reply=qreply(actions)
        )
    )

def ask_confirm(reply_token, user_id):
    """照会送信前の最終確認（時間・人数・送迎・ホテルを表示）
       → 送信 / 編集メニュー / 最初から
    """
    sess = SESS.get(user_id, {})
    lang = sess.get("lang", "jp")
    if not sess.get("time_iso") or not sess.get("pax"):
        reply_or_push(user_id, reply_token, TextSendMessage(
            lang_text(lang, "情報が不足しています。最初からやり直してください。", "Session missing. Please start over.")
        ))
        return

    t_str = datetime.datetime.fromisoformat(sess["time_iso"]).astimezone(JST).strftime("%H:%M")
    pick  = "希望" if sess.get("pickup") else "不要"
    hotel = sess.get("hotel") or "-"

    jp = (f"この内容で照会します。\n"
          f"時間：{t_str}\n人数：{sess['pax']}名\n送迎：{pick}（{hotel}）\n\n"
          "よろしければ『照会を送る』を押してください。")
    en = (f"We will inquire with:\n"
          f"Time: {t_str}\nParty: {sess['pax']}\nPickup: {'Need' if sess.get('pickup') else 'No'} ({hotel})\n\n"
          "If OK, tap “Send request”.")

    actions = [
        PostbackAction(label=lang_text(lang, "照会を送る", "Send request"),
                       data=json.dumps({"step":"confirm","v":"yes"})),
        PostbackAction(label=lang_text(lang, "内容を修正", "Edit details"),
                       data=json.dumps({"step":"edit_request_menu"})),
        PostbackAction(label=lang_text(lang, "最初から", "Start over"),
                       data=json.dumps({"step":"confirm","v":"no"})),
    ]
    reply_or_push(user_id, reply_token, TextSendMessage(lang_text(lang, jp, en), quick_reply=qreply(actions)))
# ★ここから追加：時間/人数/送迎/ホテルのどれを直すか
def ask_edit_request_menu(reply_token, user_id):
    lang = SESS.get(user_id, {}).get("lang", "jp")
    jp = "どこを修正しますか？"
    en = "What would you like to edit?"
    actions = [
        PostbackAction(label=lang_text(lang, "時間を修正", "Edit time"),
                       data=json.dumps({"step":"edit_request","target":"time"})),
        PostbackAction(label=lang_text(lang, "人数を修正", "Edit party"),
                       data=json.dumps({"step":"edit_request","target":"pax"})),
        PostbackAction(label=lang_text(lang, "送迎を修正", "Edit pickup"),
                       data=json.dumps({"step":"edit_request","target":"pickup"})),
        PostbackAction(label=lang_text(lang, "ホテル名を修正", "Edit hotel"),
                       data=json.dumps({"step":"edit_request","target":"hotel"})),
        PostbackAction(label=lang_text(lang, "修正なし（戻る）", "No change (back)"),
                       data=json.dumps({"step":"edit_request","target":"back"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(lang_text(lang, jp, en), quick_reply=qreply(actions)))
# ★ここまで追加


# ★ここから新規置換：店舗決定＋氏名/電話入力後の最終確認（編集メニュー付き）
def ask_booking_confirm(reply_token, user_id):
    """店舗決定後、氏名・電話まで受け取った後の最終予約確認
       → 予約確定 / 氏名だけ直す / 電話だけ直す / やめる
    """
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

    # 見やすく改行
    jp = (
        "【入力情報の確認】\n"
        f"店舗：{st['name']}\n"
        f"時間：{t_str}\n"
        f"人数：{req['pax']}名\n"
        f"送迎：{pick}（{hotel}）\n"
        f"お名前：{pb['name']}\n"
        f"電話：{pb['phone']}\n\n"
        "この内容でよろしければ「予約確定」を押してください。"
    )
    en = (
        "[Please review your details]\n"
        f"Restaurant: {st['name']}\n"
        f"Time: {t_str}\n"
        f"Party: {req['pax']}\n"
        f"Pickup: {'Need' if req['pickup'] else 'No'} ({hotel})\n"
        f"Name: {pb['name']}\n"
        f"Phone: {pb['phone']}\n\n"
        "If everything looks good, tap “Confirm booking”."
    )

    actions = [
        # 予約確定（従来のYes）
        PostbackAction(label=lang_text(lang, "予約確定", "Confirm booking"),
                       data=json.dumps({"step":"book_confirm","v":"yes"})),
        # 氏名/電話の片方だけ直すメニューへ
        PostbackAction(label=lang_text(lang, "氏名/電話を修正", "Edit name/phone"),
                       data=json.dumps({"step":"edit_personal_menu"})),
        # 取り消して最初から
        PostbackAction(label=lang_text(lang, "やめる", "Cancel"),
                       data=json.dumps({"step":"book_confirm","v":"no"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(lang_text(lang, jp, en), quick_reply=qreply(actions)))
# ★ここまで置換

# ★ここから追加：氏名/電話のどちらを修正するか選ばせる
def ask_edit_personal_menu(reply_token, user_id):
    lang = SESS.get(user_id, {}).get("lang", "jp")
    jp = "どちらを修正しますか？"
    en = "What would you like to edit?"
    actions = [
        PostbackAction(label=lang_text(lang, "名前を修正", "Edit name"),
                       data=json.dumps({"step":"edit_personal","target":"name"})),
        PostbackAction(label=lang_text(lang, "電話を修正", "Edit phone"),
                       data=json.dumps({"step":"edit_personal","target":"phone"})),
        PostbackAction(label=lang_text(lang, "修正なし（戻る）", "No change (back)"),
                       data=json.dumps({"step":"edit_personal","target":"back"})),
    ]
    reply_or_push(user_id, reply_token,
                  TextSendMessage(lang_text(lang, jp, en), quick_reply=qreply(actions)))
# ★ここまで追加


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

    # --- ユーザーへ確定案内 + バックれ防止の強い注意書き（言語＆送迎で分岐） ---
    if lang_code == "jp":
        if req.get("pickup"):
            # 送迎あり：集合場所へ
            warning = (
                "\n\n🚨🚨🚨 重要なお知らせ（ドタキャン防止） 🚨🚨🚨\n"
                "必ず **予約時間までに集合場所へ** お越しください。\n"
                "もし間に合わない場合は、**予約時刻の15分前までに必ずお店へお電話**ください。\n"
                "連絡なしの遅刻・不着は、❌ **予約は自動キャンセル** となります。\n"
                "ご協力をお願いいたします！🙏"
            )
        else:
            # 店舗に直接来店
            warning = (
                "\n\n🚨🚨🚨 重要なお知らせ（ドタキャン防止） 🚨🚨🚨\n"
                "必ず **予約時間までにご来店** ください。\n"
                "もし間に合わない場合は、**予約時刻の15分前までに必ずお店へお電話**ください。\n"
                "連絡なしで来店されない場合は、❌ **予約は自動キャンセル** となります。\n"
                "ご理解とご協力をお願いいたします！🙏"
            )

        user_msg = (
            f"ご予約が確定しました。\n"
            f"店舗：{store['name']}\n"
            f"時間：{tstr}／{req['pax']}名\n"
            f"送迎：{pickup_label}（{hotel}）\n"
            f"Googleマップ：{store['map_url']}"
            f"{warning}\n"
            f"\n※キャンセル・変更は必ずお電話でお願いします。"
        )
    else:
        # English
        if req.get("pickup"):
            warning = (
                "\n\n🚨🚨🚨 IMPORTANT (No-show prevention) 🚨🚨🚨\n"
                "Please **be at the meeting point by your reservation time**.\n"
                "If you’re running late, **call the restaurant at least 15 minutes before** your time.\n"
                "Without contact, your booking may be **automatically cancelled** ❌.\n"
                "Thank you for your cooperation! 🙏"
            )
        else:
            warning = (
                "\n\n🚨🚨🚨 IMPORTANT (No-show prevention) 🚨🚨🚨\n"
                "Please **arrive at the restaurant by your reservation time**.\n"
                "If you’re running late, **call the restaurant at least 15 minutes before** your time.\n"
                "Without contact, your booking may be **automatically cancelled** ❌.\n"
                "Thank you for your cooperation! 🙏"
            )

        user_msg = (
            f"Your booking is confirmed.\n"
            f"Restaurant: {store['name']}\n"
            f"Time: {tstr} / {req['pax']} people\n"
            f"Pickup: {'Need' if req.get('pickup') else 'No'} ({hotel})\n"
            f"Google Maps: {store['map_url']}"
            f"{warning}\n"
            f"\n*For cancellation/changes, please call the restaurant.*"
        )

    # ▼追加：送迎希望のときだけ集合場所を追記（送迎不要なら出さない）
    pickup_point = (store.get("pickup_point") or "").strip()
    if req.get("pickup") and pickup_point:
        if lang_code == "jp":
            user_msg += f"\n\n📍集合場所：{pickup_point}"
        else:
            user_msg += f"\n\n📍Pickup point: {pickup_point}"

    # まず reply、失敗時のみ push（重複送信を避ける）
    try:
        line_bot_api.reply_message(reply_token, TextSendMessage(user_msg))
    except Exception as e:
        try:
            line_bot_api.push_message(user_id, TextSendMessage(user_msg))
            print("[FALLBACK] confirm reply→push:", e)
        except Exception as e2:
            print("[FALLBACK] confirm both failed:", e, e2)

    # --- 15分前リマインドをセット（多重防止つき） ---
    schedule_prearrival_reminder(pb["req_id"])

    # 後片付け
    PENDING_BOOK.pop(user_id, None)


