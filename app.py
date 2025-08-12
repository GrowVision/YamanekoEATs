import os, json, re, math, datetime
from datetime import timedelta, timezone
from flask import Flask, request, abort

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
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

# ====== 簡易セッション／リクエスト保持（メモリ） ======
SESS = {}       # user_id -> {lang,time_iso,pax,pickup,hotel, req_id}
REQUESTS = {}   # req_id -> {user_id, deadline, wanted_iso, pax, pickup, hotel, candidates:set, closed:bool}
PENDING_BOOK = {}  # user_id -> {"req_id","store_id","step", "name"}

# ====== ユーティリティ ======
def now_jst():
    return datetime.datetime.now(JST)

def next_half_hour_slots(n=6):
    t = now_jst()
    minute = 30 if t.minute < 30 else 60
    start = t.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minute)
    slots = []
    for i in range(n):
        slots.append(start + timedelta(minutes=30*i))
    return slots

def qreply(items):
    return QuickReply(items=[QuickReplyButton(action=a) for a in items])

def lang_text(lang, jp, en):
    return jp if lang == "jp" else en

def make_req_id():
    return "REQ-" + now_jst().strftime("%Y%m%d-%H%M%S")

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

# ====== Webhook ======
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return "OK"  # ブラウザで https://＜URL＞/webhook が確認できる

    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        # 開発中は 200 返しで前に進む。運用に慣れたら 400 に戻してOK
        return "OK", 200
    return "OK"


# ====== 受付：テキスト ======
@handler.add(MessageEvent, message=TextMessage)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()

    # --- 5+ の数値入力を待っている場合 ---
    if SESS.get(user_id, {}).get("await") == "pax_number":
        m = re.match(r"^\d{1,2}$", text)
        if not m:
            line_bot_api.reply_message(event.reply_token, TextSendMessage("人数を数字で入力してください（例：6）"))
            return
        SESS[user_id]["pax"] = int(text)
        SESS[user_id].pop("await", None)
        # 次へ（送迎）
        ask_pickup(event.reply_token, SESS[user_id]["lang"])
        return

    # --- ホテル名入力を待っている場合（任意） ---
    if SESS.get(user_id, {}).get("await") == "hotel_name":
        SESS[user_id]["hotel"] = text
        SESS[user_id].pop("await", None)
        # 3要素揃っているはずなので照会へ
        start_inquiry(event.reply_token, user_id)
        return

    # トリガー
    if text in ["予約をはじめる", "Start reservation"]:
        SESS[user_id] = {}
        ask_lang(event.reply_token)
        return

    # 予約フロー：名前・電話
    if user_id in PENDING_BOOK:
        pb = PENDING_BOOK[user_id]
        if pb["step"] == "name":
            PENDING_BOOK[user_id]["name"] = text
            PENDING_BOOK[user_id]["step"] = "phone"
            line_bot_api.reply_message(event.reply_token, TextSendMessage("電話番号を入力してください（例：07012345678）"))
            return
        elif pb["step"] == "phone":
            if not re.match(r"^0\d{9,10}$", text):
                line_bot_api.reply_message(event.reply_token, TextSendMessage("電話番号の形式で入力してください（例：07012345678）"))
                return
            PENDING_BOOK[user_id]["phone"] = text
            finalize_booking(event.reply_token, user_id)
            return

    # デフォルト
    line_bot_api.reply_message(event.reply_token, TextSendMessage("下のリッチメニュー「予約 / Reserve」を押して開始してください。"))

# ====== 受付：ポストバック ======
@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    data = {}
    try:
        data = json.loads(event.postback.data or "{}")
    except Exception:
        pass

    # 店舗側からの回答（OK/不可）
    if data.get("type") == "store_reply":
        req_id = data.get("req_id")
        status = data.get("status")
        store_id = data.get("store_id")
        req = REQUESTS.get(req_id)
        if not req:
            return  # 流すだけ
        # 締切後は完全無視
        if now_jst() > req["deadline"] or req.get("closed"):
            return
        if status == "ok":
            req["candidates"].add(store_id)
            # 逐次ユーザーへ候補カード送信
            store = STORE_BY_ID.get(store_id)
            if store:
                lang = SESS.get(req["user_id"], {}).get("lang", "jp")
                bubble = candidate_bubble(store, lang)
                line_bot_api.push_message(
                    req["user_id"],
                    FlexSendMessage(alt_text="候補が届きました / New option available", contents=bubble)
                )
            if len(req["candidates"]) >= 3:
                req["closed"] = True
        elif status == "no":
            pass
        return

    step = data.get("step")
    if step == "lang":
        v = data.get("v", "jp")
        SESS.setdefault(user_id, {})["lang"] = v
        ask_time(event.reply_token, v)
        return

    if step == "time":
        SESS.setdefault(user_id, {})["time_iso"] = data.get("iso")
        ask_pax(event.reply_token, SESS[user_id].get("lang", "jp"))
        return

    if step == "pax":
        SESS.setdefault(user_id, {})["pax"] = int(data.get("v", 2))
        ask_pickup(event.reply_token, SESS[user_id].get("lang", "jp"))
        return

    if step == "pickup":
        need = data.get("need")  # true/false
        SESS.setdefault(user_id, {})["pickup"] = bool(need)
        if need:
            # 任意でホテル名（入れなくてもOKにする）
            SESS[user_id]["await"] = "hotel_name"
            txt = lang_text(SESS[user_id].get("lang","jp"), "ホテル名をご記入ください（任意）", "Please enter your hotel name (optional)")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(txt))
        else:
            start_inquiry(event.reply_token, user_id)
        return

    if data.get("type") == "book":
        # 予約申請開始
        req_id = SESS.get(user_id, {}).get("req_id")
        if not req_id:
            # 古い候補でもOKにするため、直近のREQUESTSから user_id で検索して最後のもの
            # MVPなので簡略化
            for rid, r in reversed(list(REQUESTS.items())):
                if r["user_id"] == user_id:
                    req_id = rid
                    break
        store_id = data.get("store_id")
        PENDING_BOOK[user_id] = {"req_id": req_id, "store_id": store_id, "step": "name"}
        line_bot_api.reply_message(event.reply_token, TextSendMessage("お名前を入力してください"))
        return

# ====== 質問UI ======
def ask_lang(reply_token):
    actions = [
        PostbackAction(label="日本語", data=json.dumps({"step":"lang","v":"jp"})),
        PostbackAction(label="English", data=json.dumps({"step":"lang","v":"en"})),
    ]
    line_bot_api.reply_message(reply_token, TextSendMessage("言語を選んでください / Choose your language", quick_reply=qreply(actions)))

def ask_time(reply_token, lang):
    slots = next_half_hour_slots(6)
    actions = []
    for s in slots:
        label = s.strftime("%H:%M")
        actions.append(PostbackAction(label=label, data=json.dumps({"step":"time","iso":s.isoformat()})))
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(lang_text(lang, "ご希望の時間を選んでください", "Choose your time"), quick_reply=qreply(actions))
    )

def ask_pax(reply_token, lang):
    actions = [
        PostbackAction(label="1", data=json.dumps({"step":"pax","v":1})),
        PostbackAction(label="2", data=json.dumps({"step":"pax","v":2})),
        PostbackAction(label="3", data=json.dumps({"step":"pax","v":3})),
        PostbackAction(label="4", data=json.dumps({"step":"pax","v":4})),
        PostbackAction(label="5+", data=json.dumps({"step":"pax5plus"})),
    ]
    # 5+ はテキスト入力待ちにする
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(lang_text(lang, "人数を選んでください", "Select number of people"), quick_reply=qreply(actions))
    )

@handler.add(PostbackEvent)
def on_pax5plus(event: PostbackEvent):
    # 5+専用の分岐（SDKのハンドラはイベント単位なので上書きしないため注意）
    try:
        data = json.loads(event.postback.data or "{}")
    except:
        data = {}
    if data.get("step") == "pax5plus":
        user_id = event.source.user_id
        SESS.setdefault(user_id, {})["await"] = "pax_number"
        line_bot_api.reply_message(event.reply_token, TextSendMessage("人数を数字で入力してください（例：6）"))

def ask_pickup(reply_token, lang):
    actions = [
        PostbackAction(label=lang_text(lang, "必要", "Need"), data=json.dumps({"step":"pickup","need":True})),
        PostbackAction(label=lang_text(lang, "不要", "No"), data=json.dumps({"step":"pickup","need":False})),
    ]
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(lang_text(lang, "送迎は必要ですか？", "Need pickup service?"), quick_reply=qreply(actions))
    )

# ====== 照会スタート → 店舗一斉送信 ======
def start_inquiry(reply_token, user_id):
    sess = SESS.get(user_id, {})
    lang = sess.get("lang", "jp")
    req_id = make_req_id()
    deadline = now_jst() + timedelta(minutes=15)

    REQUESTS[req_id] = {
        "user_id": user_id,
        "deadline": deadline,
        "wanted_iso": sess.get("time_iso"),
        "pax": sess.get("pax"),
        "pickup": sess.get("pickup"),
        "hotel": sess.get("hotel", ""),
        "candidates": set(),
        "closed": False
    }
    SESS[user_id]["req_id"] = req_id

    # ユーザーに受付メッセージ
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(lang_text(lang,
            "照会中です。最大15分、候補が届き次第表示します。",
            "Request sent. We’ll show options as they reply (up to 15 min)."))
    )

    # 店舗に一斉送信（営業フィルタ等はMVPでは省略）
    wanted = datetime.datetime.fromisoformat(sess["time_iso"]).astimezone(JST).strftime("%H:%M")
    pax = sess["pax"]
    pickup_label = "希望" if sess["pickup"] else "不要"
    hotel = sess.get("hotel") or "-"
    deadline_str = deadline.strftime("%H:%M")
    remain = int((deadline - now_jst()).total_seconds() // 60)

    for s in STORES:
        # 送迎条件フィルタ（必要なら）
        if sess["pickup"] and not s["pickup_ok"]:
            continue

        text = f"【照会】{wanted}／{pax}名／送迎：{pickup_label}（{hotel}）\n" \
               f"⏰ 締切：{deadline_str}（あと{remain}分）\n" \
               f"押すだけで返信👇\n" \
               f"REQ: {req_id}"

        actions = [
            PostbackAction(label="OK", data=json.dumps(
                {"type":"store_reply","req_id":req_id,"store_id":s["store_id"],"status":"ok"})),
            PostbackAction(label="不可", data=json.dumps(
                {"type":"store_reply","req_id":req_id,"store_id":s["store_id"],"status":"no"})),
        ]
        try:
            line_bot_api.push_message(
                s["line_user_id"],
                TextSendMessage(text=text, quick_reply=qreply(actions))
            )
        except Exception as e:
            print("push to store failed:", s["name"], e)

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

    # 店舗へ確定通知
    wanted = datetime.datetime.fromisoformat(req["wanted_iso"]).astimezone(JST).strftime("%H:%M")
    pickup_label = "希望" if req["pickup"] else "不要"
    hotel = req.get("hotel") or "-"
    msg = f"【予約確定】\nお名前：{pb['name']}\n電話：{pb['phone']}\n" \
          f"希望：{wanted}／{req['pax']}名／送迎：{pickup_label}（{hotel}）"
    try:
        line_bot_api.push_message(store["line_user_id"], TextSendMessage(msg))
    except Exception as e:
        print("push confirm failed:", e)

    # ユーザーへ確定案内
    lang = SESS.get(user_id, {}).get("lang", "jp")
    user_msg = lang_text(lang,
        f"ご予約が確定しました。キャンセルはお電話のみでお願いします。\nGoogleマップ：{store['map_url']}",
        f"Your booking is confirmed. For cancellation, please call the restaurant directly.\nGoogle Maps: {store['map_url']}"
    )
    line_bot_api.reply_message(reply_token, TextSendMessage(user_msg))

    # 後片付け（MVP）
    PENDING_BOOK.pop(user_id, None)
    # REQUESTS は残してもOK。軽量運用なら削除してもよい。
    # REQUESTS.pop(pb["req_id"], None)

@app.route("/health")
def health():
    return "ok"

@app.route("/")
def index():
    return "yamanekoEATS bot running"

# ←検証用。GET/POSTどちらでも200を返す
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return "OK"
    # POSTの中身は無視して常に200
    return "OK"
