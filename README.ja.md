<!-- markdownlint-disable MD033 MD041 -->
<h1 align="center">mt5-ws-stream</h1>

<p align="center">
  <strong>MetaTrader 5 のティックを低レイテンシで WebSocket 配信する。</strong><br>
  Expert Advisor → Python ブリッジ → 任意の WebSocket クライアント。
</p>

<p align="center">
  <a href="https://github.com/komo135/mt5-ws-stream/actions/workflows/ci.yml"><img src="https://github.com/komo135/mt5-ws-stream/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pypi.org/project/mt5-ws-stream/"><img src="https://img.shields.io/pypi/v/mt5-ws-stream.svg?cacheSeconds=300" alt="PyPI"></a>
  <a href="https://pypi.org/project/mt5-ws-stream/"><img src="https://img.shields.io/pypi/pyversions/mt5-ws-stream.svg?cacheSeconds=300" alt="Python versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT"></a>
</p>

<p align="center">
  <a href="README.md">English README</a> ·
  <a href="docs/architecture.md">アーキテクチャ</a> ·
  <a href="docs/protocol.md">プロトコル</a> ·
  <a href="docs/latency.md">レイテンシ</a> ·
  <a href="docs/troubleshooting.md">トラブルシューティング</a>
</p>

---

```
MetaTrader 5                 mt5-ws-stream                  あなたのコード
┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│ TickStreamer.mq5 │  TCP   │    ブリッジ      │WS/REST │ ブラウザ / bot / │
│                  ├───────►│ (1 ポート: /ws + ├───────►│ 記録 / チャート  │
└──────────────────┘        │  /api/v1 + docs) │        │ / ダッシュボード │
                            └──────────────────┘        └──────────────────┘
```

EA をチャートに載せる。ティックは手元の Python ブリッジへ行き、ブリッジが
WebSocket クライアントへ送る。既定は JSON。ストリーム・REST・ダッシュボード・
OpenAPI は同じポート。

## 動作環境

| | |
| --- | --- |
| Python | >= 3.14 |
| 実行時依存 | Python 3.14 対応版の websockets、AnyIO、FastAPI、Pydantic、Starlette、uvicorn |
| ライブの市場データ | Windows 上の MetaTrader 5 |
| EA のコンパイル | MetaEditor（端末に同梱） |

## インストール

```bash
pip install mt5-ws-stream
```

## クイックスタート

1. `mql5/Experts/TickStreamer/TickStreamer.mq5` を端末のデータフォルダ配下の
   `MQL5/Experts/` にコピーする（**ファイル → データフォルダを開く**）。
2. MetaEditor でコンパイルする（<kbd>F7</kbd>）。
3. **ツール → オプション → エキスパートアドバイザー → 「WebRequest を許可する URL
   リスト」** に `127.0.0.1` を追加する。MQL5 のソケット関数はこの許可リストを
   共有しており、登録がないと `SocketCreate()` がエラー 4014 で失敗する。
4. ツールバーの **アルゴリズム取引** を有効にする。
5. ブリッジを起動し、EA をチャートにドラッグする:

```bash
mt5-ws-stream bridge
```

成功すると、エキスパートタブに `[TickStreamer] connected to 127.0.0.1:9800` と
出る。

ティックを表示する:

```bash
mt5-ws-stream client --print
```

ダッシュボードを開く:

```bash
mt5-ws-stream dashboard
```

`http://127.0.0.1:8765/dashboard` が開く。ブリッジが配信している。

<p align="center">
  <img src="docs/images/dashboard.png" alt="ライブダッシュボード: スパークライン付きの bid/ask タイル、ティックレートと配信レイテンシのチャート" width="880">
</p>

## 使い方

### Python

```python
import asyncio
from mt5_ws_stream import TickStreamClient


async def main() -> None:
    async with TickStreamClient(symbols=["EURUSD", "USDJPY"]) as stream:
        async for tick in stream:
            print(f"{tick.symbol} {tick.bid} / {tick.ask}  spread={tick.spread:.5f}")


asyncio.run(main())
```

最新価格だけが必要なら（ダッシュボード、ティッカー）:

```python
TickStreamClient(symbols=["EURUSD"], backpressure="conflate")
```

`async for tick in stream` は `stream.ticks()`。ティックを 1 件ずつ返す。JSON
でも binary でも同じ。フレーム単位（ティック数、`stats` / `ack`、hop）が必要
なら `stream.stream()`。WebSocket メッセージ 1 件につきデコード済みフレームが
1 つ返る:

```python
from mt5_ws_stream import ControlFrame, TickFrame

async with TickStreamClient() as stream:
    async for frame in stream.stream():
        if isinstance(frame, TickFrame):
            print(len(frame.ticks), "ticks, hop", frame.hop)
        elif isinstance(frame, ControlFrame):
            print(frame.kind, frame.payload)
```

`subscribe()`、`unsubscribe()`、`set_format()`、`request_stats()`、`ping()` は
対応する op を送る。応答は `stream.stream()` に `ControlFrame` として届く。
`frame.hop` は `received_at - rx`（秒）。送信タイムスタンプのないバイナリ
フレームでは `None`。

### ブラウザ / JavaScript

```js
const ws = new WebSocket("ws://127.0.0.1:8765/ws?symbols=EURUSD");
ws.onmessage = (event) => {
  const frame = JSON.parse(event.data);
  if (frame.t !== "ticks") return;
  for (const d of frame.d) console.log(d.s, d.b, d.a);
};
```

[`examples/`](examples/) に最小の Python クライアント、CSV レコーダ、Node.js
クライアント、単一ファイルの HTML ページがある。

## シンボルを増やす

`InpSymbols` が空: EA を載せたチャートのシンボル。

`InpSymbols=EURUSD,USDJPY`（または Market Watch 全体の `*`）: 同じチャートから
追加シンボルも配信する。追加シンボルは端末のタイマーで集める（`InpPollMs`。
Windows では 10〜16 ms）。ティックは欠けない。追加シンボルはタイマー 1 周期
まで待つ。

シンボルごとにチャートへ EA を 1 つ載せると、どれもイベント駆動のままになる。

チャートを増やさず追加シンボルのタイマー待ちを外す方法は
[docs/latency.md](docs/latency.md#extra-symbols-polling-or-events)
（`EXTRA_EVENT`）。

端末は `InpSymbols` を 244 文字で切り詰める
（[詳細](docs/troubleshooting.md#inpsymbols-is-truncated-at-244-characters)）。

## 設定

### `mt5-ws-stream bridge`

| フラグ | 既定値 | 意味 |
| --- | --- | --- |
| `--tcp-host` | `127.0.0.1` | EA を受けるバインドアドレス |
| `--tcp-port` | `9800` | EA の `InpPort` と一致させる |
| `--ws-host` | `127.0.0.1` | 受け手（HTTP + WebSocket）を受けるバインドアドレス |
| `--http-port` | `8765` | 受け手用ポート。HTTP と WebSocket を提供する |
| `--queue-limit` | `20000` | `lossless` の受け手 1 つあたり、切り捨てるまでにバッファするティック数 |
| `--stats-interval` | `10.0` | 統計ログ行と `stats` フレームの間隔（秒）。`0` で両方とも無効 |
| `--allow-origin` | *(制限なし)* | WebSocket でブラウザの `Origin` の値を制限する。複数回指定可 |

全体フラグ: デバッグログを出す `-v` / `--verbose`、`--version`。

| コマンド | 用途 |
| --- | --- |
| `client` | ティックの表示、または `--bench SECONDS` |
| `dashboard` | 同梱のダッシュボードを開く（`--url`、`--print-path`） |

### Expert Advisor の入力

| 入力 | 既定値 | 意味 |
| --- | --- | --- |
| `InpHost` | `127.0.0.1` | ブリッジのホスト |
| `InpPort` | `9800` | ブリッジの TCP ポート |
| `InpReconnectMs` | `2000` | 再接続を試みるまでのバックオフ（ミリ秒） |
| `InpHeartbeatMs` | `1000` | ハートビート間隔（ミリ秒）。`0` で無効 |
| `InpSymbols` | *(空)* | 追加シンボル。カンマ区切り、または Market Watch 全体の `*`。空ならチャートのシンボルのみ |
| `InpPollMs` | `10` | 追加シンボルのタイマー周期（ミリ秒）。端末の実際の分解能は 10〜16 ms |
| `InpUtcTimestamps` | `true` | `time_msc` をブローカーのサーバ時刻から UTC に変換する |
| `InpVerbose` | `true` | 接続イベントとエラーをログに出す |
| `InpStatsSec` | `60` | N 秒ごとにスループットのサマリを出す。`0` で無効 |

## API

### WebSocket

接続先は `ws://host:8765/ws`。`/` はストリームではなく HTTP のインデックス。

| クエリパラメータ | 既定値 | 意味 |
| --- | --- | --- |
| `symbols` | すべて | カンマ区切りの許可リスト |
| `format` | `json` | `json` または `binary`（生の 64 バイトレコード） |
| `conflate` | `0` | `1` にすると、負荷時にシンボルごとの最新ティックだけを残す |
| `heartbeats` | `0` | `1` にすると、キープアライブレコードも配信する |

未知のパラメータは無視される。同じパラメータが 2 回あるときは最後の値。
`#` を含むシンボル名は `%23` にパーセントエンコードする。

サーバ側フレームの種別は `t`: `hello`（最初の 1 回、最新価格のスナップショット
付き）、`ticks`、`stats`、`ack`、`pong`、`error`。クライアント側の制御フレーム
は `subscribe`、`unsubscribe`、`format`、`stats`、`ping`。リファレンスは
[docs/protocol.md](docs/protocol.md)。

`hello.symbols` はこの接続のフィルタ（全シンボルなら `null`、なしなら `[]`）。
`hello.available` は起動以降にブリッジが見たすべてのシンボル。シンボル選択 UI
は `available`、自分の購読を追う受け手は `symbols`。

### REST

エンドポイントはすべて `GET`。CORS は任意のオリジンに開き、`GET` に限定。
WebSocket は `--allow-origin` を別途持つ。

| エンドポイント | 返すもの |
| --- | --- |
| `/` | 以下のルートの JSON インデックス |
| `/api/v1/health` | 死活確認: ステータス、稼働時間、バージョン |
| `/api/v1/symbols` | 起動以降に観測した全シンボルの最新クォート。`?symbols=A,B` で絞り込み |
| `/api/v1/symbols/{symbol}` | 1 シンボルの最新クォート。未知なら 404 |
| `/api/v1/stats` | WebSocket の `stats` フレームと同じカウンタ |
| `/api/v1/feeders` | 接続中のフィーダ |
| `/dashboard` | 同梱の単一ファイルダッシュボード |
| `/docs`, `/openapi.json` | 対話的な API ドキュメント / OpenAPI スキーマ |

```bash
curl http://127.0.0.1:8765/api/v1/symbols
```

## 性能

ループバック、JSON、ブリッジ → 受け手: 200 ticks/s で **p50 0.091 ms / p99
0.176 ms**、20,000 ticks/s で **p50 0.101 ms / p99 0.186 ms**。ライブの hop
p50 は 0.16〜0.18 ms。表と測定方法は
[docs/latency.md](docs/latency.md)。

## ドキュメント

| 知りたいこと | 場所 |
| --- | --- |
| エラー 4014、更新が疎、欠落 | [docs/troubleshooting.md](docs/troubleshooting.md) |
| バイナリレコードと WebSocket フレーム | [docs/protocol.md](docs/protocol.md) |
| 実測レイテンシ、追加シンボル | [docs/latency.md](docs/latency.md) |
| コンポーネントと障害時の動き | [docs/architecture.md](docs/architecture.md) |
| Python / Node.js / ブラウザのサンプル | [examples/README.md](examples/README.md) |
| テスト、CI、合成フィーダ | [CONTRIBUTING.md](CONTRIBUTING.md) |
| リリース履歴 | [CHANGELOG.md](CHANGELOG.md) |

## ライセンス

MIT — [LICENSE](LICENSE) を参照。
