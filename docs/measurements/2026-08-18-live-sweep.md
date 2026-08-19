# Live sweep — 2026-08-18

Raw record of the EA capacity study and the WS/REST verification, run against a
live XMTrading demo terminal (MetaTrader 5, account 75537514, server
`XMTrading-MT5 3`) by `benchmarks/live/`. The results directory itself is not
tracked — this file is the tracked copy, so every number quoted in
[`../latency.md`](../latency.md) has a source.

Session: Asian, crossing the 09:00 JST Tokyo open. Per-symbol tick rates
0.1–1.7/s. Local clock 0.918 s behind NTP, which is why every `broker_lag_ms`
reads about −0.80 s.

Commands: `python -m benchmarks.live.run smoke | sweep | remeasure |
groundtruth`.

---

# Live rig -- 2026-08-18

Started 2026-08-18 08:13:12 (local).

## Phase (b) sweep -- setup

- chart symbol: **BTCUSD#** (preference XAUUSD#, BTCUSD#, ETHUSD#, GOLD#, EURUSD#; the chart symbol is delivered by `OnTick`, never collected)
- instrument universe: 55 `#`-suffixed symbols with a tick database on this broker
- clock skew: 0.9199 s (`w32tm` server-minus-local; positive = this PC is behind, which makes `broker_lag_ms` read negative by the same amount)
- warm-up 60 s + measurement 60 s per run, `InpStatsSec=30`

## Builds

- `head`: TickStreamer.mq5 0 errors, TickSpy.mq5 0 errors, CountTicks.mq5 0 errors
- `e0`: TickStreamer.mq5 0 errors

## Run: discovery 1/2 HEAD-POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `discovery 1/2 HEAD-POLL` | disc | 27 | 527 | 11.7 | 0.179 | 0.259 | -793 | -772 | 23/162/81 | 0/138 | 0 | 0 | 55562/433 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=474 (15.8/s) dropped=0 send_us avg=55 max=142 reconnects=0 total_sent=2266 symbols=29 mode=poll poll_n=1901 poll_us_avg=23 poll_us_max=162 poll_us_p99=81 ping_us=219759 extra_obs=55562 extra_sent=433 ct_n=55129 ct_us_avg=0 ct_us_max=138 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Run: discovery 2/2 HEAD-POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `discovery 2/2 HEAD-POLL` | disc | 22 | 420 | 9.3 | 0.176 | 0.270 | -795 | -775 | 21/123/58 | 0/99 | 0 | 0 | 55427/307 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=347 (11.6/s) dropped=0 send_us avg=49 max=116 reconnects=0 total_sent=1319 symbols=25 mode=poll poll_n=1888 poll_us_avg=21 poll_us_max=123 poll_us_p99=58 ping_us=219759 extra_obs=55427 extra_sent=307 ct_n=47200 ct_us_avg=0 ct_us_max=99 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Symbol sets (ranked by measured ticks/s during discovery)

- symbols that ticked at all: **49** of 54 collected, over 2 chunk(s) x 45 s
- N=10: `US100Cash#`, `GOLD#`, `USDNOK#`, `GBPJPY#`, `SILVER#`, `AUDJPY#`, `GBPAUD#`, `USDSEK#`, `EURAUD#`, `CADJPY#` (0 silent during discovery)
- N=29: the largest set a chart `<inputs>` line can carry (240 of 244 characters); 0 of them were silent. **N=50 is not expressible** -- the terminal truncates the line at 255 characters including the key, silently and mid-name, so 25 ranked symbols could not be asked for.
- N=all: `InpSymbols="*"`, i.e. every Market Watch symbol -- which the discovery chunks have just populated with the whole universe.

| # | symbol | ticks | ticks/s |
|---|---|---:|---:|
| 1 | `US100Cash#` | 77 | 1.71 |
| 2 | `GOLD#` | 68 | 1.51 |
| 3 | `USDNOK#` | 53 | 1.18 |
| 4 | `GBPJPY#` | 43 | 0.96 |
| 5 | `SILVER#` | 39 | 0.87 |
| 6 | `AUDJPY#` | 34 | 0.76 |
| 7 | `GBPAUD#` | 34 | 0.76 |
| 8 | `USDSEK#` | 33 | 0.73 |
| 9 | `EURAUD#` | 33 | 0.73 |
| 10 | `CADJPY#` | 31 | 0.69 |
| 11 | `USDHUF#` | 30 | 0.67 |
| 12 | `AUDCAD#` | 30 | 0.67 |
| 13 | `EURJPY#` | 26 | 0.58 |
| 14 | `AUDNZD#` | 24 | 0.53 |
| 15 | `US500Cash#` | 22 | 0.49 |

## Run: E0 N=1 loss=off

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=1 loss=off` | 1 | 1 | 17 | 0.3 | 0.178 | 0.215 | -799 | -786 | 0/0/0 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |

```
last 30s: ticks=37 (1.2/s) dropped=0 send_us avg=64 max=135 reconnects=0 total_sent=219 symbols=0 poll_n=0 poll_us_avg=0 poll_us_max=0 poll_us_p99=0 ping_us=219759
```

## Run: E0 N=1 loss=on

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=1 loss=on` | 1 | 1 | 27 | 0.4 | 0.189 | 0.219 | -800 | -787 | 0/0/0 | -/- | - | - | 0/0 | -/-/- | 0 | 0 | 0 |

```
last 30s: ticks=45 (1.5/s) dropped=0 send_us avg=60 max=184 reconnects=0 total_sent=238 symbols=0 poll_n=0 poll_us_avg=0 poll_us_max=0 poll_us_p99=0 ping_us=219759 extra_obs=0 extra_sent=0 ticks_lost=0 loss_err=0
```

## Run: HEAD N=1 POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=1 POLL` | 1 | 1 | 16 | 0.3 | 0.183 | 0.221 | -801 | -785 | 0/0/0 | 0/0 | 0 | 0 | 0/0 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=39 (1.3/s) dropped=0 send_us avg=63 max=115 reconnects=0 total_sent=232 symbols=0 mode=poll poll_n=0 poll_us_avg=0 poll_us_max=0 poll_us_p99=0 ping_us=219759 extra_obs=0 extra_sent=0 ct_n=0 ct_us_avg=0 ct_us_max=0 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Run: HEAD N=1 EVENT

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=1 EVENT` | 1 | 1 | 35 | 0.6 | 0.177 | 0.243 | -801 | -786 | 0/0/0 | 0/0 | 0 | 0 | 0/0 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=44 (1.5/s) dropped=0 send_us avg=64 max=117 reconnects=0 total_sent=247 symbols=0 mode=event poll_n=0 poll_us_avg=0 poll_us_max=0 poll_us_p99=0 ping_us=219759 extra_obs=0 extra_sent=0 ct_n=0 ct_us_avg=0 ct_us_max=0 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

**evt_n=0 -- spies not running; this is a POLL measurement**

## Run: E0 N=10 loss=off

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=10 loss=off` | 10 | 11 | 608 | 10.1 | 0.175 | 0.244 | -796 | -764 | 2/87/14 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |

```
last 30s: ticks=318 (10.6/s) dropped=0 send_us avg=53 max=530 reconnects=0 total_sent=2076 symbols=10 poll_n=1905 poll_us_avg=2 poll_us_max=87 poll_us_p99=14 ping_us=219759
```

## Run: E0 N=10 loss=on

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=10 loss=on` | 10 | 11 | 575 | 9.6 | 0.174 | 0.280 | -796 | -775 | 3/80/25 | -/- | - | - | 319/319 | -/-/- | 0 | 0 | 0 |

```
last 30s: ticks=356 (11.9/s) dropped=0 send_us avg=52 max=138 reconnects=0 total_sent=1678 symbols=10 poll_n=1895 poll_us_avg=3 poll_us_max=80 poll_us_p99=25 ping_us=219759 extra_obs=319 extra_sent=319 ticks_lost=0 loss_err=0
```

## Run: HEAD N=10 POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=10 POLL` | 10 | 11 | 665 | 11.1 | 0.174 | 0.229 | -796 | -768 | 12/163/61 | 1/114 | 0 | 0 | 19085/335 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=371 (12.4/s) dropped=0 send_us avg=56 max=179 reconnects=0 total_sent=2156 symbols=10 mode=poll poll_n=1875 poll_us_avg=12 poll_us_max=163 poll_us_p99=61 ping_us=219759 extra_obs=19085 extra_sent=335 ct_n=18750 ct_us_avg=1 ct_us_max=114 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Checkpoint: WS/REST verification at N=10 (real load, multiple symbols)

| Check | Result | Detail |
| --- | --- | --- |
| `rest_health` | PASS | status=ok |
| `rest_symbols` | PASS | 52 symbol(s), 11 seen in the last 2 min, ask>=bid: True |
| `rest_symbol_one` | PASS | /symbols/1INCHUSD# |
| `rest_stats` | PASS | ticks=9187 rate=10.8/s |
| `rest_feeders` | PASS | 1 feeder(s) connected |
| `ws_hello_first` | PASS | first frame t='hello', symbols=None, available=52 symbol(s), record_size=64 |
| `ws_hello_filtered` | PASS | ?symbols=1INCHUSD# -> hello.symbols=['1INCHUSD#'] (contrast: unfiltered is null) |
| `ws_heartbeats` | PASS | heartbeat record seen: seq=2183 flags=0x80000000 |
| `ws_binary_matches_json` | PASS | 133 record(s) in both windows, 0 differ (json=133, binary=133, window=10s) |
| `ctl_ping_pong` | PASS | pong echo=4242 rx=1787010109.655985 |
| `ctl_subscribe_merge` | PASS | null -> ['1INCHUSD#'] -> ['1INCHUSD#', 'AUDCAD#'] -> [] (a list narrows; [] is 'none', distinct from null) |
| `ctl_format_switch` | PASS | ack.format='binary'; next ticks frame decoded 1 binary record(s) |
| `ctl_stats_pure_read` | PASS | two reads: same key set (True), counters non-decreasing (True), REST view not reset by the WS read (True); ticks 9313 -> 9313 |
| `ctl_error_frame` | PASS | garbage -> error('invalid json'); connection still answers ping |
| `ctl_unknown_op` | PASS | unknown op -> error("unknown op: 'nope'") |
| `ws_conflate` | PASS | 86 frame(s), 0 same-symbol duplicate(s) within a frame |
| `session_seq_gaps_and_drops` | PASS | seq_gaps=0 dropped=0 over 1720s and 9407 records |

17/17 checks passed.

## Checkpoint: reconnect watchdog at N=10 (bridge down 20 s)

```
MR	0	08:41:59.819	TickStreamer (BTCUSD#,M1)	[TickStreamer] SocketSend sent -1 of 64 bytes (error 5273); 1 of 1 records lost; reconnecting
LQ	0	08:42:02.826	TickStreamer (BTCUSD#,M1)	[TickStreamer] connect to 127.0.0.1:9800 timed out after 1016 ms (error 0): nothing is listening there, or this host does not refuse closed loopback ports (WSL2 mirrored networking, firewall). Start the bridge with 'mt5-ws-stream bridge'.
GM	0	08:42:05.832	TickStreamer (BTCUSD#,M1)	[TickStreamer] connect to 127.0.0.1:9800 timed out after 1000 ms (error 0): nothing is listening there, or this host does not refuse closed loopback ports (WSL2 mirrored networking, firewall). Start the bridge with 'mt5-ws-stream bridge'.
OO	0	08:42:20.363	TickStreamer (BTCUSD#,M1)	[TickStreamer] connected to 127.0.0.1:9800
```

## Run: HEAD N=10 EVENT

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=10 EVENT` | 10 | 11 | 985 | 16.4 | 0.167 | 0.250 | -804 | -783 | 14/99/45 | 1/91 | 0 | 0 | 3760/380 | 380/56/0 | 0 | 0 | - |

```
last 30s: ticks=419 (14.0/s) dropped=0 send_us avg=38 max=97 reconnects=0 total_sent=2082 symbols=10 mode=event poll_n=300 poll_us_avg=14 poll_us_max=99 poll_us_p99=45 ping_us=219759 extra_obs=3760 extra_sent=380 ct_n=3380 ct_us_avg=1 ct_us_max=91 ct_err=0 cursor_skip=0 evt_n=380 evt_us_avg=34 evt_us_max=101 evt_late=56 evt_bad=0
```

evt_late/evt_n = 56/380 (15%)

## Run: E0 N=29 loss=off

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=29 loss=off` | 29 | 30 | 1178 | 19.6 | 0.177 | 0.246 | -794 | -770 | 6/167/32 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |

```
last 30s: ticks=526 (17.5/s) dropped=0 send_us avg=55 max=175 reconnects=0 total_sent=4938 symbols=29 poll_n=1908 poll_us_avg=6 poll_us_max=167 poll_us_p99=32 ping_us=219759
```

## Run: HEAD N=29 POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=29 POLL` | 29 | 30 | 1342 | 22.4 | 0.175 | 0.285 | -794 | -772 | 23/210/71 | 0/182 | 0 | 0 | 55479/698 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=743 (24.8/s) dropped=0 send_us avg=50 max=171 reconnects=0 total_sent=3142 symbols=29 mode=poll poll_n=1889 poll_us_avg=23 poll_us_max=210 poll_us_p99=71 ping_us=219759 extra_obs=55479 extra_sent=698 ct_n=54781 ct_us_avg=0 ct_us_max=182 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Run: HEAD N=29 EVENT

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=29 EVENT` | 29 | 30 | 1593 | 26.5 | 0.167 | 0.260 | -803 | -720 | 29/148/82 | 0/104 | 0 | 0 | 10098/703 | 695/48/0 | 0 | 0 | - |

```
last 30s: ticks=738 (24.6/s) dropped=0 send_us avg=30 max=109 reconnects=0 total_sent=3418 symbols=29 mode=event poll_n=300 poll_us_avg=29 poll_us_max=148 poll_us_p99=82 ping_us=219759 extra_obs=10098 extra_sent=703 ct_n=9395 ct_us_avg=0 ct_us_max=104 ct_err=0 cursor_skip=0 evt_n=695 evt_us_avg=30 evt_us_max=102 evt_late=48 evt_bad=0
```

evt_late/evt_n = 48/695 (7%)

## Run: E0 N=all loss=off

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `E0 N=all loss=off` | all | 71 | 2976 | 49.6 | 0.174 | 0.257 | -798 | -776 | 13/171/49 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |

```
last 30s: ticks=1360 (45.3/s) dropped=0 send_us avg=48 max=185 reconnects=0 total_sent=8503 symbols=72 poll_n=1891 poll_us_avg=13 poll_us_max=171 poll_us_p99=49 ping_us=219759
```

## Run: HEAD N=all POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=all POLL` | all | 70 | 5710 | 95.1 | 0.170 | 0.258 | -797 | -772 | 43/272/101 | 0/240 | 0 | 0 | 141008/2929 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=2970 (99.0/s) dropped=0 send_us avg=46 max=343 reconnects=0 total_sent=13433 symbols=72 mode=poll poll_n=1914 poll_us_avg=43 poll_us_max=272 poll_us_p99=101 ping_us=219759 extra_obs=141008 extra_sent=2929 ct_n=137808 ct_us_avg=0 ct_us_max=240 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

## Run: HEAD N=all EVENT

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `HEAD N=all EVENT` | all | 70 | 5890 | 98.1 | 0.148 | 0.237 | -806 | -776 | 47/110/91 | 0/70 | 0 | 0 | 28457/3434 | 3423/236/0 | 0 | 0 | - |

```
last 30s: ticks=3471 (115.7/s) dropped=0 send_us avg=20 max=214 reconnects=0 total_sent=15309 symbols=72 mode=event poll_n=300 poll_us_avg=47 poll_us_max=110 poll_us_p99=91 ping_us=219759 extra_obs=28457 extra_sent=3434 ct_n=25023 ct_us_avg=0 ct_us_max=70 ct_err=0 cursor_skip=0 evt_n=3423 evt_us_avg=20 evt_us_max=218 evt_late=236 evt_bad=0
```

evt_late/evt_n = 236/3423 (7%)

## Phase (b) results

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `discovery 1/2 HEAD-POLL` | disc | 27 | 527 | 11.7 | 0.179 | 0.259 | -793 | -772 | 23/162/81 | 0/138 | 0 | 0 | 55562/433 | 0/0/0 | 0 | 0 | - |
| `discovery 2/2 HEAD-POLL` | disc | 22 | 420 | 9.3 | 0.176 | 0.270 | -795 | -775 | 21/123/58 | 0/99 | 0 | 0 | 55427/307 | 0/0/0 | 0 | 0 | - |
| `E0 N=1 loss=off` | 1 | 1 | 17 | 0.3 | 0.178 | 0.215 | -799 | -786 | 0/0/0 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |
| `E0 N=1 loss=on` | 1 | 1 | 27 | 0.4 | 0.189 | 0.219 | -800 | -787 | 0/0/0 | -/- | - | - | 0/0 | -/-/- | 0 | 0 | 0 |
| `HEAD N=1 POLL` | 1 | 1 | 16 | 0.3 | 0.183 | 0.221 | -801 | -785 | 0/0/0 | 0/0 | 0 | 0 | 0/0 | 0/0/0 | 0 | 0 | - |
| `HEAD N=1 EVENT` | 1 | 1 | 35 | 0.6 | 0.177 | 0.243 | -801 | -786 | 0/0/0 | 0/0 | 0 | 0 | 0/0 | 0/0/0 | 0 | 0 | - |
| `E0 N=10 loss=off` | 10 | 11 | 608 | 10.1 | 0.175 | 0.244 | -796 | -764 | 2/87/14 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |
| `E0 N=10 loss=on` | 10 | 11 | 575 | 9.6 | 0.174 | 0.280 | -796 | -775 | 3/80/25 | -/- | - | - | 319/319 | -/-/- | 0 | 0 | 0 |
| `HEAD N=10 POLL` | 10 | 11 | 665 | 11.1 | 0.174 | 0.229 | -796 | -768 | 12/163/61 | 1/114 | 0 | 0 | 19085/335 | 0/0/0 | 0 | 0 | - |
| `HEAD N=10 EVENT` | 10 | 11 | 985 | 16.4 | 0.167 | 0.250 | -804 | -783 | 14/99/45 | 1/91 | 0 | 0 | 3760/380 | 380/56/0 | 0 | 0 | - |
| `E0 N=29 loss=off` | 29 | 30 | 1178 | 19.6 | 0.177 | 0.246 | -794 | -770 | 6/167/32 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |
| `HEAD N=29 POLL` | 29 | 30 | 1342 | 22.4 | 0.175 | 0.285 | -794 | -772 | 23/210/71 | 0/182 | 0 | 0 | 55479/698 | 0/0/0 | 0 | 0 | - |
| `HEAD N=29 EVENT` | 29 | 30 | 1593 | 26.5 | 0.167 | 0.260 | -803 | -720 | 29/148/82 | 0/104 | 0 | 0 | 10098/703 | 695/48/0 | 0 | 0 | - |
| `E0 N=all loss=off` | all | 71 | 2976 | 49.6 | 0.174 | 0.257 | -798 | -776 | 13/171/49 | -/- | - | - | -/- | -/-/- | 0 | 0 | - |
| `HEAD N=all POLL` | all | 70 | 5710 | 95.1 | 0.170 | 0.258 | -797 | -772 | 43/272/101 | 0/240 | 0 | 0 | 141008/2929 | 0/0/0 | 0 | 0 | - |
| `HEAD N=all EVENT` | all | 70 | 5890 | 98.1 | 0.148 | 0.237 | -806 | -776 | 47/110/91 | 0/70 | 0 | 0 | 28457/3434 | 3423/236/0 | 0 | 0 | - |

## State left behind

- terminal: **running** on profile `TickBench`, HEAD build, `InpSymbols` = the N=10 set, chart `BTCUSD#`
- bridge: **stopped** (phase (c) starts its own; the EA is retrying 127.0.0.1:9800 every ~3 s until it does)
- N=10 set: US100Cash#, GOLD#, USDNOK#, GBPJPY#, SILVER#, AUDJPY#, GBPAUD#, USDSEK#, EURAUD#, CADJPY#
- largest expressible N on this platform: 29 (chart `<inputs>` line cap)

## Run: spy=MN1 N=all POLL

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `spy=MN1 N=all POLL` | all | 72 | 3897 | 64.9 | 0.172 | 0.277 | -797 | -773 | 42/157/114 | 0/104 | 0 | 0 | 142755/2038 | 0/0/0 | 0 | 0 | - |

```
last 30s: ticks=2083 (69.4/s) dropped=0 send_us avg=44 max=106 reconnects=0 total_sent=12661 symbols=72 mode=poll poll_n=1919 poll_us_avg=42 poll_us_max=157 poll_us_p99=114 ping_us=219759 extra_obs=142755 extra_sent=2038 ct_n=138168 ct_us_avg=0 ct_us_max=104 ct_err=0 cursor_skip=0 evt_n=0 evt_us_avg=0 evt_us_max=0 evt_late=0 evt_bad=0
```

resources: {'baseline': {'working_set_mb': 59.0, 'cpu_seconds': 0.1}, 'after': {'working_set_mb': 431.0, 'cpu_seconds': 18.3}, 'spy_period': 'MN1'}

- warmed up 72 of 72 extra symbols in 2078 ms

## Run: spy=MN1 N=all EVENT

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `spy=MN1 N=all EVENT` | all | 72 | 5015 | 83.6 | 0.160 | 0.243 | -807 | -780 | 48/137/89 | 0/90 | 0 | 0 | 27160/2618 | 2607/267/0 | 0 | 0 | - |

```
last 30s: ticks=2653 (88.4/s) dropped=0 send_us avg=22 max=208 reconnects=0 total_sent=13293 symbols=72 mode=event poll_n=300 poll_us_avg=48 poll_us_max=137 poll_us_p99=89 ping_us=219759 extra_obs=27160 extra_sent=2618 ct_n=24207 ct_us_avg=0 ct_us_max=90 ct_err=0 cursor_skip=0 evt_n=2607 evt_us_avg=21 evt_us_max=214 evt_late=267 evt_bad=0
```

resources: {'baseline': {'working_set_mb': 58.5, 'cpu_seconds': 0.2}, 'after': {'working_set_mb': 554.0, 'cpu_seconds': 33.9}, 'spy_period': 'MN1'}

- warmed up 72 of 72 extra symbols in 1469 ms
- attached 72 of 72 tick spies on PERIOD_MN1 in 343 ms

evt_late/evt_n = 267/2607 (10%)

## spy=MN1 (spy period MN1)

| Run | N | ticked | ticks | ticks/s | hop p50 | hop p99 | lag p50 | lag p99 | poll_us a/m/p99 | ct_us a/m | ct_err | cur_skip | obs/sent | evt n/late/bad | drop | gaps | ticks_lost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `spy=MN1 N=all POLL` | all | 72 | 3897 | 64.9 | 0.172 | 0.277 | -797 | -773 | 42/157/114 | 0/104 | 0 | 0 | 142755/2038 | 0/0/0 | 0 | 0 | - |
| `spy=MN1 N=all EVENT` | all | 72 | 5015 | 83.6 | 0.160 | 0.243 | -807 | -780 | 48/137/89 | 0/90 | 0 | 0 | 27160/2618 | 2607/267/0 | 0 | 0 | - |

## EVENT memory investigation (phase (c) block 1)

Hypotheses (b) and (c) were closed by inspection: `TickSpy.mq5` already declares
`indicator_buffers 0` / `indicator_plots 0` and draws nothing, and the EA already
calls `IndicatorRelease` for every handle in `OnDeinit`. Hypothesis (a) was the
live one and is confirmed.

`iCustom(symbol, period, ...)` binds the handle to a *(symbol, period)
timeseries*, which the terminal must build and hold to hand `OnCalculate` its
rates arrays. With `MaxBars=100000000` (read from `common.ini`, not changed) a
`PERIOD_M1` series per spied symbol is deep: the docs warn that "memory used for
storing timeseries and indicator buffers can become hundreds of megabytes"
(mql5.com/en/docs/series/timeseries_access), and 16.8 GB / 72 symbols is ~233 MB
each. The handle's period never affected *when* the spy fires -- "In indicators,
the OnCalculate() function is called after the arrival of each tick"
(mql5.com/en/docs/series/copyticks) -- so the deep series bought nothing.

Fix: `InpSpyPeriod`, defaulting to `PERIOD_MN1`.

| N=all, 72 extra symbols | working set | vs POLL | CPU-s | spy attach |
| --- | ---: | ---: | ---: | ---: |
| POLL (no spies) | 423 MB | 1.0x | 24.6 | -- |
| EVENT, `PERIOD_M1` (phase b) | **16 783 MB** | **39.7x** | 100.0 | 5125 ms |
| POLL, after fix | 431 MB | 1.0x | 18.3 | -- |
| EVENT, `PERIOD_MN1` (fixed) | **554 MB** | **1.29x** | 33.9 | 343 ms |

Baselines this time are same-process (sampled before `OnInit` attached anything):
POLL 59.0 MB -> 431.0 MB (+372 MB); EVENT 58.5 MB -> 554.0 MB (+496 MB).

A 30x reduction, inside the "within ~2x of POLL" target, and spy attachment got
15x faster (5125 ms -> 343 ms) because there are no longer 72 deep M1 series to
build. Delivery is unchanged: `evt_n=2607`, `evt_late=267` (10%), `evt_bad=0`,
`extra_obs` 27 160 against POLL's 142 755 (5.3x fewer reads) for `extra_sent`
2618 against 2038, hop p50 0.160 ms against 0.172 ms, `dropped=0`, `seq_gaps=0`,
`ct_err=0`, `cursor_skip=0`. The EA logs the period it got --
`attached 72 of 72 tick spies on PERIOD_MN1 in 343 ms` -- so the setting is
verifiable from the log rather than assumed.

## Ground truth: E0 N=10 (120 s window)

- window: `1787013182623` .. `1787013302877` (UTC ms, receive time)
- wire: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\scaling-ground-truth-e0-n-10.csv`
- terminal: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\TickStreamer_counts_e0.csv`

```
symbol         terminal       wire       lost
---------------------------------------------
AUDJPY#              14        135       -121
BTCUSD#               0         30        -30
CADJPY#               6         94        -88
EURAUD#              22         97        -75
GBPAUD#              29        161       -132
GBPJPY#              37        272       -235
GOLD#                 0        412       -412
SILVER#               0        237       -237
US100Cash#            0        294       -294
USDNOK#             288        110        178
USDSEK#              73        105        -32
---------------------------------------------
TOTAL               469       1947      -1478
```

`lost` = terminal - wire. The two windows are bounded by different clocks (receive time vs the broker's `time_msc`), so a few ticks either way at the edges are the measurement's slop, not loss.

## Ground truth: HEAD N=10 (120 s window)

- window: `1787013437026` .. `1787013557224` (UTC ms, receive time)
- wire: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\scaling-ground-truth-head-n-10.csv`
- terminal: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\TickStreamer_counts_head.csv`

```
symbol         terminal       wire       lost
---------------------------------------------
AUDJPY#               4        139       -135
BTCUSD#               0         24        -24
CADJPY#               2        135       -133
EURAUD#              13        135       -122
GBPAUD#              14        153       -139
GBPJPY#              23        307       -284
GOLD#                 0        363       -363
SILVER#               0        167       -167
US100Cash#            0        238       -238
USDNOK#             370        157        213
USDSEK#              96         89          7
---------------------------------------------
TOTAL               522       1907      -1385
```

`lost` = terminal - wire. The two windows are bounded by different clocks (receive time vs the broker's `time_msc`), so a few ticks either way at the edges are the measurement's slop, not loss.

## Ground truth: E0 N=10 (120 s window)

- window: `1787024677651` .. `1787024797901` (broker server-time ms; the collector's UTC receive window shifted by the EA's reported server_utc_offset of +3.0 h, because `CopyTicksRange` filters on `MqlTick.time_msc`)
- wire: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\scaling-ground-truth-e0-n-10.csv`
- terminal: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\TickStreamer_counts_e0.csv`

```
symbol         terminal       wire       lost
---------------------------------------------
AUDJPY#               0        182       -182
BTCUSD#               0         37        -37
CADJPY#               0        143       -143
EURAUD#               0        150       -150
GBPAUD#               0        143       -143
GBPJPY#               0        318       -318
GOLD#                 0        349       -349
SILVER#               0        172       -172
US100Cash#            0        325       -325
USDNOK#               0        109       -109
USDSEK#               0         99        -99
---------------------------------------------
TOTAL                 0       2027      -2027
```

`lost` = terminal - wire. The two windows are bounded by different clocks (receive time vs the broker's `time_msc`), so a few ticks either way at the edges are the measurement's slop, not loss.

## Ground truth: HEAD N=10 (120 s window)

- window: `1787024933728` .. `1787025053912` (broker server-time ms; the collector's UTC receive window shifted by the EA's reported server_utc_offset of +3.0 h, because `CopyTicksRange` filters on `MqlTick.time_msc`)
- wire: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\scaling-ground-truth-head-n-10.csv`
- terminal: `C:\Users\komo_\project\mt5-ws-stream\benchmarks\results\live-20260818\TickStreamer_counts_head.csv`

```
symbol         terminal       wire       lost
---------------------------------------------
AUDJPY#             184        186         -2
BTCUSD#               0         39        -39
CADJPY#             178        178          0
EURAUD#             123        123          0
GBPAUD#             139        138          1
GBPJPY#             346        351         -5
GOLD#               200        200          0
SILVER#             171        170          1
US100Cash#          262        262          0
USDNOK#              64         64          0
USDSEK#             125        125          0
---------------------------------------------
TOTAL              1792       1836        -44
```

`lost` = terminal - wire. The two windows are bounded by different clocks (receive time vs the broker's `time_msc`), so a few ticks either way at the edges are the measurement's slop, not loss.

## Ground truth, definitive (phase (c) block 2)

Supersedes the two attempts above. The first passed the collector's **UTC**
window to `CopyTicksRange`, which filters on `MqlTick.time_msc` -- the broker's
server clock, UTC+3 here. That counted a window three hours earlier: zero ticks
for every metal and index (the 21:00 UTC rollover break) and a burst on the
exotic FX pairs (rollover spread churn). The MQL5 reference says only
"milliseconds since 1970.01.01" and does not name the clock; this is settled by
measurement, and the window is now shifted by the offset the EA itself reports
(`server_utc_offset=+3.0h`).

The second attempt then returned zero for **E0 only**. Cause: `CopyTicksRange`
reads the terminal's tick *database*, and a symbol whose database has never
been synchronised returns nothing rather than an error -- the first `CopyTicks`
is what triggers the sync. E0 with `loss=off` never calls `CopyTicks` at all,
so nothing had ever synchronised those symbols; HEAD polls with `CopyTicks`, so
its identical window counted fine. The E0 window was recounted once the
databases were warm. `count_ticks_headless` now retries an empty count once.

Window 120 s, N=10, same session, minutes apart.

### E0 (`loss=off`), window `1787024677651`..`1787024797901` (broker ms)

| symbol | terminal | wire | lost |
| --- | ---: | ---: | ---: |
| AUDJPY# | 183 | 182 | +1 |
| CADJPY# | 146 | 143 | +3 |
| EURAUD# | 151 | 150 | +1 |
| GBPAUD# | 143 | 143 | 0 |
| GBPJPY# | 319 | 318 | +1 |
| GOLD# | 350 | 349 | +1 |
| SILVER# | 172 | 172 | 0 |
| US100Cash# | 325 | 325 | 0 |
| USDNOK# | 110 | 109 | +1 |
| USDSEK# | 100 | 99 | +1 |
| **extras total** | **1999** | **1990** | **+9** (+0.45%) |
| BTCUSD# (chart) | 0 | 37 | n/a -- see below |

### HEAD-POLL, window `1787024933728`..`1787025053912` (broker ms)

| symbol | terminal | wire | lost |
| --- | ---: | ---: | ---: |
| AUDJPY# | 184 | 186 | -2 |
| CADJPY# | 178 | 178 | 0 |
| EURAUD# | 123 | 123 | 0 |
| GBPAUD# | 139 | 138 | +1 |
| GBPJPY# | 346 | 351 | -5 |
| GOLD# | 200 | 200 | 0 |
| SILVER# | 171 | 170 | +1 |
| US100Cash# | 262 | 262 | 0 |
| USDNOK# | 64 | 64 | 0 |
| USDSEK# | 125 | 125 | 0 |
| **extras total** | **1792** | **1797** | **-5** (-0.28%) |
| BTCUSD# (chart) | 0 | 39 | n/a -- see below |

**Verdict: no coalescing signature in either build.** Both totals sit inside
±0.5%, and the per-symbol differences are 0..5 ticks with no relationship to
symbol activity -- `GBPJPY#` (the busiest, 318-351) and `SILVER#` (172) differ
by the same handful. That is the documented edge slop: the wire window is
bounded by *receive* time and the count by broker `time_msc`, so a tick landing
near either edge falls inside one and outside the other. A coalescing signature
would be one-sided and would scale with activity.

This does **not** show E0 loses nothing. It shows this session was too thin to
make it lose anything: E0 samples the newest tick per poll, so loss needs two
ticks inside one `InpTimerMs=1` interval, and at ~1.5 ticks/s per symbol that
essentially never happens. The same reason phase (b)'s `ticks_lost=` read 0.

The chart symbol reads `terminal=0` by construction: it reaches the wire through
`OnTick`, so nothing calls `CopyTicks` on it and its tick database is never
synchronised. The ground truth is about the *extra* symbols -- they are the ones
on the polled path where coalescing could occur at all.

## WS/REST verification — N=10 under load (JSON source)

| Check | Result | Detail |
| --- | --- | --- |
| `rest_health` | PASS | status=ok |
| `rest_symbols` | PASS | 52 symbol(s), 11 seen in the last 2 min, ask>=bid: True |
| `rest_symbol_one` | PASS | /symbols/1INCHUSD# |
| `rest_stats` | PASS | ticks=9187 rate=10.8/s |
| `rest_feeders` | PASS | 1 feeder(s) connected |
| `ws_hello_first` | PASS | first frame t='hello', symbols=None, available=52 symbol(s), record_size=64 |
| `ws_hello_filtered` | PASS | ?symbols=1INCHUSD# -> hello.symbols=['1INCHUSD#'] (contrast: unfiltered is null) |
| `ws_heartbeats` | PASS | heartbeat record seen: seq=2183 flags=0x80000000 |
| `ws_binary_matches_json` | PASS | 133 record(s) in both windows, 0 differ (json=133, binary=133, window=10s) |
| `ctl_ping_pong` | PASS | pong echo=4242 rx=1787010109.655985 |
| `ctl_subscribe_merge` | PASS | null -> ['1INCHUSD#'] -> ['1INCHUSD#', 'AUDCAD#'] -> [] (a list narrows; [] is 'none', distinct from null) |
| `ctl_format_switch` | PASS | ack.format='binary'; next ticks frame decoded 1 binary record(s) |
| `ctl_stats_pure_read` | PASS | two reads: same key set (True), counters non-decreasing (True), REST view not reset by the WS read (True); ticks 9313 -> 9313 |
| `ctl_error_frame` | PASS | garbage -> error('invalid json'); connection still answers ping |
| `ctl_unknown_op` | PASS | unknown op -> error("unknown op: 'nope'") |
| `ws_conflate` | PASS | 86 frame(s), 0 same-symbol duplicate(s) within a frame |
| `session_seq_gaps_and_drops` | PASS | seq_gaps=0 dropped=0 over 1720s and 9407 records |

17/17 checks passed.
