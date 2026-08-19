//+------------------------------------------------------------------+
//|                                                 TickStreamer.mq5 |
//|                    Part of mt5-ws-stream (MIT licensed)          |
//|              https://github.com/komo135/mt5-ws-stream            |
//+------------------------------------------------------------------+
//| Streams ticks out of MetaTrader 5 with the lowest latency the     |
//| platform allows.                                                  |
//|                                                                   |
//| Why this shape:                                                   |
//|   * OnTick() is event-driven. The terminal calls it the moment a  |
//|     tick lands, so there is no polling interval to wait out.      |
//|   * Each record is a fixed 64 bytes, so the reader needs no       |
//|     delimiter scan and no parser -- the classic source of both    |
//|     latency and framing bugs.                                     |
//|   * The handler does one thing: fill a buffer and send it. Every  |
//|     microsecond spent here is added latency, and MetaTrader       |
//|     coalesces ticks that arrive while OnTick() is still running.  |
//|                                                                   |
//| Structure:                                                        |
//|   MQL5 gives an EA three entry points and one namespace, so the   |
//|   state is grouped into structs instead: each one owns its data   |
//|   and the functions that may touch it, and there is exactly one   |
//|   instance of each.                                               |
//|                                                                   |
//|     ServerClock  the broker-server-time -> UTC offset and when to |
//|                  re-estimate it. ToUtcMsc() is the only converter.|
//|     SendBuffer   the outgoing bytes, their length and the record  |
//|                  sequence number. Append() is the only writer of  |
//|                  the 64-byte layout, and seq advances only when a |
//|                  record lands in the buffer -- so a record the    |
//|                  full buffer refused leaves no gap on the wire.   |
//|                  A record already buffered and then lost to a     |
//|                  failed Flush() does burn its seq; the socket is  |
//|                  torn down in the same breath, so the gap falls   |
//|                  at a connection boundary rather than inside one. |
//|                  Which also means the bridge cannot see it: seq   |
//|                  continuity is per connection there, so the first |
//|                  record over a fresh link starts the count again  |
//|                  and no gap is reported however many records the  |
//|                  old one lost on its way out. The EA's own        |
//|                  dropped= and reconnects= are the counters that   |
//|                  answer "did a reconnect cost anything" -- the    |
//|                  wire carries no signal for it, by design.        |
//|     IntervalStats  every counter behind the periodic summary,     |
//|                  lifetime totals included. Nothing else counts.   |
//|     Link         the socket, its backoff and its watchdog: how a  |
//|                  connect attempt is classified, when a connection |
//|                  counts as short-lived, and when the reconnect-   |
//|                  storm hint stands down.                          |
//|     Heartbeat    when the next heartbeat is due.                  |
//|     SymbolFeed   one symbol's delivery cursor: its name, the last |
//|                  time_msc delivered, how many ticks were already  |
//|                  taken from that millisecond, and its spy handle. |
//|                  The chart symbol is one of these; ExtraSymbolList|
//|                  holds the extra ones and owns their collection.  |
//|                                                                   |
//|   Three free functions compose them, and they are the only place  |
//|   two modules meet: EmitQuote(), EmitHeartbeat() and Flush().     |
//|   The UTC shift is applied by EmitQuote(), which knows it is      |
//|   holding a broker timestamp -- SendBuffer never inspects flags   |
//|   to work out what kind of record it is writing.                  |
//|                                                                   |
//| OnTimer ordering (the guarantee, not a comment):                  |
//|   OnTimer() is three named steps.                                 |
//|     TimerAlways()        stats line, server-clock refresh, the    |
//|                          staged symbol warm-up. Runs whether or   |
//|                          not the socket is up.                    |
//|     TimerServiceLink()   the watchdog. Returns false when the     |
//|                          link needed repair, and repair costs the |
//|                          rest of the tick.                        |
//|     TimerWhenConnected() poll extra symbols, heartbeat, flush.    |
//|   Work that must survive a dead socket goes in the first step and |
//|   cannot accidentally end up behind the early return, because the |
//|   early return is in a different function.                        |
//|                                                                   |
//| Heartbeat cadence (why InpHeartbeatMs is a floor, not a period):  |
//|   Heartbeats are claimed in TimerWhenConnected(), so the timer is |
//|   the only thing that can emit one. The effective interval is     |
//|   therefore max(InpHeartbeatMs, timer period), and the timer      |
//|   period is 200 ms chart-only, InpPollMs with extra symbols, or   |
//|   InpEventBackstopMs in a fully spied EXTRA_EVENT list. A         |
//|   backstop of 500 ms and InpHeartbeatMs = 100 gives beats every   |
//|   500 ms. OnInit() prints the effective interval when it exceeds  |
//|   InpHeartbeatMs rather than speeding the timer up to match: the  |
//|   timer period is chosen by the delivery path, and quietly        |
//|   overriding it for a keepalive would cost the CPU that path is   |
//|   budgeted. Keep the effective interval under the bridge's idle   |
//|   timeout, not merely InpHeartbeatMs.                             |
//|                                                                   |
//| Required terminal setup:                                          |
//|   1. Tools > Options > Expert Advisors                            |
//|      > "Allow WebRequest for listed URL" -> add 127.0.0.1         |
//|      (MQL5 socket functions share this allow-list; without the    |
//|       entry SocketCreate() fails with error 4014.)                |
//|   2. Enable "Algo Trading" on the toolbar.                        |
//|   3. Start the bridge first: `mt5-ws-stream bridge`               |
//|   4. For InpExtraMode = EXTRA_EVENT only: copy TickSpy.mq5 to     |
//|      MQL5\Indicators\TickStreamer\ and compile it (F7). iCustom() |
//|      resolves the relative name "TickStreamer\\TickSpy" under     |
//|      MQL5\Indicators, so the subfolder is part of the name.       |
//|                                                                   |
//| Multi-symbol guidance:                                            |
//|   One EA instance per chart stays the fully event-driven option   |
//|   and the lowest latency one: every symbol then flows through its |
//|   own OnTick().                                                   |
//|   InpSymbols adds extra symbols to a single instance: a comma     |
//|   separated list, or "*" for every symbol currently visible in    |
//|   Market Watch. Those extras stream *every* tick in either mode:  |
//|   a collection asks CopyTicks() for everything that arrived since |
//|   the last record this EA sent for that symbol, so nothing        |
//|   between two collections is coalesced away. The chart symbol is  |
//|   never collected -- OnTick() already covers it -- so listing it  |
//|   in InpSymbols is harmless and changes nothing.                  |
//|                                                                   |
//| Two delivery modes for extra symbols (InpExtraMode):              |
//|   What differs is only what *wakes* the collection up, never what |
//|   it does when awake -- both call the same Poll().                |
//|                                                                   |
//|   EXTRA_POLL (default)  the terminal timer, every InpPollMs. The  |
//|     floor is the poll period plus the timer's own resolution,     |
//|     which is 10-16 ms on Windows however low InpPollMs is set     |
//|     ("timer events are generated no more than 1 time in 10-16     |
//|     milliseconds due to hardware limitations",                    |
//|     .../docs/eventfunctions/eventsetmillisecondtimer).            |
//|                                                                   |
//|   EXTRA_EVENT  one TickSpy indicator per extra symbol, created    |
//|     with iCustom() and never drawn. "In indicators, the           |
//|     OnCalculate() function is called after the arrival of each    |
//|     tick" (.../docs/series/copyticks), and an indicator cannot    |
//|     open a socket (error 4014, .../docs/network/socketcreate), so |
//|     the spy's whole body is one EventChartCustom() aimed at this  |
//|     EA's chart. OnChartEvent() then runs that symbol's Poll() at  |
//|     once. No extra chart, no second socket, one seq space.        |
//|     The timer keeps running underneath as a backstop, at          |
//|     InpEventBackstopMs -- and drops back to InpPollMs for the     |
//|     whole list if any symbol failed to get a spy, because such a  |
//|     symbol has nothing else collecting it.                        |
//|                                                                   |
//|   Why the backstop is not optional: chart events are explicitly   |
//|   droppable. "If the ChartEvent is already in an mql5 program     |
//|   queue or such an event is being handled, then a new event of    |
//|   this type is not placed into a queue", and "Event queues have a |
//|   limited but sufficient size ... When the queue overflows, new   |
//|   events are discarded without being set into a queue"            |
//|   (.../docs/event_handlers). The design answers that by carrying  |
//|   no data in the event: it is an alarm clock, and the cursor is   |
//|   the truth, so a coalesced or discarded event costs latency and  |
//|   never a tick. evt_late in the stats line is how often that      |
//|   happened -- the backstop got there first.                       |
//|                                                                   |
//|   Why the spy handle uses the *coarsest* timeframe (InpSpyPeriod): |
//|   a spy is an alarm clock and its timeframe is irrelevant to when   |
//|   it rings -- "In indicators, the OnCalculate() function is called  |
//|   after the arrival of each tick" (.../docs/series/copyticks). But  |
//|   iCustom(symbol, period, ...) still binds the handle to a          |
//|   (symbol, period) *timeseries*, and the terminal has to build and  |
//|   hold that series to hand OnCalculate its rates arrays. Its size   |
//|   is bounded by Tools > Options > Charts > "Max bars in charts",    |
//|   which "restricts number of bars in HC format available to charts, |
//|   indicators and mql5 programs ... and serves, first of all, to     |
//|   save computer resources"; the docs warn that with deep history on |
//|   small timeframes "memory used for storing timeseries and          |
//|   indicator buffers can become hundreds of megabytes"               |
//|   (.../docs/series/timeseries_access).                              |
//|                                                                     |
//|   Measured, on a terminal with Max bars = 100 000 000 and 72 extra  |
//|   symbols: PERIOD_M1 spies drove the terminal's working set to      |
//|   16.8 GB -- ~233 MB per spied symbol, one deep M1 series each --   |
//|   against 423 MB for the same 72 symbols in EXTRA_POLL. PERIOD_MN1  |
//|   asks for the same events off a series of a few hundred bars.      |
//|   Nothing about delivery changes; only what the handle drags along. |
//|                                                                     |
//|   EXTRA_EVENT needs MQL5\Indicators\TickStreamer\TickSpy.ex5      |
//|   compiled in this terminal. Without it every iCustom() returns   |
//|   INVALID_HANDLE, the EA says so once per symbol and behaves      |
//|   exactly like EXTRA_POLL.                                        |
//|                                                                   |
//| How the extra-symbol cursor works:                                |
//|   CopyTicks(from) is inclusive ("ticks with time >= from"), so a  |
//|   poll always gets the last tick it already sent back again, and  |
//|   a millisecond can hold more than one tick. Bumping `from` by    |
//|   1 ms would therefore drop the siblings of the last tick sent.   |
//|   Instead each feed remembers last_msc *and* how many ticks it    |
//|   has already taken from that millisecond (seen_at_last_msc), and |
//|   the poll skips exactly that many. Both are re-derived from the  |
//|   tail of every batch, so the cursor is self-correcting.          |
//|   One collection takes at most EXTRA_MAX_TICKS per symbol; the    |
//|   remainder waits for the next one. That is 256 x ~60-100 timer   |
//|   polls per second in EXTRA_POLL mode, or 256 per backstop tick   |
//|   plus 256 per spy event in EXTRA_EVENT mode -- either way orders |
//|   of magnitude above any real FX tick rate.                       |
//|   The cap has one hard edge, and it is a bound this documents     |
//|   rather than hides: a single millisecond holding EXTRA_MAX_TICKS |
//|   ticks or more cannot be drained by asking again, since          |
//|   the same capped batch comes back every time and the cursor      |
//|   never leaves that millisecond. Poll() detects it -- a full      |
//|   batch that produced no record, i.e. every tick in it was        |
//|   already delivered -- and forces the cursor to last_msc + 1.     |
//|   That millisecond's remainder is lost, deliberately, so the      |
//|   symbol keeps streaming; everything after it arrives normally.   |
//|   cursor_skip in the stats line counts how often it happened.     |
//|   It counts occurrences, not records: the ticks past the cap were |
//|   never returned by CopyTicks(), so there is nothing to count     |
//|   them by. Any non-zero value means raising EXTRA_MAX_TICKS.      |
//|   A backlog deeper than the terminal's 4096-tick memory cache is  |
//|   served from the on-disk tick database: slower, not lost.        |
//|   "Every tick" means every tick while the link is up. Records     |
//|   that do not fit or that a failed send discards are counted in   |
//|   `dropped` and are not replayed after a reconnect -- the cursor  |
//|   has moved on, and re-sending stale quotes would corrupt every   |
//|   latency number downstream.                                      |
//|                                                                   |
//| Warm-up (why the first CopyTicks() is not in the timer):          |
//|   The first CopyTicks() for a symbol synchronises that symbol's   |
//|   tick database and may block the calling thread for up to 45 s.  |
//|   An EA is single-threaded, so paying that inside OnTimer() would |
//|   freeze OnTick() and lose the chart symbol's ticks. OnInit()     |
//|   therefore warms symbols up front, seeding each cursor from the  |
//|   newest tick so no history is streamed on start, and stops after |
//|   WARMUP_BUDGET_MS. Whatever is left is warmed one symbol per     |
//|   timer tick and is not polled until it has been: a straggler     |
//|   pays its possible sync block in the single tick that warms it,  |
//|   once, and the steady-state timer never meets it again.          |
//|                                                                   |
//| Timestamps:                                                       |
//|   MqlTick.time_msc is broker server time, while the wire protocol |
//|   defines time_msc as UTC milliseconds, so the EA normalises it.  |
//|   The offset is estimated as TimeTradeServer() - TimeGMT() rounded|
//|   to the nearest 30 minutes and re-checked once a minute, so DST  |
//|   changes on the broker side are picked up on their own.          |
//|                                                                   |
//| The periodic stats line (InpStatsSec, InpVerbose):                |
//|   One line of key=value pairs per interval. Fields:               |
//|     ticks=        quote+heartbeat records sent this interval      |
//|     dropped=      records that never reached the bridge: ones a   |
//|                   full buffer refused, ones a flush with the link |
//|                   down discarded, and -- when SocketSend returns  |
//|                   short -- only the records past the byte it      |
//|                   stopped at, a straddling record counted lost in |
//|                   full. The records that did go out in a short    |
//|                   send are counted in ticks=, because they        |
//|                   arrived. A failed send is not a lost buffer.    |
//|     send_us avg/max  time inside SocketSend()                     |
//|     reconnects=   connects completed this interval                |
//|     total_sent=   records sent since OnInit()                     |
//|     symbols=      extra symbols collected (0 = chart only)        |
//|     mode=         poll | event (InpExtraMode)                     |
//|     poll_n=       timer polls measured this interval              |
//|     poll_us_avg=  mean OnTimer extra-symbol poll-loop duration    |
//|     poll_us_max=  worst poll-loop duration                        |
//|     poll_us_p99=  ~99th percentile over the last <=1024 polls of  |
//|                   the interval (approximate: a ring that size)    |
//|     ping_us=      TERMINAL_PING_LAST, the terminal->broker ping.  |
//|                   A free proxy for the hop this EA cannot see.    |
//|     extra_obs=    ticks CopyTicks() returned for extra symbols    |
//|     extra_sent=   records the poll produced from them. The        |
//|                   difference is the duplicates the inclusive      |
//|                   `from` hands back, one per symbol per poll that |
//|                   returned anything -- not loss.                  |
//|     ct_n=         CopyTicks() calls made collecting extra symbols |
//|                   -- from the timer *and* from spy events         |
//|     ct_us_avg/max time inside CopyTicks(), warm-up excluded       |
//|     ct_err=       CopyTicks() calls that returned -1              |
//|     cursor_skip=  times a cursor was forced past a millisecond    |
//|                   holding EXTRA_MAX_TICKS ticks or more. An       |
//|                   occurrence count, not a record count: what sat  |
//|                   past the cap was never returned, so it cannot   |
//|                   be counted. Non-zero means raise the cap.       |
//|     evt_n=        spy events handled (0 unless InpExtraMode is    |
//|                   EXTRA_EVENT; 0 *in* that mode means the spies   |
//|                   are not running -- see the fields below)        |
//|     evt_us_avg/max time inside OnChartEvent: the collect and the  |
//|                   send it triggered. The event-mode equivalent of |
//|                   poll_us_*, which stays timer-loop-only.         |
//|     evt_late=     handled events whose collect produced no record |
//|                   -- the backstop, or an earlier event, had       |
//|                   already taken that tick. A ratio near evt_n     |
//|                   means the events are not buying any latency.    |
//|     evt_bad=      events whose symbol index or name this EA does  |
//|                   not recognise: a stale event from a previous    |
//|                   list, or another program using the same id.     |
//|                   Ignored, never guessed at.                      |
//|   Events arriving while the socket is down are ignored and not    |
//|   counted at all: the cursor stays put and the first collect      |
//|   after the reconnect picks the ticks up.                         |
//|                                                                   |
//| Measuring the symbol scaling table:                               |
//|   poll_us_* and ct_us_* both come out of one run now: CopyTicks() |
//|   is the delivery path, not a diagnostic bolted onto it, so       |
//|   nothing has to be measured with the mode off. Ground truth for  |
//|   "were any ticks lost" is taken offline instead of by the EA:    |
//|   CopyTicksRange(sym, buf, COPY_TICKS_ALL, t0*1000, t1*1000) --   |
//|   both ends inclusive -- counted against the records the wire     |
//|   carried for that symbol in [t0, t1]. By construction the two    |
//|   agree; `dropped` accounts for any difference.                   |
//+------------------------------------------------------------------+
#property copyright "mt5-ws-stream contributors"
#property link      "https://github.com/komo135/mt5-ws-stream"
// MetaEditor warns that a 0.x version is "incompatible with MQL5 Market":
// the check is on the major number, not the notation, and every 0.x spelling
// raises it. This EA is pre-1.0 and the docs call it 0.2, so the warning is
// left standing rather than answered with a version number that is not true.
#property version   "0.2"
#property description "Streams ticks to a local bridge over TCP (64-byte binary records)."

//--- Inputs ---------------------------------------------------------
input group "Connection"
input string InpHost           = "127.0.0.1"; // Bridge host
input int    InpPort           = 9800;        // Bridge TCP port
input int    InpReconnectMs    = 2000;        // Reconnect backoff (ms)
input int    InpHeartbeatMs    = 1000;        // Heartbeat interval (ms, 0 = off; the timer period is its floor)

//--- How extra symbols learn that they have a new tick. Both modes deliver
//--- every tick -- the difference is what wakes the collection up, and
//--- therefore how long a tick waits before it is collected.
enum ExtraSymbolMode
  {
   EXTRA_POLL  = 0,   // Timer polling only
   EXTRA_EVENT = 1    // Spy indicators wake the EA; timer polls as a backstop
  };

input group "Symbols"
input string          InpSymbols        = "";           // Extra symbols, streamed in full: comma list, or * = all Market Watch (empty = chart only)
input ExtraSymbolMode InpExtraMode      = EXTRA_POLL;   // How extra symbols are collected (EXTRA_EVENT needs Indicators\TickStreamer\TickSpy.ex5)
input int             InpPollMs         = 10;           // How often extra symbols are polled (ms; real timer resolution is 10-16 ms)
input int             InpEventBackstopMs = 100;         // EXTRA_EVENT only: backstop poll period (ms; <=0 = use InpPollMs; also floors the heartbeat)
input ENUM_TIMEFRAMES InpSpyPeriod      = PERIOD_MN1;  // EXTRA_EVENT only: spy handle timeframe (coarsest = least memory; see header)

input group "Timestamps"
input bool   InpUtcTimestamps  = true;        // Convert time_msc from server time to UTC

input group "Diagnostics"
input bool   InpVerbose        = true;        // Log connection events and errors
input int    InpStatsSec       = 60;          // Print throughput/send-time summary every N s (0 = off)

//--- Wire protocol (must match mt5_ws_stream/protocol.py) -----------
#define REC_SIZE        64
#define WIRE_MAGIC      0x4B54                // 'TK'
#define FLAG_HEARTBEAT  0x80000000
#define MAX_BATCH       256                   // Records per SocketSend at most

//--- Watchdog tuning ------------------------------------------------
#define CONNECT_TIMEOUT_MS 1000               // SocketConnect() budget per attempt
#define SHORT_LIVED_MS  3000                  // A connection dying sooner than this is "short-lived"
#define SHORT_LIVED_MAX 3                     // Short-lived connections in a row before the hint
#define UTC_REFRESH_MS  60000                 // Re-estimate the server-time offset this often

//--- Extra-symbol collection ----------------------------------------
#define EXTRA_MAX_TICKS   256                 // Ticks one poll may take from one symbol
#define WARMUP_BUDGET_MS  5000                // OnInit() stops warming symbols up after this

//--- The spy indicator (EXTRA_EVENT mode) ---------------------------
//--- Name as iCustom() resolves it: relative names are looked for in the
//--- calling program's folder first and then under MQL5\Indicators, so the
//--- file must be installed as MQL5\Indicators\TickStreamer\TickSpy.ex5.
#define SPY_INDICATOR   "TickStreamer\\TickSpy"
#define EVT_SPY_TICK    1                     // Must match TickSpy.mq5's EVT_SPY_TICK

//--- Measurement ----------------------------------------------------
#define POLL_RING       1024                  // Poll durations kept for the p99 estimate

//--- Composition: the three functions where two modules meet. Declared
//--- here because the poll loop, which lives inside ExtraSymbolList,
//--- calls them.
void EmitQuote(const string symbol, const MqlTick &tick);
bool Flush();

//--- Bit-exact double <-> ulong conversion --------------------------
union DoubleBits
  {
   double            value;
   ulong             bits;
  };

//+------------------------------------------------------------------+
//| Little-endian writers                                             |
//|                                                                   |
//| These mirror protocol.py's `<HHI12sqddddI` layout field for field. |
//+------------------------------------------------------------------+
void WriteU16(uchar &buf[], int &pos, const ushort value)
  {
   buf[pos]     = (uchar)( value        & 0xFF);
   buf[pos + 1] = (uchar)((value >>  8) & 0xFF);
   pos += 2;
  }

void WriteU32(uchar &buf[], int &pos, const uint value)
  {
   for(int i = 0; i < 4; i++)
      buf[pos + i] = (uchar)((value >> (8 * i)) & 0xFF);
   pos += 4;
  }

void WriteU64(uchar &buf[], int &pos, const ulong value)
  {
   for(int i = 0; i < 8; i++)
      buf[pos + i] = (uchar)((value >> (8 * i)) & 0xFF);
   pos += 8;
  }

void WriteI64(uchar &buf[], int &pos, const long value)
  {
   WriteU64(buf, pos, (ulong)value);
  }

void WriteDouble(uchar &buf[], int &pos, const double value)
  {
   DoubleBits conv;
   conv.value = value;
   WriteU64(buf, pos, conv.bits);
  }

//--- 12-byte NUL-padded symbol field
void WriteSymbol(uchar &buf[], int &pos, const string symbol)
  {
   uchar tmp[];
   int len = StringToCharArray(symbol, tmp, 0, WHOLE_ARRAY, CP_UTF8);
   for(int i = 0; i < 12; i++)
     {
      uchar c = 0;
      if(i < len && tmp[i] != 0)
         c = tmp[i];
      buf[pos + i] = c;
     }
   pos += 12;
  }

//+------------------------------------------------------------------+
//| ServerClock -- broker server time -> UTC                          |
//|                                                                   |
//| MqlTick.time_msc is stamped in broker server time; the protocol    |
//| wants UTC. Real broker offsets are whole half hours, so rounding   |
//| to 30 minutes turns the noisy difference into an exact figure.     |
//| The offset stays 0 while the conversion is off, which keeps the    |
//| hot path down to a single subtraction either way.                  |
//+------------------------------------------------------------------+
struct ServerClock
  {
private:
   long              m_offset_ms;       // Server time minus UTC, in ms
   ulong             m_next_ms;         // Next re-estimate

   //--- Returns true when the offset changed.
   bool              Estimate()
     {
      datetime server = TimeTradeServer();
      if(server == 0)          // No server time yet (terminal not connected)
         return false;

      long offset_s = (long)server - (long)TimeGMT();
      offset_s = (long)MathRound((double)offset_s / 1800.0) * 1800;

      long offset_ms = offset_s * 1000;
      if(offset_ms == m_offset_ms)
         return false;

      m_offset_ms = offset_ms;
      return true;
     }

public:
                     ServerClock() { m_offset_ms = 0; m_next_ms = 0; }

   long              ToUtcMsc(const long server_msc) const { return server_msc - m_offset_ms; }
   double            OffsetHours() const { return (double)m_offset_ms / 3600000.0; }

   void              Start(const ulong now)
     {
      if(InpUtcTimestamps)
         Estimate();
      m_next_ms = now + UTC_REFRESH_MS;
     }

   //--- Re-estimate now and then: brokers move with DST and the terminal
   //--- re-syncs its clock. One comparison per timer tick.
   void              Refresh(const ulong now)
     {
      if(!InpUtcTimestamps || now < m_next_ms)
         return;
      m_next_ms = now + UTC_REFRESH_MS;
      double before = OffsetHours();
      if(Estimate() && InpVerbose)
         PrintFormat("[TickStreamer] server-UTC offset changed %+.1fh -> %+.1fh",
                     before, OffsetHours());
     }
  };

ServerClock g_clock;

//+------------------------------------------------------------------+
//| SendBuffer -- the outgoing bytes and the record layout            |
//|                                                                   |
//| Append() is the only code that knows the 64-byte layout, and the   |
//| sequence number lives here because it is a property of the wire,   |
//| not of the sender: it advances only when a record actually lands,  |
//| so a record dropped for want of room leaves no gap for the bridge  |
//| to report. Append() is handed a finished time_msc -- deciding      |
//| whether it needed the UTC shift is the caller's job, because the   |
//| caller is what knows which kind of record it is producing.         |
//+------------------------------------------------------------------+
struct SendBuffer
  {
private:
   uchar             m_bytes[];
   int               m_len;
   uint              m_seq;

public:
                     SendBuffer() { m_len = 0; m_seq = 0; }

   void              Init() { ArrayResize(m_bytes, REC_SIZE * MAX_BATCH); m_len = 0; }
   int               Len()     const { return m_len; }
   int               Records() const { return m_len / REC_SIZE; }
   bool              IsFull()  const { return m_len >= REC_SIZE * MAX_BATCH; }
   void              Clear() { m_len = 0; }

   //--- Returns false when the record did not fit; nothing is written and
   //--- the sequence number does not move.
   bool              Append(const string symbol, const long time_msc,
                            const MqlTick &tick, const uint extra_flags)
     {
      if(m_len + REC_SIZE > ArraySize(m_bytes))
         return false;

      int pos = m_len;
      WriteU16(m_bytes, pos, (ushort)WIRE_MAGIC);
      WriteU16(m_bytes, pos, (ushort)REC_SIZE);
      WriteU32(m_bytes, pos, m_seq++);
      WriteSymbol(m_bytes, pos, symbol);
      WriteI64(m_bytes, pos, time_msc);
      WriteDouble(m_bytes, pos, tick.bid);
      WriteDouble(m_bytes, pos, tick.ask);
      WriteDouble(m_bytes, pos, tick.last);
      WriteDouble(m_bytes, pos, tick.volume_real);
      WriteU32(m_bytes, pos, (uint)tick.flags | extra_flags);
      m_len = pos;
      return true;
     }

   //--- One syscall. Only Link calls this, so the socket handle stays
   //--- private to Link and the bytes stay private to the buffer.
   int               SendTo(const int socket) { return SocketSend(socket, m_bytes, (uint)m_len); }
  };

SendBuffer g_out;

//+------------------------------------------------------------------+
//| IntervalStats -- every counter behind the periodic summary        |
//|                                                                   |
//| Lifetime totals live here too: they are incremented at the same    |
//| four moments as the interval counters, so keeping them apart would |
//| just mean two places to forget.                                    |
//+------------------------------------------------------------------+
struct IntervalStats
  {
private:
   ulong             m_total_sent;
   ulong             m_total_dropped;

   ulong             m_since_ms;
   ulong             m_due_ms;
   ulong             m_sent;
   ulong             m_dropped;
   ulong             m_send_us_sum;
   ulong             m_send_us_max;
   ulong             m_send_count;
   ulong             m_reconnects;

   //--- OnTimer poll-loop duration, in microseconds. The running sum, count
   //--- and max are exact; the percentile is read off a ring of the last
   //--- POLL_RING samples, which is fixed-size on purpose -- the poll body
   //--- must not allocate. Sorting happens once per interval, in the print.
   ulong             m_poll_count;
   ulong             m_poll_us_sum;
   ulong             m_poll_us_max;
   uint              m_poll_ring[POLL_RING];
   int               m_poll_len;               // Valid samples, <= POLL_RING
   int               m_poll_pos;               // Next write position
   uint              m_poll_sorted[];          // Scratch, touched once per interval

   //--- Extra-symbol collection. Not a diagnostic mode: CopyTicks() is the
   //--- delivery path, so these count what actually happened. `m_extra_obs`
   //--- minus `m_extra_sent` is the duplicates the inclusive `from` hands
   //--- back and the poll skips -- the proof the cursor is working, not loss.
   ulong             m_extra_obs;              // Ticks CopyTicks() returned
   ulong             m_extra_sent;             // Records emitted from them
   ulong             m_ct_count;               // CopyTicks() calls in the poll loop
   ulong             m_ct_us_sum;
   ulong             m_ct_us_max;
   ulong             m_ct_err;                 // CopyTicks() calls that returned -1

   //--- Times a cursor had to be forced past a millisecond holding more ticks
   //--- than one collection may take. An occurrence count, not a record count:
   //--- the ticks past the cap were never returned, so there is nothing to
   //--- count them by. See ExtraSymbolList::Poll().
   ulong             m_cursor_skip;

   //--- Spy-indicator wake-ups (EXTRA_EVENT mode). `m_evt_late` is the
   //--- diagnostic that matters: an event whose poll found nothing new means
   //--- the backstop timer, or an earlier event, already collected that tick.
   //--- A high ratio says the events are arriving too late to be worth having.
   ulong             m_evt_n;                  // Custom events handled
   ulong             m_evt_us_sum;             // Time inside the handler
   ulong             m_evt_us_max;
   ulong             m_evt_late;               // Handled events that produced no record
   ulong             m_evt_bad;                // Events with an index or symbol we do not know

   //--- Approximate p99 of the poll duration: exact over the samples the
   //--- ring holds, which are the last <= POLL_RING polls of the interval.
   //--- Ring slots 0..len-1 are the valid ones whether or not it wrapped.
   ulong             PollP99()
     {
      int n = m_poll_len;
      if(n <= 0)
         return 0;
      if(ArrayResize(m_poll_sorted, n) != n)
         return 0;
      if(ArrayCopy(m_poll_sorted, m_poll_ring, 0, 0, n) != n)
         return 0;
      ArraySort(m_poll_sorted);

      int idx = (int)MathCeil(0.99 * (double)n) - 1;
      if(idx < 0)
         idx = 0;
      if(idx >= n)
         idx = n - 1;
      return (ulong)m_poll_sorted[idx];
     }

public:
   //--- OnInit() resets again against the real clock; this only guarantees no
   //--- member is ever read before that happens.
                     IntervalStats() { m_total_sent = 0; m_total_dropped = 0; Reset(0); }

   ulong             TotalSent()    const { return m_total_sent; }
   ulong             TotalDropped() const { return m_total_dropped; }
   bool              Due(const ulong now) const { return InpStatsSec > 0 && now >= m_due_ms; }

   void              NoteSent(const int records)
     {
      m_sent       += (ulong)records;
      m_total_sent += (ulong)records;
     }

   void              NoteDropped(const int records)
     {
      m_dropped       += (ulong)records;
      m_total_dropped += (ulong)records;
     }

   //--- Two clock reads and a running sum: cheap enough for the hot path.
   void              NoteSendTime(const ulong us)
     {
      m_send_us_sum += us;
      m_send_count++;
      if(us > m_send_us_max)
         m_send_us_max = us;
     }

   void              NoteReconnect() { m_reconnects++; }

   //--- One poll-loop duration. Three adds, one compare and one store:
   //--- cheap enough to run on every timer tick, which is the point --
   //--- the number being measured is the timer callback's own cost.
   void              NotePoll(const ulong us)
     {
      m_poll_count++;
      m_poll_us_sum += us;
      if(us > m_poll_us_max)
         m_poll_us_max = us;

      m_poll_ring[m_poll_pos] = (uint)us;   // A poll body cannot reach 2^32 us
      m_poll_pos++;
      if(m_poll_pos >= POLL_RING)
         m_poll_pos = 0;
      if(m_poll_len < POLL_RING)
         m_poll_len++;
     }

   //--- One CopyTicks() call: what it returned, what the poll made of it, and
   //--- what it cost. Called once per symbol per poll, so it stays down to a
   //--- few adds and one compare.
   void              NoteCopyTicks(const ulong us, const int observed, const int sent)
     {
      m_ct_count++;
      m_ct_us_sum += us;
      if(us > m_ct_us_max)
         m_ct_us_max = us;
      if(observed > 0)
         m_extra_obs += (ulong)observed;
      if(sent > 0)
         m_extra_sent += (ulong)sent;
     }

   void              NoteCopyTicksError() { m_ct_err++; }

   void              NoteCursorSkip() { m_cursor_skip++; }

   //--- One handled spy event and what it was worth. `sent` is the number of
   //--- records the poll it triggered produced, so 0 means the event brought
   //--- no news -- counted separately rather than hidden in the average.
   void              NoteSpyEvent(const ulong us, const int sent)
     {
      m_evt_n++;
      m_evt_us_sum += us;
      if(us > m_evt_us_max)
         m_evt_us_max = us;
      if(sent <= 0)
         m_evt_late++;
     }

   void              NoteSpyEventBad() { m_evt_bad++; }

   void              Reset(const ulong now)
     {
      m_since_ms    = now;
      m_due_ms      = now + (ulong)(InpStatsSec > 0 ? InpStatsSec : 0) * 1000;
      m_sent        = 0;
      m_dropped     = 0;
      m_send_us_sum = 0;
      m_send_us_max = 0;
      m_send_count  = 0;
      m_reconnects  = 0;

      m_poll_count  = 0;
      m_poll_us_sum = 0;
      m_poll_us_max = 0;
      m_poll_len    = 0;   // The percentile describes this interval, not the last one
      m_poll_pos    = 0;

      m_extra_obs   = 0;
      m_extra_sent  = 0;
      m_ct_count    = 0;
      m_ct_us_sum   = 0;
      m_ct_us_max   = 0;
      m_ct_err      = 0;
      m_cursor_skip = 0;

      m_evt_n       = 0;
      m_evt_us_sum  = 0;
      m_evt_us_max  = 0;
      m_evt_late    = 0;
      m_evt_bad     = 0;
     }

   void              Print(const ulong now, const int extra_count)
     {
      ulong  span_ms = (now > m_since_ms) ? now - m_since_ms : 0;
      double span_s  = (double)span_ms / 1000.0;
      double rate    = (span_s > 0.0) ? (double)m_sent / span_s : 0.0;
      ulong  avg_us  = (m_send_count > 0) ? m_send_us_sum / m_send_count : 0;

      ulong  poll_avg = (m_poll_count > 0) ? m_poll_us_sum / m_poll_count : 0;
      ulong  ct_avg   = (m_ct_count > 0) ? m_ct_us_sum / m_ct_count : 0;
      ulong  evt_avg  = (m_evt_n > 0) ? m_evt_us_sum / m_evt_n : 0;

      // The evt_* fields are printed in both modes, zeros included: one line
      // shape means one parser, and "evt_n=0 in EXTRA_EVENT mode" is itself
      // the answer to a question someone will ask.
      PrintFormat("[TickStreamer] last %ds: ticks=%d (%.1f/s) dropped=%d "
                  "send_us avg=%d max=%d reconnects=%d total_sent=%s "
                  "symbols=%d mode=%s poll_n=%s poll_us_avg=%d poll_us_max=%d poll_us_p99=%d "
                  "ping_us=%d extra_obs=%s extra_sent=%s "
                  "ct_n=%s ct_us_avg=%d ct_us_max=%d ct_err=%s cursor_skip=%s "
                  "evt_n=%s evt_us_avg=%d evt_us_max=%d evt_late=%s evt_bad=%s",
                  (int)(span_ms / 1000), (int)m_sent, rate, (int)m_dropped,
                  (int)avg_us, (int)m_send_us_max, (int)m_reconnects,
                  (string)m_total_sent,
                  extra_count, (InpExtraMode == EXTRA_EVENT) ? "event" : "poll",
                  (string)m_poll_count,
                  (int)poll_avg, (int)m_poll_us_max, (int)PollP99(),
                  (int)TerminalInfoInteger(TERMINAL_PING_LAST),
                  (string)m_extra_obs, (string)m_extra_sent,
                  (string)m_ct_count, (int)ct_avg, (int)m_ct_us_max,
                  (string)m_ct_err, (string)m_cursor_skip,
                  (string)m_evt_n, (int)evt_avg, (int)m_evt_us_max,
                  (string)m_evt_late, (string)m_evt_bad);
     }
  };

IntervalStats g_stats;

//+------------------------------------------------------------------+
//| Link -- the socket, its backoff and its watchdog                  |
//+------------------------------------------------------------------+

//--- How a connect attempt ended. The three failures are three different
//--- pieces of advice to the user, and only one of them says a connection
//--- ever existed -- which is what the short-lived counter counts.
enum ConnectOutcome
  {
   CONNECT_LIVE,           // Connected, and the socket is up
   CONNECT_TIMED_OUT,      // Burned the whole budget without an answer
   CONNECT_DROPPED,        // Accepted, then gone before it could be used
   CONNECT_UNREACHABLE     // Refused, or failed outright
  };

struct Link
  {
private:
   int               m_socket;
   ulong             m_connected_at_ms;   // 0 = no live connection
   ulong             m_retry_after_ms;
   int               m_short_lived;       // Connections dropped within SHORT_LIVED_MS, in a row
   bool              m_hint_shown;        // The "is a bridge really listening" hint is one-shot

   void              Backoff() { m_retry_after_ms = GetTickCount64() + (ulong)InpReconnectMs; }

   //--- A connection that is accepted and then dies almost immediately means
   //--- something really is listening but hangs up on us -- a peer that is not
   //--- the bridge, or a bridge that keeps restarting. A connect that never
   //--- completed is reported separately and must not land here.
   void              NoteShortLived()
     {
      m_short_lived++;
      if(m_short_lived >= SHORT_LIVED_MAX && !m_hint_shown)
        {
         m_hint_shown = true;
         PrintFormat("[TickStreamer] connection dropped right after connecting %d times in a row: "
                     "something is listening on %s:%d but hanging up. Is it really the bridge, "
                     "and does it stay up? Start it with 'mt5-ws-stream bridge'.",
                     m_short_lived, InpHost, InpPort);
        }
     }

   void              ClearShortLived()
     {
      m_short_lived = 0;
      m_hint_shown  = false;
     }

   //--- SocketConnect() returns true even when it never connected at all: on a
   //--- host that drops SYNs to closed ports instead of refusing them (WSL2
   //--- mirrored networking, some firewalls) it simply burns the whole timeout
   //--- and still reports success, leaving SocketIsConnected() false. That looks
   //--- exactly like a handshake that died on arrival, so the elapsed time is
   //--- what tells the two apart -- a real peer answers in single-digit ms.
   //--- Without the reset the error code below is whatever failed last -- a
   //--- SocketSend from the previous connection, say -- reported as if this
   //--- connect raised it.
   ConnectOutcome    Attempt(uint &elapsed_ms, int &err)
     {
      ResetLastError();
      ulong started_at = GetTickCount64();
      bool  reached    = SocketConnect(m_socket, InpHost, InpPort, CONNECT_TIMEOUT_MS);
      err        = GetLastError();
      elapsed_ms = (uint)(GetTickCount64() - started_at);

      if(reached && SocketIsConnected(m_socket))
         return CONNECT_LIVE;
      if(elapsed_ms >= CONNECT_TIMEOUT_MS)
         return CONNECT_TIMED_OUT;
      if(reached)
         return CONNECT_DROPPED;
      return CONNECT_UNREACHABLE;
     }

   void              ReportFailure(const ConnectOutcome outcome, const uint elapsed_ms, const int err)
     {
      if(outcome == CONNECT_TIMED_OUT)
         PrintFormat("[TickStreamer] connect to %s:%d timed out after %u ms (error %d): "
                     "nothing is listening there, or this host does not refuse closed "
                     "loopback ports (WSL2 mirrored networking, firewall). "
                     "Start the bridge with 'mt5-ws-stream bridge'.",
                     InpHost, InpPort, elapsed_ms, err);
      else if(outcome == CONNECT_DROPPED)
         PrintFormat("[TickStreamer] connection to %s:%d was accepted but dropped at once "
                     "after %u ms (error %d). Is the bridge running?",
                     InpHost, InpPort, elapsed_ms, err);
      else
         PrintFormat("[TickStreamer] cannot reach %s:%d after %u ms (error %d). "
                     "Is the bridge running?", InpHost, InpPort, elapsed_ms, err);
     }

public:
                     Link()
     {
      m_socket          = INVALID_HANDLE;
      m_connected_at_ms = 0;
      m_retry_after_ms  = 0;
      m_short_lived     = 0;
      m_hint_shown      = false;
     }

   bool              IsOpen()  const { return m_socket != INVALID_HANDLE; }
   bool              IsUp()    const { return m_socket != INVALID_HANDLE && SocketIsConnected(m_socket); }
   bool              IsStale() const { return m_socket != INVALID_HANDLE && !SocketIsConnected(m_socket); }
   bool              RetryDue(const ulong now) const { return now >= m_retry_after_ms; }

   int               Send(SendBuffer &out) { return out.SendTo(m_socket); }

   bool              Connect()
     {
      if(IsUp())
         return true;

      if(IsOpen())
        {
         SocketClose(m_socket);
         m_socket = INVALID_HANDLE;
        }

      m_socket = SocketCreate();
      if(m_socket == INVALID_HANDLE)
        {
         if(InpVerbose)
            PrintFormat("[TickStreamer] SocketCreate failed (error %d). Add %s to "
                        "Tools > Options > Expert Advisors > allowed URLs.",
                        GetLastError(), InpHost);
         Backoff();
         return false;
        }

      // Keep timeouts short: SocketSend runs inside OnTick() and must not stall it.
      SocketTimeouts(m_socket, 200, 200);

      uint elapsed_ms = 0;
      int  err        = 0;
      ConnectOutcome outcome = Attempt(elapsed_ms, err);
      if(outcome != CONNECT_LIVE)
        {
         if(InpVerbose)
            ReportFailure(outcome, elapsed_ms, err);
         SocketClose(m_socket);
         m_socket = INVALID_HANDLE;
         Backoff();
         // Only CONNECT_DROPPED produced a connection at all. A connect that ran
         // out its timeout never did, so counting it would fire the reconnect-
         // storm hint at what is really just an absent bridge.
         if(outcome == CONNECT_DROPPED)
            NoteShortLived();
         return false;
        }

      m_connected_at_ms = GetTickCount64();
      g_stats.NoteReconnect();   // The first connect from OnInit() counts as one too.
      PrintFormat("[TickStreamer] connected to %s:%d", InpHost, InpPort);
      return true;
     }

   void              Close()
     {
      if(m_socket != INVALID_HANDLE)
        {
         SocketClose(m_socket);
         m_socket = INVALID_HANDLE;
        }
      m_connected_at_ms = 0;
      Backoff();
     }

   //--- Classify a connection that just went away by how long it lived.
   void              NoteLost()
     {
      if(m_connected_at_ms > 0 && GetTickCount64() - m_connected_at_ms < SHORT_LIVED_MS)
         NoteShortLived();
      else
         ClearShortLived();
      m_connected_at_ms = 0;
     }

   //--- The one place the reconnect-storm hint stands down: a connection that
   //--- has outlived the short-lived window proves the peer is real. Both the
   //--- successful send and the timer ask through here, so the rule cannot
   //--- drift into two versions of itself. The clock is read only when there
   //--- is a storm to clear, so the hot path pays nothing.
   void              NoteAliveIfProven()
     {
      if(m_short_lived <= 0 || m_connected_at_ms == 0)
         return;
      if(GetTickCount64() - m_connected_at_ms > SHORT_LIVED_MS)
         ClearShortLived();
     }
  };

Link g_link;

//+------------------------------------------------------------------+
//| Heartbeat -- when the next beat is due                            |
//+------------------------------------------------------------------+
struct Heartbeat
  {
private:
   ulong             m_due_ms;

public:
                     Heartbeat() { m_due_ms = 0; }

   //--- Checks and reschedules in one step, so the schedule can never be
   //--- advanced without a beat actually being emitted. Due at 0 means the
   //--- first connected timer tick beats immediately.
   bool              Claim(const ulong now)
     {
      if(InpHeartbeatMs <= 0 || now < m_due_ms)
         return false;
      m_due_ms = now + (ulong)InpHeartbeatMs;
      return true;
     }
  };

Heartbeat g_beat;

//+------------------------------------------------------------------+
//| SymbolFeed / ExtraSymbolList -- per-symbol delivery state         |
//|                                                                   |
//| One struct per symbol instead of parallel arrays kept in lock-step |
//| by hand. A feed is a *cursor*, not a last-value cache: last_msc    |
//| plus seen_at_last_msc name a position in the symbol's tick stream  |
//| exactly, which is what lets a poll ask for "everything after this" |
//| when a millisecond can hold more than one tick.                    |
//| The chart symbol is one of these too; it only ever uses            |
//| `last_msc`, because OnTick() sees every tick and coalesces none.   |
//+------------------------------------------------------------------+
struct SymbolFeed
  {
   string            name;
   long              last_msc;           // Raw broker server time of the last tick delivered
   int               seen_at_last_msc;   // Ticks already delivered out of that millisecond
   bool              warmed;             // The up-front CopyTicks() has run for this symbol
   bool              ready;              // The cursor holds a real position; safe to poll
   int               spy;                // TickSpy handle, or INVALID_HANDLE (EXTRA_POLL, or iCustom failed)

                     SymbolFeed()
     {
      name             = "";
      last_msc         = 0;
      seen_at_last_msc = 0;
      warmed           = false;
      ready            = false;
      spy              = INVALID_HANDLE;
     }
  };

struct ExtraSymbolList
  {
private:
   SymbolFeed        m_items[];
   MqlTick           m_ticks[];          // CopyTicks() destination, reused every call
   int               m_warm_next;        // First item that may still need warming
   int               m_spy_failed;       // Symbols whose iCustom() call did not produce a handle

   //--- Take the cursor from the tail of a batch: the newest millisecond in it,
   //--- and how many of the returned ticks carry that millisecond. Both are
   //--- re-derived from scratch rather than incremented, so a batch that is
   //--- not what we expected corrects the cursor instead of corrupting it.
   //--- Truncation is safe: when a poll hit its cap mid-millisecond the count
   //--- is partial, and the next poll asks from that millisecond again and
   //--- skips exactly the partial count.
   void              AdoptTail(SymbolFeed &feed, const int got)
     {
      long newest = m_ticks[got - 1].time_msc;
      if(newest < feed.last_msc)        // Never move the cursor backwards
         return;

      int n = 0;
      for(int i = got - 1; i >= 0 && m_ticks[i].time_msc == newest; i--)
         n++;

      feed.last_msc         = newest;
      feed.seen_at_last_msc = n;
      feed.ready            = true;
     }

   //--- Anchor a symbol whose tick database had nothing to give at warm-up.
   //--- The Market Watch quote is enough to say "start here" and costs no
   //--- synchronisation. The anchoring tick is deliberately not emitted: with
   //--- seen_at_last_msc = 0 the next poll returns it together with every
   //--- sibling sharing its millisecond, so nothing is lost and nothing is
   //--- sent twice.
   void              Anchor(SymbolFeed &feed)
     {
      MqlTick tick;
      if(!SymbolInfoTick(feed.name, tick) || tick.time_msc <= 0)
         return;
      feed.last_msc         = tick.time_msc;
      feed.seen_at_last_msc = 0;
      feed.ready            = true;
     }

   //--- One symbol's first CopyTicks(): it synchronises the tick database and
   //--- is the call that can block for up to 45 s, which is why it never runs
   //--- from the poll loop. Asking with from = 0 returns the newest ticks, so
   //--- the cursor lands on the present and no history is streamed on start.
   void              WarmUpOne(SymbolFeed &feed)
     {
      // Without the reset the error reported below is whatever failed last --
      // a SocketSend from OnInit()'s connect, say -- blamed on this call.
      ResetLastError();
      ulong t0  = GetMicrosecondCount();
      int   got = CopyTicks(feed.name, m_ticks, COPY_TICKS_ALL, 0, EXTRA_MAX_TICKS);
      ulong us  = GetMicrosecondCount() - t0;
      int   err = GetLastError();
      feed.warmed = true;

      if(got > 0)
         AdoptTail(feed, got);
      else if(InpVerbose)
         PrintFormat("[TickStreamer] warm-up %s returned %d (error %d); its cursor "
                     "is anchored from Market Watch on the first poll instead",
                     feed.name, got, err);

      // A symbol whose ticks had to come from the trade server is the whole
      // reason this runs outside the timer, so it is worth its own line.
      if(us >= 1000000 && InpVerbose)
         PrintFormat("[TickStreamer] warm-up %s took %d ms (tick database synchronised)",
                     feed.name, (int)(us / 1000));
     }

   //--- One symbol, one poll: every tick that arrived since the cursor, in
   //--- order, as records. This is the whole extra-symbol delivery mechanism
   //--- and the single function a delivery change has to replace. Both the
   //--- timer loop and a spy event run exactly this body -- the two modes
   //--- differ only in what wakes it up, never in what it does.
   //--- Returns how many records it produced, which is what tells a spy event
   //--- apart from a spy event that arrived after the backstop already
   //--- collected the tick.
   //--- `from` is inclusive and shares MqlTick.time_msc's clock (broker server
   //--- time), which is exactly what feed.last_msc holds -- unshifted, because
   //--- the UTC conversion happens in EmitQuote(), one layer up.
   int               Poll(SymbolFeed &feed)
     {
      if(!feed.warmed)         // Its first CopyTicks() has not run yet
         return 0;
      if(!feed.ready)
        {
         Anchor(feed);         // Cheap, and cannot block
         return 0;
        }

      ulong t0  = GetMicrosecondCount();
      int   got = CopyTicks(feed.name, m_ticks, COPY_TICKS_ALL,
                            (ulong)feed.last_msc, EXTRA_MAX_TICKS);
      ulong us  = GetMicrosecondCount() - t0;

      if(got < 0)
        {
         // The cursor is untouched, so the next poll asks for the same span
         // again: a failed call costs latency, never data.
         g_stats.NoteCopyTicksError();
         g_stats.NoteCopyTicks(us, 0, 0);
         return 0;
        }

      int sent = 0;
      int skip = feed.seen_at_last_msc;
      for(int i = 0; i < got; i++)
        {
         long msc = m_ticks[i].time_msc;
         if(msc < feed.last_msc)        // Older than the cursor: cannot happen
            continue;                   // with an inclusive `from`, ignored if it does
         if(msc == feed.last_msc && skip > 0)
           {
            skip--;                     // Already delivered out of this millisecond
            continue;
           }
         EmitQuote(feed.name, m_ticks[i]);
         sent++;
         if(g_out.IsFull())
            Flush();
        }

      // A full batch that produced nothing means every tick in it was already
      // delivered -- EXTRA_MAX_TICKS ticks or more share feed.last_msc. The
      // cursor cannot get past that millisecond by adopting the tail: the next
      // call would ask from it again and be handed the same capped batch for
      // ever, and the rest of the stream behind it would never arrive. Step
      // over the millisecond instead. Whatever sat past the cap is lost and
      // cannot even be counted -- it was never returned -- so the occurrence
      // is what gets counted, in cursor_skip. (At exactly the cap there is no
      // remainder and the advance is lossless; the counter cannot tell, which
      // is the honest reading of a number nobody can measure.)
      if(got >= EXTRA_MAX_TICKS && sent == 0)
        {
         long stuck            = feed.last_msc;
         feed.last_msc         = stuck + 1;
         feed.seen_at_last_msc = 0;
         g_stats.NoteCursorSkip();
         if(InpVerbose)
            PrintFormat("[TickStreamer] %s: %d ticks or more at time_msc=%s, all already "
                        "sent; cursor forced past that millisecond, anything past the "
                        "cap is lost",
                        feed.name, EXTRA_MAX_TICKS, (string)stuck);
         g_stats.NoteCopyTicks(us, got, sent);
         return sent;
        }

      if(got > 0)
         AdoptTail(feed, got);
      g_stats.NoteCopyTicks(us, got, sent);
      return sent;
     }

   //--- Add one symbol to the polled list. The chart symbol is skipped
   //--- (OnTick() already owns it) and so are repeats, so no symbol can
   //--- end up on the wire twice.
   bool              Add(const string symbol)
     {
      if(StringLen(symbol) == 0 || symbol == _Symbol)
         return false;

      int n = ArraySize(m_items);
      for(int i = 0; i < n; i++)
         if(m_items[i].name == symbol)
            return false;

      if(!SymbolSelect(symbol, true))
        {
         PrintFormat("[TickStreamer] cannot add %s to Market Watch; skipping", symbol);
         return false;
        }

      if(ArrayResize(m_items, n + 1) != n + 1)
         return false;
      m_items[n].name             = symbol;
      m_items[n].last_msc         = 0;
      m_items[n].seen_at_last_msc = 0;
      m_items[n].warmed           = false;
      m_items[n].ready            = false;
      m_items[n].spy              = INVALID_HANDLE;
      return true;
     }

public:
                     ExtraSymbolList() { m_warm_next = 0; m_spy_failed = 0; }

   int               Count() const { return ArraySize(m_items); }

   //--- Symbols that asked for a spy and did not get one. OnInit() reads this
   //--- to decide how fast the backstop has to run: a list that is only
   //--- partly event-driven still needs a poll period a polled symbol can
   //--- live with.
   int               SpyFailures() const { return m_spy_failed; }

   //--- Turn InpSymbols into the polled list: "*" means every symbol
   //--- currently visible in Market Watch, anything else is a comma list.
   void              Resolve()
     {
      // One allocation for the life of the EA: the poll body must not allocate,
      // and CopyTicks() reuses the memory a reserved array already holds.
      ArrayResize(m_ticks, EXTRA_MAX_TICKS, EXTRA_MAX_TICKS);

      string list = InpSymbols;
      StringTrimLeft(list);
      StringTrimRight(list);

      if(StringLen(list) == 0)
         return;

      if(list == "*")
        {
         int total = SymbolsTotal(true);
         for(int i = 0; i < total; i++)
            Add(SymbolName(i, true));
         return;
        }

      string parts[];
      int count = StringSplit(list, ',', parts);
      for(int i = 0; i < count; i++)
        {
         string symbol = parts[i];
         StringTrimLeft(symbol);
         StringTrimRight(symbol);
         Add(symbol);
        }
     }

   //--- Warm exactly one symbol: the next one that has never been warmed.
   //--- Returns false when the whole list is warm, which is the condition the
   //--- staged warm-up in the timer stops on. One symbol per call is the point:
   //--- the caller decides how much of a 45 s worst case it is prepared to pay
   //--- in one go.
   bool              WarmUpNext()
     {
      int n = ArraySize(m_items);
      while(m_warm_next < n && m_items[m_warm_next].warmed)
         m_warm_next++;
      if(m_warm_next >= n)
         return false;

      WarmUpOne(m_items[m_warm_next]);
      m_warm_next++;
      return true;
     }

   //--- OnInit()'s share of the warm-up: as many symbols as fit in the budget.
   //--- The budget is checked after each symbol, not before, so one slow symbol
   //--- can overrun it -- there is no way to ask CopyTicks() to hurry. What is
   //--- left over is warmed by the timer, one symbol per tick, and is not
   //--- polled until then.
   void              WarmUpWithin(const uint budget_ms)
     {
      int n = ArraySize(m_items);
      if(n <= 0)
         return;

      ulong t0     = GetTickCount64();
      int   warmed = 0;
      while(WarmUpNext())
        {
         warmed++;
         if(GetTickCount64() - t0 >= (ulong)budget_ms)
            break;
        }

      PrintFormat("[TickStreamer] warmed up %d of %d extra symbols in %d ms%s",
                  warmed, n, (int)(GetTickCount64() - t0),
                  (warmed < n) ? "; the rest are warmed one per timer tick" : "");
     }

   //--- The poll loop is the thing the scaling table measures, so it is timed
   //--- on its own: the clock reads bracket exactly this loop and nothing else
   //--- in OnTimer -- not the stats print, not the heartbeat or the flush that
   //--- follow it. An empty list is not a poll and is not counted.
   void              PollAll()
     {
      int n = ArraySize(m_items);
      if(n <= 0)
         return;

      ulong t0 = GetMicrosecondCount();
      for(int i = 0; i < n; i++)
         Poll(m_items[i]);
      g_stats.NotePoll(GetMicrosecondCount() - t0);
     }

   //--- EXTRA_EVENT mode: one TickSpy per symbol, each told which chart to
   //--- wake and which slot of this list it speaks for. The handle is what
   //--- keeps the indicator alive -- an iCustom() handle nobody holds is a
   //--- calculation block the terminal is free to drop -- so it is kept in the
   //--- feed and released in OnDeinit().
   //--- A symbol that fails to get one is not a failure of the EA: it simply
   //--- stays on the timer, like every symbol does in EXTRA_POLL mode. The
   //--- message is printed per symbol because the usual cause is one specific
   //--- thing (TickSpy.ex5 not installed) and the user needs to be told it.
   void              AttachSpies(const long target_chart)
     {
      int n = ArraySize(m_items);
      if(n <= 0)
         return;

      ulong t0 = GetTickCount64();
      for(int i = 0; i < n; i++)
        {
         ResetLastError();
         m_items[i].spy = iCustom(m_items[i].name, InpSpyPeriod, SPY_INDICATOR,
                                  target_chart, (long)i);
         if(m_items[i].spy != INVALID_HANDLE)
            continue;

         m_spy_failed++;
         PrintFormat("[TickStreamer] no tick spy for %s (iCustom \"%s\" failed, error %d); "
                     "that symbol falls back to timer polling. Compile "
                     "MQL5\\Indicators\\TickStreamer\\TickSpy.mq5 in this terminal.",
                     m_items[i].name, SPY_INDICATOR, GetLastError());
        }

      PrintFormat("[TickStreamer] attached %d of %d tick spies on %s in %d ms",
                  n - m_spy_failed, n, EnumToString(InpSpyPeriod),
                  (int)(GetTickCount64() - t0));
     }

   void              ReleaseSpies()
     {
      int n = ArraySize(m_items);
      for(int i = 0; i < n; i++)
        {
         if(m_items[i].spy == INVALID_HANDLE)
            continue;
         IndicatorRelease(m_items[i].spy);
         m_items[i].spy = INVALID_HANDLE;
        }
     }

   //--- A spy said "my symbol ticked". The event carries no price: it is an
   //--- alarm clock, and the cursor is the truth, so the answer is the same
   //--- Poll() the timer runs -- which is why a coalesced or discarded event
   //--- costs latency and never data.
   //--- The index is what makes this O(1) instead of a string search; the name
   //--- is checked against it because an index is only meaningful for the list
   //--- that issued it, and an event queued before a re-init would carry an
   //--- index from the previous list.
   void              HandleSpyEvent(const long index, const string symbol)
     {
      int n = ArraySize(m_items);
      if(index < 0 || index >= (long)n || m_items[(int)index].name != symbol)
        {
         g_stats.NoteSpyEventBad();
         return;
        }

      // With the link down, Flush() would drop whatever this produced and the
      // cursor would move on past it. Leaving the tick where it is costs
      // nothing: the next poll after the socket returns collects it.
      if(!g_link.IsUp())
         return;

      ulong t0   = GetMicrosecondCount();
      int   sent = Poll(m_items[(int)index]);
      Flush();
      g_stats.NoteSpyEvent(GetMicrosecondCount() - t0, sent);
     }
  };

ExtraSymbolList g_extra;
SymbolFeed      g_chart;

//+------------------------------------------------------------------+
//| Composition: producing records and getting them out               |
//+------------------------------------------------------------------+

//--- A quote carries a broker timestamp, so this is where the UTC shift is
//--- applied -- the one call site that knows what kind of record it holds.
void EmitQuote(const string symbol, const MqlTick &tick)
  {
   if(!g_out.Append(symbol, g_clock.ToUtcMsc(tick.time_msc), tick, 0))
      g_stats.NoteDropped(1);
  }

//--- A heartbeat is stamped in UTC at the source, so it is the one record
//--- the server-clock offset must not touch.
void EmitHeartbeat()
  {
   MqlTick beat;
   ZeroMemory(beat);
   long time_msc = (long)TimeGMT() * 1000;
   if(!g_out.Append("", time_msc, beat, FLAG_HEARTBEAT))
      g_stats.NoteDropped(1);
  }

//--- Send whatever is buffered, in one syscall. Four steps, each belonging to
//--- a different module: hand the bytes to the link, tell the stats how long
//--- that took, account for what was lost if it failed, and let the link know
//--- the peer answered if it did not.
bool Flush()
  {
   if(g_out.Len() == 0)
      return true;

   int records = g_out.Records();

   if(!g_link.IsUp())
     {
      g_stats.NoteDropped(records);
      g_out.Clear();
      return false;
     }

   int   len     = g_out.Len();
   ulong send_t0 = GetMicrosecondCount();
   int   sent    = g_link.Send(g_out);
   g_stats.NoteSendTime(GetMicrosecondCount() - send_t0);

   if(sent != len)
     {
      // A short send is not a lost buffer: the bytes SocketSend did take are on
      // the wire and the bridge will decode them. Only the tail is gone. A
      // record straddling the cut counts as lost in full -- it cannot be
      // completed, and its half already sent is precisely why the socket is
      // torn down in the same breath: leaving that half on the wire would
      // desynchronise the framing for every record after it.
      int unsent = (sent > 0) ? len - sent : len;
      int lost   = (unsent + REC_SIZE - 1) / REC_SIZE;
      if(lost > records)      // Unreachable: unsent <= len, so lost <= records
         lost = records;

      if(InpVerbose)
         PrintFormat("[TickStreamer] SocketSend sent %d of %d bytes (error %d); "
                     "%d of %d records lost; reconnecting",
                     sent, len, GetLastError(), lost, records);
      if(records > lost)
         g_stats.NoteSent(records - lost);
      g_stats.NoteDropped(lost);
      g_out.Clear();
      g_link.NoteLost();
      g_link.Close();
      return false;
     }

   g_stats.NoteSent(records);
   g_out.Clear();
   g_link.NoteAliveIfProven();
   return true;
  }

//+------------------------------------------------------------------+
//| Lifecycle                                                         |
//+------------------------------------------------------------------+
int OnInit()
  {
   g_out.Init();
   g_chart.name = _Symbol;

   g_extra.Resolve();

   // Before the timer, and before the socket: the first CopyTicks() per symbol
   // may block for up to 45 s, and the whole point of doing it here is that
   // nothing else of this EA is running yet.
   g_extra.WarmUpWithin(WARMUP_BUDGET_MS);

   // After the warm-up, so a spy cannot fire at a cursor that has no position
   // yet, and before the timer, so the backstop period can be chosen knowing
   // how many symbols actually got a spy.
   if(InpExtraMode == EXTRA_EVENT)
      g_extra.AttachSpies(ChartID());

   // In chart-only mode the timer only handles reconnects, heartbeats and
   // stats, so it can tick slowly and stay off the hot path entirely.
   // With spies attached the timer is a backstop rather than the delivery
   // path, so it may run slower -- but only while *every* symbol has one: a
   // symbol that fell back to polling needs the poll period it was promised,
   // and that is cheaper to give the whole list than to run two timers.
   int period = 200;
   if(g_extra.Count() > 0)
     {
      period = (InpPollMs < 1) ? 1 : InpPollMs;
      if(InpExtraMode == EXTRA_EVENT && g_extra.SpyFailures() == 0 && InpEventBackstopMs > 0)
         period = InpEventBackstopMs;
     }
   EventSetMillisecondTimer(period);

   // The heartbeat is claimed on the timer, so its real cadence is
   // max(InpHeartbeatMs, timer period) -- a backstop or poll period longer than
   // InpHeartbeatMs silently stretches it. The timer is not bent to fit; the
   // number is simply stated, once, so nobody has to derive it from a stats
   // line. (The bridge's idle timeout is what this has to stay under.)
   if(InpHeartbeatMs > 0 && period > InpHeartbeatMs)
      PrintFormat("[TickStreamer] heartbeats are claimed on the timer, so the effective "
                  "interval is %d ms (the timer period), not InpHeartbeatMs=%d ms",
                  period, InpHeartbeatMs);

   ulong now = GetTickCount64();
   g_stats.Reset(now);
   g_clock.Start(now);

   g_link.Connect();

   PrintFormat("[TickStreamer] started chart=%s extra_symbols=%d mode=%s timer=%dms stats=%ds "
               "server_utc_offset=%+.1fh",
               _Symbol, g_extra.Count(),
               (InpExtraMode == EXTRA_EVENT) ? "event" : "poll",
               period, InpStatsSec, g_clock.OffsetHours());
   return INIT_SUCCEEDED;
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   g_extra.ReleaseSpies();
   Flush();
   g_link.Close();
   Print("[TickStreamer] stopped (reason ", reason,
         ", sent ", (string)g_stats.TotalSent(),
         ", dropped ", (string)g_stats.TotalDropped(), ")");
  }

//+------------------------------------------------------------------+
//| Hot path: the chart symbol, fully event-driven                    |
//+------------------------------------------------------------------+
void OnTick()
  {
   MqlTick tick;
   if(!SymbolInfoTick(g_chart.name, tick))
      return;

   // Deduplicate: OnTick can fire without a new quote (e.g. depth changes).
   if(tick.time_msc == g_chart.last_msc)
      return;
   g_chart.last_msc = tick.time_msc;

   EmitQuote(g_chart.name, tick);
   Flush();
  }

//+------------------------------------------------------------------+
//| Hot path: an extra symbol, woken by its spy indicator             |
//|                                                                   |
//| The same shape as OnTick(): collect one symbol, send. The mode     |
//| test is first and unconditional -- an event queued before a        |
//| re-init can still be delivered after it, and in EXTRA_POLL mode    |
//| there is nothing this EA wants from any custom event at all.       |
//+------------------------------------------------------------------+
void OnChartEvent(const int id, const long &lparam, const double &dparam, const string &sparam)
  {
   if(InpExtraMode != EXTRA_EVENT || id != CHARTEVENT_CUSTOM + EVT_SPY_TICK)
      return;

   g_extra.HandleSpyEvent(lparam, sparam);
  }

//+------------------------------------------------------------------+
//| Timer: three steps, in an order the structure enforces            |
//+------------------------------------------------------------------+

//--- Runs on every timer tick, connected or not. The stats line, the
//--- server-clock refresh and the staged warm-up belong here precisely because
//--- they must keep going while the socket is down; nothing in this function
//--- may touch the link.
//--- The warm-up call is the one place in the timer that can block: a symbol
//--- OnInit()'s budget did not reach synchronises its tick database here, and
//--- that can cost up to 45 s of this one tick. It happens at most once per
//--- symbol, before that symbol is polled at all, and never again afterwards.
void TimerAlways(const ulong now)
  {
   if(g_stats.Due(now))
     {
      if(InpVerbose)
         g_stats.Print(now, g_extra.Count());
      g_stats.Reset(now);
     }

   g_clock.Refresh(now);
   g_extra.WarmUpNext();
  }

//--- The watchdog. Returns true only when the link was already live at the
//--- start of this tick: both repair paths cost the rest of the tick on
//--- purpose. Tearing a dead connection down here and letting the backoff pace
//--- the retry keeps this thread out of SocketConnect(), which would otherwise
//--- park it for up to a second and swallow OnTick events with it.
bool TimerServiceLink(const ulong now)
  {
   if(g_link.IsStale())
     {
      if(InpVerbose)
         PrintFormat("[TickStreamer] connection lost; reconnecting in %d ms", InpReconnectMs);
      g_link.NoteLost();
      g_link.Close();
      return false;
     }

   if(!g_link.IsOpen())
     {
      if(g_link.RetryDue(now))
         g_link.Connect();
      return false;
     }

   g_link.NoteAliveIfProven();
   return true;
  }

//--- Everything that needs a live socket.
void TimerWhenConnected(const ulong now)
  {
   g_extra.PollAll();

   if(g_beat.Claim(now))
      EmitHeartbeat();

   Flush();
  }

void OnTimer()
  {
   ulong now = GetTickCount64();

   TimerAlways(now);
   if(!TimerServiceLink(now))
      return;
   TimerWhenConnected(now);
  }
//+------------------------------------------------------------------+
