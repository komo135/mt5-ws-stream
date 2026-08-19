//+------------------------------------------------------------------+
//|                                                  CountTicks.mq5 |
//|                    Part of mt5-ws-stream (MIT licensed)          |
//|              https://github.com/komo135/mt5-ws-stream            |
//+------------------------------------------------------------------+
//| Offline ground truth for "were any ticks lost" -- docs/latency.md |
//| ("Symbol scaling table") and ADR-0004. This is not part of the    |
//| EA on purpose: an implementation cannot be its own witness, so    |
//| the wire-side count (benchmarks/symbol_scaling.py, run against    |
//| the running bridge) and the terminal-side count (this script, run |
//| against the terminal's own tick database) have to come from two   |
//| independent places for the comparison to mean anything.           |
//|                                                                   |
//| What it does: for each symbol, call CopyTicksRange() over exactly |
//| the window the wire side was measured in, count what comes back,  |
//| and discard the ticks themselves -- only the count is kept. One   |
//| line per symbol goes to the Experts log (`symbol,count`), the     |
//| same rows go to InpCsvFile under MQL5\Files\, and a final total   |
//| closes both.                                                       |
//|                                                                   |
//| WHICH CLOCK the window is in:                                     |
//|   InpFromMsc/InpToMsc are compared against MqlTick.time_msc, which |
//|   is the BROKER's server clock -- not UTC. The MQL5 reference says |
//|   only "milliseconds since 1970.01.01" and leaves the base unsaid, |
//|   so this is stated from measurement: on a UTC+3 broker, a two-    |
//|   minute window passed as UTC returned zero ticks for every metal  |
//|   and index and a burst on the exotic FX pairs -- the 21:00 UTC    |
//|   rollover break three hours earlier, not the window asked for.    |
//|   A wire-side window is UTC (the EA normalises time_msc on its way |
//|   out), so add the server offset before passing it here. The EA    |
//|   prints the offset it is using on every start:                    |
//|   "started ... server_utc_offset=+3.0h".                           |
//|                                                                    |
//| Where the window comes from:                                      |
//|   benchmarks/wizard_baseline_sweep.sh --mode after, stage 9,       |
//|   records the N=10 receive window it fed to symbol_scaling.py as   |
//|   both an ISO-8601 UTC string and a Unix epoch second (GT_N10_T0/  |
//|   GT_N10_T1 and their _EPOCH twins). Multiply the epoch seconds by |
//|   1000 to get InpFromMsc / InpToMsc: CopyTicksRange() takes        |
//|   milliseconds since 1970-01-01, and both ends are inclusive       |
//|   ("ticks with time >= from_msc" and "<= to_msc" --                |
//|   https://www.mql5.com/en/docs/series/copyticksrange).             |
//|                                                                   |
//| Reading the CSV against symbol_scaling.py's wire count             |
//| (benchmarks/compare_tick_counts.py does this arithmetic):          |
//|   ticks_lost = terminal_count - wire_count, expected 0 after E2    |
//|   (extra symbols now stream every tick via CopyTicks(), not just   |
//|   the latest one per poll).                                        |
//|                                                                    |
//|   * Heartbeats need no adjustment on either side: they carry no    |
//|     tick payload, so symbol_scaling.py's Aggregator never counts   |
//|     them as ticks in the first place (docs/protocol.md).           |
//|   * The two windows are not measured the same way. The wire window |
//|     is *receive* time -- when symbol_scaling.py's collector saw a  |
//|     frame arrive over the socket. CopyTicksRange() filters on      |
//|     time_msc, the broker's own timestamp for the tick. A tick that |
//|     lands right at the window's t0 or t1 can therefore fall inside |
//|     one window and outside the other by however long it took that  |
//|     tick to travel broker -> terminal -> EA -> bridge -> collector. |
//|     Treat a few ticks of slop at the edges as that gap, not loss:  |
//|     bounded by InpPollMs for extra symbols (they are only ever as   |
//|     fresh as the last poll) and by the OnTick()/SocketSend hop for  |
//|     the chart symbol, which is close to zero (docs/latency.md,     |
//|     "Where the time actually goes"). If InpFromMsc/InpToMsc are the |
//|     wizard's recorded window verbatim, a difference of a handful of |
//|     ticks at N=10 is this slop, not E2 regressing -- widen the      |
//|     window by a poll period or two on each side and re-run if it    |
//|     matters which explanation it is.                                |
//+------------------------------------------------------------------+
#property copyright "mt5-ws-stream contributors"
#property link      "https://github.com/komo135/mt5-ws-stream"
// See TickStreamer.mq5 for why a 0.x version is used despite MetaEditor's
// "incompatible with MQL5 Market" warning: this repo is pre-1.0.
#property version   "0.2"
#property description "Ground-truth tick counter: CopyTicksRange per symbol over a wizard-recorded window."
#property script_show_inputs

//--- Inputs ---------------------------------------------------------

input group "Window (from the wizard's stage 9 output)"
input ulong  InpFromMsc  = 0;  // Window start, UTC epoch ms, inclusive
input ulong  InpToMsc    = 0;  // Window end, UTC epoch ms, inclusive

input group "Symbols"
input string InpSymbols  = "*"; // Comma list, or * = every symbol currently in Market Watch

input group "Options"
input uint   InpFlags    = COPY_TICKS_ALL;             // Tick kinds to count (see CopyTicksRange docs)
input string InpCsvFile  = "TickStreamer_counts.csv";  // Written under MQL5\Files\

//+------------------------------------------------------------------+
//| Turn InpSymbols into the list to count: "*" is every symbol       |
//| currently in Market Watch -- mirrors TickStreamer.mq5's own       |
//| ExtraSymbolList::Resolve(), so the two never disagree about what   |
//| "*" means. Anything else is a comma list, trimmed per entry.       |
//+------------------------------------------------------------------+
void ResolveSymbols(string &out[])
  {
   string list = InpSymbols;
   StringTrimLeft(list);
   StringTrimRight(list);

   ArrayResize(out, 0);
   if(StringLen(list) == 0)
      return;

   if(list == "*")
     {
      int total = SymbolsTotal(true);
      ArrayResize(out, total);
      for(int i = 0; i < total; i++)
         out[i] = SymbolName(i, true);
      return;
     }

   string parts[];
   int count = StringSplit(list, ',', parts);
   for(int i = 0; i < count; i++)
     {
      string symbol = parts[i];
      StringTrimLeft(symbol);
      StringTrimRight(symbol);
      if(StringLen(symbol) == 0)
         continue;
      int n = ArraySize(out);
      ArrayResize(out, n + 1);
      out[n] = symbol;
     }
  }

//+------------------------------------------------------------------+
//| Script entry point.                                                |
//+------------------------------------------------------------------+
void OnStart()
  {
   if(InpFromMsc == 0 || InpToMsc == 0 || InpFromMsc >= InpToMsc)
     {
      PrintFormat("[TickStreamer][CountTicks] InpFromMsc/InpToMsc are not a valid window "
                  "(from=%I64u to=%I64u). Paste the epoch-ms values stage 9 of "
                  "benchmarks/wizard_baseline_sweep.sh --mode after printed (GT_N10_T0_EPOCH / "
                  "GT_N10_T1_EPOCH, multiplied by 1000).",
                  InpFromMsc, InpToMsc);
      return;
     }

   string symbols[];
   ResolveSymbols(symbols);
   int n = ArraySize(symbols);
   if(n == 0)
     {
      PrintFormat("[TickStreamer][CountTicks] no symbols resolved from InpSymbols=\"%s\"", InpSymbols);
      return;
     }

   // FileOpen()'s delimiter defaults to a tab even with FILE_CSV -- pass ','
   // explicitly so the file matches what benchmarks/compare_tick_counts.py
   // (and Python's csv module generally) expects.
   int handle = FileOpen(InpCsvFile, FILE_WRITE | FILE_CSV | FILE_ANSI, ',');
   if(handle == INVALID_HANDLE)
     {
      PrintFormat("[TickStreamer][CountTicks] FileOpen(%s) failed (error %d)",
                  InpCsvFile, GetLastError());
      return;
     }

   FileWrite(handle, "symbol", "count");

   long total  = 0;
   int  errors = 0;

   for(int i = 0; i < n; i++)
     {
      string symbol = symbols[i];

      // One dynamic array per symbol: CopyTicksRange() hands back the whole
      // window in a single call rather than a chunk at a time, so for a
      // 50-plus-symbol sweep the array is counted and freed immediately
      // rather than left to grow across the loop.
      MqlTick ticks[];
      ResetLastError();
      int got = CopyTicksRange(symbol, ticks, InpFlags, InpFromMsc, InpToMsc);
      int err = GetLastError();

      if(got < 0)
        {
         errors++;
         PrintFormat("[TickStreamer][CountTicks] %s: CopyTicksRange failed (error %d)",
                     symbol, err);
         FileWrite(handle, symbol, "error");
         ArrayFree(ticks);
         continue;
        }

      int count = ArraySize(ticks);
      ArrayFree(ticks);
      total += count;

      PrintFormat("[TickStreamer][CountTicks] %s,%d", symbol, count);
      FileWrite(handle, symbol, count);
     }

   FileWrite(handle, "TOTAL", total);
   FileWrite(handle, "");
   FileWrite(handle, "from_msc", InpFromMsc);
   FileWrite(handle, "to_msc", InpToMsc);
   FileWrite(handle, "flags", InpFlags);
   FileWrite(handle, "errors", errors);
   FileClose(handle);

   PrintFormat("[TickStreamer][CountTicks] done: symbols=%d total=%I64d errors=%d csv=%s",
               n, total, errors, InpCsvFile);
  }
//+------------------------------------------------------------------+
