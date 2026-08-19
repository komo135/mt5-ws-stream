//+------------------------------------------------------------------+
//|                                                      TickSpy.mq5 |
//|                    Part of mt5-ws-stream (MIT licensed)          |
//|              https://github.com/komo135/mt5-ws-stream            |
//+------------------------------------------------------------------+
//| An alarm clock for TickStreamer.mq5, and nothing else.            |
//|                                                                   |
//| Why it exists:                                                    |
//|   An EA's OnTick() only ever fires for its own chart symbol, so    |
//|   TickStreamer collects its extra symbols from the terminal timer  |
//|   -- and that timer "generates events no more than 1 time in       |
//|   10-16 milliseconds due to hardware limitations"                  |
//|   (https://www.mql5.com/en/docs/eventfunctions/eventsetmillisecondtimer).
//|   An indicator has no such floor: "In indicators, the              |
//|   OnCalculate() function is called after the arrival of each tick" |
//|   (https://www.mql5.com/en/docs/series/copyticks, Note).           |
//|   So the EA creates one of these per extra symbol with iCustom()   |
//|   and this indicator pokes the EA the moment its symbol ticks.     |
//|                                                                   |
//| Why it only pokes:                                                |
//|   An indicator cannot open a socket -- "If calling from an         |
//|   indicator, GetLastError() returns the error 4014"                |
//|   (https://www.mql5.com/en/docs/network/socketcreate) -- so it     |
//|   cannot send anything itself. It cannot Sleep() either, because   |
//|   indicators run in the interface thread                           |
//|   (https://www.mql5.com/en/docs/common/sleep), and every indicator |
//|   on one symbol shares one thread, so a slow OnCalculate() here    |
//|   would stall every other indicator on that symbol. The body is    |
//|   therefore a single EventChartCustom() call and a return.         |
//|                                                                   |
//| What it deliberately does not carry:                               |
//|   The tick itself. The EA answers the event with                    |
//|   CopyTicks(from = its own cursor), which returns *everything*     |
//|   since the last record it sent for that symbol. That is what      |
//|   makes a lost or coalesced event cost latency instead of data:    |
//|   the event is an alarm clock, the cursor is the truth. It matters |
//|   because chart events are not guaranteed to arrive -- "if the     |
//|   ChartEvent is already in an mql5 program queue or such an event  |
//|   is being handled, then a new event of this type is not placed    |
//|   into a queue", and "when the queue overflows, new events are     |
//|   discarded without being set into a queue"                        |
//|   (https://www.mql5.com/en/docs/event_handlers).                   |
//|                                                                   |
//| Install:                                                          |
//|   MQL5\Indicators\TickStreamer\TickSpy.ex5 in the terminal's data  |
//|   folder (File > Open Data Folder), compiled with F7. The EA asks  |
//|   for it by the relative name "TickStreamer\\TickSpy", which       |
//|   iCustom() resolves under MQL5\Indicators. Nothing else has to    |
//|   be done with it: it is never dragged onto a chart by hand, and   |
//|   doing so is harmless (see InpTargetChart below).                 |
//|                                                                   |
//| Inputs are positional: TickStreamer passes them through iCustom()  |
//| in this order. Do not reorder them without changing the EA.        |
//+------------------------------------------------------------------+
#property copyright "mt5-ws-stream contributors"
#property link      "https://github.com/komo135/mt5-ws-stream"
#property version   "0.2"
#property description "Wakes TickStreamer.mq5 on every tick of this symbol. Not a chart indicator."

//--- No buffers, no plots, nothing drawn: this indicator produces no
//--- values at all. indicator_chart_window is still required, because an
//--- indicator must declare which window it belongs to.
#property indicator_chart_window
#property indicator_buffers 0
#property indicator_plots   0

//--- Must match TickStreamer.mq5's EVT_SPY_TICK.
#define EVT_SPY_TICK 1

//--- Chart id of the TickStreamer instance to wake. 0 would mean "the
//--- current chart" to EventChartCustom, which is exactly wrong here, so
//--- 0 disables this indicator instead: dropped on a chart by hand, it
//--- does nothing rather than spamming whatever EA is running there.
input long InpTargetChart = 0;   // Target chart id (0 = disabled)
//--- This symbol's position in the EA's extra-symbol list. Sent as lparam
//--- so the EA can find the feed without a string compare; the symbol
//--- name goes along as sparam so the EA can verify the index.
input long InpSymbolIndex = 0;   // Index of this symbol in the EA's list

//+------------------------------------------------------------------+
//| The whole indicator                                               |
//|                                                                   |
//| rates_total is returned unchanged: there is nothing to calculate   |
//| and nothing to keep between calls, so prev_calculated is ignored   |
//| on purpose -- every call means "a tick arrived", which is the only |
//| fact this indicator is interested in.                              |
//+------------------------------------------------------------------+
int OnCalculate(const int rates_total,
                const int prev_calculated,
                const datetime &time[],
                const double &open[],
                const double &high[],
                const double &low[],
                const double &close[],
                const long &tick_volume[],
                const long &volume[],
                const int &spread[])
  {
   if(InpTargetChart <= 0)
      return rates_total;

   EventChartCustom(InpTargetChart, (ushort)EVT_SPY_TICK, InpSymbolIndex, 0.0, _Symbol);
   return rates_total;
  }
//+------------------------------------------------------------------+
