using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Sparrow.Meters
{
    internal class TransactionPerformanceMetrics : PerformanceMetrics
    {
        protected class TransactionMeterItem : MeterItem
        {
            public InternalWindowDuration InternalDurations;
            public long CommandsCounter;
        }

        private InternalWindowDuration _lastState;

        public class InternalWindowDuration
        {
            public DateTime Start;
            public DateTime End;
            public TimeSpan Duration => End - Start;

            public InternalWindowDuration Prev;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Start)] = Start.GetDefaultRavenFormat(isUtc: true),
                    [nameof(End)] = End.GetDefaultRavenFormat(isUtc: true),
                    [nameof(Duration)] = Math.Round(Duration.TotalMilliseconds, 2)
                };
            }
        }

        private InternalWindowDuration _tail;

        public TransactionPerformanceMetrics(int currentBufferSize, int summaryBufferSize) : base(currentBufferSize, summaryBufferSize)
        {
            Type = DatabasePerformanceMetrics.MetricType.Transaction;
        }

        protected override void MarkInternalWindowStart()
        {
            if (_lastState != null)
                throw new IOException(
                        "_lastState not equels to null when trying to set start of intervalWindow");

            _lastState = new InternalWindowDuration
            {
                Start = DateTime.UtcNow
            };
        }
        protected override void MarkInternalWindowEnd()
        {
            if (_lastState == null)
                throw new IOException("_lastState equels to null when trying to set end of intervalWindow!");

            _lastState.End = DateTime.UtcNow;

            if (Math.Abs(Math.Round((_lastState.End - _lastState.Start).TotalMilliseconds, 2)) < double.Epsilon)
            {
                _lastState = null;
                return;
            }

            _lastState.Prev = _tail;
            _tail = _lastState;
            _lastState = null;
        }


        protected override void Mark(long counter, long commandsCounter, DateTime start, DateTime end)
        {
            var meterItem = new TransactionMeterItem()
            {
                Start = start,
                Counter = counter,
                CommandsCounter = commandsCounter,
                End = end,
                InternalDurations = _tail
            };

            _tail = null;

            var pos = Interlocked.Increment(ref BufferPos);
            var adjustedTail = pos % Buffer.Length;

            if (Interlocked.CompareExchange(ref Buffer[adjustedTail], meterItem, null) == null)
                return;

            StoreInSummarizeItem(meterItem, adjustedTail);
        }

        protected class TransactionPerformanceMetricsRecentStats : PerformanceMetricsRecentStats
        {
            public List<InternalWindowDuration> InternalWindows { get; set; }

            public override DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Start)] = Start,
                    [nameof(Counter)] = Counter,
                    [nameof(CommandsCounter)] = CommandsCounter,
                    [nameof(Duration)] = Duration,
                    [nameof(InternalWindows)] = new DynamicJsonArray(InternalWindows.Select(x => x.ToJson())),
                    [nameof(Type)] = Type
                };
            }
        }

        public override DynamicJsonValue ToJson()
        {
            var recent = GetRecentMetrics();
            var history = GetSummerizedItems();

            var stats = new PerformanceMetricsStats(Type);
            foreach (var meter in recent)
            {
                var recentStats = new TransactionPerformanceMetricsRecentStats
                {
                    Start = meter.Start.GetDefaultRavenFormat(isUtc: true),
                    Counter = meter.Counter,
                    CommandsCounter = meter.CommandsCounter,
                    Duration = Math.Round(meter.Duration.TotalMilliseconds, 2),
                    InternalWindows = new List<InternalWindowDuration>(),
                    Type = Type
                };

                var cur = meter.InternalDurations;
                while (cur != null)
                {
                    recentStats.InternalWindows.Add(cur);
                    cur = cur.Prev;
                }

                recentStats.InternalWindows.Reverse();
                stats.Recent.Add(recentStats);
            }
            AddHistoryStats(history, stats);

            return stats.ToJson();
        }


        protected IEnumerable<TransactionMeterItem> GetCurrentItems()
        {
            for (int pos = 0; pos < Buffer.Length; pos++)
            {
                var item = Buffer[pos] as TransactionMeterItem;
                if (item == null)
                    continue;
                yield return item;
            }
        }

        protected List<TransactionMeterItem> GetRecentMetrics()
        {
            var list = new List<TransactionMeterItem>();
            list.AddRange(GetCurrentItems());
            list.Sort((x, y) => x.Start.CompareTo(y.Start));
            return list;
        }
    }
}
