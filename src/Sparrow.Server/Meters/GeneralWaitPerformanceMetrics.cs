using System;
using System.Collections.Generic;
using System.Threading;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Sparrow.Meters
{
    internal class GeneralWaitPerformanceMetrics : PerformanceMetrics
    {
        public GeneralWaitPerformanceMetrics(int currentBufferSize, int summaryBufferSize) : base(currentBufferSize, summaryBufferSize)
        {
            Type = DatabasePerformanceMetrics.MetricType.GeneralWait;
        }

        protected override void Mark(long counter, long commandsCounter, DateTime start, DateTime end)
        {
            var meterItem = new MeterItem
            {
                Start = start,
                Counter = counter,
                End = end
            };

            var pos = Interlocked.Increment(ref BufferPos);
            var adjustedTail = pos % Buffer.Length;

            if (Interlocked.CompareExchange(ref Buffer[adjustedTail], meterItem, null) == null)
                return;
            StoreInSummarizeItem(meterItem, adjustedTail);
        }

        public override DynamicJsonValue ToJson()
        {
            var recent = GetRecentMetrics();
            var history = GetSummerizedItems();

            var stats = new PerformanceMetricsStats(Type);
            foreach (var meter in recent)
            {
                var recentStats = new PerformanceMetricsRecentStats
                {
                    Start = meter.Start.GetDefaultRavenFormat(isUtc: true),
                    Counter = meter.Counter,
                    Duration = Math.Round(meter.Duration.TotalMilliseconds, 2),
                    Type = Type
                };
                stats.Recent.Add(recentStats);
            }

            AddHistoryStats(history, stats);


            return stats.ToJson();
        }

        protected IEnumerable<MeterItem> GetCurrentItems()
        {
            for (int pos = 0; pos < Buffer.Length; pos++)
            {
                var item = Buffer[pos];
                if (item == null)
                    continue;
                yield return item;
            }
        }

        protected List<MeterItem> GetRecentMetrics()
        {
            var list = new List<MeterItem>();
            list.AddRange(GetCurrentItems());
            list.Sort((x, y) => x.Start.CompareTo(y.Start));
            return list;
        }

        protected override void MarkInternalWindowStart()
        {
            throw new NotSupportedException();
        }

        protected override void MarkInternalWindowEnd()
        {
            throw new NotSupportedException();
        }
    }
}
