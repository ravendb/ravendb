using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Sparrow.Server.Meters
{
    public abstract class PerformanceMetrics
    {
        protected class MeterItem
        {
            public long Counter;
            public DateTime Start;
            public DateTime End;
            public TimeSpan Duration => End - Start;
        }

        protected class SummerizedItem
        {
            public long TotalCounter;
            public TimeSpan TotalTime;
            public TimeSpan MinTime;
            public TimeSpan MaxTime;
            public DateTime TotalTimeStart;
            public DateTime TotalTimeEnd;
            public long Count;
        }

        protected readonly MeterItem[] Buffer;
        protected int BufferPos = -1;

        private readonly SummerizedItem[] _summerizedBuffer;
        private int _summerizedPos = -1;

        protected DatabasePerformanceMetrics.MetricType Type;

        protected PerformanceMetrics(int currentBufferSize, int summaryBufferSize)
        {
            Buffer = new MeterItem[currentBufferSize];
            _summerizedBuffer = new SummerizedItem[summaryBufferSize];
        }

        protected void StoreInSummarizeItem(MeterItem meterItem, int adjustedTail)
        {
            var newSummary = new SummerizedItem
            {
                TotalTimeStart = meterItem.Start,
                TotalTimeEnd = meterItem.End,
                Count = 1,
                MaxTime = meterItem.Duration,
                MinTime = meterItem.Duration,
                TotalTime = meterItem.Duration,
                TotalCounter = meterItem.Counter,
            };

            for (int i = 0; i < Buffer.Length; i++)
            {
                var oldVal = Interlocked.Exchange(ref Buffer[(adjustedTail + i) % Buffer.Length], null);
                if (oldVal != null)
                {
                    newSummary.TotalTimeStart = newSummary.TotalTimeStart > oldVal.Start ? oldVal.Start : newSummary.TotalTimeStart;
                    newSummary.TotalTimeEnd = newSummary.TotalTimeEnd > oldVal.End ? newSummary.TotalTimeEnd : oldVal.End;
                    newSummary.Count++;
                    newSummary.MaxTime = newSummary.MaxTime > oldVal.Duration ? newSummary.MaxTime : oldVal.Duration;
                    newSummary.MinTime = newSummary.MinTime > oldVal.Duration ? oldVal.Duration : newSummary.MinTime;
                    newSummary.TotalCounter += oldVal.Counter;
                    newSummary.TotalTime += oldVal.Duration;
                }
            }
            var increment = Interlocked.Increment(ref _summerizedPos);
            _summerizedBuffer[increment % _summerizedBuffer.Length] = newSummary;
        }

        protected class PerformanceMetricsRecentStats
        {
            public string Start { get; set; }
            public long Counter { get; set; }
            public long CommandsCounter { get; set; } // relevant only for Transaction meter
            public double Duration { get; set; }
            public DatabasePerformanceMetrics.MetricType Type { get; set; }

            public virtual DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Start)] = Start,
                    [nameof(Counter)] = Counter,
                    [nameof(Duration)] = Duration,
                    [nameof(Type)] = Type
                };
            }
        }

        protected class PerformanceMetricsHistoryStats
        {
            public string Start { get; set; }
            public string End { get; set; }
            public long Counter { get; set; }
            public double Duration { get; set; }
            public double ActiveDuration { get; set; }
            public double MaxDuration { get; set; }
            public double MinDuration { get; set; }
            public DatabasePerformanceMetrics.MetricType Type { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Start)] = Start,
                    [nameof(End)] = End,
                    [nameof(Counter)] = Counter,
                    [nameof(Duration)] = Duration,
                    [nameof(ActiveDuration)] = ActiveDuration,
                    [nameof(MaxDuration)] = MaxDuration,
                    [nameof(MinDuration)] = MinDuration,
                    [nameof(Type)] = Type,
                };
            }
        }

        protected class PerformanceMetricsStats
        {
            public PerformanceMetricsStats(DatabasePerformanceMetrics.MetricType meterType)
            {
                MeterType = meterType;
                Recent = new List<PerformanceMetricsRecentStats>();
                History = new List<PerformanceMetricsHistoryStats>();
            }

            public List<PerformanceMetricsRecentStats> Recent { get; set; }
            public List<PerformanceMetricsHistoryStats> History { get; set; }
            internal DatabasePerformanceMetrics.MetricType MeterType;


            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(DatabasePerformanceMetrics.MetricType)] = MeterType,
                    [nameof(Recent)] = new DynamicJsonArray(Recent.Select(x => x.ToJson())),
                    [nameof(History)] = new DynamicJsonArray(History.Select(x => x.ToJson()))
                };
            }
        }

        protected void AddHistoryStats(IEnumerable<SummerizedItem> history, PerformanceMetricsStats stats)
        {
            foreach (var meter in history)
            {
                var historyStats = new PerformanceMetricsHistoryStats
                {
                    Start = meter.TotalTimeStart.GetDefaultRavenFormat(isUtc: true),
                    End = meter.TotalTimeEnd.GetDefaultRavenFormat(isUtc: true),
                    Counter = meter.TotalCounter,
                    Duration = Math.Round((meter.TotalTimeEnd - meter.TotalTimeStart).TotalMilliseconds, 2),
                    ActiveDuration = Math.Round(meter.TotalTime.TotalMilliseconds, 2),
                    MaxDuration = Math.Round(meter.MaxTime.TotalMilliseconds, 2),
                    MinDuration = Math.Round(meter.MinTime.TotalMilliseconds, 2),
                    Type = Type
                };
                stats.History.Add(historyStats);
            }
        }

        protected IEnumerable<SummerizedItem> GetSummerizedItems()
        {
            for (int pos = 0; pos < _summerizedBuffer.Length; pos++)
            {
                var summerizedItem = _summerizedBuffer[pos];
                if (summerizedItem == null)
                    continue;
                yield return summerizedItem;
            }
        }

        protected abstract void MarkInternalWindowStart();
        protected abstract void MarkInternalWindowEnd();
        protected abstract void Mark(long counter, long commandsCounter, DateTime start, DateTime end);
        public abstract DynamicJsonValue ToJson();

        public struct DurationMeasurement : IDisposable
        {
            private readonly PerformanceMetrics _parent;
            private long _counter;
            private long _commandsCounter;
            private readonly DateTime _start;

            public DurationMeasurement(PerformanceMetrics parent)
            {
                _parent = parent;
                _counter = 0;
                _commandsCounter = 0;
                _start = DateTime.UtcNow;
            }

            public void IncrementCounter(long incVal) => _counter += incVal;
            public void IncrementCommands(int incVal) => _commandsCounter += incVal;
            public void MarkInternalWindowStart() => _parent.MarkInternalWindowStart();
            public void MarkInternalWindowEnd() => _parent.MarkInternalWindowEnd();
            public void Dispose() => _parent.Mark(_counter, _commandsCounter, _start, DateTime.UtcNow);
        }
    }
}
