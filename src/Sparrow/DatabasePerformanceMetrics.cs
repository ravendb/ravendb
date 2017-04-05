using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow
{
    public abstract class PerformanceMetrics
    {
        public enum InternalWindowState
        {
            Start,
            Stop
        }
        protected DatabasePerformanceMetrics.MetricType Type;
        protected int CurrentBufferSize;
        protected int SummaryBufferSize;

        public struct DurationMeasurement : IDisposable
        {
            public readonly PerformanceMetrics Parent;
            public long Counter;
            private readonly DateTime _start;

            public DurationMeasurement(PerformanceMetrics parent)
            {
                Parent = parent;
                Counter = 0;
                _start = DateTime.UtcNow;
            }

            public void IncreamentCounter(long incVal) => Counter += incVal;
            public void MarkInternalWindow(InternalWindowState state) => Parent.MarkInternalWindow(state);
            public void Dispose() => Parent.Mark(Counter, _start, DateTime.UtcNow);

        }

        internal abstract void MarkInternalWindow(InternalWindowState state);

        internal abstract void Mark(long counter, DateTime start, DateTime end);
        

        public abstract DynamicJsonValue ToJson();
        
    }

    public class DatabasePerformanceMetrics
    {
        public enum MetricType
        {
            Transaction,
            GeneralWait
        }

        public MetricType MeterType;
        public PerformanceMetrics Buffer;

        public DatabasePerformanceMetrics(MetricType type, int currentBufferSize, int summaryBufferSize)
        {
            BufferSize = currentBufferSize;
            SummaryBufferSize = summaryBufferSize;
            MeterType = type;
            switch (MeterType)
            {
                case MetricType.GeneralWait:
                    Buffer = new GeneralWaitPerformanceMetrics(currentBufferSize, summaryBufferSize);
                    break;
                case MetricType.Transaction:
                    Buffer = new TransactionPerformanceMetrics(currentBufferSize, summaryBufferSize);
                    break;
                default:
                    throw new ArgumentException("Invalid metric type passed to DatabasePerfomanceMetrics " + type);
            }


        }

        public int BufferSize { get; }
        public int SummaryBufferSize { get; }

        public PerformanceMetrics.DurationMeasurement MeterPerformanceRate()
        {
            return new PerformanceMetrics.DurationMeasurement(Buffer); // TODO : what happens on conccurent calls ?
        }

    }

    internal class GeneralWaitPerformanceMetrics : PerformanceMetrics
    {
        public class MeterItem
        {
            public long Counter;
            public DateTime Start;
            public DateTime End;
            public TimeSpan Duration => End - Start;
        }

        public class SummerizedItem
        {
            public long TotalCounter;
            public TimeSpan TotalTime;
            public TimeSpan MinTime;
            public TimeSpan MaxTime;
            public DateTime TotalTimeStart;
            public DateTime TotalTimeEnd;
            public long Count;
        }

        private readonly MeterItem[] _buffer;
        private int _bufferPos = -1;

        private readonly SummerizedItem[] _summerizedBuffer;
        private int _summerizedPos = -1;

        public GeneralWaitPerformanceMetrics(int currentBufferSize, int summaryBufferSize)
        {
            Type = DatabasePerformanceMetrics.MetricType.GeneralWait;
            CurrentBufferSize = currentBufferSize;
            SummaryBufferSize = summaryBufferSize;
            _buffer = new MeterItem[CurrentBufferSize];
            _summerizedBuffer = new SummerizedItem[SummaryBufferSize];
        }

        internal override void MarkInternalWindow(InternalWindowState state)
        {
            throw new NotImplementedException();
        }

        internal override void Mark(long counter, DateTime start, DateTime end)
        {
            var meterItem = new MeterItem
            {
                Start = start,
                Counter = counter,
                End = end
            };

            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos % _buffer.Length;

            if (Interlocked.CompareExchange(ref _buffer[adjustedTail], meterItem, null) == null)
                return;

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

            for (int i = 0; i < _buffer.Length; i++)
            {
                var oldVal = Interlocked.Exchange(ref _buffer[(adjustedTail + i) % _buffer.Length], null);
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

        public class PerformanceMetricsRecentStats
        {
            public string Start { get; set; }
            public long Counter { get; set; }
            public double Duration { get; set; }
            public DatabasePerformanceMetrics.MetricType Type { get; set; }

            public DynamicJsonValue ToJson()
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

        public class PerformanceMetricsHistoryStats
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

        public class PerformanceMetricsStats
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

        public override DynamicJsonValue ToJson()
        {
            var recent = GetRecentMetrics();
            var history = GetSummerizedItems();

            var stats = new PerformanceMetricsStats(Type);
            foreach (var meter in recent)
            {
                var recentStats = new PerformanceMetricsRecentStats
                {
                    Start = meter.Start.GetDefaultRavenFormat(),
                    Counter = meter.Counter,
                    Duration = Math.Round(meter.Duration.TotalMilliseconds, 2),
                    Type = Type
                };
                stats.Recent.Add(recentStats);
            }

            foreach (var meter in history)
            {
                var historyStats = new PerformanceMetricsHistoryStats
                {
                    Start = meter.TotalTimeStart.GetDefaultRavenFormat(),
                    End = meter.TotalTimeEnd.GetDefaultRavenFormat(),
                    Counter = meter.TotalCounter,
                    Duration = Math.Round((meter.TotalTimeEnd - meter.TotalTimeStart).TotalMilliseconds, 2),
                    ActiveDuration = Math.Round(meter.TotalTime.TotalMilliseconds, 2),
                    MaxDuration = Math.Round(meter.MaxTime.TotalMilliseconds, 2),
                    MinDuration = Math.Round(meter.MinTime.TotalMilliseconds, 2),
                    Type = Type
                };
                stats.History.Add(historyStats);
            }


            return stats.ToJson();
        }


        public List<MeterItem> GetRecentMetrics()
        {
            var list = new List<MeterItem>();
            list.AddRange(GetCurrentItems());
            list.Sort((x, y) => x.Start.CompareTo(y.Start));
            return list;
        }

        public IEnumerable<SummerizedItem> GetSummerizedItems()
        {
            for (int pos = 0; pos < _summerizedBuffer.Length; pos++)
            {
                var summerizedItem = _summerizedBuffer[pos];
                if (summerizedItem == null)
                    continue;
                yield return summerizedItem;
            }
        }

        public IEnumerable<MeterItem> GetCurrentItems()
        {
            for (int pos = 0; pos < _buffer.Length; pos++)
            {
                var item = _buffer[pos];
                if (item == null)
                    continue;
                yield return item;
            }
        }
    }
}

internal class TransactionPerformanceMetrics : PerformanceMetrics
{
    public TransactionPerformanceMetrics(int currentBufferSize, int summaryBufferSize)
    {
        this.Type = DatabasePerformanceMetrics.MetricType.Transaction;
        this.CurrentBufferSize = currentBufferSize;
        this.SummaryBufferSize = summaryBufferSize;
    }

    internal override void MarkInternalWindow(InternalWindowState state)
    {
        if (state == InternalWindowState.Start)
        {
            
        }
    }

    internal override void Mark(long counter, DateTime start, DateTime end)
    {
        throw new NotImplementedException();
    }

    public override DynamicJsonValue ToJson()
    {
        throw new NotImplementedException();
    }
}
