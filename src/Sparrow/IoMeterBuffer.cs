using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Sparrow
{
    public class IoMeterBuffer
    {
        public class MeterItem
        {
            public long Size;
            public long Start;
            public long End;

            public long Duration => End - Start;
        }

        public class SummerizedItem
        {
            public long TotalSize;
            public long TotalTime;
            public long MinTime;
            public long MaxTime; 
            public long TotalTimeStart;
            public long TotalTimeEnd;
            public long Count;
        }

        public IoMeterBuffer(IoMetrics ioMetrics)
        {
            _buffer = new MeterItem[ioMetrics.BuffSize];
            _summerizedBuffer = new SummerizedItem[ioMetrics.SummaryBuffSize];
            _ioMetrics = ioMetrics;
        }

        private readonly MeterItem[] _buffer;
        private readonly SummerizedItem[] _summerizedBuffer;
        private int _bufferPos;
        private int _summerizedPos;
        private readonly IoMetrics _ioMetrics;

        public IEnumerable<SummerizedItem> GetSummerizedItems()
        {
            for (int pos = 0; pos < _ioMetrics.SummaryBuffSize; pos++)
            {
                var summerizedItem = _summerizedBuffer[pos];
                if (summerizedItem == null)
                    continue;
                yield return summerizedItem;
            }
        }

        public IEnumerable<MeterItem> GetCurrentItems()
        {
            for (int pos = 0; pos < _ioMetrics.BuffSize; pos++)
            {
                var item = _buffer[pos];
                if (item  == null)
                    continue;
                yield return item;
            }
        }

        public struct DurationMeasurement : IDisposable
        {
            private readonly IoMeterBuffer _parent;
            private readonly long _size;
            private readonly long _start;

            public DurationMeasurement(IoMeterBuffer parent, long size)
            {
                _parent = parent;
                _size = size;
                _start = Stopwatch.GetTimestamp();
            }

            public void Dispose()
            {
                _parent.Mark(_size, _start);
            }
        }

        private void Mark(long size, long start)
        {
            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos%_ioMetrics.BuffSize;
            var meterItem = new MeterItem
            {
                Start = start,
                Size = size,
                End = Stopwatch.GetTimestamp()
            };
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
                TotalSize = size,
            };
            for (int i = 0; i < _buffer.Length; i++)
            {
                var oldVal = Interlocked.Exchange(ref _buffer[adjustedTail], null);
                if (oldVal != null)
                {
                    newSummary.TotalTimeStart = Math.Min(newSummary.TotalTimeStart, oldVal.Start);
                    newSummary.TotalTimeEnd = Math.Max(newSummary.TotalTimeEnd, oldVal.End);
                    newSummary.Count++;
                    newSummary.MaxTime = Math.Max(newSummary.MaxTime, oldVal.Duration);
                    newSummary.MinTime = Math.Min(newSummary.MinTime, oldVal.Duration);
                    newSummary.TotalSize += oldVal.Size;
                    newSummary.TotalTime += oldVal.Duration;
                }
            }
            var increment = Interlocked.Increment(ref _summerizedPos);
            _summerizedBuffer[increment%_summerizedBuffer.Length] = newSummary;
        }
    }
}