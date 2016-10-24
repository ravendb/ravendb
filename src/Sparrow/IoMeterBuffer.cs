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
            public IoMetrics.MeterType Type;
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
            public IoMetrics.MeterType Type;
        }

        public IoMeterBuffer(int metricsBufferSize, int summaryBufferSize)
        {
            _buffer = new MeterItem[metricsBufferSize];
            _summerizedBuffer = new SummerizedItem[summaryBufferSize];
        }

        private readonly MeterItem[] _buffer;
        private readonly SummerizedItem[] _summerizedBuffer;
        private int _bufferPos;
        private int _summerizedPos;

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
                if (item  == null)
                    continue;
                yield return item;
            }
        }

        public struct DurationMeasurement : IDisposable
        {
            private readonly IoMeterBuffer _parent;
            private readonly IoMetrics.MeterType _type;
            public long Size;
            private readonly long _start;

            public DurationMeasurement(IoMeterBuffer parent, IoMetrics.MeterType type, long size)
            {
                _parent = parent;
                _type = type;
                Size = size;
                _start = Stopwatch.GetTimestamp();
            }

            public void IncrementSize(long size)
            {
                Size += size;
            }

            public void Dispose()
            {
                _parent.Mark(Size, _start, _type);
            }
        }

        private void Mark(long size, long start, IoMetrics.MeterType type)
        {
            var meterItem = new MeterItem
            {
                Start = start,
                Size = size,
                Type = type,
                End = Stopwatch.GetTimestamp()
            };

            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos%_buffer.Length;
            
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
                Type = type
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