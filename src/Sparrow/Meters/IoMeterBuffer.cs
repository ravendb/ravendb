using System;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow
{
    public class IoMeterBuffer
    {
        public class MeterItem
        {
            public long Size;
            public long FileSize;
            public DateTime Start;
            public DateTime End;
            public IoMetrics.MeterType Type;
            public int Acceleration;
            public long CompressedSize;
            public TimeSpan Duration => End - Start;
        }

        public class SummerizedItem
        {
            public long TotalSize;
            public TimeSpan TotalTime;
            public TimeSpan MinTime;
            public TimeSpan MaxTime; 
            public DateTime TotalTimeStart;
            public DateTime TotalTimeEnd;
            public long Count;
            public int MaxAcceleration;
            public int MinAcceleration;
            public long TotalCompressedSize;
            public IoMetrics.MeterType Type;
            public long TotalFileSize;
        }

        public IoMeterBuffer(int metricsBufferSize, int summaryBufferSize)
        {
            _buffer = new MeterItem[metricsBufferSize];
            _summerizedBuffer = new SummerizedItem[summaryBufferSize];
        }

        private readonly MeterItem[] _buffer;
        private int _bufferPos = -1;

        private readonly SummerizedItem[] _summerizedBuffer;
        private int _summerizedPos = -1;

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
            public readonly IoMeterBuffer Parent;
            public readonly IoMetrics.MeterType Type;
            public long Size;
            public long CompressedSize;
            public int Acceleration;
            public DateTime Start;
            public long FileSize;
            public Action<MeterItem> OnFileChange;
            public DateTime End;

            public DurationMeasurement(IoMeterBuffer parent, IoMetrics.MeterType type, long size, long fileSize, Action<MeterItem> onFileChange)
            {
                Parent = parent;
                Type = type;
                Size = size;
                FileSize = fileSize;
                Start = DateTime.UtcNow;
                End = default(DateTime);
                OnFileChange = onFileChange;
                CompressedSize = 0;
                Acceleration = 1;
            }

            public void IncrementSize(long size)
            {
                Size += size;
            }

            public void Dispose()
            {
                End = DateTime.UtcNow;
                Parent.Mark(ref this);
            }

            public void IncrementFileSize(long fileSize)
            {
                FileSize += fileSize;
            }

            public void SetFileSize(long fileSize)
            {
                FileSize = fileSize;
            }

            public void SetCompressionResults(long originalSize, long compressedSize, int acceleration)
            {
                Size = originalSize;
                CompressedSize = compressedSize;
                Acceleration = acceleration;
            }
        }

        internal void Mark(ref DurationMeasurement item)
        {
            var meterItem = new MeterItem
            {
                Start = item.Start,
                Size = item.Size,            
                FileSize  = item.FileSize,
                Type = item.Type,
                End = item.End,
                CompressedSize = item.CompressedSize,
                Acceleration = item.Acceleration,
            };

            item.OnFileChange?.Invoke(meterItem);

            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos%_buffer.Length;

            if (Interlocked.CompareExchange(ref _buffer[adjustedTail], meterItem, null) == null)
                return;

            var newSummary = new SummerizedItem
            {
                TotalTimeStart = meterItem.Start,
                TotalTimeEnd = meterItem.End,
                Count = 1,
                TotalCompressedSize = meterItem.CompressedSize,
                MaxAcceleration = meterItem.Acceleration,
                MinAcceleration = meterItem.Acceleration,
                MaxTime = meterItem.Duration,
                MinTime = meterItem.Duration,
                TotalTime = meterItem.Duration,
                TotalSize = meterItem.Size,
                TotalFileSize = meterItem.FileSize,
                Type = meterItem.Type
            };

            for (int i = 0; i < _buffer.Length; i++)
            {
                var oldVal = Interlocked.Exchange(ref _buffer[(adjustedTail + i) % _buffer.Length], null);
                if (oldVal != null)
                {
                    newSummary.TotalTimeStart = newSummary.TotalTimeStart > oldVal.Start ? oldVal.Start : newSummary.TotalTimeStart;
                    newSummary.TotalTimeEnd = newSummary.TotalTimeEnd > oldVal.End ? newSummary.TotalTimeEnd : oldVal.End;
                    newSummary.Count++;
                    newSummary.MaxAcceleration = Math.Max(newSummary.MaxAcceleration, oldVal.Acceleration);
                    newSummary.MinAcceleration = Math.Min(newSummary.MinAcceleration, oldVal.Acceleration);
                    newSummary.MaxTime = newSummary.MaxTime > oldVal.Duration ? newSummary.MaxTime : oldVal.Duration;
                    newSummary.MinTime = newSummary.MinTime > oldVal.Duration ? oldVal.Duration : newSummary.MinTime;
                    newSummary.TotalSize += oldVal.Size;
                    newSummary.TotalCompressedSize+= oldVal.CompressedSize;
                    newSummary.TotalFileSize = oldVal.FileSize; // take last size to history
                    newSummary.TotalTime += oldVal.Duration;
                }
            }
            var increment = Interlocked.Increment(ref _summerizedPos);
            _summerizedBuffer[increment%_summerizedBuffer.Length] = newSummary;
        }
    }
}