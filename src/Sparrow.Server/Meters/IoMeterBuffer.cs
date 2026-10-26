using System;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow.Server.Meters
{
    public sealed class IoMeterBuffer
    {
        public sealed class MeterItem
        {
            public long Size;
            public long FileSize;
            public DateTime Start;
            public DateTime End;
            public IoMetrics.MeterType Type;
            public int Acceleration;
            public long CompressedSize;

            // Compression
            public string Algorithm;

            // JournalWrite
            public bool Pipelined;
            public int BatchOperations; // 0 when the write did not come from the transaction merger

            // DataSync, Duration covers the paced writeback and the final sync
            public IoMetrics.SyncReason SyncReason;
            public bool Drain;
            public long WritebackSize;
            public long WritebackRanges;
            public TimeSpan WritebackDuration;

            public TimeSpan Duration => End - Start;
            public double SizeInMb => Size / (double)1024 / 1024;
            public double RateOfWritesInMbPerSec => SizeInMb / Duration.TotalSeconds;
        }

        public sealed class SummerizedItem
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

        public void RemoveUntil(long ticks)
        {
            if (_bufferPos != -1)
            {
                for (var pos = 0; pos < _buffer.Length; pos++)
                {
                    if (_buffer[pos] == null)
                        continue;

                    if (_buffer[pos].End.Ticks < ticks)
                        Interlocked.Exchange(ref _buffer[pos], null);
                }
            }

            if (_summerizedPos != -1)
            {
                for (var pos = 0; pos < _summerizedBuffer.Length; pos++)
                {
                    if (_summerizedBuffer[pos] == null)
                        continue;

                    if (_summerizedBuffer[pos].TotalTimeEnd.Ticks < ticks)
                        Interlocked.Exchange(ref _summerizedBuffer[pos], null);
                }
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
            public string Algorithm;
            public bool Pipelined;
            public int BatchOperations;
            public IoMetrics.SyncDetails Sync;

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
                Algorithm = null;
                Pipelined = false;
                BatchOperations = 0;
                Sync = default;
            }

            public void IncrementSize(long size)
            {
                Size += size;
            }

            public void Dispose()
            {
                End = DateTime.UtcNow;
                Parent?.Mark(ref this);
            }

            public void IncrementFileSize(long fileSize)
            {
                FileSize += fileSize;
            }

            public void SetFileSize(long fileSize)
            {
                FileSize = fileSize;
            }

            public void SetCompressionResults(long originalSize, long compressedSize, int acceleration, string algorithm = null)
            {
                Size = originalSize;
                CompressedSize = compressedSize;
                Acceleration = acceleration;
                Algorithm = algorithm;
            }

            public void SetJournalWrite(bool pipelined, int batchOperations)
            {
                Pipelined = pipelined;
                BatchOperations = batchOperations;
            }

            // the sync is measured from before the paced writeback, the same span the drain decision uses
            public void SetSync(DateTime start, in IoMetrics.SyncDetails sync)
            {
                Start = start;
                Sync = sync;
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
                Algorithm = item.Algorithm,
                Pipelined = item.Pipelined,
                BatchOperations = item.BatchOperations,
                SyncReason = item.Sync.Reason,
                Drain = item.Sync.Drain,
                WritebackSize = item.Sync.WritebackSize,
                WritebackRanges = item.Sync.WritebackRanges,
                WritebackDuration = item.Sync.WritebackDuration,
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
