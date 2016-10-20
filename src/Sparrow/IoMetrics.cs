using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow
{
    public class IoMetrics
    {
        public enum MeterType
        {
            JournalWrite,
            DataFlush,
            DataSync,
        }

        private readonly ConcurrentDictionary<string, FileIoMetrics> _fileMetrics =
            new ConcurrentDictionary<string, FileIoMetrics>();

        private readonly ConcurrentQueue<string> _closedFiles = new ConcurrentQueue<string>();

        public IoMetrics(int currentBufferSize, int summaryBufferSize)
        {
            BufferSize = currentBufferSize;
            SummaryBufferSize = summaryBufferSize;
        }

        public int BufferSize { get; }
        public int SummaryBufferSize { get; }

        public IEnumerable<FileIoMetrics> Files => _fileMetrics.Values;

        public void FileClosed(string filename)
        {
            FileIoMetrics value;
            if (!_fileMetrics.TryGetValue(filename, out value))
                return;
            value.Closed = true;
            _closedFiles.Enqueue(filename);
            while (_closedFiles.Count > 16)
            {
                if (_closedFiles.TryDequeue(out filename) == false)
                    return;
                _fileMetrics.TryRemove(filename, out value);
            }
        }

        public IoMeterBuffer.DurationMeasurement MeterIoRate(string filename, MeterType type, long size)
        {
            var fileIoMetrics = _fileMetrics.GetOrAdd(filename,
                name => new FileIoMetrics(name, BufferSize, SummaryBufferSize));
            IoMeterBuffer buffer;
            switch (type)
            {
                case MeterType.JournalWrite:
                    buffer = fileIoMetrics.JournalWrite;
                    break;
                case MeterType.DataFlush:
                    buffer = fileIoMetrics.DataFlush;
                    break;
                case MeterType.DataSync:
                    buffer = fileIoMetrics.DataSync;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            return new IoMeterBuffer.DurationMeasurement(buffer, type, size);
        }

        public class FileIoMetrics
        {
            public string FileName;
            public IoMeterBuffer JournalWrite;
            public IoMeterBuffer DataFlush;
            public IoMeterBuffer DataSync;

            public bool Closed;

            public FileIoMetrics(string filename, int metricsBufferSize, int summaryBufferSize)
            {
                FileName = filename;

                JournalWrite = new IoMeterBuffer(metricsBufferSize, summaryBufferSize);
                DataFlush = new IoMeterBuffer(metricsBufferSize, summaryBufferSize);
                DataSync = new IoMeterBuffer(metricsBufferSize, summaryBufferSize);
            }


            public List<IoMeterBuffer.MeterItem> GetRecentMetrics()
            {
                var list = new List<IoMeterBuffer.MeterItem>();
                list.AddRange(DataSync.GetCurrentItems());
                list.AddRange(JournalWrite.GetCurrentItems());
                list.AddRange(DataFlush.GetCurrentItems());

                list.Sort((x, y) => x.Start.CompareTo(y.Start));

                return list;
            }

            public List<IoMeterBuffer.SummerizedItem> GetSummaryMetrics()
            {
                var list = new List<IoMeterBuffer.SummerizedItem>();
                list.AddRange(DataSync.GetSummerizedItems());
                list.AddRange(DataFlush.GetSummerizedItems());
                list.AddRange(JournalWrite.GetSummerizedItems());

                list.Sort((x, y) => x.TotalTimeStart.CompareTo(y.TotalTimeStart));

                return list;
            }
        }
    }
}