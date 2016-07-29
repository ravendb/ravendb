using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Sparrow
{
    public class IoMetrics
    {
        public enum MeterType
        {
            Write,
            Sync
        }

        private readonly ConcurrentDictionary<string, FileIoMetrics> _fileMetrics =
            new ConcurrentDictionary<string, FileIoMetrics>();

        public IoMetrics(int currentBufferSize, int summaryBufferSize)
        {
            BufferSize = currentBufferSize;
            SummaryBufferSize = summaryBufferSize;
        }

        public int BufferSize { get; }
        public int SummaryBufferSize { get; }


        //public IEnumerable<IoMeterBuffer.SummerizedItem> GetAllSummerizedItems()
        //{
        //    foreach (IoMeterBuffer buff in _buffers)
        //        if (buff != null)
        //            foreach (var item in buff.GetSummerizedItems())
        //                yield return item;
        //}

        //public IEnumerable<IoMeterBuffer.MeterItem> GetAllCurrentItems()
        //{
        //    foreach (IoMeterBuffer buff in _buffers)
        //        if (buff != null)
        //            foreach (var item in buff.GetCurrentItems())
        //                yield return item;
        //}

        public IoMeterBuffer.DurationMeasurement MeterIoRate(string filename, MeterType type, long size)
        {
            var fileIoMetrics = _fileMetrics.GetOrAdd(filename,
                name => new FileIoMetrics(name, BufferSize, SummaryBufferSize));
            IoMeterBuffer buffer;
            switch (type)
            {
                case MeterType.Write:
                    buffer = fileIoMetrics.Write;
                    break;
                case MeterType.Sync:
                    buffer = fileIoMetrics.Sync;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            return new IoMeterBuffer.DurationMeasurement(buffer, size);
        }

        public class FileIoMetrics
        {
            public string FileName;
            public IoMeterBuffer Sync;
            public IoMeterBuffer Write;

            public FileIoMetrics(string filename, int metricsBufferSize, int summaryBufferSize)
            {
                FileName = filename;

                Write = new IoMeterBuffer(metricsBufferSize, summaryBufferSize);

                Sync = new IoMeterBuffer(metricsBufferSize, summaryBufferSize);
            }
        }
    }
}