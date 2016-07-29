using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow
{
    public class IoMetrics
    {
        public enum MeterType
        {
            None = 0,
            WriteJournalFile = 1,
            FlushJournalFile =2,
            WriteDataFile = 3,
            FlushDataFile = 4
        }

        public IoMetrics(int currentBufferSize, int summaryBufferSize)
        {
            BuffSize = currentBufferSize;
            SummaryBuffSize = summaryBufferSize;
            _buffers = new[]
            {
                new IoMeterBuffer(this),
                new IoMeterBuffer(this),
                new IoMeterBuffer(this),
                new IoMeterBuffer(this),
                new IoMeterBuffer(this),
            };
        }

        public int BuffSize { get; }
        public int SummaryBuffSize { get; }

        private readonly IoMeterBuffer[] _buffers;

        public IEnumerable<IoMeterBuffer.SummerizedItem> GetAllSummerizedItems()
        {
            foreach (IoMeterBuffer buff in _buffers)
                if (buff != null)
                    foreach (var item in buff.GetSummerizedItems())
                        yield return item;
        }

        public IoMeterBuffer.DurationMeasurement MeterIoRate(MeterType type, long size)
        {
            return new IoMeterBuffer.DurationMeasurement(_buffers[(int)type], size);
        }

        public IEnumerable<IoMeterBuffer.MeterItem> GetAllCurrentItems()
        {
            foreach (IoMeterBuffer buff in _buffers)
                if (buff != null)
                    foreach (var item in buff.GetCurrentItems())
                        yield return item;
        }
    }
}
