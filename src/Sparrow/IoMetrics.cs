using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Sparrow.Json.Parsing;

namespace Sparrow
{
    public class SparrowDisposableAction : IDisposable // ADIADI :: good name and place plz
    {
        private readonly Action _action;
        public SparrowDisposableAction(Action action)
        {
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }

    public class IoMetrics
    {
        private struct MeterItem
        {
            public long Size;
            public long Start;
            public long End;
        }

        private struct SummerizedItem
        {
            public long Size;
            public long BusyTime; // neto

            public long TotalTimeStart; // bruto
            public long TotalTimeEnd;
        }

        /// <summary>
        /// I/O Meter
        /// </summary>
        /// <param name="currentBufferSize">Current buffer size of each point messured</param>
        /// <param name="summaryDurationPerItem">How many seconds (bruto) to summerize in each summerized item</param>
        public IoMetrics(int currentBufferSize, int summaryDurationPerItem, int summaryBufferSize)
        {
            _buffSize = currentBufferSize;
            _summaryBuffSize = summaryBufferSize;
            _buffer = new MeterItem[currentBufferSize];
            _summerizedBuffer = new SummerizedItem[currentBufferSize];
            _summaryDurationPerItem = TimeSpan.TicksPerSecond * summaryDurationPerItem;
        }

        private readonly Stopwatch _sp = Stopwatch.StartNew();
        private readonly MeterItem[] _buffer;
        private readonly SummerizedItem[] _summerizedBuffer;
        private int _bufferPos;
        private int _summerizedPos;
        private readonly int _buffSize;
        private readonly int _summaryBuffSize;
        private readonly long _summaryDurationPerItem; // in ticks
        private int _currentlyIncSumPtr;

        public IEnumerable<DynamicJsonValue> CreateSummerizedMeterData()
        {
            yield return new DynamicJsonValue
            {
                ["BufferSize"] = _summaryBuffSize,
                ["CurrentPosition"] = _summerizedPos,
                ["Now"] = _sp.ElapsedMilliseconds,
                ["DurationPerItem"] = _summaryDurationPerItem,
                ["_currentlyIncSumPtr"] = _currentlyIncSumPtr
            };

            for (int i = _summerizedPos; i < _summerizedPos + _summaryBuffSize; i++)
            {
                var pos = i%_summaryBuffSize;
                double rate = 0;
                if (_summerizedBuffer[pos].BusyTime != 0) // dev by zero chk
                    rate = (_summerizedBuffer[pos].Size/ _summerizedBuffer[pos].BusyTime); // bytes per tick
                double rateMBs = (rate * Stopwatch.Frequency) / (1024D * 1024); // Mbytes per sec
                var rateStr = $"{rateMBs:#,#.##}MB/Sec";
                yield return new DynamicJsonValue
                {
                    ["Size"] = $"{_summerizedBuffer[pos].Size:#,#}",
                    ["BusyTime"] = $"{_summerizedBuffer[pos].BusyTime:#,#}",
                    ["TotalTime"] = $"{_summerizedBuffer[pos].TotalTimeEnd - _summerizedBuffer[pos].TotalTimeStart:#,#}",
                    ["When"] = $"{_summerizedBuffer[pos].TotalTimeEnd:#,#}",
                    ["Rate"] = rateStr,
                };
            }
        }

        private MeterItem AddItem(MeterItem meterObj)
        {
            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos % _buffSize;
            var forSumItem = _buffer[adjustedTail];
            _buffer[adjustedTail] = meterObj;
            // if (Tail > pos + _bufferSize) - we might face overrun in such case (too many concurent calls to AddItem before above swap)

            return forSumItem;
        }

        public IDisposable MeterIoRate(long size)
        {
            var meterObj = new MeterItem
            {
                Size = size,
                Start = _sp.ElapsedTicks
            };

            return new SparrowDisposableAction(() =>
            {
                meterObj.End = _sp.ElapsedTicks;
                EndMeter(meterObj);
            });
        }

        private void EndMeter(MeterItem meterObj)
        {
            var toSummerizeItem = AddItem(meterObj);

            if (Interlocked.CompareExchange(ref _currentlyIncSumPtr, 1, 0) != -1)
            {
                try
                {
                    if (_summerizedBuffer[_summerizedPos].TotalTimeEnd -
                        _summerizedBuffer[_summerizedPos].TotalTimeStart > _summaryDurationPerItem)
                    {
                        Interlocked.Increment(ref _summerizedPos);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("ADIADI" + e);
                }
            }

            // TODO :: ADIADI we do not need to do that: if lock wasn't taken by us.  but we need to free this if exception occured
            Interlocked.Exchange(ref _currentlyIncSumPtr, 0);

            var pos = _summerizedPos % _summaryBuffSize;

            if (_summerizedBuffer[pos].TotalTimeStart == 0)
                _summerizedBuffer[pos].TotalTimeStart = toSummerizeItem.Start;

            _summerizedBuffer[pos].TotalTimeEnd = toSummerizeItem.End;
            _summerizedBuffer[pos].BusyTime += toSummerizeItem.End - toSummerizeItem.Start;
            _summerizedBuffer[pos].Size += toSummerizeItem.Size;
        }
    }
}
