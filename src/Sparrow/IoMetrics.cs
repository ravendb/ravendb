using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow
{
    public class IoMetrics
    {
        private struct MeterItem
        {
            public long Size;
            public long Start;
            public long End;

            public bool IsOverrun;
        }

        private struct SummerizedItem
        {
            public long Size;
            public long BusyTime; // neto

            public long TotalTimeStart; // bruto
            public long TotalTimeEnd;

            public long Count;
        }

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
        private readonly long _summaryDurationPerItem;
        private int _currentlyIncSumPtr;

        public IEnumerable<DynamicJsonValue> CreateSummerizedMeterData()
        {
            yield return new DynamicJsonValue
            {
                ["BufferSize"] = _summaryBuffSize,
                ["CurrentPosition"] = _summerizedPos,
                ["Now"] = _sp.ElapsedMilliseconds,
                ["DurationPerItem"] = _summaryDurationPerItem,
                ["_currentlyIncSumPtr"] = _currentlyIncSumPtr,
                ["NumberOfTicksPerSecond"] = Stopwatch.Frequency
            };

            var absolutePos = _summerizedPos % _summaryBuffSize;
            for (int i = absolutePos; i < absolutePos + _summaryBuffSize; i++)
            {
                int pos = GetPositionInBuffer(absolutePos, i, _summaryBuffSize);

                yield return new DynamicJsonValue
                {
                    ["No."] = $"{pos}",
                    ["Size"] = $"{_summerizedBuffer[pos].Size:#,#}",
                    ["BusyTime"] = $"{_summerizedBuffer[pos].BusyTime:#,#}",
                    ["TotalTime"] = $"{_summerizedBuffer[pos].TotalTimeEnd - _summerizedBuffer[pos].TotalTimeStart:#,#}",
                    ["When"] = $"{_summerizedBuffer[pos].TotalTimeEnd:#,#}",
                    ["NetoRate"] = CalculateRate(_summerizedBuffer[pos].BusyTime, _summerizedBuffer[pos].Size),
                    ["BrutoRate"] = CalculateRate(_summerizedBuffer[pos].TotalTimeEnd - _summerizedBuffer[pos].TotalTimeStart,
                                                  _summerizedBuffer[pos].Size),
                    ["NumberOfHits"] = $"{_summerizedBuffer[pos].Count}"
                };
            }
        }

        public IEnumerable<DynamicJsonValue> CreateCurrentMeterData()
        {
            var absolutePos = _bufferPos % _buffSize;
            for (int i = absolutePos; i < absolutePos + _buffSize; i++)
            {
                var pos = GetPositionInBuffer(absolutePos, i, _buffSize);

                yield return new DynamicJsonValue
                {
                    ["No."] = $"{pos}",
                    ["Size"] = $"{_buffer[pos].Size:0,0}",
                    ["TotalTime"] = $"{_buffer[pos].End - _buffer[pos].Start:0,0}",
                    ["When"] = $"{_buffer[pos].End:0,0}",
                    ["Rate"] = CalculateRate(_buffer[pos].End - _buffer[pos].Start, _buffer[pos].Size),
                    ["IsOverrun"] = $"{_buffer[pos].IsOverrun}"
                };
            }
        }

        private static int GetPositionInBuffer(int absolutePos, int i, int bufSize)
        {
            var pos = absolutePos + (absolutePos - i);
            if (pos < 0)
                pos = bufSize - pos;
            return pos;
        }

        private static string CalculateRate(long timeInTicks, long sizeInBytes)
        {
            double rate = 0;
            if (timeInTicks != 0) // dev by zero chk
                rate = (sizeInBytes / timeInTicks); // bytes per tick
            else
                return "N/A";
            double rateMBs = (rate * Stopwatch.Frequency) / (1024D * 1024); // Mbytes per sec
            return $"{rateMBs:#,#.##}MB/Sec";
        }

        private MeterItem AddItem(MeterItem meterObj)
        {
            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos % _buffSize;
            var forSumItem = _buffer[adjustedTail];
            _buffer[adjustedTail] = meterObj;

            //  we might face overrun in such case (too many concurent calls to AddItem before above swap)
            // a good idea will be to increase _buffSize
            if (_bufferPos > pos + _buffSize)
                _buffer[adjustedTail].IsOverrun = true;

            return forSumItem;
        }

        public IDisposable MeterIoRate(long size)
        {
            var meterObj = new MeterItem
            {
                Size = size,
                Start = _sp.ElapsedTicks
            };

            return new DisposableAction(() =>
            {
                meterObj.End = _sp.ElapsedTicks;
                EndMeter(meterObj);
            });
        }

        private void EndMeter(MeterItem meterObj)
        {
            var toSummerizeItem = AddItem(meterObj);

            var interlockTaken = Interlocked.CompareExchange(ref _currentlyIncSumPtr, 1, 0) == 0;
            if (interlockTaken)
            {
                try
                {
                    if (_summerizedBuffer[_summerizedPos].TotalTimeEnd -
                        _summerizedBuffer[_summerizedPos].TotalTimeStart > _summaryDurationPerItem)
                    {
                        _summerizedPos++;
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _currentlyIncSumPtr, 0);
                }
            }

            var pos = _summerizedPos % _summaryBuffSize;

            if (_summerizedBuffer[pos].TotalTimeStart == 0)
                _summerizedBuffer[pos].TotalTimeStart = toSummerizeItem.Start;

            _summerizedBuffer[pos].TotalTimeEnd = toSummerizeItem.End;
            _summerizedBuffer[pos].BusyTime += toSummerizeItem.End - toSummerizeItem.Start;
            _summerizedBuffer[pos].Size += toSummerizeItem.Size;
            _summerizedBuffer[pos].Count++;
        }
    }
}
