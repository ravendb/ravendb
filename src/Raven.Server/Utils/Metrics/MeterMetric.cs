using System;
using System.Threading;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Metrics
{
    public sealed class MeterMetric
    {
        private long _count;
        private long _lastCount;

        private readonly double[] _m15Rate = new double[60 * 15];
        private int _index;

        private readonly long _startTime;
        private long _lastTick;


        public MeterMetric()
        {
            MetricsScheduler.Instance.StartTickingMetric(this);
            _startTime = Clock.Nanoseconds;
        }

        public double OneSecondRate;
        public double FifteenMinuteRate => GetRate(60 * 15);
        public double FiveMinuteRate => GetRate(60 * 5);
        public double OneMinuteRate => GetRate(60);
        public double FiveSecondRate => GetRate(5);

        private double GetRate(int count)
        {
            if (count == 0)
                return 0.0;

            var oldCount = count;
            double sum = 0;
            var index = Volatile.Read(ref _index) % _m15Rate.Length;
            for (int i = index; i >= 0 && count >= 0; i--, count--)
            {
                sum += _m15Rate[i];
            }
            for (int i = _m15Rate.Length - 1; i >= 0 && count >= 0; i--, count--)
            {
                sum += _m15Rate[i];
            }
            return sum / oldCount;
        }

        public double MeanRate => GetMeanRate(Clock.Nanoseconds - _startTime);

        public long Count => _count;


        public void Tick()
        {
            var current = Volatile.Read(ref _count);
            var last = _lastCount;
            _lastCount = current;
            var lastTime = _lastTick;
            _lastTick = Clock.Nanoseconds;
            var timeDiff = ((double)(_lastTick - lastTime) / Clock.NanosecondsInSecond);
            if (timeDiff <= 0)
            {
                OneSecondRate = 0;
            }
            else
            {
                OneSecondRate = (current - last) / timeDiff;
            }

            var index = Interlocked.Increment(ref _index);
            _m15Rate[index % (60 * 15)] = OneSecondRate;
        }

        public void Mark(long val)
        {
            if (val == 0)
                return;

            Interlocked.Add(ref _count, val);
        }

        /// <summary>
        /// This can be called if we _know_ that there can only be one thread calling
        /// this, so we don't need to expensive interlocked calls
        /// </summary>
        public void MarkSingleThreaded(long val)
        {
            if (val == 0)
                return;

            _count += val;
        }

        public double GetMeanRate(double elapsed)
        {
            var count = Volatile.Read(ref _count);
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (elapsed == 0)
            {
                return 0.0;
            }

            return count / elapsed * Clock.NanosecondsInSecond;
        }

        public void Mark()
        {
            Mark(1L);
        }

        public DynamicJsonValue CreateMeterData(bool allResults = false, bool filterEmpty = true)
        {
            var meterValue = this;

            var r = new DynamicJsonValue
            {
                ["Current"] = Math.Round(meterValue.OneSecondRate, 1),
                ["Count"] = meterValue.Count,
                ["MeanRate"] = Math.Round(meterValue.MeanRate, 1),
                ["OneMinuteRate"] = Math.Round(meterValue.OneMinuteRate, 1),
                ["FiveMinuteRate"] = Math.Round(meterValue.FiveMinuteRate, 1),
                ["FifteenMinuteRate"] = Math.Round(meterValue.FifteenMinuteRate, 1)
            };

            if (allResults == false)
                return r;

            var results = new DynamicJsonValue();
            r["Raw"] = results;
            var index = Volatile.Read(ref _index) % _m15Rate.Length;
            var current = DateTime.UtcNow;
            var now = new TimeSpan(current.Hour, current.Minute, current.Second);
            var oneSec = TimeSpan.FromSeconds(1);
            for (int i = index; i >= 0; i--)
            {
                var d = _m15Rate[i];
                if (filterEmpty == false || Math.Abs(d) > double.Epsilon)
                    results[now.ToString()] = Math.Round(d, 1);
                now = now - oneSec;
            }
            for (int i = _m15Rate.Length - 1; i >= 0; i--)
            {
                var d = _m15Rate[i];
                if (Math.Abs(d) > double.Epsilon)
                    results[now.Add(TimeSpan.FromSeconds(-i)).ToString()] = Math.Round(d, 1);
                now = now - oneSec;
            }
            return r;
        }
    }
}
