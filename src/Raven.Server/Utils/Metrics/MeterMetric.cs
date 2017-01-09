using System;
using System.Threading;

namespace Raven.Server.Utils.Metrics
{
    public sealed class MeterMetric : IDisposable
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
        public double FifteenMinuteRate => GetRate( 60 * 15);
        public double FiveMinuteRate => GetRate(60*5);
        public double OneMinuteRate => GetRate(60);

        private double GetRate(int count)
        {
            var oldCount = count;
            double sum = 0;
            var index = Volatile.Read(ref _index);
            for (int i = index; i >= 0 && count >= 0; i--, count--)
            {
                sum += _m15Rate[i];
            }
            for (int i = _m15Rate.Length-1; i >= 0 && count >= 0; i--, count--)
            {
                sum += _m15Rate[i];
            }
            return sum/ oldCount;
        }

        public double MeanRate => GetMeanRate(Clock.Nanoseconds - _startTime);

        public long Count => _count;

        public void Dispose()
        {
            MetricsScheduler.Instance.StopTickingMetric(this);
        }

        public void Tick()
        {
            var current = Volatile.Read(ref _count);
            var last = _lastCount;
            _lastCount = current;
            var lastTime = _lastTick;
            _lastTick = Clock.Nanoseconds;
            var timeDiff = ((double)(_lastTick - lastTime) / Clock.NanosecondsInSecond); ;
            if (timeDiff <= 0)
            {
                OneSecondRate = 0;
            }
            else
            {
                OneSecondRate = (current - last) / timeDiff;
            }

            var index = Volatile.Read(ref _index) + 1;
            Volatile.Write(ref _index, index);
            _m15Rate[index % (60 * 15)] = OneSecondRate;
        }

        public void Mark(long val)
        {
            Interlocked.Add(ref _count, val);
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
    }
}