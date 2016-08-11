using System;
using System.Threading;

namespace Raven.Abstractions.Util.MiniMetrics
{

    public sealed class MeterMetric : IDisposable
    {
        private long _count;
        private int _currentTickCount= 0;

        private readonly Ewma _m15Rate = Ewma.FifteenMinuteEwma();
        private readonly Ewma _m1Rate = Ewma.OneMinuteEwma();
        private readonly Ewma _m5Rate = Ewma.FiveMinuteEwma();
        private readonly Ewma _s1Rate = Ewma.OneSecondEwma();

        private readonly MetricsScheduler _tickScheduler;
        private long _startTime;


        public MeterMetric(MetricsScheduler scheduler)
        {
            _tickScheduler = scheduler;
            _tickScheduler.StartTickingMetric(this);
            _startTime = Clock.Nanoseconds;
        }

        public double OneSecondRate => _s1Rate.GetRate();
        public double FifteenMinuteRate => _m15Rate.GetRate();
        public double FiveMinuteRate => _m5Rate.GetRate();
        public double OneMinuteRate => _m1Rate.GetRate();
        public double MeanRate => GetMeanRate(Clock.Nanoseconds - _startTime);

        public long Count => _count;

        public void Dispose()
        {
            _tickScheduler.StopTickingMetric(this);
        }

        public MeterValue GetValue()
        {
            return new MeterValue(Count,MeanRate, OneMinuteRate, FifteenMinuteRate, FifteenMinuteRate);
        }

        public void Tick()
        {
            _s1Rate.Tick();
            if (_currentTickCount++ < 5)
                return;
            _currentTickCount = 0;
            _m1Rate.Tick();
            _m5Rate.Tick();
            _m15Rate.Tick();
        }

        public void Mark(long val)
        {
            Interlocked.Add(ref _count, val);
            _s1Rate.Update(val);
            _m1Rate.Update(val);
            _m5Rate.Update(val);
            _m15Rate.Update(val);
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