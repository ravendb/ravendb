using System;
using Metrics.MetricData;
using Metrics.Utils;

namespace Raven.Server.Utils.Metrics.Core
{
    

    public sealed class MeterMetric : ITickable, IDisposable
    {
        public static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

        public readonly EWMA m1Rate = EWMA.OneMinuteEWMA();
        public readonly EWMA m5Rate = EWMA.FiveMinuteEWMA();
        public readonly EWMA m15Rate = EWMA.FifteenMinuteEWMA();
        public AtomicLong count = new AtomicLong();
        public string Name { get; private set; }

        public void Tick()
        {
            this.m1Rate.Tick();
            this.m5Rate.Tick();
            this.m15Rate.Tick();
        }

        public void Mark(long val)
        {
            this.count.Add(val);
            this.m1Rate.Update(val);
            this.m5Rate.Update(val);
            this.m15Rate.Update(val);
        }

        public void Reset()
        {
            this.startTime = Clock.Nanoseconds;
            this.count.SetValue(0);
            this.m1Rate.Reset();
            this.m5Rate.Reset();
            this.m15Rate.Reset();
        }

        public MeterValue GetValue(double elapsed)
        {
            return new MeterValue(Name, this.count.Value, this.GetMeanRate(elapsed), this.OneMinuteRate, this.FiveMinuteRate, this.FifteenMinuteRate);
        }

        private double GetMeanRate(double elapsed)
        {
            if (this.count.Value == 0)
            {
                return 0.0;
            }

            return this.count.Value / elapsed * Clock.NANOSECONDS_IN_SECOND;
        }

        private double FifteenMinuteRate { get { return this.m15Rate.GetRate(); } }
        private double FiveMinuteRate { get { return this.m5Rate.GetRate(); } }
        private double OneMinuteRate { get { return this.m1Rate.GetRate(); } }
        private readonly ActionScheduler tickScheduler;

        private long startTime;
     

        public MeterMetric(string name, ActionScheduler scheduler)
        {
            this.startTime = Clock.Nanoseconds;
            this.tickScheduler = scheduler;
            this.Name = name;
            this.tickScheduler.StartTickingMetric(TickInterval, this);
        }

        public void Mark()
        {
            Mark(1L);
        }

        public MeterValue GetValue(bool resetMetric = false)
        {
            var value = this.Value;
            if (resetMetric)
            {
                this.Reset();
            }
            return value;
        }

        public MeterValue Value
        {
            get
            {
                double elapsed = (Clock.Nanoseconds - startTime);
                var value = this.GetValue(elapsed);

                return new MeterValue(Name,value.Count, value.MeanRate, value.OneMinuteRate, value.FiveMinuteRate, value.FifteenMinuteRate);
            }
        }

        public void Dispose()
        {
            this.tickScheduler.StopTickingMetric(this);
        }



        
    }
}
