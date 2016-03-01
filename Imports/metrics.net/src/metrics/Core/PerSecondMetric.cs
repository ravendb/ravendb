
using System;
using System.Collections.Concurrent;
using System.Linq;
using Metrics.MetricData;
using Metrics.Utils;
namespace Metrics.Core
{
    public sealed class PerSecondMetric : Meter, IDisposable
    {
        public static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

        public readonly EWMA mRate = EWMA.OneSecondEWMA();

        public AtomicLong count = new AtomicLong();
        public string Name { get; private set; }

        public void Tick()
        {
            mRate.Tick();
        }

        public void Mark(long count=1)
        {
            this.count.Add(count);
            mRate.Update(count);
        }

        public void Reset()
        {
            this.startTime = Clock.Nanoseconds;
            this.count.SetValue(0);
            mRate.Reset();
        }

        public double GetValue(double elapsed)
        {
            return mRate.GetRate();
        }
      
        
        private readonly Scheduler tickScheduler;
        private long startTime;
     

        public PerSecondMetric(string name,Scheduler scheduler)
        {
            this.startTime = Clock.Nanoseconds;
            this.tickScheduler = scheduler;
            this.Name = name;
            this.tickScheduler.Start(TickInterval, Tick);
        }

        public double GetValue(bool resetMetric = false)
        {
            var value = this.Value;
            if (resetMetric)
            {
                this.Reset();
            }
            return value;
        }

        public double Value
        {
            get
            {
                double elapsed = (Clock.Nanoseconds - startTime);
                return this.GetValue(elapsed);
            }
        }

        public void Dispose()
        {
            this.tickScheduler.Stop();
            using (this.tickScheduler) { }
        }

        
        
    }
}
