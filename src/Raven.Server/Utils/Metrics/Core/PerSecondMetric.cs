using System;
using Metrics.Utils;

namespace Raven.Server.Utils.Metrics.Core
{
    public sealed class PerSecondMetric : ITickable, IDisposable
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
      
        
        private readonly ActionScheduler tickScheduler;
        private long startTime;
     

        public PerSecondMetric(string name, ActionScheduler scheduler)
        {
            this.startTime = Clock.Nanoseconds;
            this.tickScheduler = scheduler;
            this.Name = name;
            this.tickScheduler.StartTickingMetric(TickInterval, this);
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
            this.tickScheduler.StopTickingMetric(this);
        }

        
        
    }
}
