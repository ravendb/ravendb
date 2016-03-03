using System;
using Metrics.Utils;

namespace Raven.Server.Utils.Metrics.Core
{
    public sealed class BufferedAverageMeter : ITickable, IDisposable
    {
        public readonly AverageOfLastTicks mRate ;

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
            this.count.SetValue(0);
            mRate.Reset();
        }
        private readonly ActionScheduler tickScheduler;
     

        public BufferedAverageMeter(string name, ActionScheduler scheduler,int bufferSize = 10, int intervalInSeconds = 1)
        {
            this.tickScheduler = scheduler;
            this.Name = name;
            mRate = new AverageOfLastTicks(bufferSize,intervalInSeconds);
            this.tickScheduler.StartTickingMetric(TimeSpan.FromSeconds(intervalInSeconds), this);
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
                return mRate.GetRate();
            }
        }

        public void Dispose()
        {
            this.tickScheduler.StopTickingMetric(this);
        }

        
    }
}
