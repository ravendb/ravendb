
using System;
using System.Collections.Concurrent;
using System.Linq;
using Metrics.MetricData;
using Metrics.Utils;
namespace Metrics.Core
{
    public sealed class BufferedAverageMeter : Meter, IDisposable
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
        private readonly Scheduler tickScheduler;
     

        public BufferedAverageMeter(string name,Scheduler scheduler,int bufferSize = 10, int intervalInSeconds = 1)
        {
            this.tickScheduler = scheduler;
            this.Name = name;
            mRate = new AverageOfLastTicks(bufferSize,intervalInSeconds);
            this.tickScheduler.Start(TimeSpan.FromSeconds(intervalInSeconds), Tick);
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
            this.tickScheduler.Stop();
            using (this.tickScheduler) { }
        }

        
    }
}
