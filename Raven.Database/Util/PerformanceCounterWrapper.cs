using System;
using System.Diagnostics;

namespace Raven.Database.Util
{
    internal class PerformanceCounterWrapper : IPerformanceCounter
    {
        private readonly PerformanceCounter counter;

        public PerformanceCounterWrapper(PerformanceCounter counter)
        {
            this.counter = counter;
        }

        public string CounterName
        {
            get
            {
                return counter.CounterName;
            }
        }

        public long Decrement()
        {
            return counter.Decrement();
        }

        public long Increment()
        {
            return counter.Increment();
        }

        public long IncrementBy(long value)
        {
            return counter.IncrementBy(value);
        }

        public float NextValue()
        {
            return counter.NextValue();
        }

        public void Close()
        {
            counter.Close();
        }

        public void RemoveInstance()
        {
            try
            {
                counter.RemoveInstance();
            }
            catch (NotImplementedException)
            {
                // This happens on mono
            }
        }
    }
}