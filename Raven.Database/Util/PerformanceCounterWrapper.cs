using System;
using System.Diagnostics;

namespace Raven.Database.Util
{
	/// <summary>
	/// Performance counters appear to always be ready to get into a state of trouble.
	/// This is the case even if you already initialized the counter 
	/// See: http://ayende.com/blog/165217/performance-counters-sucks?key=d7a56072c5954237a5c47a6498a53821
	/// This means that we have to really be careful, because if the perf counters are bad, we still want
	/// to be able to run properly
	/// </summary>
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
	            try
	            {
		            return counter.CounterName;
	            }
	            catch (Exception e)
	            {
		            return "Could not get perf counter name: " + e.Message;
	            }
            }
        }

        public long Decrement()
        {
	        try
	        {
		        return counter.Decrement();
	        }
	        catch (Exception)
	        {
		        return -1;
	        }
        }

        public long Increment()
        {
	        try
	        {
		        return counter.Increment();
	        }
	        catch (Exception)
	        {
		        return -1;
	        }
        }

        public long IncrementBy(long value)
        {
	        try
	        {
		        return counter.IncrementBy(value);
	        }
	        catch (Exception)
	        {
		        return -1;
	        }
        }

        public float NextValue()
        {
	        try
	        {
		        return counter.NextValue();
	        }
	        catch (Exception)
	        {
		        return -1;
	        }
        }

        public void Close()
        {
	        try
	        {
		        counter.Close();
	        }
	        catch (Exception)
	        {
	        }
        }

        public void RemoveInstance()
        {
            try
            {
                counter.RemoveInstance();
            }
            catch (Exception)
            {
                // This happens on mono
            }
        }
    }
}