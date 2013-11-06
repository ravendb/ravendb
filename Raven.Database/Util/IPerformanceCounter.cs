using System.Diagnostics;

namespace Raven.Database.Util
{
    public interface IPerformanceCounter
    {
        string CounterName { get; }
        long Decrement();
        long Increment();
        long IncrementBy(long value);
        float NextValue();
        void Close();
        void RemoveInstance();
    }
}