namespace Raven.Database.Util
{
    internal class NoOpPerformanceCounter : IPerformanceCounter
    {
        public string CounterName
        {
            get
            {
                return GetType().Name;
            }
        }

        public long Decrement()
        {
            return -1;
        }

        public long Increment()
        {
            return -1;
        }

        public long IncrementBy(long value)
        {
            return -1;
        }

        public float NextValue()
        {
            return -1;
        }

        public void Close()
        {

        }

        public void RemoveInstance()
        {

        }
    }
}