using System.Threading;

namespace metrics.Support
{
    /// <summary>
    /// Provides support for atomic operations around a <see cref="long" /> value
    /// </summary>
    internal class AtomicLong
    {
        private long _value;

        public AtomicLong()
        {
            Set(0);
        }

        public AtomicLong(long value)
        {
            Set(value);
        }

        /// <summary>
        /// Get the current value
        /// </summary>
        public long Get()
        {
            return Interlocked.Read(ref _value);    
        }

        /// <summary>
        /// Set to the given value
        /// </summary>
        public void Set(long value)
        {
            Interlocked.Exchange(ref _value, value);
        }

        /// <summary>
        /// Atomically add the given value to the current value
        /// </summary>
        public long AddAndGet(long amount)
        {
            return Interlocked.Add(ref _value, amount);
        }

        /// <summary>
        /// Atomically increments by one and returns the current value
        /// </summary>
        /// <returns></returns>
        public long IncrementAndGet()
        {
            return Interlocked.Increment(ref _value);
        }

        /// <summary>
        /// Atomically set the value to the given updated value if the current value == expected value
        /// </summary>
        /// <param name="expected">The expected value</param>
        /// <param name="updated">The new value</param>
        /// <returns></returns>
        public bool CompareAndSet(long expected, long updated)
        {
            var originalValue = Interlocked.CompareExchange(ref _value, updated, expected);
            return originalValue == expected;
        }

        /// <summary>
        /// Set to the given value and return the previous value
        /// </summary>
        public long GetAndSet(long value)
        {
            return Interlocked.Exchange(ref _value, value);
        }

        /// <summary>
        /// Adds the given value and return the previous value
        /// </summary>
        public long GetAndAdd(long value)
        {
            var newValue = Interlocked.Add(ref _value, value);
            return newValue - value;
        }

        public static implicit operator AtomicLong(long value)
        {
            return new AtomicLong(value);
        }

        public static implicit operator long(AtomicLong value)
        {
            return value.Get();
        }
    }
}