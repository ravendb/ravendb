
namespace Metrics.Utils
{
    public class AtomicLongHolder
    {
        private AtomicLong value;

        public AtomicLongHolder()
            : this(new AtomicLong())
        { }

        public AtomicLongHolder(long value)
            : this(new AtomicLong(value))
        { }

        public AtomicLongHolder(AtomicLong value)
        {
            this.value = value;
        }

        public long Value { get { return this.value.Value; } }

        public void SetValue(long value) { this.value.SetValue(value); }
        public long Add(long value) { return this.value.Add(value); }
        public long Increment() { return this.value.Increment(); }
        public long Decrement() { return this.value.Decrement(); }
        public long GetAndReset() { return this.value.GetAndReset(); }
        public long GetAndSet(long value) { return this.value.GetAndSet(value); }
        public bool CompareAndSet(long expected, long updated) { return this.value.CompareAndSet(expected, updated); }
    }
}
