using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using metrics.Support;

namespace metrics.Stats
{
    /// <summary>
    /// An exponentially-decaying random sample of {@code long}s. Uses Cormode et
    /// al's forward-decaying priority reservoir sampling method to produce a
    /// statistically representative sample, exponentially biased towards newer
    /// entries.
    /// </summary>
    /// <see href="http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf">
    /// Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming
    /// Systems. ICDE '09: Proceedings of the 2009 IEEE International Conference on
    /// Data Engineering (2009)
    /// </see>
    public class ExponentiallyDecayingSample : ISample<ExponentiallyDecayingSample>
    {
        private static readonly long RescaleThreshold = TimeUnit.Hours.ToNanos(1);
        /* Implemented originally as ConcurrentSkipListMap, so lookups will be much slower */
        private readonly ConcurrentDictionary<double, long> _values;
        private readonly ReaderWriterLockSlim _lock;
        private readonly int _reservoirSize;
        private readonly double _alpha;
        private readonly AtomicLong _count = new AtomicLong(0);
        private VolatileLong _startTime;
        private readonly AtomicLong _nextScaleTime = new AtomicLong(0);

        /// <param name="reservoirSize">The number of samples to keep in the sampling reservoir</param>
        /// <param name="alpha">The exponential decay factor; the higher this is, the more biased the sample will be towards newer values</param>
        public ExponentiallyDecayingSample(int reservoirSize, double alpha)
        {
            _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _values = new ConcurrentDictionary<double, long>();
            _alpha = alpha;
            _reservoirSize = reservoirSize;
            Clear();
        }

        /// <summary>
        /// Clears all recorded values
        /// </summary>
        public void Clear()
        {
            _values.Clear();
            _count.Set(0);
            _startTime = Tick();
        }
        
        /// <summary>
        /// Returns the number of values recorded
        /// </summary>
        public int Count
        {
            get { return (int) Math.Min(_reservoirSize, _count); }
        }

        /// <summary>
        /// Adds a new recorded value to the sample
        /// </summary>
        public void Update(long value)
        {
            Update(value, Tick());
        }

        private void Update(long value, long timestamp)
        {
            _lock.EnterReadLock();
            try
            {
                var priority = Weight(timestamp - _startTime) / Support.Random.NextLong();
                var newCount = _count.IncrementAndGet();
                if(newCount <= _reservoirSize)
                {
                    _values.AddOrUpdate(priority, value, (p, v) => v);
                }
                else
                {
                    var first = _values.Keys.First();
                    if(first < priority)
                    {
                        _values.AddOrUpdate(priority, value, (p, v) => v);

                        long removed;
                        while(!_values.TryRemove(first, out removed))
                        {
                            first = _values.Keys.First();
                        }
                    }
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }

            var now = DateTime.Now.Ticks;
            var next = _nextScaleTime.Get();
            if(now >= next)
            {
                Rescale(now, next);
            }
        }

        /// <summary>
        ///  Returns a copy of the sample's values
        /// </summary>
        public ICollection<long> Values
        {
            get
            {
                _lock.EnterReadLock();
                try
                {
                    return new List<long>(_values.Values);
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }
        }

        private static long Tick()
        {
            return DateTime.Now.Ticks;
        }

        private double Weight(long t)
        {
            return Math.Exp(_alpha * t);
        }
        
        /// <summary>
        /// "A common feature of the above techniques—indeed, the key technique that
        /// allows us to track the decayed weights efficiently—is that they maintain
        /// counts and other quantities based on g(ti − L), and only scale by g(t − L)
        /// at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
        /// and one, the intermediate values of g(ti − L) could become very large. For
        /// polynomial functions, these values should not grow too large, and should be
        /// effectively represented in practice by floating point values without loss of
        /// precision. For exponential functions, these values could grow quite large as
        /// new values of (ti − L) become large, and potentially exceed the capacity of
        /// common floating point types. However, since the values stored by the
        /// algorithms are linear combinations of g values (scaled sums), they can be
        /// rescaled relative to a new landmark. That is, by the analysis of exponential
        /// decay in Section III-A, the choice of L does not affect the final result. We
        /// can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
        /// and obtain the correct value as if we had instead computed relative to a new
        /// landmark L′ (and then use this new L′ at query time). This can be done with
        /// a linear pass over whatever data structure is being used."
        /// </summary>
        /// <param name="now"></param>
        /// <param name="next"></param>
        private void Rescale(long now, long next)
        {
            if (!_nextScaleTime.CompareAndSet(next, now + RescaleThreshold))
            {
                return;
            }

            _lock.EnterWriteLock();
            try
            {
                var oldStartTime = _startTime;
                _startTime = Tick();
                var keys = new List<double>(_values.Keys);
                foreach (var key in keys)
                {
                    long value;
                    _values.TryRemove(key, out value);
                    _values.AddOrUpdate(key*Math.Exp(-_alpha*(_startTime - oldStartTime)), value, (k, v) => v);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        [IgnoreDataMember]
        public ExponentiallyDecayingSample Copy
        {
            get
            {
                var copy = new ExponentiallyDecayingSample(_reservoirSize, _alpha);
                copy._startTime.Set(_startTime);
                copy._count.Set(_count);
                copy._nextScaleTime.Set(_nextScaleTime);
                foreach(var value in _values)
                {
                    copy._values.AddOrUpdate(value.Key, value.Value, (k, v) => v);
                }
                return copy;
            }
        }
    }
}