using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Raven.Client.Util
{
    internal class AtomicDictionary<TVal> : IEnumerable<KeyValuePair<string, TVal>>
    {
        private readonly ConcurrentDictionary<string, object> _locks;
        private readonly ConcurrentDictionary<string, TVal> _items;
        private readonly EasyReaderWriterLock _globalLocker = new EasyReaderWriterLock();
        private List<TVal> _snapshot;
        private long _snapshotVersion;
        private long _version;
        private static readonly string NullValue = "Null Replacement: " + Guid.NewGuid();

        public AtomicDictionary()
        {
            _items = new ConcurrentDictionary<string, TVal>();
            _locks = new ConcurrentDictionary<string, object>();
        }

        public AtomicDictionary(IEqualityComparer<string> comparer)
        {
            _items = new ConcurrentDictionary<string, TVal>(comparer);
            _locks = new ConcurrentDictionary<string, object>(comparer);
        }

        /// <summary>
        /// This locks the entire dictionary. Use carefully.
        /// </summary>
        public IEnumerable<TVal> Values => _items.Values;

        public TVal GetOrAdd(string key, Func<string, TVal> valueGenerator)
        {
            using (_globalLocker.EnterReadLock())
            {
                var actualGenerator = valueGenerator;
                if (key == null)
                    actualGenerator = s => valueGenerator(null);
                key = key ?? NullValue;
                TVal val;
                if (_items.TryGetValue(key, out val))
                    return val;
                lock (_locks.GetOrAdd(key, new object()))
                {
                    var result = _items.GetOrAdd(key, actualGenerator);
                    Interlocked.Increment(ref _version);
                    return result;
                }
            }
        }


        /// <summary>
        /// This locks the entire dictionary. Use carefully.
        /// </summary>
        public List<TVal> ValuesSnapshot
        {
            get
            {
                var currentVersion = Interlocked.Read(ref _version);
                if (currentVersion != _snapshotVersion || _snapshot == null)
                {
                    _snapshot = _items.Values.ToList();
                    _snapshotVersion = currentVersion;
                }
                return _snapshot;
            }
        }

        public IDisposable WithLockFor(string key)
        {
            using (_globalLocker.EnterReadLock())
            {
                var locker = _locks.GetOrAdd(key, new object());
                var release = new DisposableAction(() => Monitor.Exit(locker));
                Monitor.Enter(locker);
                return release;
            }
        }

        public void Set(string key, Func<string, TVal> valueGenerator)
        {
            using (_globalLocker.EnterReadLock())
            {
                key = key ?? NullValue;
                lock (_locks.GetOrAdd(key, new object()))
                {
                    var addValue = valueGenerator(key);
                    _items.AddOrUpdate(key, addValue, (s, val) => addValue);
                    Interlocked.Increment(ref _version);
                }
            }
        }

        public void Remove(string key)
        {
            using (_globalLocker.EnterReadLock())
            {
                key = key ?? NullValue;
                object value;
                if (_locks.TryGetValue(key, out value) == false)
                {
                    _items.TryRemove(key, out _); // just to be on the safe side
                    Interlocked.Increment(ref _version);
                    return;
                }
                lock (value)
                {
                    _locks.TryRemove(key, out _);
                    _items.TryRemove(key, out _);
                    Interlocked.Increment(ref _version);
                }
            }
        }

        public IEnumerator<KeyValuePair<string, TVal>> GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            _items.Clear();
            _locks.Clear();

            Interlocked.Increment(ref _version);
        }

        public bool TryGetValue(string key, out TVal val)
        {
            return _items.TryGetValue(key, out val);
        }

        public bool TryRemove(string key, out TVal val)
        {
            using (_globalLocker.EnterReadLock())
            {
                var result = _items.TryRemove(key, out val);
                _locks.TryRemove(key, out _);

                Interlocked.Increment(ref _version);
                return result;
            }
        }

        public IDisposable WithAllLocks()
        {
            return _globalLocker.EnterWriteLock();
        }

        public IDisposable TryWithAllLocks()
        {
            return _globalLocker.TryEnterWriteLock(TimeSpan.FromSeconds(3));
        }
    }
}