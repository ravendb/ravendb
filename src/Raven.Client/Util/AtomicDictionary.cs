using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Raven.Client.Util
{
    internal class AtomicDictionary<TVal> : IEnumerable<KeyValuePair<RequestDestination, TVal>>
    {
        private readonly ConcurrentDictionary<RequestDestination, object> _locks;
        private readonly ConcurrentDictionary<RequestDestination, TVal> _items;
        private readonly EasyReaderWriterLock _globalLocker = new EasyReaderWriterLock();
        private List<TVal> _snapshot;
        private long _snapshotVersion;
        private long _version;
        private static readonly string NullValue = "Null Replacement: " + Guid.NewGuid();

        public AtomicDictionary()
        {
            _items = new ConcurrentDictionary<RequestDestination, TVal>();
            _locks = new ConcurrentDictionary<RequestDestination, object>();
        }

        public AtomicDictionary(IEqualityComparer<string> strComparer)
        {
            var comparer = new AtomicDictionaryComparer(strComparer);

            _items = new ConcurrentDictionary<RequestDestination, TVal>(comparer);
            _locks = new ConcurrentDictionary<RequestDestination, object>(comparer);
        }

        /// <summary>
        /// This locks the entire dictionary. Use carefully.
        /// </summary>
        public IEnumerable<TVal> Values => _items.Values;

        public TVal GetOrAdd(RequestDestination key, Func<RequestDestination, TVal> valueGenerator)
        {
            using (_globalLocker.EnterReadLock())
            {
                var actualGenerator = valueGenerator;
                if (key == null)
                    actualGenerator = s => valueGenerator(null);

                key.DatabaseName = key.DatabaseName ?? NullValue;

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

        public IDisposable WithLockFor(RequestDestination key)
        {
            using (_globalLocker.EnterReadLock())
            {
                var locker = _locks.GetOrAdd(key, new object());
                var release = new DisposableAction(() => Monitor.Exit(locker));
                Monitor.Enter(locker);
                return release;
            }
        }

        public void Set(RequestDestination key, Func<RequestDestination, TVal> valueGenerator)
        {
            using (_globalLocker.EnterReadLock())
            {
                key.DatabaseName = key.DatabaseName ?? NullValue;
                lock (_locks.GetOrAdd(key, new object()))
                {
                    var addValue = valueGenerator(key);
                    _items.AddOrUpdate(key, addValue, (s, val) => addValue);
                    Interlocked.Increment(ref _version);
                }
            }
        }

        public void Remove(RequestDestination key)
        {
            using (_globalLocker.EnterReadLock())
            {
                key.DatabaseName = key.DatabaseName ?? NullValue;
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

        public IEnumerator<KeyValuePair<RequestDestination, TVal>> GetEnumerator()
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

        public bool TryGetValue(RequestDestination key, out TVal val)
        {
            return _items.TryGetValue(key, out val);
        }

        public bool TryRemove(RequestDestination key, out TVal val)
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

    internal class AtomicDictionaryComparer : IEqualityComparer<RequestDestination>
    {
        private readonly IEqualityComparer<string> _stringComparer;

        public AtomicDictionaryComparer(IEqualityComparer<string> strComparer)
        {
            _stringComparer = strComparer;
        }

        public bool Equals(RequestDestination n1, RequestDestination n2)
        {
            return _stringComparer.Equals(n1.DatabaseName, n2.DatabaseName) && _stringComparer.Equals(n1.NodeTag, n2.NodeTag);
        }

        public int GetHashCode(RequestDestination n)
        {
           return n.GetHashCode();
        }
    }

    internal class RequestDestination
    {
        public string DatabaseName { get; set; }
        public string NodeTag { get; set; }
    }
}
