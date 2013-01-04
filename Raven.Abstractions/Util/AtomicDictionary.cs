using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Extensions;
#if SILVERLIGHT
using Raven.Client.Silverlight.MissingFromSilverlight;
#endif

namespace Raven.Abstractions.Util
{
	public class AtomicDictionary<TVal> : IEnumerable<KeyValuePair<string, TVal>>
	{
		private readonly ConcurrentDictionary<string, object> locks;
		private readonly ConcurrentDictionary<string, TVal> items;
		private readonly EasyReaderWriterLock globalLocker = new EasyReaderWriterLock();
		private static readonly string NullValue = "Null Replacement: " + Guid.NewGuid();

		public AtomicDictionary()
		{
			items = new ConcurrentDictionary<string, TVal>();
			locks = new ConcurrentDictionary<string, object>();
		}

		public AtomicDictionary(IEqualityComparer<string> comparer)
		{
			items = new ConcurrentDictionary<string, TVal>(comparer);
			locks = new ConcurrentDictionary<string, object>(comparer);

		}

		public IEnumerable<TVal> Values
		{
			get { return items.Values; }
		}

		public TVal GetOrAdd(string key, Func<string, TVal> valueGenerator)
		{
			using(globalLocker.EnterReadLock())
			{
				var actualGenerator = valueGenerator;
				if (key == null)
					actualGenerator = s => valueGenerator(null);
				key = key ?? NullValue;
				TVal val;
				if (items.TryGetValue(key, out val))
					return val;
				lock (locks.GetOrAdd(key, new object()))
				{
					return items.GetOrAdd(key, actualGenerator);
				}
			}
		}

		public IDisposable WithLockFor(string key)
		{
			using(globalLocker.EnterReadLock())
			{
				var locker = locks.GetOrAdd(key, new object());
				var release = new DisposableAction(() => Monitor.Exit(locker));
				Monitor.Enter(locker);
				return release;
			}
		}

		public void Remove(string key)
		{
			using(globalLocker.EnterReadLock())
			{
				key = key ?? NullValue;
				object value;
				if (locks.TryGetValue(key, out value) == false)
				{
					TVal val;
					items.TryRemove(key, out val); // just to be on the safe side
					return;
				}
				lock (value)
				{
					object o;
					locks.TryRemove(key, out o);
					TVal val;
					items.TryRemove(key, out val);
				}
			}
		}

		public IEnumerator<KeyValuePair<string, TVal>> GetEnumerator()
		{
			return items.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Clear()
		{
			items.Clear();
			locks.Clear();
		}

		public bool TryGetValue(string key, out TVal val)
		{
			return items.TryGetValue(key, out val);
		}

		public bool TryRemove(string key, out TVal val)
		{
			using(globalLocker.EnterReadLock())
			{
				var result = items.TryRemove(key, out val);
				object value;
				locks.TryRemove(key, out value);
				return result;
			}
		}

		public IDisposable WithAllLocks()
		{
			return globalLocker.EnterWriteLock();
		}
	}
}