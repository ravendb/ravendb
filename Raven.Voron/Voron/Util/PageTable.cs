using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Voron.Impl;
using Voron.Impl.Journal;

namespace Voron.Util
{
	using System.Diagnostics;

	/// <summary>
	/// This class assumes a single writer and many readers
	/// </summary>
	public class PageTable
	{
		private readonly ConcurrentDictionary<long, ImmutableAppendOnlyList<PagePosition>> _values = new ConcurrentDictionary<long, ImmutableAppendOnlyList<PagePosition>>( LongEqualityComparer.Instance );

		private long _maxSeenTransaction;

		public bool IsEmpty
		{
			get
			{
				return _values.Count != 0;
			}
		}

		public void SetItems(Transaction tx, Dictionary<long, PagePosition> items)
		{
			UpdateMaxSeenTxId(tx);

			foreach (var item in items)
			{
				var copy = item;
				_values.AddOrUpdate(copy.Key, l => ImmutableAppendOnlyList<PagePosition>.Empty.Append(copy.Value),
				(l, list) => list.Append(copy.Value));
			}
		}	

		private void UpdateMaxSeenTxId(Transaction tx)
		{
			if (_maxSeenTransaction > tx.Id)
			{
				throw new InvalidOperationException("Transaction ids has to always increment, but got " + tx.Id +
				                                    " when already seen tx " + _maxSeenTransaction);
			}
			_maxSeenTransaction = tx.Id;
		}

		public void Remove(IEnumerable<long> pages, long lastSyncedTransactionId, List<PagePosition> unusedPages)
		{
			foreach (var page in pages)
			{
				ImmutableAppendOnlyList<PagePosition> list;
				if (_values.TryGetValue(page, out list) == false)
					continue;

				var newList = list.RemoveWhile(value => value.TransactionId <= lastSyncedTransactionId, unusedPages);

				if (newList.Count != 0)
					_values.AddOrUpdate(page, newList, (l, values) => newList);
				else
					_values.TryRemove(page, out list);
			}
		}

		public bool TryGetValue(Transaction tx, long page, out PagePosition value)
		{
			ImmutableAppendOnlyList<PagePosition> list;
			if (_values.TryGetValue(page, out list) == false)
			{
				value = null;
				return false;
			}
			for (int i = list.Count - 1; i >= 0; i--)
			{
				var it = list[i];

				if (it.TransactionId > tx.Id)
					continue;

				if (it.IsFreedPageMarker)
					break;

				value = it;
				Debug.Assert(value != null);
				return true;
			}

			// all the current values are _after_ this transaction started, so it sees nothing
			value = null;
			return false;
		}

		public long MaxTransactionId()
		{
			return _values.Values.Select(x => x[x.Count - 1])
				.Where(x => x != null)
				.Max(x => x.TransactionId);
		}

		public List<long> KeysWhereAllPagesOlderThan(long lastSyncedTransactionId)
		{
			return _values.Where(x =>
			{
				var val = x.Value[x.Value.Count - 1];
				return val.TransactionId <= lastSyncedTransactionId;
			}).Select(x => x.Key).ToList();
		}

		public List<long> KeysWhereSomePagesOlderThan(long lastSyncedTransactionId)
		{
			return _values.Where(x => x.Value.Any(p => p.TransactionId <= lastSyncedTransactionId)).Select(x => x.Key).ToList();
		}

		public long GetLastSeenTransactionId()
		{
			return Thread.VolatileRead(ref _maxSeenTransaction);
		}

		public IEnumerable<KeyValuePair<long, PagePosition>> IterateLatestAsOf(long latestTxId)
		{
			foreach (var value in _values)
			{
				for (var i = value.Value.Count - 1; i >= 0; i--)
				{
					var val = value.Value[i];
					if (val.TransactionId > latestTxId)
						continue;

					// intentionally commenting the below code in order to expose such marker over multiple journals files
					// handling of free page markers moved to the caller of this method

					//if (val.IsFreedPageMarker) 
					//	break;

					yield return new KeyValuePair<long, PagePosition>(value.Key, val);
					break;
				}
			}
		}
	}
}