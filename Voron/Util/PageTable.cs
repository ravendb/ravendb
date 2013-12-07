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
	/// <summary>
	/// This class assumes a single writer and many readers
	/// </summary>
	public class PageTable
	{
		private class PageValue
		{
			public long Transaction;
			public JournalFile.PagePosition Value;
		}

		private readonly ConcurrentDictionary<long, ImmutableAppendOnlyList<PageValue>> _values =
			new ConcurrentDictionary<long, ImmutableAppendOnlyList<PageValue>>();

		private long _maxSeenTransaction;

		public bool IsEmpty
		{
			get
			{
				return _values.Count != 0;
			}
		}

		public void SetItems(Transaction tx, Dictionary<long, JournalFile.PagePosition> items)
		{
			UpdateMaxSeenTxId(tx);
			var oldestTransaction = tx.Environment.OldestTransaction;
			foreach (var item in items)
			{
				var copy = item;
				_values.AddOrUpdate(copy.Key, l => ImmutableAppendOnlyList<PageValue>.Empty.Append(new PageValue
				{
					Transaction = tx.Id,
					Value = copy.Value
				}), (l, list) => list.Append(new PageValue
				{
					Transaction = tx.Id,
					Value = copy.Value
				}).RemoveWhile(value => value.Transaction < oldestTransaction));
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

		public void Remove(Transaction tx, IEnumerable<long> pages)
		{
			UpdateMaxSeenTxId(tx);
			var deleteMarker = new PageValue {Transaction = tx.Id};
			var oldestTransaction = tx.Environment.OldestTransaction;
			foreach (var page in pages)
			{
				ImmutableAppendOnlyList<PageValue> list;
				if (_values.TryGetValue(page, out list) == false)
					continue;

				var newList = list
					.RemoveWhile(value => value.Transaction < oldestTransaction)
					.Append(deleteMarker);

				_values.AddOrUpdate(page, newList, (l, values) => newList);
			}
		}

		public bool TryGetValue(Transaction tx, long page, out JournalFile.PagePosition value)
		{
			ImmutableAppendOnlyList<PageValue> list;
			if (_values.TryGetValue(page, out list) == false)
			{
				value = null;
				return false;
			}
			for (int i = list.Count - 1; i >= 0; i--)
			{
				var it = list[i];

				if (it.Transaction > tx.Id)
					continue;
				value = it.Value;
				return it.Value != null;
			}

			// all the current values are _after_ this transaction started, so it sees nothing
			value = null;
			return false;
		}

		public long MaxJournalPos()
		{
			return _values.Values.Select(x => x[x.Count - 1].Value)
				.Where(x => x != null)
				.Max(x => x.JournalPos);
		}

		public List<KeyValuePair<long, JournalFile.PagePosition>> AllPagesOlderThan(long oldestActiveTransaction)
		{
			return _values.Where(x =>
			{
				var val = x.Value[x.Value.Count - 1];
				return val.Value != null && val.Value.TransactionId < oldestActiveTransaction;
			}).Select(x => new KeyValuePair<long, JournalFile.PagePosition>(x.Key, x.Value[x.Value.Count - 1].Value))
				.ToList();

		}

		public void SetItemsNoTransaction(Dictionary<long, JournalFile.PagePosition> ptt)
		{
			foreach (var item in ptt)
			{
				var result = _values.TryAdd(item.Key, ImmutableAppendOnlyList<PageValue>.Empty.Append(new PageValue
				{
					Transaction = -1,
					Value = item.Value
				}));
				if (result == false)
					throw new InvalidOperationException("Duplicate item or calling SetItemsNoTransaction twice? " + item.Key);
			}
		}

		public long GetLastSeenTransaction()
		{
			return Thread.VolatileRead(ref _maxSeenTransaction);
		}

		public IEnumerable<KeyValuePair<long, JournalFile.PagePosition>> IterateLatestAsOf(long latestTxId)
		{
			foreach (var value in _values)
			{
				for (var i = value.Value.Count - 1; i >= 0; i--)
				{
					var val = value.Value[i];
					if (val.Transaction > latestTxId)
						continue;
					if (val.Value != null)
						yield return new KeyValuePair<long, JournalFile.PagePosition>(value.Key, val.Value);
					break;
				}
			}
		}
	}
}