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
    using Sparrow;
    using System.Diagnostics;

    /// <summary>
    /// This class assumes a single writer and many readers
    /// </summary>
    public class PageTable
    {
        private readonly ConcurrentDictionary<long, ImmutableAppendOnlyList<PagePosition>> _values = new ConcurrentDictionary<long, ImmutableAppendOnlyList<PagePosition>>(NumericEqualityComparer.Instance);
        private readonly SortedList<long, Dictionary<long, PagePosition>> _transactionPages = new SortedList<long, Dictionary<long, PagePosition>>();
        private long _maxSeenTransaction;

        public bool IsEmpty => _values.Count != 0;

        public void SetItems(LowLevelTransaction tx, Dictionary<long, PagePosition> items)
        {
            lock (_transactionPages)
            {
                UpdateMaxSeenTxId(tx);
                _transactionPages.Add(tx.Id, items);
            }

            foreach (var item in items)
            {
                var copy = item;
                _values.AddOrUpdate(copy.Key, l => ImmutableAppendOnlyList<PagePosition>.Empty.Append(copy.Value),
                    (l, list) => list.Append(copy.Value));
            }
        }

        private void UpdateMaxSeenTxId(LowLevelTransaction tx)
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

        public bool TryGetValue(LowLevelTransaction tx, long page, out PagePosition value)
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

        public long GetLastSeenTransactionId()
        {
            return Volatile.Read(ref _maxSeenTransaction);
        }

        public IEnumerable<KeyValuePair<long, PagePosition>> Iterate(long minTxInclusive, long maxTxInclusive)
        {
            var list = new List<Dictionary<long, PagePosition>>();
            lock (_transactionPages)
            {
                var start = _transactionPages.IndexOfKey(minTxInclusive);
                if (start == -1)
                {
                    for (long i = minTxInclusive + 1; i <= maxTxInclusive; i++)
                    {
                        start = _transactionPages.IndexOfKey(i);
                        if (start != -1)
                            break;
                    }
                }
                if (start != -1)
                {
                    for (int i = start; i < _transactionPages.Count; i++)
                    {
                        if (_transactionPages.Keys[i] > maxTxInclusive)
                            break;

                        var val = _transactionPages.Values[i];
                        list.Add(val);
                    }
                }
            }
            foreach (var dic in list)
            {
                foreach (var kvp in dic)
                {
                    yield return kvp;
                }
            }

        }
    }
}