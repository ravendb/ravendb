using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Server.Collections.LockFree;
using Voron.Impl;

namespace Voron.Util
{
    using Sparrow;
    using System.Diagnostics;

    /// <summary>
    /// This class assumes a single writer and many readers
    /// </summary>
    public class PageTable
    {
        private readonly LockFreeConcurrentDictionary<long, PagesBuffer> _values = new LockFreeConcurrentDictionary<long, PagesBuffer>(NumericEqualityComparer.BoxedInstanceInt64);
        private readonly SortedList<long, Dictionary<long, PagePosition>> _transactionPages = new SortedList<long, Dictionary<long, PagePosition>>(NumericComparer.BoxedInstanceInt64);
        private long _maxSeenTransaction;

        public void Clear()
        {
            lock (_transactionPages)
            {
                _values.Clear();
                _transactionPages.Clear();    
            }
        }
        
        private class PagesBuffer
        {
            public readonly PagePosition[] PagePositions;
            public int Start, End;

            public PagesBuffer(PagePosition[] buffer, PagesBuffer previous)
            {
                PagePositions = buffer;
                if (previous == null)
                    return;
                End = previous.End - previous.Start;
                Array.Copy(previous.PagePositions, previous.Start, PagePositions, 0, End);
            }

            public int Count
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return End - Start; }
            }

            public bool CanAdd
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return End < PagePositions.Length; }
            }

            public int Capacity
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return PagePositions.Length; }
            }

            public void Add(PagePosition p)
            {
                PagePositions[End++] = p;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void RemoveBefore(long lastSyncedTransactionId, FastList<PagePosition> unusedPages)
            {
                while (
                    Start < PagePositions.Length &&
                    PagePositions[Start] != null &&
                    PagePositions[Start].TransactionId <= lastSyncedTransactionId
                    )
                {
                    unusedPages.Add(PagePositions[Start++]);
                }
            }

            public PagePosition GetLastPagePosition()
            {
                if (End == 0)
                    return null;
                return PagePositions[End - 1];
            }
        }

        public bool IsEmpty => _values.Count == 0;

        public void SetItems(LowLevelTransaction tx, Dictionary<long, PagePosition> items)
        {
            lock (_transactionPages)
            {
                UpdateMaxSeenTxId(tx);
                _transactionPages.Add(tx.Id, items);
            }

            // here we rely on the fact that only one thread can update the concurrent dictionary
            foreach (var item in items)
            {
                if (_values.TryGetValue(item.Key, out var value) == false)
                {
                    value = new PagesBuffer(new PagePosition[2], null);
                    _values.TryAdd(item.Key, value);
                }
                if (value.CanAdd == false)
                {
                    var newVal = new PagesBuffer(new PagePosition[value.Capacity*2], value);
                    _values.TryUpdate(item.Key, newVal, value);
                    value = newVal;
                }
                value.Add(item.Value);
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

        public void RemoveKeysWhereAllPagesOlderThan(long lastSyncedTransactionId, FastList<PagePosition> unusedPages)
        {
            foreach (var kvp in _values)
            {
                var valueBuffer = kvp.Value;
                var position = valueBuffer.GetLastPagePosition();
                if (position == null)
                    continue;

                if (position.TransactionId > lastSyncedTransactionId)
                    continue;

                valueBuffer.RemoveBefore(lastSyncedTransactionId, unusedPages);
                if (valueBuffer.Count != 0)
                    continue;

                PagesBuffer _;
                _values.TryRemove(kvp.Key,out _);
            }
        }

        public bool TryGetValue(LowLevelTransaction tx, long page, out PagePosition value)
        {
            if (_values.TryGetValue(page, out var bufferHolder) == false )
            {
                value = null;
                return false;
            }
            var bufferStart = bufferHolder.Start;
            var bufferPagePositions = bufferHolder.PagePositions;
            
            for (int i = bufferHolder.End - 1; i >= bufferStart; i--)
            {
                var position = bufferPagePositions[i];
                if (position == null || position.TransactionId > tx.Id)
                    continue;

                if (position.IsFreedPageMarker)
                    break;

                value = position;
                Debug.Assert(value != null);
                return true;
            }

            // all the current values are _after_ this transaction started, so it sees nothing
            value = null;
            return false;
        }

        public long MaxTransactionId()
        {
            long maxTx = 0;

            foreach (var bufferHolder in _values.Values)
            {
                var position = bufferHolder.GetLastPagePosition();
                if (position != null && maxTx < position.TransactionId)
                    maxTx = position.TransactionId;
            }

            return maxTx;
        }

        public long GetLastSeenTransactionId()
        {
            return Interlocked.Read(ref _maxSeenTransaction);
        }

        public List<Dictionary<long, PagePosition>> GetModifiedPagesForTransactionRange(long minTxInclusive, long maxTxInclusive)
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
            return list;
        }
    }
}
