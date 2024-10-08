using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;
using Sparrow.Threading;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Util
{
    public sealed class ActiveTransactions
    {
        private RacyConcurrentBag _activeTxs = new RacyConcurrentBag(growthFactor: 64);

        private long _oldestTransaction;

        public long OldestTransaction => Volatile.Read(ref _oldestTransaction);

        public void Add(LowLevelTransaction tx)
        {
            var oldTx = _oldestTransaction;
            _activeTxs.Add(tx);
            while (oldTx == 0 || oldTx > tx.Id)
            {
                var result = Interlocked.CompareExchange(ref _oldestTransaction, tx.Id, oldTx);
                if (result == oldTx)
                    break;
                oldTx = result;
            }
        }

        public void ForceRecheckingOldestTransactionByFlusherThread()
        {
            // RavenDB-13302
            // this is being run from the flusher, since we can afford to 
            // do an actual here, without stopping transaction speed

            var oldTx = Volatile.Read(ref _oldestTransaction);
            Interlocked.MemoryBarrier();
            var currentOldest = _activeTxs.ScanOldest();
            if (oldTx != currentOldest)
            {
                // this is optimistic, but doesn't matter, we'll be called again anyway
                Interlocked.CompareExchange(ref _oldestTransaction, currentOldest, oldTx);
            }
        }

        public bool TryRemove(LowLevelTransaction tx)
        {
            if (_activeTxs.Remove(tx) == false)
                return false;

            var oldTx = _oldestTransaction;

            // PERF: No point in trying to scan for older transactions, the current 
            // transaction is already the latest published one, so we won't change it
            long latestReadTransactionId = tx.Environment.CurrentStateRecord.TransactionId;
            if (latestReadTransactionId == oldTx)
                return true;

            if (tx.Flags is TransactionFlags.ReadWrite)
            {
                if (_activeTxs.HasTransactions == false)
                {
                    // We are in a write transaction, and there aren't any current read transactions _right now_.
                    // The idea is to move the oldest transaction marker to the latest value we can. We cannot use the current transaction id
                    // because we haven't finished publishing that. Instead, we mark the oldest transaction as the current published transaction.
                    // This way, if a new read transaction is started _after_ this line, but before we finalize the commit, the oldest transaction
                    // on record would match it.
                    Interlocked.CompareExchange(ref _oldestTransaction, latestReadTransactionId, oldTx);
                }
                // ReSharper disable once RedundantIfElseBlock
                else
                {
                    // nothing to do here, we'll let the cleanup of the read transactions to move it for us.
                }
                return true;

            }

            while (tx.Id <= oldTx)
            {
                var currentOldest = _activeTxs.ScanOldest(); // This is non-thread safe call (therefor from time to time we ForceRecheckingOldestTransactionByFlusherThread)
                if (currentOldest == tx.Id)// another tx with same id, they can cleanup after us
                    break;
                var result = Interlocked.CompareExchange(ref _oldestTransaction, currentOldest, oldTx);
                if (result == oldTx)
                    break;
                oldTx = result;
            }

            return true;
        }


        internal List<ActiveTransaction> AllTransactions => _activeTxs.Select(transaction => new ActiveTransaction
        {
            Id = transaction.Id,
            Flags = transaction.Flags,
            AsyncCommit = transaction.AsyncCommit != null
        }).ToList();

        internal List<LowLevelTransaction> AllTransactionsInstances => _activeTxs.ToList();

        internal IEnumerable<LowLevelTransaction> Enumerate() => _activeTxs;

        public bool Contains(LowLevelTransaction tx)
        {
            return tx.ActiveTransactionNode.Value == tx;
        }
    }

    public sealed class RacyConcurrentBag : IEnumerable<LowLevelTransaction>
    {
        public sealed class Node
        {
            public LowLevelTransaction Value;
        }

        private Node[] _array = [];

        private int _inUse;

        private readonly int _growthFactor;

        public RacyConcurrentBag(int growthFactor)
        {
            _growthFactor = growthFactor;
        }

        public IEnumerable<T> Select<T>(Func<LowLevelTransaction, T> func)
        {
            var copy = _array;
            for (int i = 0; i < copy.Length; i++)
            {
                var item = copy[i].Value;
                if (item != null && item != InvalidLowLevelTransaction)
                {
                    yield return func(item);
                }
            }
        }

        public List<LowLevelTransaction> ToList()
        {
            var copy = _array;
            var list = new List<LowLevelTransaction>();
            for (int i = 0; i < copy.Length; i++)
            {
                var item = copy[i].Value;
                if (item != null && item != InvalidLowLevelTransaction)
                {
                    list.Add(item);
                }
            }
            return list;
        }

        private static readonly LowLevelTransaction InvalidLowLevelTransaction = (LowLevelTransaction)RuntimeHelpers.GetUninitializedObject(typeof(LowLevelTransaction));

        private readonly MultipleUseFlag _compactionInProgress = new MultipleUseFlag();

        public bool Remove(LowLevelTransaction tx)
        {
            var copy = _array;
            var node = tx.ActiveTransactionNode;
            if (Interlocked.CompareExchange(ref node.Value, null, tx) != tx)
                return false;

            var result = Interlocked.Decrement(ref _inUse);
            if (result > 0 ||
                copy.Length < _growthFactor * 4)
                return true;

            if (_compactionInProgress.Raise() == false)
                return true;

            try
            {
                bool failed = false;
                for (int i = 0; i < copy.Length; i++)
                {
                    if (Interlocked.CompareExchange(ref copy[i].Value, InvalidLowLevelTransaction, null) != null)
                    {
                        failed = true;
                        break;
                    }
                }

                if (failed == false)
                {
                    var newArray = new Node[_growthFactor];
                    for (int i = 0; i < newArray.Length; i++)
                    {
                        newArray[i] = new Node();
                    }
                    if (Interlocked.CompareExchange(ref _array, newArray, copy) == copy)
                        return true;
                }

                // someone raced us for this, let's clean up
                for (int i = 0; i < copy.Length; i++)
                {
                    Interlocked.CompareExchange(ref copy[i].Value, null, InvalidLowLevelTransaction);
                }

            }
            finally
            {
                _compactionInProgress.Lower();
            }
            return true;
        }

        public void Add(LowLevelTransaction item)
        {
            Interlocked.Increment(ref _inUse);
            var copy = _array;
            while (true)
            {
                bool compactionInProgress = false;
                for (int i = 0; i < copy.Length; i++)
                {
                    var node = copy[i];
                    var value = node.Value;
                    if (value == null)
                    {
                        value = Interlocked.CompareExchange(ref node.Value, item, null);
                        if (value == null)
                        {
                            item.ActiveTransactionNode = node;
                            return;
                        }
                    }
                    if (value == InvalidLowLevelTransaction)
                    {
                        compactionInProgress = true;
                    }
                }

                if (compactionInProgress || _compactionInProgress.IsRaised())
                {
                    // let the Remove() a chance to do its work
                    Thread.Yield();
                    copy = Volatile.Read(ref _array);// refresh the instance
                    continue;
                }

                // we scanned the array, couldn't find any available space
                var newArray = new Node[copy.Length + _growthFactor];
                Array.Copy(copy, newArray, copy.Length);
                for (int i = copy.Length; i < newArray.Length; i++)
                {
                    newArray[i] = new Node();
                }
                newArray[copy.Length].Value = item;
                var result = Interlocked.CompareExchange(ref _array, newArray, copy);
                if (result == copy)
                {
                    item.ActiveTransactionNode = newArray[copy.Length];
                    return;
                }
                copy = result;
            }
        }

        public bool HasTransactions => _inUse != 0;

        public long ScanOldest()
        {
            var copy = _array;
            long val = long.MaxValue;
            for (int i = 0; i < copy.Length; i++)
            {
                var item = copy[i].Value;
                if (item is null || item == InvalidLowLevelTransaction) 
                    continue;
                if (val > item.Id)
                    val = item.Id;
            }
            if (val == long.MaxValue)
                return 0;
            return val;
        }

        public IEnumerator<LowLevelTransaction> GetEnumerator()
        {
            var copy = _array;
            for (int i = 0; i < copy.Length; i++)
            {
                var item = copy[i].Value;
                if (item != null && item != InvalidLowLevelTransaction)
                {
                    yield return item;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
