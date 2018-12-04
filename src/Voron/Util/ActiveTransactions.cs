using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Util
{
    public class ActiveTransactions
    {
        private RacyConcurrentBag _activeTxs = new RacyConcurrentBag(growthFactor: 16);

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

        public bool TryRemove(LowLevelTransaction tx)
        {
            if (_activeTxs.Remove(tx) == false)
                return false;

            var oldTx = _oldestTransaction;

            while (tx.Id <= oldTx)
            {
                var currentOldest = _activeTxs.ScanOldest();
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

        public bool Contains(LowLevelTransaction tx)
        {
            return tx.ActiveTransactionNode.Value == tx;
        }
    }

    public class RacyConcurrentBag
    {
        public class Node
        {
            public LowLevelTransaction Value;
        }

        private Node[] _array = Array.Empty<Node>();

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
                if (item != null)
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
                if (item != null)
                {
                    list.Add(item);
                }
            }
            return list;
        }

        private static LowLevelTransaction InvalidLowLevelTransaction = (LowLevelTransaction)FormatterServices.GetUninitializedObject(typeof(LowLevelTransaction));

        public bool Remove(LowLevelTransaction tx)
        {
            var copy = _array;
            var node = tx.ActiveTransactionNode;
            if (Interlocked.CompareExchange(ref node.Value, null, tx) != tx)
                return false;

            var result = Interlocked.Decrement(ref _inUse);
            if (result > 0 && copy.Length > _growthFactor * 4)
                return true;

            bool failed = false;
            for (int i = 0; i < copy.Length; i++)
            {
                if(Interlocked.CompareExchange(ref copy[i].Value, InvalidLowLevelTransaction, null) != null)
                {
                    failed = true;
                    break;
                }
            }

            if(failed == false)
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
                    if(value == InvalidLowLevelTransaction)
                    {
                        compactionInProgress = true;
                    }
                }

                if (compactionInProgress)
                {
                    // let the Remove() a chance to do its work
                    Thread.Yield();
                    copy = Volatile.Read(ref _array);// refresh the instance
                    continue;
                }

                // we scanned the array, couldn't find any available space
                var newArray = new Node[copy.Length + _growthFactor];
                Array.Copy(copy, newArray, copy.Length);
                for (int i = copy.Length; i < _growthFactor; i++)
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

        public long ScanOldest()
        {
            var copy = _array;
            long val = long.MaxValue;
            for (int i = 0; i < copy.Length; i++)
            {
                var item = copy[i].Value;
                if (item != null)
                {
                    if (val > item.Id)
                        val = item.Id;
                }
            }
            if (val == long.MaxValue)
                return 0;
            return val;
        }
    }
}
