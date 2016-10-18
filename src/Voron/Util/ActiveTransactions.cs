using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Util
{
    public class ActiveTransactions
    {
        public class Node
        {
            public LowLevelTransaction Transaction;
        }

        public class DynamicArray 
        {
            public Node[] Items = new Node[4];
            public int Length;

            public struct Enumerator : IEnumerator<Node>
            {
                private int _index, _max;
                private readonly Node[] _array;

                public Enumerator(DynamicArray parent)
                {
                    _array = parent.Items;
                    _max = Math.Min(parent.Length, _array.Length);
                    _index = -1;
                }

                public bool MoveNext()
                {
                    return ++_index < _max;
                }

                public void Reset()
                {
                    _index = -1;
                }

                public Node Current => _array[_index];

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(this);
            }

            public void Add(Node node)
            {
                if (Length < Items.Length)
                {
                    Items[Length++] = node;
                    return;
                }
                var newItems = new Node[Items.Length*2];
                Array.Copy(Items, newItems, Items.Length);
                Items = newItems;
                Items[Length++] = node;
            }
        }
        /// <summary>
        /// Note that this is using thread local variable, but a transaction can _move_ between threads!
        /// </summary>
        private readonly ThreadLocal<DynamicArray> _activeTransactions = new ThreadLocal<DynamicArray>(
            () => new DynamicArray(),
            trackAllValues: true);

        public long OldestTransaction
        {
            get
            {
                var largestTx = long.MaxValue;
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var threadActiveTransactions in _activeTransactions.Values)
                {
                    foreach (var activeTransaction in threadActiveTransactions)
                    {
                        var activeTransactionTransaction = activeTransaction.Transaction;
                        // ReSharper disable once UseNullPropagation
                        if (activeTransactionTransaction == null)
                            continue;

                        if (largestTx > activeTransactionTransaction.Id)
                            largestTx = activeTransactionTransaction.Id;
                    }
                }
                if (largestTx == long.MaxValue)
                    return 0;
                return largestTx;
            }
        }

        public void Add(LowLevelTransaction tx)
        {
            var threadActiveTxs = _activeTransactions.Value;
            foreach (var node in threadActiveTxs)
            {
                if (node.Transaction != null)
                    continue;

                tx.ActiveTransactionNode = node;
                node.Transaction = tx;
                return;
            }
            tx.ActiveTransactionNode = new Node
            {
                Transaction = tx
            };
            threadActiveTxs.Add(tx.ActiveTransactionNode);
        }

        internal List<ActiveTransaction> AllTransactions
        {
            get
            {
                var list = new List<ActiveTransaction>();

                foreach (var threadActiveTransactions in _activeTransactions.Values)
                {
                    foreach (var activeTransaction in threadActiveTransactions)
                    {
                        var transaction = activeTransaction.Transaction;
                        if (transaction == null)
                            continue;

                        list.Add(new ActiveTransaction
                        {
                            Id = transaction.Id,
                            Flags = transaction.Flags
                        });
                    }
                }

                return list;
            }
        }

        public bool Contains(LowLevelTransaction tx)
        {
            return Volatile.Read(ref tx.ActiveTransactionNode.Transaction) == tx;
        }

        public bool TryRemove(LowLevelTransaction tx)
        {
            if (tx.ActiveTransactionNode.Transaction != tx)
                return false;

            tx.ActiveTransactionNode.Transaction = null;
            tx.ActiveTransactionNode = null;

            return true;
        }
    }
}