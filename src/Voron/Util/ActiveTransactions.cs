using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Collections;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Util
{
    public class ActiveTransactions
    {
        private ConcurrentSet<LowLevelTransaction> _activeTxs = 
            new ConcurrentSet<LowLevelTransaction>();

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
            if (_activeTxs.TryRemove(tx) == false)
                return false;

            var oldTx = _oldestTransaction;

            while (tx.Id <= oldTx)
            {
                var currentOldest = ScanOldest(tx.Id);
                if (currentOldest == tx.Id)// another tx with same id, they can cleanup after us
                    break;
                var result = Interlocked.CompareExchange(ref _oldestTransaction, currentOldest, oldTx);
                if (result == oldTx)
                    break;
                oldTx = result;
            }

            return true;
        }

        private long ScanOldest(long current)
        {
            var oldest = long.MaxValue;

            foreach (var item in _activeTxs)
            {
                if (item.Id < oldest)
                {
                    oldest = item.Id;
                    if (oldest == current)
                        return current;
                }
            }

            if (oldest == long.MaxValue)
                return 0;
            return oldest;
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
            return _activeTxs.Contains(tx);
        }

    }
}
