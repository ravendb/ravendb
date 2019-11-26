using System;
using System.Threading;
using Raven.Client.Util;
using Voron.Impl;

namespace Raven.Server.Utils
{
    public class TransactionHolder
    {
        private readonly Transaction _tx;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public TransactionHolder(Transaction tx)
        {
            _tx = tx;
        }

        public IDisposable AcquireTransaction(out Transaction tx)
        {
            _lock.Wait();

            tx = _tx;

            return new DisposableAction(() =>
            {
                _lock.Release();
            });
        }
    }
}
