using System;

using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class RavenTransaction : IDisposable
    {
        public readonly Transaction InnerTransaction;

        public RavenTransaction(Transaction transaction)
        {
            InnerTransaction = transaction;
        }

        public virtual void Commit()
        {
            InnerTransaction.Commit();
        }

        public virtual void Dispose()
        {
            InnerTransaction?.Dispose();
        }
    }
}