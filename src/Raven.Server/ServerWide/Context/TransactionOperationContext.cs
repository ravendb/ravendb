using System;
using Raven.Server.Json;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionOperationContext : TransactionOperationContext<RavenTransaction>
    {
        private readonly StorageEnvironment _environment;

        public TransactionOperationContext(UnmanagedBuffersPool pool, StorageEnvironment environment)
            : base(pool)
        {
            _environment = environment;
        }

        protected override RavenTransaction CreateReadTransaction()
        {
            return new RavenTransaction(_environment.ReadTransaction());
        }

        protected override RavenTransaction CreateWriteTransaction()
        {
            return new RavenTransaction(_environment.WriteTransaction());
        }
    }

    public abstract class TransactionOperationContext<TTransaction> : JsonOperationContext
        where TTransaction : RavenTransaction
    {
        public TTransaction Transaction;

        protected TransactionOperationContext(UnmanagedBuffersPool pool)
            : base(pool)
        {
        }

        public RavenTransaction OpenReadTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
                throw new InvalidOperationException("Transaction is already opened");

            return Transaction = CreateReadTransaction();
        }

        protected abstract TTransaction CreateReadTransaction();

        protected abstract TTransaction CreateWriteTransaction();

        public virtual RavenTransaction OpenWriteTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                throw new InvalidOperationException("Transaction is already opened");
            }
            return Transaction = CreateWriteTransaction();
        }

        public override void Reset()
        {
            base.Reset();

            Transaction?.Dispose();
            Transaction = null;
        }
    }
}