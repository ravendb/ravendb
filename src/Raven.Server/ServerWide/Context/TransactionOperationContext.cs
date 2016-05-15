using System;
using Raven.Server.Documents;
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

        protected override RavenTransaction CreateLazyWriteTransaction()
        {
            var newTx = new RavenTransaction(_environment.WriteTransaction());
            newTx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = true;
            return newTx;
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

        protected abstract TTransaction CreateLazyWriteTransaction();

        public virtual RavenTransaction OpenWriteTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                throw new InvalidOperationException("Transaction is already opened");
            }
            return Transaction = CreateWriteTransaction();
        }

        public virtual RavenTransaction OpenLazyWriteTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                throw new InvalidOperationException("Transaction is already opened");
            }
            return Transaction = CreateLazyWriteTransaction();
        }

        public override void Reset()
        {
            base.Reset();

            Transaction?.Dispose();
            Transaction = null;
        }
    }
}