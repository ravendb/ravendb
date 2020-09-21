using System;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionOperationContext : TransactionOperationContext<RavenTransaction>
    {
        public bool IgnoreStalenessDueToReduceOutputsToDelete;

        public readonly StorageEnvironment Environment;

        public TransactionOperationContext(StorageEnvironment environment, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            Environment = environment;
        }

        protected override RavenTransaction CloneReadTransaction(RavenTransaction previous)
        {
            var clonedTx = new RavenTransaction(Environment.CloneReadTransaction(previous.InnerTransaction, PersistentContext, Allocator));

            previous.Dispose();

            return clonedTx;
        }

        protected override RavenTransaction CreateReadTransaction()
        {
            return new RavenTransaction(Environment.ReadTransaction(PersistentContext, Allocator));
        }

        protected override RavenTransaction CreateWriteTransaction(TimeSpan? timeout = null)
        {
            return new RavenTransaction(Environment.WriteTransaction(PersistentContext, Allocator, timeout));
        }
    }

    public abstract class TransactionOperationContext<TTransaction> : JsonOperationContext
        where TTransaction : RavenTransaction
    {
        internal const short DefaultTransactionMarker = 2;

        public readonly ByteStringContext Allocator;
        public readonly TransactionPersistentContext PersistentContext;

        public TTransaction Transaction;

        protected TransactionOperationContext(int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            PersistentContext = new TransactionPersistentContext();
            Allocator = new ByteStringContext(lowMemoryFlag);
        }

        public TTransaction OpenReadTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
                ThrowTransactionAlreadyOpened();

            Transaction = CreateReadTransaction();

            return Transaction;
        }

        public TTransaction CloneReadTransaction()
        {
            if (Transaction == null || Transaction.Disposed || Transaction.InnerTransaction.IsWriteTransaction)
                ThrowReadTransactionMustBeOpen();

            Transaction = CloneReadTransaction(Transaction);

            return Transaction;
        }

        protected abstract TTransaction CloneReadTransaction(TTransaction previous);

        public bool HasTransaction => Transaction != null && Transaction.Disposed == false;

        public short TransactionMarkerOffset;

        protected short CurrentTxMarker;

        public short GetTransactionMarker()
        {
            if (Transaction != null && Transaction.Disposed == false && Transaction.InnerTransaction.LowLevelTransaction.Flags != TransactionFlags.ReadWrite)
                ThrowWriteTransactionMustBeOpen();

            var value = (short)(CurrentTxMarker + TransactionMarkerOffset);

            if (value == 0)
                return DefaultTransactionMarker;
            if (value < 0)
                return (short)-value;

            return value;
        }

        private static void ThrowWriteTransactionMustBeOpen()
        {
            throw new InvalidOperationException("Write transaction must be opened");
        }

        private static void ThrowReadTransactionMustBeOpen()
        {
            throw new InvalidOperationException("Read transaction must be opened");
        }

        protected abstract TTransaction CreateReadTransaction();

        protected abstract TTransaction CreateWriteTransaction(TimeSpan? timeout = null);

        public TTransaction OpenWriteTransaction(TimeSpan? timeout = null)
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                ThrowTransactionAlreadyOpened();
            }

            Transaction = CreateWriteTransaction(timeout);

            return Transaction;
        }

        private static void ThrowTransactionAlreadyOpened()
        {
            throw new InvalidOperationException("Transaction is already opened");
        }

        public void CloseTransaction()
        {
            Transaction?.Dispose();
            Transaction = null;
        }

        public override void Dispose()
        {
            base.Dispose();

            Allocator.Dispose();
        }

        protected internal override void Reset(bool forceResetLongLivedAllocator = false)
        {
            CloseTransaction();

            base.Reset(forceResetLongLivedAllocator);

            Allocator.Reset();
        }
    }
}
