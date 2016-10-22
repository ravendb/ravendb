using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Server.Documents;
using Raven.Server.Json;
using Sparrow.Json;
using Voron;
using Sparrow;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionOperationContext : TransactionOperationContext<RavenTransaction>
    {
        private readonly StorageEnvironment _environment;
        public TransactionPersistentContext PersistentContext = new TransactionPersistentContext();

        public TransactionOperationContext(StorageEnvironment environment, int initialSize, int longLivedSize) :
            base(initialSize, longLivedSize)
        {
            _environment = environment;
        }

        protected override RavenTransaction CreateReadTransaction(ByteStringContext context)
        {
            return new RavenTransaction(_environment.ReadTransaction(PersistentContext, context));
        }

        protected override RavenTransaction CreateWriteTransaction(ByteStringContext context)
        {
            return new RavenTransaction(_environment.WriteTransaction(PersistentContext, context));
        }

        public StorageEnvironment Environment => _environment;
    }

    public abstract class TransactionOperationContext<TTransaction> : JsonOperationContext
        where TTransaction : RavenTransaction
    {
        private List<GCHandle> _pinnedObjects;

        public ByteStringContext Allocator;
        public TTransaction Transaction;

        protected TransactionOperationContext(int initialSize, int longLivedSize):
            base(initialSize, longLivedSize)
        {
            Allocator = new ByteStringContext();
        }

        public RavenTransaction OpenReadTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
                throw new InvalidOperationException("Transaction is already opened");

            Transaction = CreateReadTransaction(Allocator);

            return Transaction;
        }

        protected abstract TTransaction CreateReadTransaction(ByteStringContext allocator);

        protected abstract TTransaction CreateWriteTransaction(ByteStringContext allocator);

        public virtual RavenTransaction OpenWriteTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                throw new InvalidOperationException("Transaction is already opened");
            }

            Transaction = CreateWriteTransaction(Allocator);

            return Transaction;
        }

        public void CloseTransaction()
        {
            Transaction?.Dispose();
            Transaction = null;
        }

        public IntPtr PinObjectAndGetAddress(object obj)
        {
            var handle = GCHandle.Alloc(obj, GCHandleType.Pinned);

             if (_pinnedObjects == null)
                _pinnedObjects = new List<GCHandle>();

             _pinnedObjects.Add(handle);

            return handle.AddrOfPinnedObject();
        }

        public override void Dispose()
        {
            base.Dispose();

            Allocator?.Dispose();

            if (_pinnedObjects != null)
            {
                foreach (var pinnedObject in _pinnedObjects)
                {
                    pinnedObject.Free();
                }
            }
        }

        public override void ResetAndRenew()
        {
            base.Reset();
            CloseTransaction();

            // we skip on creating / disposing the allocator

            base.Renew();
        }

        protected override void Renew()
        {
            base.Renew();
            if (Allocator == null)
                Allocator = new ByteStringContext();
        }

        protected override void Reset()
        {
            base.Reset();

            CloseTransaction();

            if (Allocator?.ShouldDisposeOnReset != true)
                return;

            Allocator?.Dispose();
            Allocator = null;
        }
    }
}