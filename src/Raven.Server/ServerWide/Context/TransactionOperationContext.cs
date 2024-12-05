using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public sealed class TransactionOperationContext : TransactionOperationContext<RavenTransaction>
    {
        public bool IgnoreStalenessDueToReduceOutputsToDelete;

        public TransactionOperationContext(StorageEnvironment environment, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(environment, initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
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

    public abstract class TransactionOperationContext<TTransaction> : JsonOperationContext, IChangeVectorOperationContext 
        where TTransaction : RavenTransaction
    {
        internal const short DefaultTransactionMarker = 2;

        public readonly ByteStringContext Allocator;
        public readonly TransactionPersistentContext PersistentContext;

        public TTransaction Transaction;

        private readonly int _maxOfAllocatedChangeVectors;
        private int _numberOfAllocatedChangeVectors;
        private FastList<ChangeVector> _allocatedChangeVectors;
        public readonly StorageEnvironment Environment;

        protected TransactionOperationContext(StorageEnvironment environment, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            Environment = environment;
            PersistentContext = new TransactionPersistentContext();
            Allocator = new ByteStringContext(lowMemoryFlag);

            _maxOfAllocatedChangeVectors = 2048;
            if (ChangeVector.PerCoreChangeVectors.TryPull(out _allocatedChangeVectors) == false)
                _allocatedChangeVectors = new FastList<ChangeVector>(256);
        }

        public TTransaction OpenReadTransaction([CallerMemberName] string caller = null)
        {
            if (Transaction != null && Transaction.Disposed == false)
                ThrowTransactionAlreadyOpened();

            Transaction = CreateReadTransaction();

            if (caller != null)
                Transaction.InnerTransaction.LowLevelTransaction.CallerName = caller;

            return Transaction;
        }

        public TTransaction CloneReadTransaction()
        {
            if (Transaction == null || Transaction.Disposed || Transaction.InnerTransaction.IsWriteTransaction)
                ThrowReadTransactionMustBeOpen();

            Allocator.DefragmentSegments();

            Transaction = CloneReadTransaction(Transaction);

            return Transaction;
        }

        protected abstract TTransaction CloneReadTransaction(TTransaction previous);

        public override long AllocatedMemory => _arenaAllocator.Allocated + Allocator._totalAllocated;

        public override long UsedMemory => _arenaAllocator.TotalUsed + Allocator._currentlyAllocated;

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

        [DoesNotReturn]
        private static void ThrowWriteTransactionMustBeOpen()
        {
            throw new InvalidOperationException("Write transaction must be opened");
        }

        [DoesNotReturn]
        private static void ThrowReadTransactionMustBeOpen()
        {
            throw new InvalidOperationException("Read transaction must be opened");
        }

        protected abstract TTransaction CreateReadTransaction();

        protected abstract TTransaction CreateWriteTransaction(TimeSpan? timeout = null);

        public TTransaction OpenWriteTransaction(TimeSpan? timeout = null, [CallerMemberName] string caller = null)
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                ThrowTransactionAlreadyOpened();
            }

            Transaction = CreateWriteTransaction(timeout);

            if (caller != null)
                Transaction.InnerTransaction.LowLevelTransaction.CallerName = caller;

            return Transaction;
        }

        [DoesNotReturn]
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

            if (_allocatedChangeVectors != null)
            {
                if (ChangeVector.PerCoreChangeVectors.TryPush(_allocatedChangeVectors) == false)
                {
                    // GC will take care of this
                }

                _allocatedChangeVectors = null;
            }
        }

        protected internal override void Renew()
        {
            base.Renew();

            if (_allocatedChangeVectors == null)
            {
                if (ChangeVector.PerCoreChangeVectors.TryPull(out _allocatedChangeVectors) == false)
                    _allocatedChangeVectors = new FastList<ChangeVector>(256);

                _numberOfAllocatedChangeVectors = 0;
            }
        }

        protected internal override void Reset(bool forceResetLongLivedAllocator = false)
        {
            CloseTransaction();

            base.Reset(forceResetLongLivedAllocator);

            Allocator.Reset();

            if (_allocatedChangeVectors != null)
            {
                if (ChangeVector.PerCoreChangeVectors.TryPush(_allocatedChangeVectors) == false)
                {
                    // GC will take care of this
                }

                _allocatedChangeVectors = null;
            }
        }

        public ChangeVector GetChangeVector(string changeVector) => GetChangeVector(changeVector, throwOnRecursion: false);

        public ChangeVector GetEmptyChangeVector() => GetChangeVector(null);

        public ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion)
        {
            ChangeVector allocatedChangeVector;
            if (_numberOfAllocatedChangeVectors < _allocatedChangeVectors.Count)
            {
                allocatedChangeVector = _allocatedChangeVectors[_numberOfAllocatedChangeVectors++];
                allocatedChangeVector.Renew(changeVector, throwOnRecursion, this);
                return allocatedChangeVector;
            }

            allocatedChangeVector = new ChangeVector(changeVector, throwOnRecursion, this);
            if (_numberOfAllocatedChangeVectors < _maxOfAllocatedChangeVectors)
            {
                _allocatedChangeVectors.Add(allocatedChangeVector);
                _numberOfAllocatedChangeVectors++;
            }

            return allocatedChangeVector;
        }

        public ChangeVector GetChangeVector(string version, string order)
        {
            ChangeVector allocatedChangeVector;
            var versionChangeVector = GetChangeVector(version, throwOnRecursion: true);
            var orderChangeVector = GetChangeVector(order, throwOnRecursion: true);

            if (_numberOfAllocatedChangeVectors < _allocatedChangeVectors.Count)
            {
                allocatedChangeVector = _allocatedChangeVectors[_numberOfAllocatedChangeVectors++];
                allocatedChangeVector.Renew(versionChangeVector, orderChangeVector);
                return allocatedChangeVector;
            }

            allocatedChangeVector = new ChangeVector(versionChangeVector, orderChangeVector);
            if (_numberOfAllocatedChangeVectors < _maxOfAllocatedChangeVectors)
            {
                _allocatedChangeVectors.Add(allocatedChangeVector);
                _numberOfAllocatedChangeVectors++;
            }

            return allocatedChangeVector;
        }
    }
}
