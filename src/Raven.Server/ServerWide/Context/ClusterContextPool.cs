using System;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public sealed class ClusterContextPool : JsonContextPoolBase<ClusterOperationContext>, ITransactionContextPool<ClusterOperationContext>
    {
        private ClusterChanges _changes;
        private StorageEnvironment _storageEnvironment;
        public StorageEnvironment StorageEnvironment => _storageEnvironment;

        public ClusterContextPool(ClusterChanges changes, StorageEnvironment storageEnvironment, Size? maxContextSizeToKeepInMb = null)
            : base(maxContextSizeToKeepInMb)
        {
            _changes = changes ?? throw new ArgumentNullException(nameof(changes));
            _storageEnvironment = storageEnvironment ?? throw new ArgumentNullException(nameof(storageEnvironment));
        }

        protected override ClusterOperationContext CreateContext()
        {
            int initialSize;
            int maxNumberOfAllocatedStringValues;
            if (_storageEnvironment.Options.RunningOn32Bits)
            {
                initialSize = 4096;
                maxNumberOfAllocatedStringValues = 2 * 1024;
            }
            else
            {
                initialSize = 32 * 1024;
                maxNumberOfAllocatedStringValues = 8 * 1024;
            }

            return new ClusterOperationContext(_changes, _storageEnvironment, initialSize, 16 * 1024, maxNumberOfAllocatedStringValues, LowMemoryFlag);
        }

        public override void Dispose()
        {
            _storageEnvironment = null;
            _changes = null;
            base.Dispose();
        }
    }
}
