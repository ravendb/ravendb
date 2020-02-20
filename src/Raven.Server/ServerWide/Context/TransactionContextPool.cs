// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionContextPool : JsonContextPoolBase<TransactionOperationContext>, ITransactionContextPool<TransactionOperationContext>
    {
        private StorageEnvironment _storageEnvironment;

        public TransactionContextPool(StorageEnvironment storageEnvironment, Size? maxContextSizeToKeepInMb = null) : base(maxContextSizeToKeepInMb)
        {
            _storageEnvironment = storageEnvironment;
        }

        protected override TransactionOperationContext CreateContext()
        {
            int initialSize;
            if (_storageEnvironment.Options.RunningOn32Bits)
            {
                initialSize = 4096;
            }
            else
            {
                initialSize = 32 * 1024;
            }

            return new TransactionOperationContext(_storageEnvironment, initialSize, 16 * 1024, LowMemoryFlag);
        }

        public override void Dispose()
        {
            _storageEnvironment = null;
            base.Dispose();
        }
    }
}
