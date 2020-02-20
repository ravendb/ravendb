// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionContextPool : JsonContextPoolBase<TransactionOperationContext> ,ITransactionContextPool
    {
        private StorageEnvironment _storageEnvironment;
        private readonly ClusterChanges _clusterChanges;

        public TransactionContextPool(StorageEnvironment storageEnvironment, ClusterChanges clusterChanges = null, Size? maxContextSizeToKeepInMb = null) : base(maxContextSizeToKeepInMb)
        {
            _storageEnvironment = storageEnvironment;
            _clusterChanges = clusterChanges;
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
                initialSize = 32*1024;
                }

            return new TransactionOperationContext(_storageEnvironment, initialSize, 16*1024, LowMemoryFlag, _clusterChanges);
        }

        public override void Dispose()
        {
            _storageEnvironment = null;
            base.Dispose();
        }
    }
}
