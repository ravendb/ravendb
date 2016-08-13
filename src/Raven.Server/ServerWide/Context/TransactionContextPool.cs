// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Diagnostics;

using Raven.Server.Json;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionContextPool : JsonContextPool<TransactionOperationContext> ,ITransactionContextPool
    {
        private readonly StorageEnvironment _storageEnvironment;

        public TransactionContextPool(StorageEnvironment storageEnvironment)
        {
            _storageEnvironment = storageEnvironment;
        }

        protected override TransactionOperationContext CreateContext()
        {
            return new TransactionOperationContext(_storageEnvironment, 1024*1024, 16*1024);
        }
    }
}