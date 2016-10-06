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
    public class TransactionContextPool : JsonContextPoolBase<TransactionOperationContext> ,ITransactionContextPool
    {
        private readonly StorageEnvironment _storageEnvironment;
        private readonly int _initialSize;

        public TransactionContextPool(StorageEnvironment storageEnvironment, int initialSize = 1024 * 1024)
        {
            _storageEnvironment = storageEnvironment;
            _initialSize = initialSize;
        }

        protected override TransactionOperationContext CreateContext()
        {
            return new TransactionOperationContext(_storageEnvironment, _initialSize, 16*1024);
        }
    }
}