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
    public class TransactionContextPool : ITransactionContextPool
    {
        private readonly StorageEnvironment _storageEnvironment;

        private readonly ConcurrentStack<TransactionOperationContext> _contextPool;

        public TransactionContextPool(StorageEnvironment storageEnvironment)
        {
            _storageEnvironment = storageEnvironment;
            _contextPool = new ConcurrentStack<TransactionOperationContext>();
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            TransactionOperationContext ctx;
            if (_contextPool.TryPop(out ctx) == false)
                ctx = new TransactionOperationContext(_storageEnvironment);

            context = ctx;

            return new ReturnRequestContext
            {
                Parent = this,
                Context = ctx
            };
        }

        public IDisposable AllocateOperationContext(out TransactionOperationContext context)
        {
            Debug.Assert(_storageEnvironment != null);

            TransactionOperationContext ctx;
            if (_contextPool.TryPop(out ctx) == false)
                ctx = new TransactionOperationContext(_storageEnvironment);

            context = ctx;

            return new ReturnRequestContext
            {
                Parent = this,
                Context = ctx
            };
        }

        private class ReturnRequestContext : IDisposable
        {
            public TransactionOperationContext Context;
            public TransactionContextPool Parent;
            public void Dispose()
            {
                Context.Reset();
                //TODO: this probably should have low memory handle
                //TODO: need better policies, stats, reporting, etc
                if (Parent._contextPool.Count > 25) // don't keep too much of them around
                {
                    Context.Dispose();
                    return;
                }
                Parent._contextPool.Push(Context);
            }
        }

        public void Dispose()
        {
            TransactionOperationContext result;
            while (_contextPool.TryPop(out result))
            {
                result.Dispose();
            }
        }
    }
}