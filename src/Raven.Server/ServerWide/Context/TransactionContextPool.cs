// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionContextPool : JsonContextPoolBase<TransactionOperationContext> ,ITransactionContextPool
    {
        private StorageEnvironment _storageEnvironment;

        // this is safe to do across instances, because all the pools in a thread are going to share
        // the same optimizations
        [ThreadStatic]
        private static bool _mostlyThreadDedicatedWork;

        public void SetMostWorkInGoingToHappenOnThisThread()
        {
            _mostlyThreadDedicatedWork = true;
        }

        public TransactionContextPool(StorageEnvironment storageEnvironment)
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
                initialSize = 32*1024;

                if (_mostlyThreadDedicatedWork)
                {
                    // if this is a context pool dedicated for a thread (like for indexes), we probably won't do a lot of 
                    // work on that outside of its thread, so let not allocate a lot of memory for that. We just need enough
                    // there process simple stuff like IsStale, etc, so let us start small
                    initialSize = 16 * 1024 * 1024;  // the initial budget is 32 MB, so let us now blow through that all at once
                }
            }

            return new TransactionOperationContext(_storageEnvironment, initialSize, 16*1024, LowMemoryFlag);
        }

        public override void Dispose()
        {
            _storageEnvironment = null;
            base.Dispose();
        }
    }
}
