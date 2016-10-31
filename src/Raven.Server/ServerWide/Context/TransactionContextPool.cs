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
        private readonly StorageEnvironment _storageEnvironment;

        private ThreadLocal<bool> _mostlyThreadDedicatedWork;

        public void SetMostWorkInGoingToHappenonThisThread()
        {
            _mostlyThreadDedicatedWork = new ThreadLocal<bool>();
            _mostlyThreadDedicatedWork.Value = true;
        }

        public TransactionContextPool(StorageEnvironment storageEnvironment)
        {
            _storageEnvironment = storageEnvironment;
        }

        protected override TransactionOperationContext CreateContext()
        {
            var initialSize = 1024 *1024;
            if (_mostlyThreadDedicatedWork != null)
            {
                // if this is a context pool dedicated for a thread (like for indexes), we probably won't do a lot of 
                // work on that outside of its thread, so let not allocate a lot of memory for that. We just need enough
                // there process simple stuff like IsStale, etc, so let us start small
                initialSize = _mostlyThreadDedicatedWork.Value ? 
                    16*1024*1024 : // the initial budget is 32 MB, so let us now blow through that all at once
                    32*1024;
            }
            return new TransactionOperationContext(_storageEnvironment,
                initialSize, 
                16*1024);
        }
    }
}