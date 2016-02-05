// -----------------------------------------------------------------------
//  <copyright file="ContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using Raven.Server.Json;
using Voron;

namespace Raven.Server.ServerWide
{
    public class ContextPool : IDisposable
    {
        private readonly UnmanagedBuffersPool _pool;
        private readonly StorageEnvironment _env;
        private readonly ConcurrentStack<RavenOperationContext> _contextPool;

        public ContextPool(UnmanagedBuffersPool pool, StorageEnvironment env)
        {
            _pool = pool;
            _env = env;
            _contextPool = new ConcurrentStack<RavenOperationContext>();
        }

        public IDisposable AllocateOperationContext(out RavenOperationContext context)
        {
            if (_contextPool.TryPop(out context) == false)
                context = new RavenOperationContext(_pool)
                {
                    Environment = _env
                };
           
            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        private class ReturnRequestContext : IDisposable
        {
            public RavenOperationContext Context;
            public ContextPool Parent;
            public void Dispose()
            {
                Context.Transaction?.Dispose();
                Context.Reset();
                //TODO: this probably should have low memory handle
                //TODO: need better policies, stats, reporting, etc
                if (Parent._contextPool.Count > 25) // don't keep too much of them around
                {
                    Context.Dispose();
                    return;
                }
                Context.MaterializeDocumentKeys = true;// reset value if was changed
                Parent._contextPool.Push(Context);
            }
        }

        public void Dispose()
        {
            RavenOperationContext result;
            while (_contextPool.TryPop(out result))
            {
                result.Dispose();
            }
        }
    }
}