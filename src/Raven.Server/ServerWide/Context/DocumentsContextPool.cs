// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Diagnostics;

using Raven.Server.Documents;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsContextPool : IDocumentsContextPool
    {
        private readonly UnmanagedBuffersPool _pool;

        private readonly DocumentDatabase _documentDatabase;

        private readonly ConcurrentStack<DocumentsOperationContext> _contextPool;

        public DocumentsContextPool(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
        {
            _pool = pool;
            _documentDatabase = documentDatabase;
            _contextPool = new ConcurrentStack<DocumentsOperationContext>();
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            DocumentsOperationContext ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;

            return disposable;
        }

        public IDisposable AllocateOperationContext(out DocumentsOperationContext context)
        {
            Debug.Assert(_documentDatabase != null);

            if (_contextPool.TryPop(out context) == false)
                context = new DocumentsOperationContext(_pool, _documentDatabase);

            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        private class ReturnRequestContext : IDisposable
        {
            public DocumentsOperationContext Context;
            public DocumentsContextPool Parent;
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
            DocumentsOperationContext result;
            while (_contextPool.TryPop(out result))
            {
                result.Dispose();
            }
        }
    }
}