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
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsContextPool : IDocumentsContextPool
    {
        private readonly DocumentDatabase _documentDatabase;

        private readonly ConcurrentStack<DocumentsOperationContext> _contextPool;

        public DocumentsContextPool(DocumentDatabase documentDatabase)
        {
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
                context = new DocumentsOperationContext(_documentDatabase);

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
                Parent._contextPool.Push(Context);
                //TODO: this probably should have low memory handle
                //TODO: need better policies, stats, reporting, etc
                Parent._contextPool.ReduceSizeIfTooBig(4096);
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