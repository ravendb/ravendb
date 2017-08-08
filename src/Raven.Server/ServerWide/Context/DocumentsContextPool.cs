// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsContextPool : JsonContextPoolBase<DocumentsOperationContext>, IDocumentsContextPool
    {
        private readonly DocumentDatabase _database;

        public DocumentsContextPool(DocumentDatabase database)
        {
            _database = database;
        }

        protected override DocumentsOperationContext CreateContext()
        {
            if (sizeof(int) == IntPtr.Size || _database.Configuration.Storage.ForceUsing32BitsPager)
                return new DocumentsOperationContext(_database, 32 * 1024, 4 * 1024, LowMemoryFlag);
            return new DocumentsOperationContext(_database, 1024 * 1024, 16 * 1024, LowMemoryFlag);
        }
    }
}