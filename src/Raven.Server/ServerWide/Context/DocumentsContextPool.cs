// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;

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
            return new DocumentsOperationContext(_database, 1024*1024, 16*1024);
        }
    }
}