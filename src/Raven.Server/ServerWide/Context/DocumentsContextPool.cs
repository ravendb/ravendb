// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Server.Documents;
using Raven.Server.Logging;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Context
{
    public sealed class DocumentsContextPool : JsonContextPoolBase<DocumentsOperationContext>, IDocumentsContextPool
    {
        private DocumentDatabase _database;

        public DocumentsContextPool(DocumentDatabase database)
            : base(database.Configuration.Memory.MaxContextSizeToKeep, RavenLogManager.Instance.GetLoggerForDatabase<DocumentsContextPool>(database))
        {
            _database = database;
        }

        protected override DocumentsOperationContext CreateContext()
        {
            return _database.Is32Bits ?
                new DocumentsOperationContext(_database, 32 * 1024, 4 * 1024, 2 * 1024, LowMemoryFlag) :
                new DocumentsOperationContext(_database, 64 * 1024, 16 * 1024, 8 * 1024, LowMemoryFlag);
        }

        public override void Dispose()
        {
            _database = null;
            base.Dispose();
        }
    }
}
