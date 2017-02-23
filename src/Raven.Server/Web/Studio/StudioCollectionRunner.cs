using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    internal class StudioCollectionRunner : CollectionRunner
    {
        private readonly HashSet<LazyStringValue> _excludeIds;

        public StudioCollectionRunner(DocumentDatabase database, DocumentsOperationContext context, HashSet<LazyStringValue> excludeIds) : base(database, context)
        {
            _excludeIds = excludeIds;
        }

        public override IOperationResult ExecuteDelete(string collectionName, CollectionOperationOptions options, DocumentsOperationContext documentsOperationContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            if (_excludeIds.Count == 0)
            {
                return base.ExecuteDelete(collectionName, options, documentsOperationContext, onProgress, token);
            }

            return ExecuteOperation(collectionName, options, _context, onProgress, key =>
            {
                if (_excludeIds.Contains(key) == false)
                {
                    _database.DocumentsStorage.Delete(_context, key, null);
                }
            }, token);
        }
    }
}