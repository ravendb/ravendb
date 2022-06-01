using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio;

namespace Raven.Server.Documents.Handlers.Processors.Studio
{
    internal class StudioCollectionHandlerProcessorForDeleteCollection : AbstractStudioCollectionHandlerProcessorForDeleteCollection<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StudioCollectionHandlerProcessorForDeleteCollection([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask DeleteCollectionAsync(DocumentsOperationContext context, IDisposable returnContextToPool, string collectionName, HashSet<string> excludeIds, long operationId)
        {
            ExecuteCollectionOperation((runner, collectionNameParam, options, onProgress, token) => Task.Run(async () => await runner.ExecuteDelete(collectionNameParam, 0, long.MaxValue, options, onProgress, token)),
                context, returnContextToPool, OperationType.DeleteByCollection, collectionName, operationId, excludeIds);
            return ValueTask.CompletedTask;
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        private void ExecuteCollectionOperation(Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext docsContext, IDisposable returnContextToPool, OperationType operationType, string collectionName, long operationId, HashSet<string> excludeIds)
        {
            var token = RequestHandler.CreateTimeLimitedCollectionOperationToken();

            var collectionRunner = new StudioCollectionRunner(RequestHandler.Database, docsContext, excludeIds);
            
            // use default options
            var options = new CollectionOperationOptions();

            var task = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                operationType,
                collectionName,
                detailedDescription: null,
                onProgress => operation(collectionRunner, collectionName, options, onProgress, token),
                token: token);

            _ = task.ContinueWith(_ => returnContextToPool.Dispose());
        }
    }
}
