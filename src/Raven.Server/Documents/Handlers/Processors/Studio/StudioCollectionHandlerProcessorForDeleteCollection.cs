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

        protected override void ScheduleDeleteCollection(DocumentsOperationContext context, IDisposable returnToContextPool, string collectionName, HashSet<string> excludeIds,
            long operationId)
        {
            ExecuteCollectionOperation(
                (runner, collectionNameParam, options, onProgress, tokenParam) =>
                    Task.Run(async () => await runner.ExecuteDelete(collectionNameParam, 0, long.MaxValue, options, onProgress, tokenParam)),
                context, returnToContextPool, OperationType.DeleteByCollection, collectionName, operationId, excludeIds);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        private void ExecuteCollectionOperation(
            Func<CollectionRunner, string, CollectionOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation,
            DocumentsOperationContext docsContext, IDisposable returnToContextPool, OperationType operationType, string collectionName, long operationId,
            HashSet<string> excludeIds)
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

            _ = task.ContinueWith(_ =>
            {
                using(returnToContextPool)
                    token.Dispose();
            });
        }
    }
}
