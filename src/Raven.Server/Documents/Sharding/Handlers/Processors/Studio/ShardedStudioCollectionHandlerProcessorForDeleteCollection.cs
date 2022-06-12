using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal class ShardedStudioCollectionHandlerProcessorForDeleteCollection : AbstractStudioCollectionHandlerProcessorForDeleteCollection<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioCollectionHandlerProcessorForDeleteCollection([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleDeleteCollection(TransactionOperationContext context, IDisposable returnToContextPool, string collectionName, HashSet<string> excludeIds,
            long operationId)
        {
            using (returnToContextPool)
            {
                var token = RequestHandler.CreateTimeLimitedOperationToken();

                var shardToIds = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, excludeIds);

                var task = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult, BulkOperationResult>(
                    operationId,
                    OperationType.DeleteByCollection,
                    collectionName,
                    detailedDescription: null,
                    (_, shardNumber) =>
                    {
                        if (shardToIds.ContainsKey(shardNumber) == false)
                            return new DeleteStudioCollectionOperation.DeleteStudioCollectionCommand(operationId, collectionName, null);
                        return new DeleteStudioCollectionOperation.DeleteStudioCollectionCommand(operationId, collectionName, shardToIds[shardNumber].Ids);
                    },
                    token: token);

                _ = task.ContinueWith(_ =>
                {
                    token.Dispose();
                });
            }
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }
    }
}
