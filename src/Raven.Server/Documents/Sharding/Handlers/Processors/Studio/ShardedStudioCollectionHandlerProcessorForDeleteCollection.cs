using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal class ShardedStudioCollectionHandlerProcessorForDeleteCollection : AbstractStudioCollectionHandlerProcessorForDeleteCollection<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioCollectionHandlerProcessorForDeleteCollection([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void DeleteCollection(TransactionOperationContext context, IDisposable returnContextToPool, string collectionName, HashSet<string> excludeIds,
            long operationId, OperationCancelToken token)
        {
            var shardToIds = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, excludeIds);

            var opToken = RequestHandler.CreateTimeLimitedOperationToken();

            var task = RequestHandler.DatabaseContext.Operations.AddRemoteOperation(
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
                using (returnContextToPool)
                    opToken.Dispose();
            });
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }
    }
}
