using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Collections
{
    internal class ShardedCollectionsHandlerProcessorForGetLastChangeVector : AbstractCollectionsHandlerProcessorForGetLastChangeVector<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCollectionsHandlerProcessorForGetLastChangeVector([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<LastChangeVectorForCollectionResult> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();
            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }
    }
}
