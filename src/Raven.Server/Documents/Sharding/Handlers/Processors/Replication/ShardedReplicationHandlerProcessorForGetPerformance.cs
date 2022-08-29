using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetPerformance : AbstractReplicationHandlerProcessorForGetPerformance<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedReplicationHandlerProcessorForGetPerformance([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationPerformance> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();
            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }
    }
}
