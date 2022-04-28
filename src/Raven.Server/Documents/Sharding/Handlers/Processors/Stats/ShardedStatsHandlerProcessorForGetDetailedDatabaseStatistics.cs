using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats
{
    internal class ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<DetailedDatabaseStatistics> command)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
        }
    }
}
