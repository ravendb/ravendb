using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask<DetailedDatabaseStatistics> GetResultForCurrentNodeAsync()
        {
            throw new NotSupportedException();
        }

        protected override async ValueTask<DetailedDatabaseStatistics> GetResultForRemoteNodeAsync(RavenCommand<DetailedDatabaseStatistics> command, string nodeTag)
        {
            var shardNumber = GetShardNumber();

            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);

            return command.Result;
        }
    }
}
