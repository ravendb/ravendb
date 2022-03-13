using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => false;

        protected override ValueTask<DatabaseStatistics> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

        protected override async Task<DatabaseStatistics> GetResultForRemoteNodeAsync(RavenCommand<DatabaseStatistics> command)
        {
            var shardNumber = GetShardNumber();

            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);

            return command.Result;
        }
    }
}
