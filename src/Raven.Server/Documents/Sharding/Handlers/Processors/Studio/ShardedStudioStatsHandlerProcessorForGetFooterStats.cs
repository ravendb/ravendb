using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal sealed class ShardedStudioStatsHandlerProcessorForGetFooterStats : AbstractStudioStatsHandlerProcessorForGetFooterStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioStatsHandlerProcessorForGetFooterStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override Task HandleRemoteNodeAsync(ProxyCommand<FooterStatistics> command, OperationCancelToken token) =>
            RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);

        protected override async ValueTask<FooterStatistics> GetFooterStatisticsAsync()
        {
            var op = new ShardedGetStudioFooterStatsOperation(RequestHandler.HttpContext);
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            stats.CountOfIndexes = RequestHandler.DatabaseContext.DatabaseRecord.Indexes.Count;
            (stats.CountOfEtlTasksErrors, stats.CountOfAiTasksErrors) = await GetTaskErrorCountsAcrossAllReplicasAsync();

            return stats;
        }

        private async Task<(long EtlErrorsCount, long AiErrorsCount)> GetTaskErrorCountsAcrossAllReplicasAsync()
        {
            long etlErrorsCount = 0;
            long aiErrorsCount = 0;

            var serverStore = RequestHandler.ServerStore;
            var record = RequestHandler.DatabaseContext.DatabaseRecord;
            var clusterTopology = serverStore.GetClusterTopology();
            var certificate = serverStore.Server.Certificate.ClientCertificate;
            var conventions = serverStore.Sharding.DocumentConventionsForShard;
            var token = RequestHandler.HttpContext.RequestAborted;

            foreach ((int shardNumber, var topology) in record.Sharding.Shards)
            {
                if (topology.Count == 0)
                    continue;

                var shardDatabaseName = ShardHelper.ToShardName(record.DatabaseName, shardNumber);
                foreach (var nodeTag in topology.AllNodes)
                {
                    var url = serverStore.PublishedServerUrls.SelectUrl(nodeTag, clusterTopology);
                    using var executor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, shardDatabaseName, certificate, conventions);
                    using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        var cmd = new GetStudioFooterStatisticsOperation.GetStudioFooterStatisticsCommand();
                        await executor.ExecuteAsync(cmd, ctx, token: token);

                        if (cmd.Result == null)
                            continue;

                        etlErrorsCount += cmd.Result.CountOfEtlTasksErrors;
                        aiErrorsCount += cmd.Result.CountOfAiTasksErrors;
                    }
                }
            }

            return (etlErrorsCount, aiErrorsCount);
        }

        private readonly struct ShardedGetStudioFooterStatsOperation : IShardedOperation<FooterStatistics>
        {
            private readonly HttpContext _httpContext;

            public ShardedGetStudioFooterStatsOperation(HttpContext httpContext)
            {
                _httpContext = httpContext;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public FooterStatistics Combine(Dictionary<int, ShardExecutionResult<FooterStatistics>> results)
            {
                var result = new FooterStatistics();

                foreach (var stats in results.Values)
                    result.CombineWith(stats.Result);

                return result;
            }

            public RavenCommand<FooterStatistics> CreateCommandForShard(int shardNumber) => new GetStudioFooterStatisticsOperation.GetStudioFooterStatisticsCommand();
        }
    }
}
