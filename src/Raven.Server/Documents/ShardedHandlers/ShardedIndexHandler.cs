using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedIndexHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/indexes/stats", "GET")]
        public async Task Stats()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Implement it for the Client API");

            var shard = GetLongQueryString("shard", false);
            if (shard == null)
                throw new InvalidOperationException("In a sharded environment you must provide a shard id");

            if (ShardedContext.RequestExecutors.Length <= shard)
                throw new InvalidOperationException($"Non existing shard id, {shard}");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var executor = ShardedContext.RequestExecutors[shard.Value];
                var command = new GetIndexesStatisticsOperation.GetIndexesStatisticsCommand((int)shard.Value);
                await executor.ExecuteAsync(command, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteIndexesStats(context, command.Result);
                }
            }
        }

        [RavenShardedAction("/databases/*/indexes/performance", "GET")]
        public async Task Performance()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Implement it for the Client API");

            var shard = GetLongQueryString("shard", false);
            if (shard == null)
                throw new InvalidOperationException("In a sharded environment you must provide a shard id");

            if (ShardedContext.RequestExecutors.Length <= shard)
                throw new InvalidOperationException($"Non existing shard id, {shard}");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var executor = ShardedContext.RequestExecutors[shard.Value];
                var command = new GetIndexPerformanceStatisticsOperation.GetIndexPerformanceStatisticsCommand(null, (int)shard.Value);
                await executor.ExecuteAsync(command, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WritePerformanceStats(context, command.Result);
                }
            }
        }

        [RavenShardedAction("/databases/*/indexes/errors", "GET")]
        public async Task GetErrors()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Implement it for the Client API");

            var shard = GetLongQueryString("shard", false);
            if (shard == null)
                throw new InvalidOperationException("In a sharded environment you must provide a shard id");

            if (ShardedContext.RequestExecutors.Length <= shard)
                throw new InvalidOperationException($"Non existing shard id, {shard}");

            var names = GetStringValuesQueryString("name", required: false);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var executor = ShardedContext.RequestExecutors[shard.Value];
                var command = new GetIndexErrorsOperation.GetIndexErrorsCommand(names.ToArray(), null, (int)shard.Value);
                await executor.ExecuteAsync(command, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteIndexErrors(context, command.Result);
                }
            }
        }
    }
}


