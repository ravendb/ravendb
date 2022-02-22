using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/stats", "GET")]
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
                var command = new GetStatisticsOperation.GetStatisticsCommand(null, null, (int)shard.Value);
                await executor.ExecuteAsync(command, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    writer.WriteDatabaseStatistics(context, command.Result);
            }
        }
    }
}
