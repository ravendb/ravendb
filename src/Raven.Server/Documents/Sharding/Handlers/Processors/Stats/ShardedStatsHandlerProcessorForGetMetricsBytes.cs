using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats
{
    internal class ShardedStatsHandlerProcessorForGetMetricsBytes : AbstractStatsHandlerProcessorForGetMetrics<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetMetricsBytes([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context)
        {
            using (var token = RequestHandler.CreateOperationToken())
            {
                var metrics = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedStatsHandlerProcessorForGetMetrics.GetShardedDatabaseMetricsOperation(RequestHandler, context, puts: null, bytes: true), token.Token);
                return metrics;
            }
        }
    }
}
