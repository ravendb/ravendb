using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal class StatsHandlerProcessorForGetMetrics : AbstractStatsHandlerProcessorForGetMetrics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetMetrics([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context)
        {
            return ValueTask.FromResult(RequestHandler.Database.Metrics.ToJson());
        }
    }
}
