using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal class StatsHandlerProcessorForGetMetricsBytes : AbstractStatsHandlerProcessorForGetMetrics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetMetricsBytes([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context)
        {
            var empty = RequestHandler.GetBoolValueQueryString("empty", required: false) ?? true;

            return ValueTask.FromResult(new DynamicJsonValue
            {
                [nameof(RequestHandler.Database.Metrics.Docs)] = new DynamicJsonValue
                {
                    [nameof(RequestHandler.Database.Metrics.Docs.BytesPutsPerSec)] = RequestHandler.Database.Metrics.Docs.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(RequestHandler.Database.Metrics.Attachments)] = new DynamicJsonValue
                {
                    [nameof(RequestHandler.Database.Metrics.Attachments.BytesPutsPerSec)] = RequestHandler.Database.Metrics.Attachments.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(RequestHandler.Database.Metrics.Counters)] = new DynamicJsonValue
                {
                    [nameof(RequestHandler.Database.Metrics.Counters.BytesPutsPerSec)] = RequestHandler.Database.Metrics.Counters.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(RequestHandler.Database.Metrics.TimeSeries)] = new DynamicJsonValue
                {
                    [nameof(RequestHandler.Database.Metrics.TimeSeries.BytesPutsPerSec)] = RequestHandler.Database.Metrics.TimeSeries.BytesPutsPerSec.CreateMeterData(true, empty)
                }
            });
        }
    }
}
