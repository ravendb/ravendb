using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal class StatsHandlerProcessorForGetMetricsPuts : AbstractStatsHandlerProcessorForGetMetrics<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForGetMetricsPuts([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context)
        {
            var empty = RequestHandler.GetBoolValueQueryString("empty", required: false) ?? true;

            return ValueTask.FromResult(new DynamicJsonValue
            {
                [nameof(RequestHandler.Database.Metrics.Docs)] =
                    new DynamicJsonValue
                    {
                        [nameof(RequestHandler.Database.Metrics.Docs.PutsPerSec)] = RequestHandler.Database.Metrics.Docs.PutsPerSec.CreateMeterData(true, empty)
                    },
                [nameof(RequestHandler.Database.Metrics.Attachments)] =
                    new DynamicJsonValue
                    {
                        [nameof(RequestHandler.Database.Metrics.Attachments.PutsPerSec)] =
                            RequestHandler.Database.Metrics.Attachments.PutsPerSec.CreateMeterData(true, empty)
                    },
                [nameof(RequestHandler.Database.Metrics.Counters)] =
                    new DynamicJsonValue
                    {
                        [nameof(RequestHandler.Database.Metrics.Counters.PutsPerSec)] =
                            RequestHandler.Database.Metrics.Counters.PutsPerSec.CreateMeterData(true, empty)
                    },
                [nameof(RequestHandler.Database.Metrics.TimeSeries)] = new DynamicJsonValue
                {
                    [nameof(RequestHandler.Database.Metrics.TimeSeries.PutsPerSec)] =
                        RequestHandler.Database.Metrics.TimeSeries.PutsPerSec.CreateMeterData(true, empty)
                }
            });
        }
    }
}
