using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats/essential", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task EssentialStats()
        {
            using (var processor = new StatsHandlerProcessorForEssentialStats(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/stats/detailed", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task DetailedStats()
        {
            using (var processor = new StatsHandlerProcessorForGetDetailedDatabaseStatistics(this))
                    await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Stats()
        {
            using (var processor = new StatsHandlerProcessorForGetDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/healthcheck", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task DatabaseHealthCheck()
        {
            NoContentStatus();
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/metrics", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Metrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, Database.Metrics.ToJson());
            }
        }

        [RavenAction("/databases/*/metrics/puts", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PutsMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var empty = GetBoolValueQueryString("empty", required: false) ?? true;

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Database.Metrics.Docs)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Docs.PutsPerSec)] = Database.Metrics.Docs.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Attachments)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Attachments.PutsPerSec)] = Database.Metrics.Attachments.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Counters)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Counters.PutsPerSec)] = Database.Metrics.Counters.PutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.TimeSeries)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.TimeSeries.PutsPerSec)] = Database.Metrics.TimeSeries.PutsPerSec.CreateMeterData(true, empty)
                    }
                });
            }
        }

        [RavenAction("/databases/*/metrics/bytes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task BytesMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var empty = GetBoolValueQueryString("empty", required: false) ?? true;

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(Database.Metrics.Docs)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Docs.BytesPutsPerSec)] = Database.Metrics.Docs.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Attachments)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Attachments.BytesPutsPerSec)] = Database.Metrics.Attachments.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.Counters)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.Counters.BytesPutsPerSec)] = Database.Metrics.Counters.BytesPutsPerSec.CreateMeterData(true, empty)
                    },
                    [nameof(Database.Metrics.TimeSeries)] = new DynamicJsonValue
                    {
                        [nameof(Database.Metrics.TimeSeries.BytesPutsPerSec)] = Database.Metrics.TimeSeries.BytesPutsPerSec.CreateMeterData(true, empty)
                    }
                });
            }
        }
    }
}
