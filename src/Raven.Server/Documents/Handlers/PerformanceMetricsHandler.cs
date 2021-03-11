using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Meters;

namespace Raven.Server.Documents.Handlers
{
    public class PerformanceMetricsHandler : DatabaseRequestHandler
    {
        public class PerformanceMetricsResponse
        {
            public PerformanceMetricsResponse()
            {
                PerfMetrics = new List<PerformanceMetrics>();
            }

            public List<PerformanceMetrics> PerfMetrics { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(PerfMetrics)] = new DynamicJsonArray(PerfMetrics.Select(x => x.ToJson()))
                };
            }
        }

        [RavenAction("/databases/*/debug/perf-metrics", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task IoMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var result = GetPerformanceMetricsResponse(Database);
                context.Write(writer, result.ToJson());
            }
        }

        public static PerformanceMetricsResponse GetPerformanceMetricsResponse(DocumentDatabase documentDatabase)
        {
            var result = new PerformanceMetricsResponse();

            foreach (var metrics in documentDatabase.GetAllPerformanceMetrics())
            {
                result.PerfMetrics.Add(metrics.Buffer);
            }

            return result;
        }
    }
}
