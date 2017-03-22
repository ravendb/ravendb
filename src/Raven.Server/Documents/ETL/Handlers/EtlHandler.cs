using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Handlers
{
    public class EtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/etl/stats", "GET")]
        public Task GetStats()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            EtlStatistics[] etlStats;
            if (string.IsNullOrEmpty(name))
            {
                etlStats = Database.EtlLoader.Processes.Select(x => x.Statistics).ToArray();
            }
            else
            {
                var etl = Database.EtlLoader.Processes.FirstOrDefault(r => r.Name == name);

                if (etl == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                etlStats = new[] { etl.Statistics };
            }
            
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteResults(context, etlStats, (w, c, stats) =>
                    {
                        w.WriteObject(context.ReadObject(stats.ToJson(), "etl/stats"));
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/debug/stats", "GET")]
        public Task GetDebugStats()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteResults(context, Database.EtlLoader.Processes, (w, c, etl) =>
                    {
                        w.WriteObject(context.ReadObject(new DynamicJsonValue
                        {
                            [nameof(etl.Name)] = etl.Name,
                            [nameof(etl.Statistics)] = etl.Statistics.ToJson(),
                            [nameof(etl.Metrics)] = etl.Metrics.ToJson(),

                        }, "etl/debug/stats"));
                    });

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/performance", "GET")]
        public Task GetDebugPref()
        {
            // TODO arek

            return Task.CompletedTask;
        }
    }
}