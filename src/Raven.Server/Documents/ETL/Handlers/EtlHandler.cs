using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Handlers
{
    public class EtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/etl/stats", "GET")]
        public Task GetStats()
        {
            var etlStats = GetProcessesToReportOn().Select(x => x.Statistics).ToArray();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(context, "Results", etlStats, (w, c, stats) =>
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
            var debugStats = GetProcessesToReportOn()
                .Select(etl => new DynamicJsonValue()
                {
                    [nameof(etl.Name)] = etl.Name,
                    [nameof(etl.Statistics)] = etl.Statistics.ToJson(),
                    [nameof(etl.Metrics)] = etl.Metrics.ToJson(),
                }).ToArray();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(context, "Results", debugStats, (w, c, stats) =>
                    {
                        w.WriteObject(context.ReadObject(stats, "etl/debug/stats"));
                    });
                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/performance", "GET")]
        public Task Performance()
        {
            var stats = GetProcessesToReportOn()
                .Select(x => new EtlProcessPerformanceStats
                {
                    ProcessName = x.Name,
                    Performance = x.GetPerformanceStats()
                })
                .ToArray();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", stats, (w, c, stat) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(stat.ProcessName));
                    w.WriteString(stat.ProcessName);
                    w.WriteComma();

                    w.WriteArray(c, nameof(stat.Performance), stat.Performance, (wp, cp, performance) =>
                    {
                        var statsDjv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        wp.WriteObject(context.ReadObject(statsDjv, "etl/performance"));
                    });

                    w.WriteEndObject();
                });
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        private IEnumerable<EtlProcess> GetProcessesToReportOn()
        {
            IEnumerable<EtlProcess> etls;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                etls = Database.EtlLoader.Processes
                    .OrderBy(x => x.Name);
            else
            {
                etls = Database.EtlLoader.Processes
                    .Where(x => names.Contains(x.Name, StringComparer.OrdinalIgnoreCase));
            }

            return etls;
        }
    }
}