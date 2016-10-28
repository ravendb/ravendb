using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Handlers
{
    public class IoMetricsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/io-metrics", "GET")]
        public Task IoMetrics()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var environments = new DynamicJsonArray();
                var result = new DynamicJsonValue
                {
                    ["Environments"] = environments
                };

                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    environments.Add(GetIoMetrics(storageEnvironment));
                }

                context.Write(writer, result);
            }
            return Task.CompletedTask;
        }

        private DynamicJsonValue GetIoMetrics(StorageEnvironment storageEnvironment)
        {
            var files = new DynamicJsonArray();
            var djv = new DynamicJsonValue
            {
                ["Path"] = storageEnvironment.Options.BasePath,
                ["Files"] = files
            };

            foreach (var fileMetric in storageEnvironment.Options.IoMetrics.Files)
            {
                files.Add(GetFileMetrics(fileMetric));
            }

            return djv;
        }

        private DynamicJsonValue GetFileMetrics(IoMetrics.FileIoMetrics fileMetric)
        {
            var recent = new DynamicJsonArray();
            var history = new DynamicJsonArray();
            var djv = new DynamicJsonValue
            {
                ["File"] = Path.GetFileName(fileMetric.FileName),
                ["Status"] = fileMetric.Closed ? "Closed" : "InUse",
                ["Recent"] = recent,
                ["History"] = history
            };


            foreach (var recentMetric in fileMetric.GetRecentMetrics())
            {
                recent.Add(new DynamicJsonValue
                {
                    ["Start"] = recentMetric.Start.GetDefaultRavenFormat(),
                    ["Size"] = recentMetric.Size,
                    ["HumaneSize"] = Sizes.Humane(recentMetric.Size),
                    ["Duration"] = Math.Round(recentMetric.Duration.TotalMilliseconds, 2),
                    ["Type"] = recentMetric.Type.ToString()
                });
            }

            foreach (var historyMetric in fileMetric.GetSummaryMetrics())
            {
                history.Add(new DynamicJsonValue
                {
                    ["Start"] = historyMetric.TotalTimeStart.GetDefaultRavenFormat(),
                    ["End"] = historyMetric.TotalTimeEnd.GetDefaultRavenFormat(),
                    ["Size"] = historyMetric.TotalSize,
                    ["HumaneSize"] = Sizes.Humane(historyMetric.TotalSize),
                    ["Duration"] = Math.Round((historyMetric.TotalTimeEnd - historyMetric.TotalTimeStart).TotalMilliseconds, 2),
                    ["ActiveDuration"] = Math.Round(historyMetric.TotalTime.TotalMilliseconds, 2),
                    ["MaxDuration"] = Math.Round(historyMetric.MaxTime.TotalMilliseconds, 2),
                    ["MinDuration"] = Math.Round(historyMetric.MinTime.TotalMilliseconds, 2),
                    ["Type"] = historyMetric.Type.ToString()
                });
            }

            return djv;
        }
    }
}
