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
        private static long FrequencyInMs = Stopwatch.Frequency / 1000;
        private static long StartTicks = Stopwatch.GetTimestamp();
        private static DateTime StartTime = DateTime.UtcNow;

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
                var start = StartTime.Add(TimeSpan.FromTicks(recentMetric.Start - StartTicks));
                recent.Add(new DynamicJsonValue
                {
                    ["Start"] = start.GetDefaultRavenFormat(),
                    ["Size"] = recentMetric.Size,
                    ["HumaneSize"] = Sizes.Humane(recentMetric.Size),
                    ["Duration"] = recentMetric.Duration / FrequencyInMs,
                    ["Type"] = recentMetric.Type.ToString()
                });
            }

            foreach (var historyMetric in fileMetric.GetSummaryMetrics())
            {
                var start = StartTime.Add(TimeSpan.FromTicks(historyMetric.TotalTimeStart - StartTicks));
                var end = StartTime.Add(TimeSpan.FromTicks(historyMetric.TotalTimeEnd - StartTicks));
                history.Add(new DynamicJsonValue
                {
                    ["Start"] = start.GetDefaultRavenFormat(),
                    ["End"] = end.GetDefaultRavenFormat(),
                    ["Size"] = historyMetric.TotalSize,
                    ["HumaneSize"] = Sizes.Humane(historyMetric.TotalSize),
                    ["Duration"] = (historyMetric.TotalTimeEnd - historyMetric.TotalTimeStart) / FrequencyInMs,
                    ["ActiveDuration"] = historyMetric.TotalTime / FrequencyInMs,
                    ["MaxDuration"] = historyMetric.MaxTime / FrequencyInMs,
                    ["MinDuration"] = historyMetric.MinTime / FrequencyInMs,
                    ["Type"] = historyMetric.Type.ToString()
                });
            }

            return djv;
        }
    }
}
