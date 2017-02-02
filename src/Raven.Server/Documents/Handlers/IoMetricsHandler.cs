using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
                var environments = new List<IOMetricsEnvironment>();
                var result = new IOMetricsResponse
                {
                    Environments = environments
                };

                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    environments.Add(GetIoMetrics(storageEnvironment.Environment));
                }

                context.Write(writer, result.ToJson());
            }
            return Task.CompletedTask;
        }

        private IOMetricsEnvironment GetIoMetrics(StorageEnvironment storageEnvironment)
        {
            var files = new List<IOMetricsFileStats>();
            var ioMetrics = new IOMetricsEnvironment
            {
                Path = storageEnvironment.Options.BasePath,
                Files = files
            };

            foreach (var fileMetric in storageEnvironment.Options.IoMetrics.Files)
            {
                files.Add(GetFileMetrics(fileMetric));
            }

            return ioMetrics;
        }
       
        private IOMetricsFileStats GetFileMetrics(IoMetrics.FileIoMetrics fileMetric)
        {
            var recent = new List<IOMetricsRecentStats>();
            var history = new List<IOMetricsHistoryStats>();
            var fileMetrics = new IOMetricsFileStats
            {
                File = Path.GetFileName(fileMetric.FileName),
                Status = fileMetric.Closed ? FileStatus.Closed : FileStatus.InUse,
                Recent = recent,
                History = history
            };

            foreach (var recentMetric in fileMetric.GetRecentMetrics())
            {
                recent.Add(new IOMetricsRecentStats
                {
                    Start = recentMetric.Start.GetDefaultRavenFormat(),
                    Size = recentMetric.Size,
                    HumanSize = Sizes.Humane(recentMetric.Size),
                    Duration = Math.Round(recentMetric.Duration.TotalMilliseconds, 2),
                    Type = recentMetric.Type
                });
            }

            foreach (var historyMetric in fileMetric.GetSummaryMetrics())
            {
                history.Add(new IOMetricsHistoryStats
                {
                    Start = historyMetric.TotalTimeStart.GetDefaultRavenFormat(),
                    End = historyMetric.TotalTimeEnd.GetDefaultRavenFormat(),
                    Size = historyMetric.TotalSize,
                    HumanSize = Sizes.Humane(historyMetric.TotalSize),
                    Duration = Math.Round((historyMetric.TotalTimeEnd - historyMetric.TotalTimeStart).TotalMilliseconds, 2),
                    ActiveDuration = Math.Round(historyMetric.TotalTime.TotalMilliseconds, 2),
                    MaxDuration = Math.Round(historyMetric.MaxTime.TotalMilliseconds, 2),
                    MinDuration = Math.Round(historyMetric.MinTime.TotalMilliseconds, 2),
                    Type = historyMetric.Type
                });
            }
            
            return fileMetrics;
        }
    }

    public class IOMetricsHistoryStats
    {
        public string Start { get; set; }
        public string End { get; set; }
        public long Size { get; set; }
        public string HumanSize { get; set; }
        public double Duration { get; set; }
        public double ActiveDuration { get; set; }
        public double MaxDuration { get; set; }
        public double MinDuration { get; set; }
        public IoMetrics.MeterType Type { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Start)] = Start,
                [nameof(End)] = End,
                [nameof(Size)] = Size,
                [nameof(HumanSize)] = HumanSize,
                [nameof(Duration)] = Duration,
                [nameof(ActiveDuration)] = ActiveDuration,
                [nameof(MaxDuration)] = MaxDuration,
                [nameof(MinDuration)] = MinDuration,
                [nameof(Type)] = Type,
            };
        }
    }

    public class IOMetricsRecentStats
    {
        public string Start { get; set; }
        public long Size { get; set; }
        public string HumanSize { get; set; }
        public double Duration { get; set; }
        public IoMetrics.MeterType Type { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Start)] = Start,
                [nameof(Size)] = Size,
                [nameof(HumanSize)] = HumanSize,
                [nameof(Duration)] = Duration,
                [nameof(Type)] = Type
            };
        }
    }

    public enum FileStatus
    {
        Closed,
        InUse
    }

    public class IOMetricsFileStats
    {
        public string File { get; set; }
        public FileStatus Status { get; set; }
        public List<IOMetricsRecentStats> Recent { get; set; }
        public List<IOMetricsHistoryStats> History { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(File)] = File,
                [nameof(Status)] = Status,
                [nameof(Recent)] = new DynamicJsonArray(Recent.Select(x => x.ToJson())),
                [nameof(History)] = new DynamicJsonArray(History.Select(x => x.ToJson()))
            };
        }
    }

    public class IOMetricsEnvironment
    {
        public string Path { get; set; }
        public List<IOMetricsFileStats> Files { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Path)] = Path,
                [nameof(Files)] = new DynamicJsonArray(Files.Select(x => x.ToJson()))
            };
        }
    }

    public class IOMetricsResponse
    {
        public List<IOMetricsEnvironment> Environments { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Environments)] = new DynamicJsonArray(Environments.Select(x => x.ToJson()))
            };
        }
    }
}
