using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;
using Sparrow.Server.Meters;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Utils.IoMetrics
{
    public class IoMetricsUtil
    {
        public static IOMetricsResponse GetIoMetricsResponse(IEnumerable<StorageEnvironmentWithType> environments, IEnumerable<DatabasePerformanceMetrics> performanceMetrics)
        {
            var result = new IOMetricsResponse();

            foreach (var storageEnvironment in environments)
            {
                var metrics = GetIoMetrics(storageEnvironment.Environment);
                metrics.Type = storageEnvironment.Type;
                result.Environments.Add(metrics);
            }

            if (performanceMetrics != null)
            {
                foreach (var metrics in performanceMetrics)
                {
                    result.Performances.Add(metrics.Buffer);
                }
            }
            else
            {
                result.Performances = new List<PerformanceMetrics>(0);
            }

            return result;
        }

        public static IOMetricsEnvironment GetIoMetrics(StorageEnvironment storageEnvironment)
        {
            var ioMetrics = new IOMetricsEnvironment
            {
                Path = storageEnvironment.Options.BasePath.FullPath
            };

            foreach (var fileMetric in storageEnvironment.Options.IoMetrics.Files)
            {
                ioMetrics.Files.Add(GetFileMetrics(fileMetric));
            }

            return ioMetrics;
        }

        private static IOMetricsFileStats GetFileMetrics(Sparrow.Server.Meters.IoMetrics.FileIoMetrics fileMetric)
        {
            var fileMetrics = new IOMetricsFileStats
            {
                File = Path.GetFileName(fileMetric.FileName),
                Status = fileMetric.Closed ? FileStatus.Closed : FileStatus.InUse
            };

            foreach (var recentMetric in fileMetric.GetRecentMetrics())
            {
                fileMetrics.Recent.Add(GetIoMetricsRecentStats(recentMetric));
            }

            foreach (var historyMetric in fileMetric.GetSummaryMetrics())
            {
                fileMetrics.History.Add(new IOMetricsHistoryStats
                {
                    Start = historyMetric.TotalTimeStart.GetDefaultRavenFormat(true),
                    End = historyMetric.TotalTimeEnd.GetDefaultRavenFormat(true),
                    Size = historyMetric.TotalSize,
                    HumaneSize = Sizes.Humane(historyMetric.TotalSize),
                    FileSize = historyMetric.TotalFileSize,
                    HumaneFileSize = Sizes.Humane(historyMetric.TotalFileSize),
                    Duration = Math.Round((historyMetric.TotalTimeEnd - historyMetric.TotalTimeStart).TotalMilliseconds, 2),
                    ActiveDuration = Math.Round(historyMetric.TotalTime.TotalMilliseconds, 2),
                    MaxDuration = Math.Round(historyMetric.MaxTime.TotalMilliseconds, 2),
                    MinDuration = Math.Round(historyMetric.MinTime.TotalMilliseconds, 2),
                    MaxAcceleration = historyMetric.MaxAcceleration,
                    MinAcceleration = historyMetric.MinAcceleration,
                    CompressedSize = historyMetric.TotalCompressedSize,
                    HumaneCompressedSize = Sizes.Humane(historyMetric.TotalCompressedSize),
                    Type = historyMetric.Type
                });
            }

            return fileMetrics;
        }

        public static IOMetricsRecentStats GetIoMetricsRecentStats(IoMeterBuffer.MeterItem recentMetric)
        {
            return new IOMetricsRecentStats
            {
                Start = recentMetric.Start.GetDefaultRavenFormat(true),
                Size = recentMetric.Size,
                Acceleration = recentMetric.Acceleration,
                CompressedSize = recentMetric.CompressedSize,
                HumaneCompressedSize = Sizes.Humane(recentMetric.CompressedSize),
                HumaneSize = Sizes.Humane(recentMetric.Size),
                FileSize = recentMetric.FileSize,
                HumaneFileSize = Sizes.Humane(recentMetric.FileSize),
                Duration = Math.Round(recentMetric.Duration.TotalMilliseconds, 2),
                Type = recentMetric.Type
            };
        }

        public static void CleanIoMetrics(IEnumerable<StorageEnvironmentWithType> environments, long untilTicks)
        {
            foreach (var storageEnvironment in environments)
            {
                foreach (var fileMetric in storageEnvironment.Environment.Options.IoMetrics.Files)
                {
                    fileMetric.Compression.RemoveUntil(untilTicks);
                    fileMetric.DataFlush.RemoveUntil(untilTicks);
                    fileMetric.DataSync.RemoveUntil(untilTicks);
                    fileMetric.JournalWrite.RemoveUntil(untilTicks);
                    fileMetric.JournalWait.RemoveUntil(untilTicks);
                }
            }
        }
    }

    public class IOMetricsHistoryStats
    {
        public string Start { get; set; }
        public string End { get; set; }
        public long Size { get; set; }
        public string HumaneSize { get; set; }
        public long FileSize { get; set; }
        public string HumaneFileSize { get; set; }
        public double Duration { get; set; }
        public double ActiveDuration { get; set; }
        public double MaxDuration { get; set; }
        public double MinDuration { get; set; }
        public int MaxAcceleration { get; set; }
        public int MinAcceleration { get; set; }
        public long CompressedSize { get; set; }
        public string HumaneCompressedSize { get; set; }
        public Sparrow.Server.Meters.IoMetrics.MeterType Type { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Start)] = Start,
                [nameof(End)] = End,
                [nameof(Size)] = Size,
                [nameof(HumaneSize)] = HumaneSize,
                [nameof(FileSize)] = FileSize,
                [nameof(HumaneFileSize)] = HumaneFileSize,
                [nameof(Duration)] = Duration,
                [nameof(ActiveDuration)] = ActiveDuration,
                [nameof(MaxDuration)] = MaxDuration,
                [nameof(MinDuration)] = MinDuration,
                [nameof(MaxAcceleration)] = MaxAcceleration,
                [nameof(MinAcceleration)] = MinAcceleration,
                [nameof(CompressedSize)] = CompressedSize,
                [nameof(HumaneCompressedSize)] = HumaneCompressedSize,
                [nameof(Type)] = Type
            };
        }
    }

    public class IOMetricsRecentStatsAdditionalTypes : IOMetricsRecentStats
    {
        public long OriginalSize;
        public string HumaneOriginalSize;
        public double CompressionRatio;
    }

    public class IOMetricsRecentStats
    {
        public string Start { get; set; }
        public long Size { get; set; }
        public string HumaneSize { get; set; }

        public long CompressedSize { get; set; }
        public string HumaneCompressedSize { get; set; }

        public int Acceleration { get; set; }

        public long FileSize { get; set; }
        public string HumaneFileSize { get; set; }
        public double Duration { get; set; }
        public Sparrow.Server.Meters.IoMetrics.MeterType Type { get; set; }

        public DynamicJsonValue ToJson()
        {
            switch (Type)
            {
                case Sparrow.Server.Meters.IoMetrics.MeterType.Compression:
                    return new DynamicJsonValue
                    {
                        [nameof(Start)] = Start,
                        [nameof(IOMetricsRecentStatsAdditionalTypes.OriginalSize)] = Size,
                        [nameof(IOMetricsRecentStatsAdditionalTypes.HumaneOriginalSize)] = HumaneSize,
                        [nameof(CompressedSize)] = CompressedSize,
                        [nameof(HumaneCompressedSize)] = HumaneCompressedSize,
                        [nameof(Acceleration)] = Acceleration,
                        [nameof(IOMetricsRecentStatsAdditionalTypes.CompressionRatio)] = CompressedSize * 1.0 / Size,
                        [nameof(Duration)] = Duration,
                        [nameof(Type)] = Type
                    };
                default:
                    return new DynamicJsonValue
                    {
                        [nameof(Start)] = Start,
                        [nameof(Size)] = Size,
                        [nameof(HumaneSize)] = HumaneSize,
                        [nameof(FileSize)] = FileSize,
                        [nameof(HumaneFileSize)] = HumaneFileSize,
                        [nameof(Duration)] = Duration,
                        [nameof(Type)] = Type
                    };
            }
        }
    }

    public enum FileStatus
    {
        Closed,
        InUse
    }

    public class IOMetricsFileStats
    {
        public IOMetricsFileStats()
        {
            Recent = new List<IOMetricsRecentStats>();
            History = new List<IOMetricsHistoryStats>();
        }

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
        public IOMetricsEnvironment()
        {
            Files = new List<IOMetricsFileStats>();
        }

        public StorageEnvironmentWithType.StorageEnvironmentType Type { get; set; }
        public string Path { get; set; }
        public List<IOMetricsFileStats> Files { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Path)] = Path,
                [nameof(Type)] = Type,
                [nameof(Files)] = new DynamicJsonArray(Files.Select(x => x.ToJson()))
            };
        }
    }

    public class IOMetricsResponse
    {
        public IOMetricsResponse()
        {
            Environments = new List<IOMetricsEnvironment>();
            Performances = new List<PerformanceMetrics>();
        }

        public List<PerformanceMetrics> Performances { get; set; }
        public List<IOMetricsEnvironment> Environments { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Environments)] = new DynamicJsonArray(Environments.Select(x => x.ToJson())),
                [nameof(Performances)] = new DynamicJsonArray(Performances.Select(x => x.ToJson()))
            };
        }
    }
}
