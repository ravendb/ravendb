using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using Size = Raven.Client.Util.Size;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class MemoryDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/memory/low-mem-log", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task LowMemLog()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = LowMemLogInternal();

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/debug/proc/status", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true, IsPosixSpecificEndpoint = true)]
        public async Task PosixMemStatus()
        {
            await WriteFile("/proc/self/status");
        }

        [RavenAction("/admin/debug/proc/meminfo", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true, IsPosixSpecificEndpoint = true)]
        public async Task PosixMemInfo()
        {
            await WriteFile("/proc/meminfo");
        }

        private async Task WriteFile(string file)
        {
            HttpContext.Response.ContentType = "text/plain";
            using (var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                await fileStream.CopyToAsync(ResponseBodyStream());
            }
        }

        public static DynamicJsonValue LowMemLogInternal()
        {
            var lowMemLog = LowMemoryNotification.Instance.LowMemEventDetailsStack;
            var dja = new DynamicJsonArray();

            foreach (var item in lowMemLog.OrderByDescending(x =>
            {
                if (x != null)
                    return x.Time;
                return DateTime.MinValue;
            }))
            {
                if (item == null || item.Reason == LowMemoryNotification.LowMemReason.None)
                    continue;

                var humanSizes = new DynamicJsonValue
                {
                    ["FreeMem"] = Size.Humane(item.FreeMem),
                    ["CurrentCommitCharge"] = Size.Humane(item.CurrentCommitCharge),
                    ["TotalUnmanaged"] = Size.Humane(item.TotalUnmanaged),
                    ["TotalScratchDirty"] = Size.Humane(item.TotalScratchDirty),
                    ["PhysicalMem"] = Size.Humane(item.PhysicalMem),
                    ["Threshold"] = Size.Humane(item.LowMemThreshold)
                };

                var json = new DynamicJsonValue
                {
                    ["Event"] = item.Reason,
                    ["FreeMem"] = item.FreeMem,
                    ["CurrentCommitCharge"] = item.CurrentCommitCharge,
                    ["TotalUnmanaged"] = item.TotalUnmanaged,
                    ["TotalScratchDirty"] = item.TotalScratchDirty,
                    ["PhysicalMem"] = item.PhysicalMem,
                    ["TimeOfEvent"] = item.Time,
                    ["HumanlyReadSizes"] = humanSizes
                };

                dja.Add(json);
            }

            var djv = new DynamicJsonValue
            {
                ["Low Memory Events"] = dja
            };

            return djv;
        }

        [RavenAction("/admin/debug/memory/smaps", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task MemorySmaps()
        {
            if (PlatformDetails.RunningOnLinux == false)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var process = Process.GetCurrentProcess())
                {
                    var sharedClean = MemoryInformation.GetSharedCleanInBytes(process);
                    var rc = Win32MemoryQueryMethods.GetMaps();
                    var djv = new DynamicJsonValue
                    {
                        ["Totals"] = new DynamicJsonValue
                        {
                            ["WorkingSet"] = process.WorkingSet64,
                            ["SharedClean"] = Sizes.Humane(sharedClean),
                            ["PrivateClean"] = "N/A",
                            ["TotalClean"] = rc.ProcessClean,
                            ["RssHumanly"] = Sizes.Humane(process.WorkingSet64),
                            ["SharedCleanHumanly"] = Sizes.Humane(sharedClean),
                            ["PrivateCleanHumanly"] = "N/A",
                            ["TotalCleanHumanly"] = Sizes.Humane(rc.ProcessClean)
                        },
                        ["Details"] = rc.Json
                    };
                    using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(write, djv);
                    }
                    return Task.CompletedTask;
                }
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var buffers = new[]
                {
                    ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize),
                    ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize)
                };
                try
                {
                    var result = new SmapsReader(buffers).CalculateMemUsageFromSmaps<SmapsReaderJsonResults>();
                    var procStatus = MemoryInformation.GetMemoryUsageFromProcStatus();
                    var djv = new DynamicJsonValue
                    {
                        ["Totals"] = new DynamicJsonValue
                        {
                            ["WorkingSet"] = result.Rss,
                            ["SharedClean"] = result.SharedClean,
                            ["PrivateClean"] = result.PrivateClean,
                            ["TotalClean"] = result.SharedClean + result.PrivateClean,
                            ["TotalDirty"] = result.TotalDirty, // This includes not only r-ws buffer and voron files, but also dotnet's and heap dirty memory
                            ["WorkingSetSwap"] = result.Swap, // Swap values sum for r-ws entries only
                            ["Swap"] = procStatus.Swap,
                            ["RssHumanly"] = Sizes.Humane(result.Rss),
                            ["SwapHumanly"] = Sizes.Humane(procStatus.Swap),
                            ["WorkingSetSwapHumanly"] = Sizes.Humane(result.Swap),
                            ["SharedCleanHumanly"] = Sizes.Humane(result.SharedClean),
                            ["PrivateCleanHumanly"] = Sizes.Humane(result.PrivateClean),
                            ["TotalCleanHumanly"] = Sizes.Humane(result.SharedClean + result.PrivateClean)
                        },
                        ["Details"] = result.SmapsResults.ReturnResults()
                    };

                    using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(write, djv);
                    }

                    return Task.CompletedTask;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffers[0]);
                    ArrayPool<byte>.Shared.Return(buffers[1]);
                }
            }
        }

        [RavenAction("/admin/debug/memory/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task MemoryStats()
        {
            var includeThreads = GetBoolValueQueryString("includeThreads", required: false) ?? true;
            var includeMappings = GetBoolValueQueryString("includeMappings", required: false) ?? true;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    WriteMemoryStats(writer, context, includeThreads, includeMappings);
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/debug/memory/encryption-buffer-pool", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task EncryptionBufferPoolStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, EncryptionBuffersPool.Instance.GetStats().ToJson());
                }

                return Task.CompletedTask;
            }
        }

        private static void WriteMemoryStats(BlittableJsonTextWriter writer, JsonOperationContext context, bool includeThreads, bool includeMappings)
        {
            writer.WriteStartObject();

            var memInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
            long managedMemoryInBytes = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();
            long totalUnmanagedAllocations = NativeMemory.TotalAllocatedMemory;
            var encryptionBuffers = EncryptionBuffersPool.Instance.GetStats();
            var dirtyMemoryState = MemoryInformation.GetDirtyMemoryState();
            var memoryUsageRecords = MemoryInformation.GetMemoryUsageRecords();

            long totalMapping = 0;
            var fileMappingByDir = new Dictionary<string, Dictionary<string, ConcurrentDictionary<IntPtr, long>>>();
            var fileMappingSizesByDir = new Dictionary<string, long>();
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var dir = Path.GetDirectoryName(mapping.Key);

                if (fileMappingByDir.TryGetValue(dir, out Dictionary<string, ConcurrentDictionary<IntPtr, long>> value) == false)
                {
                    value = new Dictionary<string, ConcurrentDictionary<IntPtr, long>>();
                    fileMappingByDir[dir] = value;
                }

                value[mapping.Key] = mapping.Value.Value.Info;
                foreach (var singleMapping in mapping.Value.Value.Info)
                {
                    fileMappingSizesByDir.TryGetValue(dir, out long prevSize);
                    fileMappingSizesByDir[dir] = prevSize + singleMapping.Value;
                    totalMapping += singleMapping.Value;
                }
            }

            var djv = new DynamicJsonValue
            {
                [nameof(MemoryInfo.PhysicalMemory)] = memInfo.TotalPhysicalMemory.ToString(),
                [nameof(MemoryInfo.WorkingSet)] = memInfo.WorkingSet.ToString(),
                [nameof(MemoryInfo.ManagedAllocations)] = Size.Humane(managedMemoryInBytes),
                [nameof(MemoryInfo.UnmanagedAllocations)] = Size.Humane(totalUnmanagedAllocations),
                [nameof(MemoryInfo.EncryptionBuffersInUse)] = Size.Humane(encryptionBuffers.CurrentlyInUseSize),
                [nameof(MemoryInfo.EncryptionBuffersPool)] = Size.Humane(encryptionBuffers.TotalPoolSize),
                [nameof(MemoryInfo.MemoryMapped)] = Size.Humane(totalMapping),
                [nameof(MemoryInfo.ScratchDirtyMemory)] = memInfo.TotalScratchDirtyMemory.ToString(),
                [nameof(MemoryInfo.IsHighDirty)] = dirtyMemoryState.IsHighDirty,
                [nameof(MemoryInfo.DirtyMemory)] = Size.Humane(dirtyMemoryState.TotalDirtyInBytes),
                [nameof(MemoryInfo.AvailableMemory)] = Size.Humane(memInfo.AvailableMemory.GetValue(SizeUnit.Bytes)),
                [nameof(MemoryInfo.AvailableMemoryForProcessing)] = memInfo.AvailableMemoryForProcessing.ToString(),
                [nameof(MemoryInfo.HighMemLastOneMinute)] = memoryUsageRecords.High.LastOneMinute.ToString(),
                [nameof(MemoryInfo.LowMemLastOneMinute)] = memoryUsageRecords.Low.LastOneMinute.ToString(),
                [nameof(MemoryInfo.HighMemLastFiveMinute)] = memoryUsageRecords.High.LastFiveMinutes.ToString(),
                [nameof(MemoryInfo.LowMemLastFiveMinute)] = memoryUsageRecords.Low.LastFiveMinutes.ToString(),
                [nameof(MemoryInfo.HighMemSinceStartup)] = memoryUsageRecords.High.SinceStartup.ToString(),
                [nameof(MemoryInfo.LowMemSinceStartup)] = memoryUsageRecords.Low.SinceStartup.ToString(),
            };

            writer.WritePropertyName(nameof(MemoryInformation));
            context.Write(writer, djv);

            writer.WriteComma();
            writer.WritePropertyName("Threads");
            writer.WriteStartArray();
            WriteThreads(includeThreads, writer, context);
            writer.WriteEndArray();

            writer.WriteComma();
            writer.WritePropertyName(nameof(MemoryInfo.Mappings));
            writer.WriteStartArray();
            WriteMappings(includeMappings, writer, context, fileMappingSizesByDir, fileMappingByDir);
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        private static void WriteThreads(bool includeThreads, BlittableJsonTextWriter writer, JsonOperationContext context)
        {
            if (includeThreads == false)
                return;

            var isFirst = true;
            foreach (var stats in NativeMemory.AllThreadStats
                .Where(x => x.IsThreadAlive())
                .GroupBy(x => x.Name)
                .OrderByDescending(x => x.Sum(y => y.TotalAllocated)))
            {
                var unmanagedAllocations = stats.Sum(x => x.TotalAllocated);
                var ids = new DynamicJsonArray(stats.OrderByDescending(x => x.TotalAllocated).Select(x => new DynamicJsonValue
                {
                    ["Id"] = x.UnmanagedThreadId,
                    ["ManagedThreadId"] = x.ManagedThreadId,
                    ["Allocations"] = x.TotalAllocated,
                    ["HumaneAllocations"] = Size.Humane(x.TotalAllocated)
                }));
                var groupStats = new DynamicJsonValue
                {
                    ["Name"] = stats.Key,
                    ["Allocations"] = unmanagedAllocations,
                    ["HumaneAllocations"] = Size.Humane(unmanagedAllocations)
                };
                if (ids.Count == 1)
                {
                    var threadStats = stats.First();
                    groupStats["Id"] = threadStats.UnmanagedThreadId;
                    groupStats["ManagedThreadId"] = threadStats.ManagedThreadId;
                }
                else
                {
                    groupStats["Ids"] = ids;
                }

                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;

                context.Write(writer, groupStats);
            }
        }

        private static void WriteMappings(bool includeMappings, BlittableJsonTextWriter writer, JsonOperationContext context,
            Dictionary<string, long> fileMappingSizesByDir, Dictionary<string, Dictionary<string, ConcurrentDictionary<IntPtr, long>>> fileMappingByDir)
        {
            if (includeMappings == false)
                return;

            bool isFirst = true;
            var prefixLength = LongestCommonPrefixLength(new List<string>(fileMappingSizesByDir.Keys));
            foreach (var sizes in fileMappingSizesByDir.OrderByDescending(x => x.Value))
            {
                if (fileMappingByDir.TryGetValue(sizes.Key, out Dictionary<string, ConcurrentDictionary<IntPtr, long>> value) == false)
                    continue;

                var details = new DynamicJsonValue();

                var dir = new DynamicJsonValue
                {
                    [nameof(MemoryInfoMappingItem.Directory)] = sizes.Key.Substring(prefixLength),
                    [nameof(MemoryInfoMappingItem.TotalDirectorySize)] = sizes.Value,
                    [nameof(MemoryInfoMappingItem.HumaneTotalDirectorySize)] = Size.Humane(sizes.Value),
                    [nameof(MemoryInfoMappingItem.Details)] = details
                };
                foreach (var file in value.OrderBy(x => x.Key))
                {
                    long totalMapped = 0;
                    var dja = new DynamicJsonArray();
                    var dic = new Dictionary<long, long>();
                    foreach (var mapping in file.Value)
                    {
                        totalMapped += mapping.Value;
                        dic.TryGetValue(mapping.Value, out long prev);
                        dic[mapping.Value] = prev + 1;
                    }

                    foreach (var maps in dic)
                    {
                        dja.Add(new DynamicJsonValue {[nameof(MemoryInfoMappingDetails.Size)] = maps.Key, [nameof(MemoryInfoMappingDetails.Count)] = maps.Value});
                    }

                    var fileSize = GetFileSize(file.Key);
                    details[Path.GetFileName(file.Key)] = new DynamicJsonValue
                    {
                        [nameof(MemoryInfoMappingFileInfo.FileSize)] = fileSize,
                        [nameof(MemoryInfoMappingFileInfo.HumaneFileSize)] = Size.Humane(fileSize),
                        [nameof(MemoryInfoMappingFileInfo.TotalMapped)] = totalMapped,
                        [nameof(MemoryInfoMappingFileInfo.HumaneTotalMapped)] = Size.Humane(totalMapped),
                        [nameof(MemoryInfoMappingFileInfo.Mappings)] = dja
                    };
                }

                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;

                context.Write(writer, dir);
            }
        }

        private static long GetFileSize(string file)
        {
            var fileInfo = new FileInfo(file);
            if (fileInfo.Exists == false)
                return -1;
            try
            {
                return fileInfo.Length;
            }
            catch (FileNotFoundException)
            {
                return -1;
            }
        }

        public static int LongestCommonPrefixLength(List<string> strings)
        {
            if (strings.Count == 0)
                return 0;

            strings = strings
                .OrderBy(x => x.Length)
                .ToList();

            var maxLength = strings.Last().Length;
            var shortestString = strings.First();

            var prefixLength = 0;
            foreach (var s in strings)
            {
                if (s == shortestString)
                    continue;

                if (shortestString[prefixLength] != s[prefixLength])
                    prefixLength = 0;

                for (var i = prefixLength; i < shortestString.Length; i++)
                {
                    var shortChar = shortestString[i];
                    var c = s[i];

                    if (shortChar != c)
                    {
                        if (prefixLength == maxLength)
                            return 0;

                        return prefixLength;
                    }

                    prefixLength = i;
                }
            }

            if (prefixLength == maxLength)
                return 0;

            return prefixLength;
        }


        internal class MemoryInfo
        {
            public string PhysicalMemory { get; set; }
            public string WorkingSet { get; set; }
            public string ManagedAllocations { get; set; }
            public string UnmanagedAllocations { get; set; }
            public string EncryptionBuffersInUse { get; set; }
            public string EncryptionBuffersPool { get; set; }
            public string MemoryMapped { get; set; }
            public string ScratchDirtyMemory { get; set; }
            public bool IsHighDirty { get; set; }
            public string DirtyMemory { get; set; }
            public string AvailableMemory { get; set; }
            public string AvailableMemoryForProcessing { get; set; }
            public string HighMemLastOneMinute { get; set; }
            public string LowMemLastOneMinute { get; set; }
            public string HighMemLastFiveMinute { get; set; }
            public string LowMemLastFiveMinute { get; set; }
            public string HighMemSinceStartup { get; set; }
            public string LowMemSinceStartup { get; set; }
            public MemoryInfoMappingItem[] Mappings { get; set; }
        }

        internal class MemoryInfoMappingItem
        {
            public string Directory { get; set; }
            public long TotalDirectorySize { get; set; }
            public string HumaneTotalDirectorySize { get; set; }
            public Dictionary<string, MemoryInfoMappingFileInfo> Details { get; set; }
        }

        internal class MemoryInfoMappingFileInfo
        {
            public long FileSize { get; set; }
            public string HumaneFileSize { get; set; }
            public long TotalMapped { get; set; }
            public string HumaneTotalMapped { get; set; }
            public MemoryInfoMappingDetails[] Mappings { get; set; }
        }

        internal class MemoryInfoMappingDetails
        {
            public long Size { get; set; }
            public long Count { get; set; }
        }
    }
}
