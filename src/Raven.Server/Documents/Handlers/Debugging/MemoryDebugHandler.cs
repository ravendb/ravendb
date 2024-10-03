﻿using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using Voron.Impl;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class MemoryDebugHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/memory/gc", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GcInfo()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(GCKind.Any)] = ToJson(GC.GetGCMemoryInfo(GCKind.Any)),
                    [nameof(GCKind.Background)] = ToJson(GC.GetGCMemoryInfo(GCKind.Background)),
                    [nameof(GCKind.Ephemeral)] = ToJson(GC.GetGCMemoryInfo(GCKind.Ephemeral)),
                    [nameof(GCKind.FullBlocking)] = ToJson(GC.GetGCMemoryInfo(GCKind.FullBlocking)),
                };

                await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
            }

            static DynamicJsonValue ToJson(GCMemoryInfo info)
            {
                return new DynamicJsonValue
                {
                    [nameof(info.Compacted)] = info.Compacted,
                    [nameof(info.Concurrent)] = info.Concurrent,
                    [nameof(info.FinalizationPendingCount)] = info.FinalizationPendingCount,
                    [nameof(info.FragmentedBytes)] = info.FragmentedBytes,
                    ["FragmentedHumane"] = Size.Humane(info.FragmentedBytes),
                    [nameof(info.Generation)] = info.Generation,
                    [nameof(info.GenerationInfo)] = new DynamicJsonArray(
                        info.GenerationInfo.ToArray().Select((x, index) => new DynamicJsonValue
                        {
                            ["GenerationName"] = index switch
                                {
                                    0 => "Heap Generation 0",
                                    1 => "Heap Generation 1",
                                    2 => "Heap Generation 2",
                                    3 => "Large Object Heap",
                                    4 => "Pinned Object Heap",
                                    _ => "Unknown Generation"
                                },
                            [nameof(x.FragmentationAfterBytes)] = x.FragmentationAfterBytes,
                            ["FragmentationAfterHumane"] = Size.Humane(x.FragmentationAfterBytes),
                            [nameof(x.FragmentationBeforeBytes)] = x.FragmentationBeforeBytes,
                            ["FragmentationBeforeHumane"] = Size.Humane(x.FragmentationBeforeBytes),
                            [nameof(x.SizeAfterBytes)] = x.SizeAfterBytes,
                            ["SizeAfterHumane"] = Size.Humane(x.SizeAfterBytes),
                            [nameof(x.SizeBeforeBytes)] = x.SizeBeforeBytes,
                            ["SizeBeforeHumane"] = Size.Humane(x.SizeBeforeBytes)
                        })),
                    [nameof(info.HeapSizeBytes)] = info.HeapSizeBytes,
                    ["HeapSizeHumane"] = Size.Humane(info.HeapSizeBytes),
                    [nameof(info.HighMemoryLoadThresholdBytes)] = info.HighMemoryLoadThresholdBytes,
                    ["HighMemoryLoadThresholdHumane"] = Size.Humane(info.HighMemoryLoadThresholdBytes),
                    [nameof(info.Index)] = info.Index,
                    [nameof(info.MemoryLoadBytes)] = info.MemoryLoadBytes,
                    ["MemoryLoadHumane"] = Size.Humane(info.MemoryLoadBytes),
                    [nameof(info.PauseDurations)] = new DynamicJsonArray(info.PauseDurations.ToArray().Cast<object>()),
                    [nameof(info.PauseTimePercentage)] = info.PauseTimePercentage,
                    [nameof(info.PinnedObjectsCount)] = info.PinnedObjectsCount,
                    [nameof(info.PromotedBytes)] = info.PromotedBytes,
                    ["PromotedHumane"] = Size.Humane(info.PromotedBytes),
                    [nameof(info.TotalAvailableMemoryBytes)] = info.TotalAvailableMemoryBytes,
                    ["TotalAvailableMemoryHumane"] = Size.Humane(info.TotalAvailableMemoryBytes),
                    [nameof(info.TotalCommittedBytes)] = info.TotalCommittedBytes,
                    ["TotalCommittedHumane"] = Size.Humane(info.TotalCommittedBytes)
                };
            }
        }

        [RavenAction("/admin/debug/memory/low-mem-log", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task LowMemLog()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = LowMemLogInternal();

                await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
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
            await using (var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read))
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
        public async Task MemorySmaps()
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

                    await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                    {
                        context.Write(write, djv);
                    }

                    return;
                }
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var buffers = new[]
                {
                    ArrayPool<byte>.Shared.Rent(SmapsFactory.BufferSize),
                    ArrayPool<byte>.Shared.Rent(SmapsFactory.BufferSize)
                };
                try
                {
                    var result = SmapsFactory.CreateSmapsReader(buffers).CalculateMemUsageFromSmaps<SmapsReaderJsonResults>();
                    var procStatus = MemoryInformation.GetMemoryUsageFromProcStatus();
                    var djv = new DynamicJsonValue
                    {
                        ["Type"] = SmapsFactory.DefaultSmapsReaderType,
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

                    await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                    {
                        context.Write(write, djv);
                    }

                    return;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffers[0]);
                    ArrayPool<byte>.Shared.Return(buffers[1]);
                }
            }
        }

        [RavenAction("/admin/debug/memory/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task MemoryStats()
        {
            var includeThreads = GetBoolValueQueryString("includeThreads", required: false) ?? true;
            var includeMappings = GetBoolValueQueryString("includeMappings", required: false) ?? true;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    WriteMemoryStats(writer, context, includeThreads, includeMappings);
                }
            }
        }

        [RavenAction("/admin/debug/memory/encryption-buffer-pool", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task EncryptionBufferPoolStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    context.Write(write, EncryptionBuffersPool.Instance.GetStats().ToJson());
                }
            }
        }

        private static void WriteMemoryStats(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, bool includeThreads, bool includeMappings)
        {
            writer.WriteStartObject();

            var memInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
            long managedMemoryInBytes = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();
            long totalUnmanagedAllocations = NativeMemory.TotalAllocatedMemory;
            var encryptionBuffers = EncryptionBuffersPool.Instance.GetStats();
            var dirtyMemoryState = MemoryInformation.GetDirtyMemoryState();

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
                [nameof(MemoryInfo.LuceneManagedAllocationsForTermCache)] = Size.Humane(NativeMemory.TotalLuceneManagedAllocationsForTermCache),
                [nameof(MemoryInfo.LuceneUnmanagedAllocationsForSorting)] = Size.Humane(NativeMemory.TotalLuceneUnmanagedAllocationsForSorting),
                [nameof(MemoryInfo.EncryptionBuffersInUse)] = Size.Humane(encryptionBuffers.CurrentlyInUseSize),
                [nameof(MemoryInfo.EncryptionBuffersPool)] = Size.Humane(encryptionBuffers.TotalPoolSize),
                [nameof(MemoryInfo.EncryptionLockedMemory)] = Size.Humane(Sodium.LockedBytes),
                [nameof(MemoryInfo.MemoryMapped)] = Size.Humane(totalMapping),
                [nameof(MemoryInfo.IsHighDirty)] = dirtyMemoryState.IsHighDirty,
                [nameof(MemoryInfo.DirtyMemory)] = dirtyMemoryState.TotalDirty.ToString(),
                [nameof(MemoryInfo.AvailableMemory)] = memInfo.AvailableMemory.ToString(),
                [nameof(MemoryInfo.AvailableMemoryForProcessing)] = memInfo.AvailableMemoryForProcessing.ToString(),
            };
            if (memInfo.Remarks != null)
            {
                djv[nameof(MemoryInfo.Remarks)] = memInfo.Remarks;
            }

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

        private static void WriteThreads(bool includeThreads, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
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

        private static void WriteMappings(bool includeMappings, AsyncBlittableJsonTextWriter writer, JsonOperationContext context,
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
                        dja.Add(new DynamicJsonValue { [nameof(MemoryInfoMappingDetails.Size)] = maps.Key, [nameof(MemoryInfoMappingDetails.Count)] = maps.Value });
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
            
            public string Remarks { get; set; }
            public string ManagedAllocations { get; set; }
            public string UnmanagedAllocations { get; set; }
            public string LuceneManagedAllocationsForTermCache { get; set; }
            public string LuceneUnmanagedAllocationsForSorting { get; set; }
            public string EncryptionBuffersInUse { get; set; }
            public string EncryptionBuffersPool { get; set; }
            public string EncryptionLockedMemory { get; set; }
            public string MemoryMapped { get; set; }
            public string ScratchDirtyMemory { get; set; }
            public bool IsHighDirty { get; set; }
            public string DirtyMemory { get; set; }
            public string AvailableMemory { get; set; }
            public string AvailableMemoryForProcessing { get; set; }
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
