﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Collections.LockFree;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Utils;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class MemoryStatsHandler : RequestHandler
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
                    ["PhysicalMem"] = Size.Humane(item.PhysicalMem),
                    ["Threshold"] = Size.Humane(item.LowMemThreshold)
                };

                var json = new DynamicJsonValue
                {
                    ["Event"] = item.Reason,
                    ["FreeMem"] = item.FreeMem,
                    ["CurrentCommitCharge"] = item.CurrentCommitCharge,
                    ["TotalUnmanaged"] = item.TotalUnmanaged,
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



        [RavenAction("/admin/debug/memory/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task MemoryStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = MemoryStatsInternal();

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }

                return Task.CompletedTask;
            }
        }

        private static DynamicJsonValue MemoryStatsInternal()
        {
            using (var currentProcess = Process.GetCurrentProcess())
            {
                var workingSet =
                    PlatformDetails.RunningOnPosix == false || PlatformDetails.RunningOnMacOsx
                        ? currentProcess.WorkingSet64
                        : MemoryInformation.GetRssMemoryUsage(currentProcess.Id);
                var memInfo = MemoryInformation.GetMemoryInfo();

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

                    value[mapping.Key] = mapping.Value;
                    foreach (var singleMapping in mapping.Value)
                    {
                        fileMappingSizesByDir.TryGetValue(dir, out long prevSize);
                        fileMappingSizesByDir[dir] = prevSize + singleMapping.Value;
                        totalMapping += singleMapping.Value;
                    }
                }

                var prefixLength = LongestCommonPrefixLength(new List<string>(fileMappingSizesByDir.Keys));

                var fileMappings = new DynamicJsonArray();
                foreach (var sizes in fileMappingSizesByDir.OrderByDescending(x => x.Value))
                {
                    if (fileMappingByDir.TryGetValue(sizes.Key, out Dictionary<string, ConcurrentDictionary<IntPtr, long>> value))
                    {
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
                                dja.Add(new DynamicJsonValue
                                {
                                    [nameof(MemoryInfoMappingDetails.Size)] = maps.Key,
                                    [nameof(MemoryInfoMappingDetails.Count)] = maps.Value
                                });
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
                        fileMappings.Add(dir);
                    }
                }

                long totalUnmanagedAllocations = 0;
                var threads = new DynamicJsonArray();
                foreach (var stats in NativeMemory.ThreadAllocations.Values
                    .Where(x => x.ThreadInstance.IsAlive)
                    .GroupBy(x => x.Name)
                    .OrderByDescending(x => x.Sum(y => y.TotalAllocated)))
                {
                    var unmanagedAllocations = stats.Sum(x => x.TotalAllocated);
                    totalUnmanagedAllocations += unmanagedAllocations;
                    var ids = new DynamicJsonArray(stats.OrderByDescending(x => x.TotalAllocated).Select(x => new DynamicJsonValue
                    {
                        ["Id"] = x.UnmanagedThreadId,
                        ["ManagedThreadId"] = x.Id,
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
                        groupStats["ManagedThreadId"] = threadStats.Id;
                    }
                    else
                    {
                        groupStats["Ids"] = ids;
                    }

                    threads.Add(groupStats);
                }

                var managedMemory = GC.GetTotalMemory(false);
                var djv = new DynamicJsonValue
                {
                    [nameof(MemoryInfo.WorkingSet)] = workingSet,
                    [nameof(MemoryInfo.TotalUnmanagedAllocations)] = totalUnmanagedAllocations,
                    [nameof(MemoryInfo.ManagedAllocations)] = managedMemory,
                    [nameof(MemoryInfo.TotalMemoryMapped)] = totalMapping,
                    [nameof(MemoryInfo.PhysicalMem)] = Size.Humane(memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.FreeMem)] = Size.Humane(memInfo.AvailableMemory.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.HighMemLastOneMinute)] = Size.Humane(memInfo.MemoryUsageRecords.High.LastOneMinute.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.LowMemLastOneMinute)] = Size.Humane(memInfo.MemoryUsageRecords.Low.LastOneMinute.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.HighMemLastFiveMinute)] = Size.Humane(memInfo.MemoryUsageRecords.High.LastFiveMinutes.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.LowMemLastFiveMinute)] = Size.Humane(memInfo.MemoryUsageRecords.Low.LastFiveMinutes.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.HighMemSinceStartup)] = Size.Humane(memInfo.MemoryUsageRecords.High.SinceStartup.GetValue(SizeUnit.Bytes)),
                    [nameof(MemoryInfo.LowMemSinceStartup)] = Size.Humane(memInfo.MemoryUsageRecords.Low.SinceStartup.GetValue(SizeUnit.Bytes)),
                    
                    [nameof(MemoryInfo.Humane)] = new DynamicJsonValue
                    {
                        [nameof(MemoryInfoHumane.WorkingSet)] = Size.Humane(workingSet),
                        [nameof(MemoryInfoHumane.TotalUnmanagedAllocations)] = Size.Humane(totalUnmanagedAllocations),
                        [nameof(MemoryInfoHumane.ManagedAllocations)] = Size.Humane(managedMemory),
                        [nameof(MemoryInfoHumane.TotalMemoryMapped)] = Size.Humane(totalMapping)
                    },
                    
                    ["Threads"] = threads,

                    [nameof(MemoryInfo.Mappings)] = fileMappings
                };
                return djv;
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
            public long WorkingSet { get; set; }
            public long TotalUnmanagedAllocations { get; set; }
            public long ManagedAllocations { get; set; }
            public long TotalMemoryMapped { get; set; }
            public string PhysicalMem { get; set; }
            public string FreeMem { get; set; }
            public string HighMemLastOneMinute { get; set; }
            public string LowMemLastOneMinute { get; set; }
            public string HighMemLastFiveMinute { get; set; }
            public string LowMemLastFiveMinute { get; set; }
            public string HighMemSinceStartup { get; set; }
            public string LowMemSinceStartup { get; set; }
            public MemoryInfoHumane Humane { get; set; }

            public MemoryInfoMappingItem[] Mappings { get; set; }
            //TODO: threads

        }

        internal class MemoryInfoHumane
        {
            public string WorkingSet { get; set; }
            public string TotalUnmanagedAllocations { get; set; }
            public string ManagedAllocations { get; set; }
            public string TotalMemoryMapped { get; set; }
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
        
        [RavenAction("/admin/debug/proc/meminfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task ProcMemInfo()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(write, new DynamicJsonValue()
                        {
                            ["ERROR"] = "End point doesn't support non linux OS",
                        });
                    }
                }
                return Task.CompletedTask;
            }
            
            var txt = File.ReadAllLines("/proc/meminfo");
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = new DynamicJsonValue();
                var djvHumanly = new DynamicJsonValue();
                foreach (var line in txt)
                {
                    var parsedLine = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);

                    if (parsedLine.Length != 3) // format should be: {filter}: <num> kb
                        continue; // an error but we ignore and do not put in the results

                    var key = parsedLine[0].Split(":")[0];

                    if (long.TryParse(parsedLine[1], out var result) == false)
                    {
                        djv[key] = "Failed To Parse Value : " + parsedLine[1];
                        continue;
                    }

                    switch (parsedLine[2].ToLowerInvariant())
                    {
                        case "kb":
                            result *= 1024L;
                            break;
                        case "mb":
                            result *= 1024L * 1024;
                            break;
                        case "gb":
                            result *= 1024L * 1024 * 1024;
                            break;
                    }

                    djv[key] = result;
                    djvHumanly[key] = Size.Humane(result);
                }

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, new DynamicJsonValue()
                    {
                        ["Humanly"] = djvHumanly,
                        ["Values"] = djv
                    });
                }

                return Task.CompletedTask;
            }
        }


        [RavenAction("/admin/debug/proc/self/status", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task ProcSelfStatus()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(write, new DynamicJsonValue()
                        {
                            ["ERROR"] = "End point doesn't support non linux OS",
                        });
                    }
                }
                return Task.CompletedTask;
            }
            
            using (var currentProcess = Process.GetCurrentProcess())
            {
                var path = $"/proc/{currentProcess.Id}/status";
                var txt = File.ReadAllLines(path);
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var djv = new DynamicJsonValue();
                    var djvHumanly = new DynamicJsonValue();
                    foreach (var line in txt)
                    {
                        var parsedLine = line.Split(new[] {' ', '\t'}, StringSplitOptions.RemoveEmptyEntries);

                        if (parsedLine.Length < 2) // format may vary
                            continue; // an error but we ignore and do not put in the results

                        var key = parsedLine[0].Split(":")[0];

                        if (parsedLine.Length == 3 && 
                            ( parsedLine[2].Equals("kB", StringComparison.InvariantCultureIgnoreCase) ||
                              parsedLine[2].Equals("mB", StringComparison.InvariantCultureIgnoreCase) ||
                              parsedLine[2].Equals("gB", StringComparison.InvariantCultureIgnoreCase)))
                        {
                            if (long.TryParse(parsedLine[1], out var result) == false)
                            {
                                djv[key] = "Failed To Parse Value : " + parsedLine[1];
                                continue;
                            }


                            switch (parsedLine[2].ToLowerInvariant())
                            {
                                case "kb":
                                    result *= 1024L;
                                    break;
                                case "mb":
                                    result *= 1024L * 1024;
                                    break;
                                case "gb":
                                    result *= 1024L * 1024 * 1024;
                                    break;
                            }
                            djv[key] = result;
                            djvHumanly[key] = Size.Humane(result);
                        }
                        else
                        {
                            var restOfTheLine = new StringBuilder(parsedLine[1]);
                            for (int i = 2; i < parsedLine.Length; i++)
                            {
                                restOfTheLine.Append(" ");
                                restOfTheLine.Append(parsedLine[i]);
                            }
                            djv[key] = restOfTheLine.ToString();
                        }
                    }

                    using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(write, new DynamicJsonValue()
                        {
                            ["Humanly"] = djvHumanly,
                            ["Values"] = djv
                        });
                    }

                    return Task.CompletedTask;
                }
            }
        }
    }
}
