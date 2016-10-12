using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Client.Linq;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using ThreadState = System.Threading.ThreadState;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class MemoryStatsHandler : RequestHandler
    {
        [RavenAction("/debug/memory/stats", "GET")]
        public Task MemoryStats()
        {
            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                //TODO: When https://github.com/dotnet/corefx/issues/10157 is done, add managed 
                //TODO: allocations per thread to the stats as well

                var currentProcess = Process.GetCurrentProcess();
                var workingSet = currentProcess.WorkingSet64;
                long totalUnmanagedAllocations = 0;
                long totalMapping = 0;
                var fileMappingByDir = new Dictionary<string, Dictionary<string, ConcurrentDictionary<IntPtr, long>>>();
                var fileMappingSizesByDir = new Dictionary<string, long>();
                foreach (var mapping in NativeMemory.FileMapping)
                {

                    var dir = Path.GetDirectoryName(mapping.Key);
                    Dictionary<string, ConcurrentDictionary<IntPtr, long>> value;
                    if (fileMappingByDir.TryGetValue(dir, out value) == false)
                    {
                        value = new Dictionary<string, ConcurrentDictionary<IntPtr, long>>();
                        fileMappingByDir[dir] = value;
                    }
                    value[mapping.Key] = mapping.Value;
                    foreach (var singleMapping in mapping.Value)
                    {
                        long prevSize;
                        fileMappingSizesByDir.TryGetValue(dir, out prevSize);
                        fileMappingSizesByDir[dir] = prevSize + singleMapping.Value;
                        totalMapping += singleMapping.Value;

                    }
                }

                var prefixLength = LongestCommonPrefixLen(new List<string>(fileMappingSizesByDir.Keys));

                var fileMappings = new DynamicJsonArray();
                foreach (var sizes in fileMappingSizesByDir.OrderByDescending(x => x.Value))
                {
                    Dictionary<string, ConcurrentDictionary<IntPtr, long>> value;
                    if (fileMappingByDir.TryGetValue(sizes.Key, out value))
                    {
                        var dir = new DynamicJsonValue
                        {
                            ["Directory"] = sizes.Key.Substring(prefixLength),
                            ["TotalDirectorySize"] = new DynamicJsonValue
                            {
                                ["Mapped"] = sizes.Value,
                                ["HumaneMapped"] = FileHeader.Humane(sizes.Value)
                            }
                        };
                        foreach (var file in value.OrderBy(x => x.Key))
                        {
                            long totalMapped = 0;
                            var dja = new DynamicJsonArray();
                            foreach (var mapping in file.Value)
                            {
                                totalMapped += mapping.Value;
                                dja.Add(new DynamicJsonValue
                                {
                                    ["Address"] = "0x" + mapping.Key.ToString("x"),
                                    ["Size"] = mapping.Value
                                });
                            }
                            dir[Path.GetFileName(file.Key)] = new DynamicJsonValue
                            {
                                ["FileSize"] = GetFileSize(file.Key),
                                ["TotalMapped"] = totalMapped,
                                ["HumaneTotalMapped"] = FileHeader.Humane(totalMapped),
                                ["Mappings"] = dja
                            };
                        }
                        fileMappings.Add(dir);
                    }
                }

                var threads = new DynamicJsonArray();
                foreach (var stats in NativeMemory.ThreadAllocations.Values
                    .Where(x => x.ThreadInstance.IsAlive)
                    .GroupBy(x => x.Name))
                {
                    var unmanagedAllocations = stats.Sum(x => x.Allocations);
                    totalUnmanagedAllocations += unmanagedAllocations;
                    var ids = new DynamicJsonArray(stats.Select(x => new DynamicJsonValue
                    {
                        ["Id"] = x.Id,
                        ["Allocations"] = x.Allocations,
                        ["HumaneAllocations"] = FileHeader.Humane(x.Allocations)
                    }));
                    var groupStats = new DynamicJsonValue
                    {
                        ["Name"] = stats.Key,
                        ["Allocations"] = unmanagedAllocations,
                        ["HumaneAllocations"] = FileHeader.Humane(unmanagedAllocations)
                    };
                    if (ids.Count == 1)
                    {
                        groupStats["Id"] = stats.First().Id;
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
                    ["WorkingSet"] = workingSet,
                    ["TotalUnmanagedAllocations"] = totalUnmanagedAllocations,
                    ["ManagedAllocations"] = managedMemory,
                    ["TotalMemoryMapped"] = totalMapping,

                    ["Humane"] = new DynamicJsonValue
                    {
                        ["WorkingSet"] = FileHeader.Humane(workingSet),
                        ["TotalUnmanagedAllocations"] = FileHeader.Humane(totalUnmanagedAllocations),
                        ["ManagedAllocations"] = FileHeader.Humane(managedMemory),
                        ["TotalMemoryMapped"] = FileHeader.Humane(totalMapping),
                    },

                    ["Threads"] = threads,

                    ["Mappings"] = fileMappings
                };

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
                return Task.CompletedTask;
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

        public static int LongestCommonPrefixLen(List<string> strings)
        {
            for (int prefixLen = 0; prefixLen < strings[0].Length; prefixLen++)
            {
                char c = strings[0][prefixLen];
                for (int i = 1; i < strings.Count; i++)
                {
                    if (prefixLen >= strings[i].Length || strings[i][prefixLen] != c)
                    {
                        // Mismatch found
                        return prefixLen;
                    }
                }
            }
            return strings[0].Length;
        }
    }
}