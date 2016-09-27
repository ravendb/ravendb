using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Client.Linq;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

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
                var currentProcess = Process.GetCurrentProcess();
                var workingSet = currentProcess.WorkingSet64;
                long totalUnmanagedAllocations = 0;
                long totalMapping = 0;
                var fileMappingByDir = new Dictionary<string, DynamicJsonValue>();
                var fileMappingSizesByDir = new Dictionary<string, long>();
                foreach (var mapping in NativeMemory.FileMapping)
                {
                    totalMapping += mapping.Value;

                    var dir = Path.GetDirectoryName(mapping.Key);
                    DynamicJsonValue value;
                    if (fileMappingByDir.TryGetValue(dir, out value) == false)
                    {
                        value = new DynamicJsonValue
                        {
                            ["Directory"] = dir,
                        };
                        fileMappingByDir[dir] = value;
                    }
                    long prevSize;
                    fileMappingSizesByDir.TryGetValue(dir, out prevSize);
                    fileMappingSizesByDir[dir] = prevSize + mapping.Value;
                    value[Path.GetFileName(mapping.Key)] = new DynamicJsonValue
                    {
                        ["Mapped"] = mapping.Value,
                        ["HumaneMapped"] = FileHeader.Humane(mapping.Value)
                    };
                }

                foreach (var sizes in fileMappingSizesByDir)
                {
                    DynamicJsonValue value;
                    if (fileMappingByDir.TryGetValue(sizes.Key, out value))
                    {
                        value["TotalDirectorySize"] = new DynamicJsonValue
                        {
                            ["Mapped"] = sizes.Value,
                            ["HumaneMapped"] = FileHeader.Humane(sizes.Value)
                        };
                    }
                }
                var fileMappings = new DynamicJsonArray(fileMappingByDir.Values);

                var threads = new DynamicJsonArray();
                foreach (var stats in NativeMemory.ThreadAllocations.Values)
                {
                    totalUnmanagedAllocations += stats.Allocations;
                    threads.Add(new DynamicJsonValue
                    {
                        ["Name"] = stats.Name,
                        ["Id"] = stats.Id,
                        ["Allocations"] = stats.Allocations,
                        ["HumaneAllocations"] = FileHeader.Humane(stats.Allocations)
                    });
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
    }
}