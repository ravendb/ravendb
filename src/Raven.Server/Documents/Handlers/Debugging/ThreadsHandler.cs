using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Debugging;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.ServerAdmin, IsDebugInformationEndpoint = true)]
        public Task RunawayThreads()
        {            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var threadAllocations = NativeMemory.ThreadAllocations.Values
                        .GroupBy(x=>x.UnmanagedThreadId)
                        .ToDictionary(g=>g.Key, x=>x.First().Name);

                    context.Write(write,
                        new DynamicJsonValue
                        {                            
                            ["Runaway Threads"] = new DynamicJsonArray(GetCurrentProcessThreads()
                                .OrderByDescending(thread => thread.TotalProcessorTime.TotalMilliseconds)
                                .Select(thread => new DynamicJsonValue
                                {
                                    [nameof(ThreadInfo.Id)] = thread.Id,
                                    [nameof(ThreadInfo.Name)] = threadAllocations.TryGetValue(thread.Id, out var threadName) ?
                                        (threadName ?? "Thread Pool Thread") : "Unmanaged Thread",
                                    [nameof(ThreadInfo.StartingTime)] = thread.StartTime,
                                    [nameof(ThreadInfo.State)] = thread.ThreadState,
                                    [nameof(ThreadInfo.WaitReason)] = thread.ThreadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null,
                                    [nameof(ThreadInfo.TotalProcessorTime)] = thread.TotalProcessorTime,
                                    [nameof(ThreadInfo.PrivilegedProcessorTime)] = thread.PrivilegedProcessorTime,
                                    [nameof(ThreadInfo.UserProcessorTime)] = thread.UserProcessorTime
                                }))
                        });
                    write.Flush();
                }
            }

            return Task.CompletedTask;
        }

        private static IEnumerable<ProcessThread> GetCurrentProcessThreads()
        {
            return Process.GetCurrentProcess().Threads.Cast<ProcessThread>();
        }
    }
}
