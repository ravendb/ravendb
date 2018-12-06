using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/top", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task TopThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var threadsUsage = new ThreadsUsage();

                    //need to wait to get a result after an interval
                    await Task.Delay(100);
                    threadsUsage.Calculate();

                    await Task.Delay(100);
                    var result = threadsUsage.Calculate();

                    context.Write(write,
                        new DynamicJsonValue
                        {
                            ["Top Threads"] = result.ToJson()
                        });
                    write.Flush();
                }
            }
        }

        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task RunawayThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var threadAllocations = NativeMemory.AllThreadStats
                        .GroupBy(x => x.UnmanagedThreadId)
                        .ToDictionary(g => g.Key, x => x.First());

                    const string unmanagedThreadName = "Unmanaged Thread";
                    context.Write(write,
                        new DynamicJsonValue
                        {
                            ["Runaway Threads"] = new DynamicJsonArray(GetCurrentProcessThreads()
                                .Select(thread =>
                                {
                                    try
                                    {
                                        int? managedThreadId = null;
                                        string threadName = null;
                                        if (threadAllocations.TryGetValue((ulong)thread.Id, out var threadAllocation))
                                        {
                                            managedThreadId = threadAllocation.Id;
                                            threadName = threadAllocation.Name ?? "Thread Pool Thread";
                                        }

                                        return (thread.TotalProcessorTime.TotalMilliseconds,
                                            new DynamicJsonValue
                                            {
                                                [nameof(ThreadInfo.Id)] = thread.Id,
                                                [nameof(ThreadInfo.ManagedThreadId)] = managedThreadId,
                                                [nameof(ThreadInfo.Name)] = threadName ?? unmanagedThreadName,
                                                [nameof(ThreadInfo.StartingTime)] = thread.StartTime.ToUniversalTime(),
                                                [nameof(ThreadInfo.Duration)] = thread.TotalProcessorTime.TotalMilliseconds,
                                                [nameof(ThreadInfo.State)] = thread.ThreadState,
                                                [nameof(ThreadInfo.WaitReason)] = thread.ThreadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null,
                                                [nameof(ThreadInfo.TotalProcessorTime)] = thread.TotalProcessorTime,
                                                [nameof(ThreadInfo.PrivilegedProcessorTime)] = thread.PrivilegedProcessorTime,
                                                [nameof(ThreadInfo.UserProcessorTime)] = thread.UserProcessorTime
                                            });
                                    }
                                    catch (Exception e)
                                    {
                                        return (-1,
                                            new DynamicJsonValue
                                            {
                                                ["Error"] = e.ToString()
                                            });
                                    }
                                })
                                .OrderByDescending(thread => thread.Item1)
                                .Select(thread => thread.Item2))
                        });
                    write.Flush();
                }
            }

            return Task.CompletedTask;
        }

        private static IEnumerable<ProcessThread> GetCurrentProcessThreads()
        {
            using (var currentProcess = Process.GetCurrentProcess())
                return currentProcess.Threads.Cast<ProcessThread>();
        }

        internal class ThreadInfo
        {
            public int Id { get; set; }
            public int ManagedThreadId { get; set; }
            public string Name { get; set; }
            public DateTime StartingTime { get; set; }
            public DateTime Duration { get; set; }
            public ThreadState State { get; set; }
            public ThreadWaitReason? WaitReason { get; set; }
            public TimeSpan TotalProcessorTime { get; set; }
            public TimeSpan PrivilegedProcessorTime { get; set; }
            public TimeSpan UserProcessorTime { get; set; }
        }
    }
}
