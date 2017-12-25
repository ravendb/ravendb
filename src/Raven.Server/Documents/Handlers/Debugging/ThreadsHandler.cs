using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/remote-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task ListRemoteConnections()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(write,
                    new DynamicJsonValue
                    {
                        ["Remote Connections"] = new DynamicJsonArray(RemoteConnection.RemoteConnectionsList.ToList()
                            .Select(connection => new DynamicJsonValue
                            {
                                [nameof(RemoteConnection.RemoteConnectionInfo.Caller)] = connection.Caller,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Destination)] = connection.Destination,
                                [nameof(RemoteConnection.RemoteConnectionInfo.StartAt)] = connection.StartAt,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Number)] = connection.Number,
                            }))
                    });
                write.Flush();
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task RunawayThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var threadAllocations = NativeMemory.ThreadAllocations.Values
                        .GroupBy(x => x.UnmanagedThreadId)
                        .ToDictionary(g => g.Key, x => x.First().Name);

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
            using (var currentProcess = Process.GetCurrentProcess())
                return currentProcess.Threads.Cast<ProcessThread>();
        }

        private class ThreadInfo
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime StartingTime { get; set; }
            public ThreadState State { get; set; }
            public ThreadWaitReason? WaitReason { get; set; }
            public TimeSpan TotalProcessorTime { get; set; }
            public TimeSpan PrivilegedProcessorTime { get; set; }
            public TimeSpan UserProcessorTime { get; set; }
        }
    }
}
