using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Diagnostics.Runtime;
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
        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task RunawayThreads()
        {
            var includeStackTrace = GetBoolValueQueryString("stackTrace", required: false) ?? false;
            var includeStackObjects = GetBoolValueQueryString("stackObjects", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var threadAllocations = NativeMemory.AllThreadStats
                        .GroupBy(x => x.UnmanagedThreadId)
                        .ToDictionary(g => g.Key, x => x.First());

                    var clrThreadsInfo = GetClrThreadsInfo(includeStackTrace, includeStackObjects);

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

                                        List<string> stackTrace = null;
                                        List<string> stackObjects = null;
                                        if (clrThreadsInfo.TryGetValue((uint)thread.Id, out var clrThreadInfo))
                                        {
                                            managedThreadId = clrThreadInfo.ManagedThreadId;
                                            if (clrThreadInfo.ThreadType != ThreadType.Other)
                                                threadName = clrThreadInfo.ThreadType.ToString();
                                            stackTrace = clrThreadInfo.StackTrace;
                                            stackObjects = clrThreadInfo.StackObjects;
                                        }

                                        return (thread.TotalProcessorTime.TotalMilliseconds,
                                            new DynamicJsonValue
                                            {
                                                [nameof(ThreadInfo.Id)] = thread.Id,
                                                [nameof(ThreadInfo.ManagedThreadId)] = managedThreadId,
                                                [nameof(ThreadInfo.Name)] = threadName ?? "Unmanaged Thread",
                                                [nameof(ThreadInfo.StartingTime)] = thread.StartTime.ToUniversalTime(),
                                                [nameof(ThreadInfo.Duration)] = thread.TotalProcessorTime.TotalMilliseconds,
                                                [nameof(ThreadInfo.State)] = thread.ThreadState,
                                                [nameof(ThreadInfo.WaitReason)] = thread.ThreadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null,
                                                [nameof(ThreadInfo.TotalProcessorTime)] = thread.TotalProcessorTime,
                                                [nameof(ThreadInfo.PrivilegedProcessorTime)] = thread.PrivilegedProcessorTime,
                                                [nameof(ThreadInfo.UserProcessorTime)] = thread.UserProcessorTime,
                                                [nameof(ThreadInfo.StackTrace)] = TypeConverter.ToBlittableSupportedType(stackTrace),
                                                [nameof(ThreadInfo.StackObjects)] = TypeConverter.ToBlittableSupportedType(stackObjects)
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
                                .OrderByDescending(thread => thread.TotalMilliseconds)
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
            public List<string> StackTrace { get; set; }
            public List<string> StackObjects { get; set; }
        }

        public static Dictionary<uint, ClrThreadInfo> GetClrThreadsInfo(bool includeStackTrace, bool includeStackObjects)
        {
            if (includeStackTrace == false && includeStackObjects == false)
                return new Dictionary<uint, ClrThreadInfo>();

            try
            {
                using (var process = Process.GetCurrentProcess())
                using (var dataTarget = DataTarget.AttachToProcess(process.Id, 1000, AttachFlag.Passive))
                {
                    var clrVersion = dataTarget.ClrVersions[0];
                    var dac = dataTarget.SymbolLocator.FindBinary(clrVersion.DacInfo);

                    return ThreadsUsage.GetClrThreadsInfo(clrVersion, dac, includeStackTrace, includeStackObjects);
                }
            }
            catch (Exception)
            {
                return new Dictionary<uint, ClrThreadInfo>();
            }
        }
    }
}
