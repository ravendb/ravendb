using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ProcStatsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/cpu/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task CpuStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = CpuStatsInternal();

                await using (var write = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
            }
        }

        [RavenAction("/admin/debug/proc/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task ProcStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = ProcStatsInternal();

                await using (var write = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
            }
        }

        private DynamicJsonValue CpuStatsInternal()
        {
            using (var proc = Process.GetCurrentProcess())
            {
                var djaCpu = new DynamicJsonArray();
                var djvCpu = new DynamicJsonValue
                {
                    ["ProcessName"] = proc.ProcessName,
                    ["ProcessorAffinity"] = proc.ProcessorAffinity.ToInt64(),
                    ["PrivilegedProcessorTime"] = proc.PrivilegedProcessorTime,
                    ["TotalProcessorTime"] = proc.TotalProcessorTime,
                    ["UserProcessorTime"] = proc.UserProcessorTime
                };

                djaCpu.Add(djvCpu);

                var djaThreadpool = new DynamicJsonArray();
                var djvThreadpool = new DynamicJsonValue();

                ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
                djvThreadpool["AvailableThreadPoolWorkerThreads"] = workerThreads;
                djvThreadpool["AvailableThreadPoolCompletionPortThreads"] = completionPortThreads;
                ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
                djvThreadpool["MinThreadPoolWorkerThreads"] = workerThreads;
                djvThreadpool["MinThreadPoolCompletionPortThreads"] = completionPortThreads;
                ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
                djvThreadpool["MaxThreadPoolWorkerThreads"] = workerThreads;
                djvThreadpool["MaxThreadPoolCompletionPortThreads"] = completionPortThreads;

                djaThreadpool.Add(djvThreadpool);

                return new DynamicJsonValue
                {
                    ["CpuStats"] = djaCpu,
                    ["ThreadPoolStats"] = djaThreadpool
                };
            }
        }

        private DynamicJsonValue ProcStatsInternal()
        {
            using (var proc = Process.GetCurrentProcess())
            {
                var dja = new DynamicJsonArray();
                var djv = new DynamicJsonValue();

                AddValue(djv, "Id", () => proc.Id);
                AddValue(djv, "Handle", () => proc.Handle.ToInt64());
                AddValue(djv, "BasePriority", () => proc.BasePriority);
                AddValue(djv, "StartTime", () => proc.StartTime);
                AddValue(djv, "MachineName", () => proc.MachineName);
                AddValue(djv, "MaxWorkingSet", () => proc.MaxWorkingSet.ToInt64());
                AddValue(djv, "MinWorkingSet", () => proc.MinWorkingSet.ToInt64());
                AddValue(djv, "NonpagedSystemMemorySize64", () => proc.NonpagedSystemMemorySize64);
                AddValue(djv, "PagedMemorySize64", () => proc.PagedMemorySize64);
                AddValue(djv, "PagedSystemMemorySize64", () => proc.PagedSystemMemorySize64);
                AddValue(djv, "PeakPagedMemorySize64", () => proc.PeakPagedMemorySize64);
                AddValue(djv, "PeakWorkingSet64", () => proc.PeakWorkingSet64);
                AddValue(djv, "PeakVirtualMemorySize64", () => proc.PeakVirtualMemorySize64);
                AddValue(djv, "PriorityBoostEnabled", () => proc.PriorityBoostEnabled);
                AddValue(djv, "PriorityClass", () => proc.PriorityClass);
                AddValue(djv, "PrivateMemorySize64", () => proc.PrivateMemorySize64);
                AddValue(djv, "ProcessName", () => proc.ProcessName);
                AddValue(djv, "ProcessorAffinity", () => proc.ProcessorAffinity.ToInt64());
                AddValue(djv, "SessionId", () => proc.SessionId);
                AddValue(djv, "StartInfo", () => proc.StartInfo);
                AddValue(djv, "HandleCount", () => proc.HandleCount);
                AddValue(djv, "VirtualMemorySize64", () => proc.VirtualMemorySize64);
                AddValue(djv, "EnableRaisingEvents", () => proc.EnableRaisingEvents);
                AddValue(djv, "StandardInput", () => proc.StandardInput);
                AddValue(djv, "StandardOutput", () => proc.StandardOutput);
                AddValue(djv, "StandardError", () => proc.StandardError);
                AddValue(djv, "WorkingSet64", () => proc.WorkingSet64);
                AddValue(djv, "Responding", () => proc.Responding);
                AddValue(djv, "MainWindowTitle", () => proc.MainWindowTitle);
                AddValue(djv, "MainWindowHandle", () => proc.MainWindowHandle.ToInt64());
                AddValue(djv, "SynchronizingObject", () => proc.SynchronizingObject);
                AddValue(djv, "MainModuleFileName", () => proc.MainModule.FileName);
                AddValue(djv, "MainModuleFileVersionInfoFileVersion", () => proc.MainModule.FileVersionInfo.FileVersion);
                AddValue(djv, "MainModuleModuleName", () => proc.MainModule.ModuleName);
                AddValue(djv, "MainModuleModuleMemorySize", () => proc.MainModule.ModuleMemorySize);
                AddValue(djv, "PrivilegedProcessorTime", () => proc.PrivilegedProcessorTime);
                AddValue(djv, "TotalProcessorTime", () => proc.TotalProcessorTime);
                AddValue(djv, "UserProcessorTime", () => proc.UserProcessorTime);
                AddValue(djv, "Site", () => proc.Site);
                AddValue(djv, "Container", () => proc.Container);

                djv["Threads"] = GetProcessThreadCollection(proc);
                djv["Modules"] = GetProcessModuleCollection(proc);

                dja.Add(djv);

                return new DynamicJsonValue
                {
                    ["ProcStats"] = dja
                };
            }
        }

        private DynamicJsonArray GetProcessThreadCollection(Process proc)
        {
            var dja = new DynamicJsonArray();
            var collection = proc.Threads;

            for (var idx = 0; idx < collection.Count; idx++)
            {
                var i = idx;
                var djv = new DynamicJsonValue();

                AddValue(djv, "Id", () => collection[i].Id);
                AddValue(djv, "BasePriority", () => collection[i].BasePriority);
                AddValue(djv, "CurrentPriority", () => collection[i].CurrentPriority);
                AddValue(djv, "PriorityBoostEnabled", () => collection[i].PriorityBoostEnabled);
                AddValue(djv, "PriorityLevel", () => collection[i].PriorityLevel);
                AddValue(djv, "StartAddress", () => collection[i].StartAddress.ToInt64());
                AddValue(djv, "ThreadState", () => collection[i].ThreadState);
                AddValue(djv, "WaitReason", () => collection[i].WaitReason);
                AddValue(djv, "PrivilegedProcessorTime", () => collection[i].PrivilegedProcessorTime);
                AddValue(djv, "StartTime", () => collection[i].StartTime);
                AddValue(djv, "TotalProcessorTime", () => collection[i].TotalProcessorTime);
                AddValue(djv, "UserProcessorTime", () => collection[i].UserProcessorTime);
                AddValue(djv, "Site", () => collection[i].Site);
                AddValue(djv, "Container", () => collection[i].Container);

                dja.Add(djv);
            }
            return dja;
        }

        private DynamicJsonArray GetProcessModuleCollection(Process proc)
        {
            var dja = new DynamicJsonArray();
            var collection = proc.Modules;

            for (var idx = 0; idx < collection.Count; idx++)
            {
                var i = idx;
                var djv = new DynamicJsonValue();

                AddValue(djv, "ModuleName", () => collection[i].ModuleName);
                AddValue(djv, "FileName", () => collection[i].FileName);
                AddValue(djv, "BaseAddress", () => collection[i].BaseAddress.ToInt64());
                AddValue(djv, "ModuleMemorySize", () => collection[i].ModuleMemorySize);
                AddValue(djv, "EntryPointAddress", () => collection[i].EntryPointAddress.ToInt64());
                AddValue(djv, "FileVersionInfoFileVersion", () => collection[i].FileVersionInfo.FileVersion);
                AddValue(djv, "Site", () => collection[i].Site);
                AddValue(djv, "Container", () => collection[i].Container);

                dja.Add(djv);
            }
            return dja;
        }

        private static void AddValue<T>(DynamicJsonValue djv, string key, Func<T> func)
        {
            try
            {
                var result = func.Invoke();
                djv[key] = result;
            }
            catch (Exception ex)
            {
                djv[key] = "Not Available : " + (ex.InnerException != null ? ex.InnerException.Message : ex.Message);
            }
        }
    }
}
