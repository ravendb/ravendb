using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Diagnostics.Runtime;
using Raven.Server.Dashboard;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class ThreadsUsage : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("ThreadsUsage");
        private (long TotalProcessorTimeTicks, long TimeTicks) _processTimes;
        private Dictionary<int, long> _threadTimesInfo = new Dictionary<int, long>();
        private readonly bool _includeStackTrace;
        private readonly bool _includeStackObjects;
        private readonly DataTarget _dataTarget;
        private readonly ClrInfo _clrVersion;
        private readonly string _dac;

        public ThreadsUsage(bool includeStackTrace = false, bool includeStackObjects = false)
        {
            _includeStackTrace = includeStackTrace;
            _includeStackObjects = includeStackObjects;

            using (var process = Process.GetCurrentProcess())
            {
                _processTimes = CpuUsage.GetProcessTimes(process);

                try
                {
                    if (_includeStackTrace || _includeStackObjects)
                    {
                        _dataTarget = DataTarget.AttachToProcess(process.Id, 1000, AttachFlag.Passive);
                        _clrVersion = _dataTarget.ClrVersions[0];
                        _dac = _dataTarget.SymbolLocator.FindBinary(_clrVersion.DacInfo);
                    }
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failed to attach to process", e);
                }
            }
        }

        public ThreadsInfo Calculate()
        {
            var threadAllocations = NativeMemory.AllThreadStats
                .GroupBy(x => x.UnmanagedThreadId)
                .ToDictionary(g => g.Key, x => x.First());

            var threadsInfo = new ThreadsInfo();

            using (var process = Process.GetCurrentProcess())
            {
                var clrThreadsInfo = GetClrThreadsInfo(_clrVersion, _dac, _includeStackTrace, _includeStackObjects);

                var previousProcessTimes = _processTimes;
                _processTimes = CpuUsage.GetProcessTimes(process);

                var processorTimeDiff = _processTimes.TotalProcessorTimeTicks - previousProcessTimes.TotalProcessorTimeTicks;
                var timeDiff = _processTimes.TimeTicks - previousProcessTimes.TimeTicks;
                var activeCores = CpuUsage.GetNumberOfActiveCores(process);
                threadsInfo.ActiveCores = activeCores;

                if (timeDiff == 0 || activeCores == 0)
                    return threadsInfo;

                var cpuUsage = (processorTimeDiff * 100.0) / timeDiff / activeCores;
                cpuUsage = Math.Min(cpuUsage, 100);

                var threadTimesInfo = new Dictionary<int, long>();
                double totalCpuUsage = 0;

                foreach (var thread in GetProcessThreads(process))
                {
                    try
                    {
                        var threadCpuUsage = GetThreadCpuUsage(thread, processorTimeDiff, cpuUsage);
                        threadTimesInfo[thread.Id] = thread.TotalProcessorTime.Ticks;
                        if (threadCpuUsage == null)
                        {
                            // no previous info about the TotalProcessorTime of this thread
                            continue;
                        }

                        totalCpuUsage += threadCpuUsage.Value;

                        int? managedThreadId = null;
                        string threadName = null;
                        if (threadAllocations.TryGetValue((ulong)thread.Id, out var threadStats))
                        {
                            threadName = threadStats.Name ?? "Thread Pool Thread";
                            managedThreadId = threadStats.Id;
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

                        var threadState = GetThreadInfoOrDefault<ThreadState?>(() => thread.ThreadState);
                        threadsInfo.List.Add(new ThreadInfo
                        {
                            Id = thread.Id,
                            CpuUsage = threadCpuUsage.Value,
                            Name = threadName ?? "Unmanaged Thread",
                            ManagedThreadId = managedThreadId,
                            StartingTime = GetThreadInfoOrDefault<DateTime?>(() => thread.StartTime.ToUniversalTime()),
                            State = threadState,
                            Priority = GetThreadInfoOrDefault<ThreadPriorityLevel?>(() => thread.PriorityLevel),
                            ThreadWaitReason = GetThreadInfoOrDefault(() => threadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null),
                            StackTrace = stackTrace,
                            StackObjects = stackObjects
                        });
                    }
                    catch (InvalidOperationException)
                    {
                        // thread has exited
                    }
                    catch (NotSupportedException)
                    {
                        // nothing to do
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failed to get thread info", e);
                    }
                }

                _threadTimesInfo = threadTimesInfo;
                threadsInfo.CpuUsage = Math.Min(totalCpuUsage, 100);
            }

            return threadsInfo;
        }

        private static T GetThreadInfoOrDefault<T>(Func<T> action)
        {
            try
            {
                return action();
            }
            catch (NotSupportedException)
            {
                return default(T);
            }
        }

        private static IEnumerable<ProcessThread> GetProcessThreads(Process process)
        {
            try
            {
                return process.Threads.Cast<ProcessThread>();
            }
            catch (PlatformNotSupportedException)
            {
                return Enumerable.Empty<ProcessThread>();
            }
        }

        private double? GetThreadCpuUsage(ProcessThread thread, long processorTimeDiff, double cpuUsage)
        {
            if (_threadTimesInfo.TryGetValue(thread.Id, out var previousTotalProcessorTimeTicks) == false)
            {
                // no previous info for this thread yet
                return null;
            }

            var threadTimeDiff = thread.TotalProcessorTime.Ticks - previousTotalProcessorTimeTicks;
            if (threadTimeDiff == 0 || processorTimeDiff == 0)
            {
                // no cpu usage
                return 0;
            }

            var threadCpuUsage = threadTimeDiff * 1.0 / processorTimeDiff * cpuUsage;

            return threadCpuUsage;
        }

        public static Dictionary<uint, ClrThreadInfo> GetClrThreadsInfo(
            ClrInfo clrVersion, string dac, bool includeStackTrace, bool includeStackObjects)
        {
            var clrThreadsInfo = new Dictionary<uint, ClrThreadInfo>();

            if (clrVersion == null || dac == null)
                return clrThreadsInfo;

            try
            {
                var runtime = clrVersion.CreateRuntime(dac);

                foreach (var clrThread in runtime.Threads)
                {
                    if (clrThread.IsAlive == false)
                        continue;

                    var threadInfo = clrThreadsInfo[clrThread.OSThreadId] = new ClrThreadInfo
                    {
                        ManagedThreadId = clrThread.ManagedThreadId,
                        ThreadType = clrThread.IsGC ? ThreadType.GC : clrThread.IsFinalizer ? ThreadType.Finalizer : ThreadType.Other
                    };

                    
                    if (includeStackTrace)
                    {
                        foreach (var frame in clrThread.EnumerateStackTrace())
                        {
                            threadInfo.StackTrace.Add(frame.DisplayString);
                        }
                    }

                    if (includeStackObjects)
                    {
                        GetStackObjects(runtime, clrThread, threadInfo);
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to get CLR threads info", e);
            }

            return clrThreadsInfo;
        }

        private static void GetStackObjects(ClrRuntime runtime, ClrThread clrThread, ClrThreadInfo threadInfo)
        {
            // We'll need heap data to find objects on the stack.
            var heap = runtime.Heap;

            // Walk each pointer aligned address on the stack.  Note that StackBase/StackLimit
            // is exactly what they are in the TEB.  This means StackBase > StackLimit on AMD64.
            var start = clrThread.StackBase;
            var stop = clrThread.StackLimit;

            // We'll walk these in pointer order.
            if (start > stop)
            {
                var tmp = start;
                start = stop;
                stop = tmp;
            }

            // Walk each pointer aligned address. Ptr is a stack address.
            for (var ptr = start; ptr <= stop; ptr += (ulong)runtime.PointerSize)
            {
                // Read the value of this pointer. If we fail to read the memory, break. The
                // stack region should be in the crash dump.
                if (runtime.ReadPointer(ptr, out var obj) == false)
                    break;

                // We check to see if this address is a valid object by simply calling
                // GetObjectType. If that returns null, it's not an object.
                var type = heap.GetObjectType(obj);
                if (type == null)
                    continue;

                // there tends to be a lot of free objects in the stack.
                if (type.IsFree == false)
                {
                    threadInfo.StackObjects.Add(type.Name);
                }
            }
        }

        public void Dispose()
        {
            _dataTarget?.Dispose();
        }
    }

    public class ClrThreadInfo
    {
        public ClrThreadInfo()
        {
            StackTrace = new List<string>();
            StackObjects = new List<string>();
        }

        public int ManagedThreadId { get; set; }

        public List<string> StackTrace { get; set; }

        public List<string> StackObjects { get; set; }

        public ThreadType ThreadType { get; set; }
    }

    public enum ThreadType
    {
        Other,
        GC,
        Finalizer
    }
}
