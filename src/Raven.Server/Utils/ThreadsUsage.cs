using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.Dashboard;
using Raven.Server.Utils.Cpu;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class ThreadsUsage
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("ThreadsUsage");
        private (long TotalProcessorTimeTicks, long TimeTicks) _processTimes;
        private Dictionary<int, long> _threadTimesInfo = new Dictionary<int, long>();

        public ThreadsUsage()
        {
            using (var process = Process.GetCurrentProcess())
            {
                foreach (var thread in GetProcessThreads(process))
                {
                    _threadTimesInfo[thread.Id] = thread.TotalProcessorTime.Ticks;
                }

                _processTimes = CpuHelper.GetProcessTimes(process);
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
                var processThreads = GetProcessThreads(process);
                var previousProcessTimes = _processTimes;
                _processTimes = CpuHelper.GetProcessTimes(process);

                var processorTimeDiff = _processTimes.TotalProcessorTimeTicks - previousProcessTimes.TotalProcessorTimeTicks;
                var timeDiff = _processTimes.TimeTicks - previousProcessTimes.TimeTicks;
                var activeCores = CpuHelper.GetNumberOfActiveCores(process);
                threadsInfo.ActiveCores = activeCores;

                if (timeDiff == 0 || activeCores == 0)
                    return threadsInfo;

                var cpuUsage = (processorTimeDiff * 100.0) / timeDiff / activeCores;
                cpuUsage = Math.Min(cpuUsage, 100);

                var threadTimesInfo = new Dictionary<int, long>();
                double totalCpuUsage = 0;
                foreach (var thread in processThreads)
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
                            managedThreadId = threadStats.ManagedThreadId;
                        }

                        var threadState = GetThreadInfoOrDefault<ThreadState?>(() => thread.ThreadState);
                        threadsInfo.List.Add(new ThreadInfo
                        {
                            Id = thread.Id,
                            CpuUsage = threadCpuUsage.Value,
                            Name = threadName ?? "Unmanaged Thread",
                            ManagedThreadId = managedThreadId,
                            StartingTime = GetThreadInfoOrDefault<DateTime?>(() => thread.StartTime.ToUniversalTime()),
                            Duration = thread.TotalProcessorTime.TotalMilliseconds,
                            TotalProcessorTime = thread.TotalProcessorTime,
                            PrivilegedProcessorTime = thread.PrivilegedProcessorTime,
                            UserProcessorTime = thread.UserProcessorTime,
                            State = threadState,
                            Priority = GetThreadInfoOrDefault<ThreadPriorityLevel?>(() => thread.PriorityLevel),
                            WaitReason = GetThreadInfoOrDefault(() => threadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null)
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

                threadsInfo.List.Sort();
                _threadTimesInfo = threadTimesInfo;
                threadsInfo.CpuUsage = Math.Min(totalCpuUsage, 100);

                return threadsInfo;
            }
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

        private static List<ProcessThread> GetProcessThreads(Process process)
        {
            try
            {
                return process.Threads.Cast<ProcessThread>().ToList();
            }
            catch (PlatformNotSupportedException)
            {
                return new List<ProcessThread>();
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
    }
}
