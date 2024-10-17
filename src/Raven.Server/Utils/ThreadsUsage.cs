using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using Raven.Server.Dashboard;
using Raven.Server.Utils.Cpu;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Utils;
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
                    using (thread)
                    {
                        try
                        {
                            _threadTimesInfo[thread.Id] = thread.TotalProcessorTime.Ticks;
                        }
                        catch (InvalidOperationException)
                        {
                            // thread has exited
                        }
                        catch (Win32Exception e) when (e.NativeErrorCode == 0x5)
                        {
                            // thread has exited
                        }
                        catch (NotSupportedException)
                        {
                            // nothing to do
                        }
                    }
                }

                _processTimes = CpuHelper.GetProcessTimes(process);
            }
        }

        public ThreadsInfo Calculate(int? take = null, HashSet<int> threadIds = null)
        {
            var threadAllocations = NativeMemory.AllThreadStats
                        .GroupBy(x => x.UnmanagedThreadId)
                        .ToDictionary(g => g.Key, x => x.First());

            var threadsInfo = new ThreadsInfo(take);

            using (var process = Process.GetCurrentProcess())
            {
                var previousProcessTimes = _processTimes;
                _processTimes = CpuHelper.GetProcessTimes(process);
                var processorTimeDiff = _processTimes.TotalProcessorTimeTicks - previousProcessTimes.TotalProcessorTimeTicks;
                var timeDiff = _processTimes.TimeTicks - previousProcessTimes.TimeTicks;
                var activeCores = CpuHelper.GetNumberOfActiveCores(process);
                threadsInfo.ActiveCores = activeCores;

                if (timeDiff == 0 || activeCores == 0)
                    return threadsInfo;

                var processCpuUsage = (processorTimeDiff * 100.0) / timeDiff / activeCores;
                processCpuUsage = Math.Min(processCpuUsage, 100);

                var threadTimesInfo = new Dictionary<int, long>();
                double totalCpuUsage = 0;
                var hasThreadIds = threadIds != null && threadIds.Count > 0;

                foreach (var thread in GetProcessThreads(process))
                {
                    using (thread)
                    {
                        if (hasThreadIds && threadIds.Contains(thread.Id) == false)
                            continue;

                        try
                        {
                            var threadTotalProcessorTime = thread.TotalProcessorTime;
                            var threadCpuUsage = GetThreadCpuUsage(thread.Id, threadTotalProcessorTime, processorTimeDiff, processCpuUsage);
                            threadTimesInfo[thread.Id] = threadTotalProcessorTime.Ticks;
                            if (threadCpuUsage == null)
                            {
                                // no previous info about the TotalProcessorTime for this thread
                                continue;
                            }

                            totalCpuUsage += threadCpuUsage.Value;

                            int? managedThreadId = null;
                            string threadName = null;
                            long? unmanagedAllocations = null;

                            if (threadAllocations.TryGetValue((ulong)thread.Id, out var threadStats))
                            {
                                managedThreadId = threadStats.ManagedThreadId;

                                if (ThreadNames.FullThreadNames.TryGetValue(managedThreadId.Value, out var fullThreadName))
                                {
                                    threadName = fullThreadName;
                                    threadsInfo.DedicatedThreadsCount++;
                                }
                                else
                                {
                                    threadName = threadStats.Name ?? ThreadHelper.GetThreadName(process.Id, thread.Id) ?? "Thread Pool Thread";
                                }

                                unmanagedAllocations = threadStats.TotalAllocated;
                            }

                            threadName ??= ThreadHelper.GetThreadName(process.Id, thread.Id);

                            var threadState = GetThreadInfoOrDefault<ThreadState?>(() => thread.ThreadState);
                            threadsInfo.List.Add(new ThreadInfo
                            {
                                Id = thread.Id,
                                CpuUsage = threadCpuUsage.Value,
                                Name = threadName ?? "Unmanaged Thread",
                                ManagedThreadId = managedThreadId,
                                UnmanagedAllocationsInBytes = unmanagedAllocations,
#pragma warning disable CA1416 // Validate platform compatibility
                                StartingTime = GetThreadInfoOrDefault<DateTime?>(() => thread.StartTime.ToUniversalTime()),
#pragma warning restore CA1416 // Validate platform compatibility
                                Duration = threadTotalProcessorTime.TotalMilliseconds,
                                TotalProcessorTime = threadTotalProcessorTime,
                                PrivilegedProcessorTime = thread.PrivilegedProcessorTime,
                                UserProcessorTime = thread.UserProcessorTime,
                                State = threadState,
#pragma warning disable CA1416 // Validate platform compatibility
                                Priority = GetThreadInfoOrDefault<ThreadPriorityLevel?>(() => thread.PriorityLevel),
#pragma warning restore CA1416 // Validate platform compatibility
                                WaitReason = GetThreadInfoOrDefault(() => threadState == ThreadState.Wait ? thread.WaitReason : (ThreadWaitReason?)null)
                            });
                        }
                        catch (InvalidOperationException)
                        {
                            // thread has exited
                        }
                        catch (Win32Exception e) when (e.HResult == 0x5)
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
                }

                _threadTimesInfo = threadTimesInfo;
                threadsInfo.CpuUsage = Math.Min(totalCpuUsage, 100);
                threadsInfo.ProcessCpuUsage = processCpuUsage;
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

        private double? GetThreadCpuUsage(int threadId, TimeSpan threadTotalProcessorTime, long processorTimeDiff, double processCpuUsage)
        {
            if (_threadTimesInfo.TryGetValue(threadId, out var previousTotalProcessorTimeTicks) == false)
            {
                // no previous info for this thread yet
                return null;
            }

            var threadTimeDiff = threadTotalProcessorTime.Ticks - previousTotalProcessorTimeTicks;
            if (threadTimeDiff == 0 || processorTimeDiff == 0)
            {
                // no cpu usage
                return 0;
            }

            return threadTimeDiff * 1.0 / processorTimeDiff * processCpuUsage;
        }
    }
}
