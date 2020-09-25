using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Utils
{
    public class AffinityHelper
    {
        private static readonly ConcurrentSet<PoolOfThreads.PooledThread> _customAffinityThreads = new ConcurrentSet<PoolOfThreads.PooledThread>();
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<AffinityHelper>("Server");

        public static void SetProcessAffinity(Process process, int cores, long? processAffinityMask, out long currentlyAssignedCores)
        {
            currentlyAssignedCores = Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
            if (currentlyAssignedCores == cores)
            {
                // we already set the correct number of assigned cores
                return;
            }

            var bitMask = 1L;
            if (processAffinityMask == null)
            {
                for (var i = 0; i < cores; i++)
                {
                    bitMask |= 1L << i;
                }
            }
            else if (Bits.NumberOfSetBits(processAffinityMask.Value) > cores)
            {
                var affinityMask = processAffinityMask.Value;
                var bitNumber = 0;
                while (cores > 0)
                {
                    if ((affinityMask & 1) != 0)
                    {
                        bitMask |= 1L << bitNumber;
                        cores--;
                    }

                    affinityMask = affinityMask >> 1;
                    bitNumber++;
                }
            }
            else
            {
                bitMask = processAffinityMask.Value;
            }

            process.ProcessorAffinity = new IntPtr(bitMask);

            // changing the process affinity resets the thread affinity
            // we need to change the custom affinity threads as well
            foreach (var pooledThread in _customAffinityThreads)
            {
                SetCustomThreadAffinity(pooledThread);
            }
        }

        internal static bool ResetThreadAffinity(PoolOfThreads.PooledThread pooledThread)
        {
            _customAffinityThreads.TryRemove(pooledThread);

            return ChangeThreadAffinityWithRetries(pooledThread.CurrentProcess, currentAffinity =>
            {
                SetThreadAffinity(pooledThread, currentAffinity);
            });
        }

        internal static void SetCustomThreadAffinity(PoolOfThreads.PooledThread pooledThread)
        {
            ChangeThreadAffinityWithRetries(pooledThread.CurrentProcess, currentAffinity =>
            {
                SetCustomThreadAffinityInternal(pooledThread, currentAffinity);
            });
        }

        private static bool ChangeThreadAffinityWithRetries(Process currentProcess, Action<long> action)
        {
            if (PlatformDetails.RunningOnMacOsx)
            {
                // Mac OSX threads API doesn't provide a way to set thread affinity
                // we can use thread_policy_set which will make sure that two threads will run
                // on different cpus, however we cannot choose which cpus will be used

                // from thread_policy.h about using THREAD_AFFINITY_POLICY:
                // This may be used to express affinity relationships between threads in
                // the task. Threads with the same affinity tag will be scheduled to
                // share an L2 cache if possible. That is, affinity tags are a hint to
                // the scheduler for thread placement.
                return true;
            }

            var retries = 10;

            while (true)
            {
                try
                {
                    currentProcess.Refresh();
                    var currentAffinity = currentProcess.ProcessorAffinity.ToInt64();
                    action(currentAffinity);
                    return true;
                }
                catch (PlatformNotSupportedException)
                {
                    // nothing to be done
                    return true;
                }
                catch (Exception e)
                {
                    if (retries-- == 0)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations("Failed to set thread affinity", e);
                        return false;
                    }

                    Thread.Sleep(10);
                }
            }
        }

        private static void SetCustomThreadAffinityInternal(PoolOfThreads.PooledThread pooledThread, long currentAffinity)
        {
            var numberOfCoresToReduce = pooledThread.NumberOfCoresToReduce;
            var threadMask = pooledThread.ThreadMask;

            if (numberOfCoresToReduce <= 0 && threadMask == null)
            {
                _customAffinityThreads.TryRemove(pooledThread);
                return;
            }

            _customAffinityThreads.TryAdd(pooledThread);

            // we can't reduce the number of cores to a zero or negative number, in this case, just use the processor cores
            if (threadMask == null && Bits.NumberOfSetBits(currentAffinity) <= numberOfCoresToReduce)
            {
                SetThreadAffinity(pooledThread, currentAffinity);
                return;
            }

            if (threadMask == null)
            {
                for (int i = 0; i < numberOfCoresToReduce; i++)
                {
                    // remove the N least significant bits
                    // we do that because it is typical that the first cores (0, 1, etc) are more
                    // powerful and we want to keep them for other things, such as request processing
                    currentAffinity &= currentAffinity - 1;
                }
            }
            else
            {
                currentAffinity &= threadMask.Value;
            }

            SetThreadAffinity(pooledThread, currentAffinity);
        }

        private static void SetThreadAffinity(PoolOfThreads.PooledThread pooledThread, long affinity)
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                // windows
#pragma warning disable CA1416 // Validate platform compatibility
                pooledThread.CurrentProcessThread.ProcessorAffinity = new IntPtr(affinity);
#pragma warning restore CA1416 // Validate platform compatibility
                return;
            }

            if (PlatformDetails.RunningOnLinux)
            {
                var ulongAffinity = (ulong)affinity;
                var result = Syscall.sched_setaffinity((int)pooledThread.CurrentUnmanagedThreadId, new IntPtr(sizeof(ulong)), ref ulongAffinity);
                if (result != 0)
                    throw new InvalidOperationException(
                        $"Failed to set affinity for thread: {pooledThread.CurrentUnmanagedThreadId}, " +
                        $"affinity: {affinity}, result: {result}, error: {Marshal.GetLastWin32Error()}");
            }
        }
    }
}
