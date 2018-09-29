using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    /// <summary>
    /// This is not a thread pool, this is a pool of threads.
    /// It is intended for checking out long running threads and will reuse them
    /// when the running job is done. 
    /// Threads checked out from here may be mutated by the caller and their state will
    /// be wiped when they are (automatically) returned to the system. 
    /// 
    /// This is intended for _BIG_ tasks and it is not a replacement for the thread pool.
    /// </summary>
    public class PoolOfThreads : IDisposable
    {
        private static readonly Lazy<PoolOfThreads> _globalRavenThreadPool = new Lazy<PoolOfThreads>(() =>
        {
            return new PoolOfThreads();
        });

        public static PoolOfThreads GlobalRavenThreadPool => _globalRavenThreadPool.Value;
        private static Logger _log = LoggingSource.Instance.GetLogger<PoolOfThreads>("Server");
        private float _minimumFreeCommittedMemory = 0.05f;

        public void SetMinimumFreeCommittedMemory(float min)
        {
            if (min <= 0)
                throw new ArgumentException("MinimumFreeCommittedMemory must be positive, but was: " + min);

            _minimumFreeCommittedMemory = min;
        }

        public void SetThreadsAffinityIfNeeded()
        {
            foreach (var pooledThread in _pool)
            {
                try
                {
                    var numberOfCoresToReduce = pooledThread.NumberOfCoresToReduce;
                    if (numberOfCoresToReduce == null)
                        continue;

                    pooledThread.SetThreadAffinity(numberOfCoresToReduce.Value, pooledThread.ThreadMask);
                }
                catch (Exception e)
                {
                    if (_log.IsOperationsEnabled)
                        _log.Operations("Failed to set thread affinity", e);
                }
            }
        }

        private readonly ConcurrentQueue<PooledThread> _pool = new ConcurrentQueue<PooledThread>();
        private bool _disposed;

        public void Dispose()
        {
            lock (this)
            {
                _disposed = true;
            }
            while (_pool.TryDequeue(out var pooled))
            {
                pooled.SetWorkForThread(null, null, null);
            }
        }

        public class LongRunningWork
        {
            [ThreadStatic] public static LongRunningWork Current;
            [ThreadStatic] internal static PooledThread CurrentPooledThread;
            private ManualResetEvent _manualResetEvent;
            public string Name;

            public LongRunningWork(ManualResetEvent manualResetEvent, string name)
            {
                _manualResetEvent = manualResetEvent;
                Name = name;
            }

            public int ManagedThreadId { get; internal set; }

            public bool Join(int timeout)
            {
                return _manualResetEvent.WaitOne(timeout);
            }

            internal void Set()
            {
                _manualResetEvent.Set();
            }
        }

        public LongRunningWork LongRunning(Action<object> action, object state, string name)
        {
            if (_pool.TryDequeue(out var pooled) == false)
            {
                MemoryInformation.AssertNotAboutToRunOutOfMemory(_minimumFreeCommittedMemory);

                pooled = new PooledThread(this);
                var thread = new Thread(pooled.Run)
                {
                    Name = name,
                    IsBackground = true,
                };
                
                thread.Start();
            }
            pooled.StartedAt = DateTime.UtcNow;
            return pooled.SetWorkForThread(action, state, name);
        }

        internal class PooledThread
        {
            private static readonly FieldInfo RuntimeThreadField;
            private static readonly FieldInfo ThreadFieldName;

            private readonly ManualResetEvent _waitForWork = new ManualResetEvent(false);
            private Action<object> _action;
            private object _state;
            private string _name;
            private readonly PoolOfThreads _parent;
            private LongRunningWork _workIsDone;
            private ulong _currentUnmangedThreadId;
            private ProcessThread _currentProcessThread;
            private Process _currentProcess;

            public int? NumberOfCoresToReduce { get; private set; }
            public long? ThreadMask { get; private set; }

            public DateTime StartedAt { get; internal set; }

            static PooledThread()
            {
                var t = Thread.CurrentThread;
                RuntimeThreadField = typeof(Thread).GetField("_runtimeThread", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                Debug.Assert(RuntimeThreadField != null);
                var runtimeThread = RuntimeThreadField.GetValue(t);
                ThreadFieldName = runtimeThread.GetType().GetField("m_Name", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                Debug.Assert(ThreadFieldName != null);
            }

            public PooledThread(PoolOfThreads pool)
            {
                _parent = pool;
            }

            public LongRunningWork SetWorkForThread(Action<object> action, object state, string name)
            {
                _action = action;
                _state = state;
                _name = name;
                _workIsDone = new LongRunningWork(new ManualResetEvent(false), name);

                _waitForWork.Set();
                return _workIsDone;
            }

            public void Run()
            {
                try
                {
                    InitializeProcessThreads();
                    LongRunningWork.CurrentPooledThread = this;

                    while (true)
                    {
                        _waitForWork.WaitOne();

                        if (DoWork() == false)
                            return;
                    }
                }
                finally
                {
                    NativeMemory.NotifyCurrentThreadAboutToClose();
                }
            }

            //https://github.com/dotnet/coreclr/issues/20156
            [MethodImpl(MethodImplOptions.NoInlining)]
            private bool DoWork()
            {
                _workIsDone.ManagedThreadId = Thread.CurrentThread.ManagedThreadId;

                if (_action == null)
                {
                    // should only happen when we shutdown
                    return false;
                }

                ResetCurrentThreadName();
                Thread.CurrentThread.Name = _name;

                try
                {
                    LongRunningWork.Current = _workIsDone;
                    _action(_state);
                }
                catch (Exception e)
                {
                    if (_log.IsOperationsEnabled)
                    {
                        _log.Operations($"An uncaught exception occurred in '{_name}' and killed the process", e);
                    }

                    throw;
                }
                finally
                {
                    _workIsDone.Set();
                    LongRunningWork.Current = null;
                }

                _action = null;
                _state = null;
                _workIsDone = null;

                ThreadLocalCleanup.Run();

                ResetCurrentThreadName();
                Thread.CurrentThread.Name = "Available Pool Thread";

                if (ResetThreadPriority() == false)
                    return false;

                if (ResetThreadAffinity() == false)
                    return false;

                _waitForWork.Reset();
                lock (_parent)
                {
                    if (_parent._disposed)
                        return false;

                    _parent._pool.Enqueue(this);
                }

                return true;
            }

            private bool ResetThreadAffinity()
            {
                if (PlatformDetails.RunningOnMacOsx)
                    return true;

                NumberOfCoresToReduce = null;
                ThreadMask = null;

                try
                {
                    _currentProcess.Refresh();
                    SetThreadAffinityByPlatform(_currentProcess.ProcessorAffinity.ToInt64());
                    return true;
                }
                catch (PlatformNotSupportedException)
                {
                    return true; // nothing to be done
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Unable to reset this thread affinity to the processor default, we'll just let it exit", e);
                    }
                    return false;
                }
            }

            private static bool ResetThreadPriority()
            {
                try
                {
                    if (Thread.CurrentThread.Priority != ThreadPriority.Normal)
                        Thread.CurrentThread.Priority = ThreadPriority.Normal;
                    return true;
                }
                catch (Exception e)
                {
                    // if we can't reset it, better just kill it
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Unable to set this thread priority to normal, since we don't want its priority of {Thread.CurrentThread.Priority}, we'll let it exit", e);
                    }
                    return false;
                }
            }

            private void InitializeProcessThreads()
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

                    return;
                }

                _currentUnmangedThreadId = PlatformDetails.GetCurrentThreadId();
                _currentProcess = Process.GetCurrentProcess();

                if (PlatformDetails.RunningOnLinux)
                {
                    // we set the thread affinity by the unmanaged thread id
                    SetLinuxThreadAffinity(_currentProcess.ProcessorAffinity.ToInt64());
                    return;
                }

                foreach (ProcessThread pt in _currentProcess.Threads)
                {
                    if (pt.Id == (uint)_currentUnmangedThreadId)
                    {
                        _currentProcessThread = pt;
                        break;
                    }
                }

                if (_currentProcessThread == null)
                    throw new InvalidOperationException("Unable to get the current process thread: " + _currentUnmangedThreadId + ", this should not be possible");
            }


            private static void ResetCurrentThreadName()
            {
                var t = Thread.CurrentThread;
                var runtimeThread = RuntimeThreadField.GetValue(t);
                ThreadFieldName.SetValue(runtimeThread, null);
            }

            internal void SetThreadAffinity(int numberOfCoresToReduce, long? threadMask)
            {
                if (PlatformDetails.RunningOnMacOsx)
                    return;

                NumberOfCoresToReduce = numberOfCoresToReduce;
                ThreadMask = threadMask;

                if (numberOfCoresToReduce <= 0 && threadMask == null)
                    return;

                _currentProcess.Refresh();
                var currentAffinity = _currentProcess.ProcessorAffinity.ToInt64();
                // we can't reduce the number of cores to a zero or negative number, in this case, just use the processor cores
                if (threadMask == null && Bits.NumberOfSetBits(currentAffinity) <= numberOfCoresToReduce)
                {
                    try
                    {
                        SetThreadAffinityByPlatform(currentAffinity);
                    }
                    catch (PlatformNotSupportedException)
                    {
                        // some platforms don't support it
                        return;
                    }
                    catch (Exception)
                    {
                        // race with the setting of the processor cores? we can live with it, since it has little
                        // impact and should be very rare
                    }
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
                
                try
                {
                    SetThreadAffinityByPlatform(currentAffinity);
                }
                catch (PlatformNotSupportedException)
                {
                    // some platforms don't support it
                    return;
                }
                catch (Exception)
                {
                    // race with the setting of the processor cores? we can live with it, since it has little
                    // impact and should be very rare
                }
            }

            private void SetThreadAffinityByPlatform(long affinity)
            {
                Debug.Assert(PlatformDetails.RunningOnMacOsx == false);

                if (PlatformDetails.RunningOnPosix == false)
                {
                    // windows
                    _currentProcessThread.ProcessorAffinity = new IntPtr(affinity);
                    return;
                }

                if (PlatformDetails.RunningOnLinux)
                {
                    SetLinuxThreadAffinity(affinity);
                }
            }

            private void SetLinuxThreadAffinity(long affinity)
            {
                var ulongAffinity = (ulong)affinity;
                var result = Syscall.sched_setaffinity((int)_currentUnmangedThreadId, new IntPtr(sizeof(ulong)), ref ulongAffinity);
                if (result != 0)
                    throw new InvalidOperationException(
                        $"Failed to set affinity for thread: {_currentUnmangedThreadId}, " +
                        $"affinity: {affinity}, result: {result}, error: {Marshal.GetLastWin32Error()}");
            }
        }
    }
}
