using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Constants = Voron.Global.Constants;

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
    public sealed class PoolOfThreads : IDisposable, ILowMemoryHandler
    {
        private static readonly Lazy<PoolOfThreads> _globalRavenThreadPool = new(() => new PoolOfThreads());

        public static PoolOfThreads GlobalRavenThreadPool => _globalRavenThreadPool.Value;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<PoolOfThreads>("Server");

        public int TotalNumberOfThreads;

        private readonly SharedMultipleUseFlag _lowMemoryFlag = new SharedMultipleUseFlag();

        public PoolOfThreads()
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);

            _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        }

        private readonly CountingConcurrentStack<PooledThread> _pool = new();
        private bool _disposed;
        private readonly Timer _cleanupTimer;

        public void Dispose()
        {
            lock (this)
            {
                _disposed = true;

                try
                {
                    _cleanupTimer?.Dispose();
                }
                catch
                {
                    // ignored
                }
            }

            Clear();
        }

        public sealed class LongRunningWork
        {
            [ThreadStatic] public static LongRunningWork Current;
            [ThreadStatic] internal static PooledThread CurrentPooledThread;
            private ManualResetEvent _manualResetEvent;

            public NativeMemory.ThreadStats CurrentThreadStats
                // this is initialize when the thread starts work,
                // setting to to empty value avoid complex null / race conditions
                = NativeMemory.ThreadStats.Empty;

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

        public LongRunningWork LongRunning(Action<object> action, object state, ThreadNames.ThreadInfo threadInfo)
        {
            var nameToUse = ThreadNames.GetNameToUse(threadInfo);

            if (_pool.TryPop(out var pooled) == false)
            {
                MemoryInformation.AssertNotAboutToRunOutOfMemory();

                pooled = new PooledThread(this);
                var thread = new Thread(pooled.Run, PlatformDetails.Is32Bits ? 512 * Constants.Size.Kilobyte : 0)
                {
                    Name = nameToUse,
                    IsBackground = true,
                };

                thread.Start();
            }

            pooled.StartedAt = DateTime.UtcNow;
            pooled.InPoolSince = DateTime.MinValue;
            return pooled.SetWorkForThread(action, state, threadInfo, nameToUse);
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            if (_lowMemoryFlag.Raise())
                Clear();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }

        private static void ReleaseThread(PooledThread pooled)
        {
            pooled.InPoolSince = DateTime.MinValue;
            pooled.SetWorkForThread(null, null, null, null);
        }

        private void Clear()
        {
            while (_pool.TryPop(out var pooled))
                ReleaseThread(pooled);
        }

        private void Cleanup(object _)
        {
            const int minNumberOfItemsInPoolToPerformCleanupCheck = 64;

            if (_disposed)
                return;

            if (_pool.Count < minNumberOfItemsInPoolToPerformCleanupCheck)
                return;

            try
            {
                var currentTime = DateTime.UtcNow;
                var idleTime = TimeSpan.FromMinutes(5);

                var numberOfThreadsToRelease = 0;
                using (var enumerator = _pool.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        var pooled = enumerator.Current;

                        var timeInPool = currentTime - pooled.InPoolSince;
                        if (timeInPool <= idleTime)
                            continue;

                        numberOfThreadsToRelease++;
                    }
                }

                if (numberOfThreadsToRelease < minNumberOfItemsInPoolToPerformCleanupCheck / 2)
                    return;

                lock (this)
                {
                    if (_disposed)
                        return;

                    var localPool = new CountingConcurrentStack<PooledThread>();
                    while (_pool.TryPop(out var pooled))
                    {
                        var timeInPool = currentTime - pooled.InPoolSince;
                        if (timeInPool <= idleTime)
                        {
                            localPool.Push(pooled);
                            continue;
                        }

                        ReleaseThread(pooled);
                    }

                    while (localPool.TryPop(out var pooled))
                        _pool.Push(pooled);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Error during cleanup.", e);
            }
        }

        internal sealed class PooledThread : PooledItem
        {
            private static readonly FieldInfo ThreadFieldName;

            private readonly ManualResetEvent _waitForWork = new ManualResetEvent(false);
            private Action<object> _action;
            private object _state;
            private string _name;
            private ThreadNames.ThreadInfo _threadInfo;
            private readonly PoolOfThreads _parent;
            private LongRunningWork _workIsDone;

            public Process CurrentProcess { get; private set; }
            public ulong CurrentUnmanagedThreadId { get; private set; }
            public ProcessThread CurrentProcessThread { get; private set; }

            public int NumberOfCoresToReduce { get; private set; }
            public long? ThreadMask { get; private set; }

            public DateTime StartedAt { get; internal set; }

            static PooledThread()
            {
                ThreadFieldName = typeof(Thread).GetField("_name", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                Debug.Assert(ThreadFieldName != null);
            }

            public PooledThread(PoolOfThreads pool)
            {
                _parent = pool;
            }

            public LongRunningWork SetWorkForThread(Action<object> action, object state, ThreadNames.ThreadInfo threadInfo, string nameToUse)
            {
                _action = action;
                _state = state;
                _threadInfo = threadInfo;
                _name = nameToUse;

                Debug.Assert(_workIsDone == null, "_workIsDone must be null here");
                var workIsDone = new LongRunningWork(new ManualResetEvent(false), nameToUse);

                _workIsDone = workIsDone;
                _waitForWork.Set();
                return workIsDone;
            }

            public void Run()
            {
                try
                {
                    Interlocked.Increment(ref _parent.TotalNumberOfThreads);
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
                    Interlocked.Decrement(ref _parent.TotalNumberOfThreads);
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
                ThreadNames.AddFullThreadName(_workIsDone.ManagedThreadId, _threadInfo.FullName);
                Thread.CurrentThread.Name = _name;

                try
                {
                    _workIsDone.CurrentThreadStats = NativeMemory.CurrentThreadStats;
                    LongRunningWork.Current = _workIsDone;
                    _action(_state);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                    {
                        Logger.Operations($"An uncaught exception occurred in '{_threadInfo.FullName}' and killed the process", e);
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

                ThreadNames.RemoveFullThreadName(Thread.CurrentThread.ManagedThreadId);
                NativeMemory.CurrentThreadStats.CurrentlyAllocatedForProcessing = 0;

                ThreadLocalCleanup.Run();

                ResetCurrentThreadName();
                Thread.CurrentThread.Name = "Available Pool Thread";


                var resetThread = ResetThreadPriority();
                resetThread &= ResetThreadAffinity();
                if (resetThread == false)
                    return false;

                _waitForWork.Reset();
                lock (_parent)
                {
                    if (_parent._disposed)
                        return false;

                    if (_parent._lowMemoryFlag.IsRaised())
                        return false;

                    InPoolSince = DateTime.UtcNow;
                    _parent._pool.Push(this);
                }

                return true;
            }

            private bool ResetThreadAffinity()
            {
                NumberOfCoresToReduce = 0;
                ThreadMask = null;

                return AffinityHelper.ResetThreadAffinity(this);
            }

            private static bool ResetThreadPriority()
            {
                var currentPriority = ThreadHelper.GetThreadPriority();
                if (currentPriority != ThreadPriority.Normal)
                {
                    if (ThreadHelper.TrySetThreadPriority(ThreadPriority.Normal, null, Logger) == false)
                    {
                        // if we can't reset it, better just kill it
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info($"Unable to set this thread priority to normal, since we don't want its priority of {currentPriority}, we'll let it exit");
                        }

                        return false;
                    }
                }

                return true;
            }

            private void InitializeProcessThreads()
            {
                if (PlatformDetails.RunningOnMacOsx)
                    return;

                CurrentUnmanagedThreadId = NativeMemory.GetCurrentUnmanagedThreadId.Invoke();
                CurrentProcess = Process.GetCurrentProcess();

                if (PlatformDetails.RunningOnPosix == false)
                {
                    foreach (ProcessThread pt in CurrentProcess.Threads)
                    {
                        if (pt.Id == (uint)CurrentUnmanagedThreadId)
                        {
                            CurrentProcessThread = pt;
                            break;
                        }

                        // Need to dispose this explicitly, otherwise it blocks the finalizer
                        pt.Dispose();
                    }

                    if (CurrentProcessThread == null)
                        throw new InvalidOperationException("Unable to get the current process thread: " + CurrentUnmanagedThreadId + ", this should not be possible");
                }

                AffinityHelper.ResetThreadAffinity(this);
            }

            public static void ResetCurrentThreadName()
            {
                var t = Thread.CurrentThread;
                ThreadFieldName.SetValue(t, null);
            }

            internal void SetThreadAffinity(int numberOfCoresToReduce, long? threadMask)
            {
                if (PlatformDetails.RunningOnMacOsx)
                    return;

                NumberOfCoresToReduce = numberOfCoresToReduce;
                ThreadMask = threadMask;

                AffinityHelper.SetCustomThreadAffinity(this);
            }

            public override void Dispose()
            {
            }
        }
    }
}
