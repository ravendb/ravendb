using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
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
            static FieldInfo _runtimeThreadField;
            static FieldInfo _threadFieldName;

            private ManualResetEvent _waitForWork = new ManualResetEvent(false);
            private Action<object> _action;
            private object _state;
            private string _name;
            private PoolOfThreads _parent;
            private LongRunningWork _workIsDone;
            private ProcessThread _currentProcessThread;
            private Process _currentProcess;

            public DateTime StartedAt { get; internal set; }

            static PooledThread()
            {
                var t = Thread.CurrentThread;
                _runtimeThreadField = typeof(Thread).GetField("_runtimeThread", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                Debug.Assert(_runtimeThreadField != null);
                var runtimeThread = _runtimeThreadField.GetValue(t);
                _threadFieldName = runtimeThread.GetType().GetField("m_Name", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                Debug.Assert(_threadFieldName != null);

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
                        _workIsDone.ManagedThreadId = Thread.CurrentThread.ManagedThreadId;
                        if (_action == null)
                            return; // should only happen when we shutdown

                        ResetCurrentThreadName();
                        Thread.CurrentThread.Name = _name;

                        try
                        {
                            LongRunningWork.Current = _workIsDone;
                            _action(_state);
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
                            return;

                        if (ResetThreadAffinity() == false)
                            return;

                        _waitForWork.Reset();
                        lock (_parent)
                        {
                            if (_parent._disposed)
                                return;

                            _parent._pool.Enqueue(this);
                        }
                    }
                }
                finally
                {
                    NativeMemory.NotifyCurrentThreadAboutToClose();
                }
            }

            private bool ResetThreadAffinity()
            {
                try
                {
                    _currentProcess.Refresh();
                    _currentProcessThread.ProcessorAffinity = _currentProcess.ProcessorAffinity;
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
                var unmanagedThreadId = PlatformDetails.GetCurrentThreadId();
                _currentProcess = Process.GetCurrentProcess();
                foreach (ProcessThread pt in _currentProcess.Threads)
                {
                    if (pt.Id == unmanagedThreadId)
                    {
                        _currentProcessThread = pt;
                        break;
                    }
                }
                if (_currentProcessThread == null)
                    throw new InvalidOperationException("Unable to get the current process thread: " + unmanagedThreadId + ", this should not be possible");
            }


            private void ResetCurrentThreadName()
            {
                var t = Thread.CurrentThread;
                var runtimeThread = _runtimeThreadField.GetValue(t);
                _threadFieldName.SetValue(runtimeThread, null);
            }

            internal void SetThreadAffinity(int numberOfCoresToReduce, long? threadMask)
            {
                if (numberOfCoresToReduce <= 0 && threadMask == null)
                    return;

                _currentProcess.Refresh();
                var currentAffinity = _currentProcess.ProcessorAffinity.ToInt64();
                // we can't reduce the number of cores to a zero or negative number, in this case, just use the processor cores
                if (threadMask == null && Bits.NumberOfSetBits(currentAffinity) <= numberOfCoresToReduce)
                {
                    try
                    {
                        _currentProcessThread.ProcessorAffinity = new IntPtr(currentAffinity);
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
                        // we do that because it is typical that the first cores (0,1, etc) are more 
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
                    _currentProcessThread.ProcessorAffinity = new IntPtr(currentAffinity);
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
        }
    }
}
