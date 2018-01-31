using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using Sparrow.Logging;
using Sparrow.LowMemory;
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

        public LongRunningWork LongRunning(Action<object> action, object state, string name, ThreadPriority priority = ThreadPriority.Normal)
        {
            if (_pool.TryDequeue(out var pooled) == false)
            {
                // we are about to create a new thread, might not always be a good idea:
                // https://ayende.com/blog/181537-B/production-test-run-overburdened-and-under-provisioned
                // https://ayende.com/blog/181569-A/threadpool-vs-pool-thread

                var memInfo = MemoryInformation.GetMemoryInfo();
                var overage = memInfo.CurrentCommitCharge * _minimumFreeCommittedMemory;
                if (overage >= memInfo.TotalCommittableMemory)
                {
                    throw new InsufficientExecutionStackException($"The amount of available memory to commit on the system is low. Commit charge: {memInfo.CurrentCommitCharge} / {memInfo.TotalCommittableMemory}." +
                        $" Will not create a new thread in this situation because it may result in a stack overflow error when trying to allocate stack space but there isn't sufficient memory for that.");
                }

                pooled = new PooledThread(this);
                var thread = new Thread(pooled.Run)
                {
                    Name = name,
                    IsBackground = true,
                    Priority = priority
                };

                thread.Start();
            }
            pooled.StartedAt = DateTime.UtcNow;
            return pooled.SetWorkForThread(action, state, name, priority);
        }

        private class PooledThread
        {
            static FieldInfo _runtimeThreadField;
            static FieldInfo _threadFieldName;



            private ManualResetEvent _waitForWork = new ManualResetEvent(false);
            private Action<object> _action;
            private object _state;
            private string _name;
            private PoolOfThreads _parent;
            private LongRunningWork _workIsDone;
            private ThreadPriority _priority;

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

            public LongRunningWork SetWorkForThread(Action<object> action, object state, string name, ThreadPriority priority = ThreadPriority.Normal)
            {
                _action = action;
                _state = state;
                _name = name;
                _priority = priority;
                _workIsDone = new LongRunningWork(new ManualResetEvent(false), name);

                _waitForWork.Set();
                return _workIsDone;
            }

            public void Run()
            {
                while (true)
                {
                    ResetCurrentThreadName();
                    try
                    {
                        if(Thread.CurrentThread.Priority != _priority)
                            Thread.CurrentThread.Priority = _priority;
                    }
                    catch (Exception e)
                    {
                        // if we can't reset it, better just kill it
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Unable to set this thread priority to normal, since we don't want its priority of {Thread.CurrentThread.Priority}, we'll let it exit", e);
                        }
                        return;
                    }
                    Thread.CurrentThread.Name = "Available Pool Thread";

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

                    _waitForWork.Reset();
                    lock (_parent)
                    {
                        if (_parent._disposed)
                            return;

                        _parent._pool.Enqueue(this);
                    }
                }
            }



            private void ResetCurrentThreadName()
            {
                var t = Thread.CurrentThread;
                var runtimeThread = _runtimeThreadField.GetValue(t);
                _threadFieldName.SetValue(runtimeThread, null);
            }
        }
    }
}
