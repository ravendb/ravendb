using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
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
        internal static readonly Lazy<PoolOfThreads> GlobalRavenThreadPool = new Lazy<PoolOfThreads>(() =>
        {
            return new PoolOfThreads();
        });


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

            public LongRunningWork(ManualResetEvent manualResetEvent)
            {
                this._manualResetEvent = manualResetEvent;
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
                _workIsDone = new LongRunningWork(new ManualResetEvent(false));

                _waitForWork.Set();
                return _workIsDone;
            }

            public void Run()
            {
                while (true)
                {
                    ResetCurrentThreadName();
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
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
