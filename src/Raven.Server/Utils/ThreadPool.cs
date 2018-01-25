using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Backup;

namespace Raven.Server.Utils
{
    public class RavenThreadPool : IDisposable
    {
        internal static readonly Lazy<RavenThreadPool> GlobalRavenThreadPool = new Lazy<RavenThreadPool>(() =>
        {
            return new RavenThreadPool();
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
            pooled.StartedAt = DateTime.Now;
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
            private RavenThreadPool _parent;
            private LongRunningWork _workIsDone;
            protected Logger _logger;

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

            public PooledThread(RavenThreadPool pool)
            {                
                _parent = pool;
                _logger = LoggingSource.Instance.GetLogger<PooledThread>("PooledThread");
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


            private void ReduceMemoryUsage()
            {                              
                // not sure about all of this "cleaning", I suspect that most of it will actually harm performance and make GC work harder...
                ByteStringMemoryCache.CleanForCurrentThread();

                using (var docsContextPool = new DocumentsContextPool(null))
                    docsContextPool.Clean();

                using (var transactionContextPool = new TransactionContextPool(null))                
                    transactionContextPool.Clean();

                using (var jsonContextPool = new JsonContextPool())
                    jsonContextPool.Clean();
                                
                new Tree(true).CleanLocalBuffer();

                StreamExtensions.CleanBuffer();

                ValueReader.CleanBuffer();

                BlittableWriter<UnmanagedWriteBuffer>.CleanPropertyArrayOffset();
                BlittableWriter<UnmanagedStreamBuffer>.CleanPropertyArrayOffset();

                LazyStringValue.CleanBuffers();

                CaseInsensitiveStringSegmentEqualityComparer.CleanBuffer();

                BatchRequestParser.CleanCache();

                JsBlittableBridge.CleanCache();

                DocumentIdWorker.CleanCache();
                Raven.Server.Json.BlittableJsonTextWriterExtensions.CleanCache();

                ChangeVectorUtils.CleanCache();


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
                    }
                    _action = null;
                    _state = null;
                    _workIsDone = null;

                    ReduceMemoryUsage();
                    //TODO: Clear _ALL_ the thread local state, such as context, json pool, etc

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
