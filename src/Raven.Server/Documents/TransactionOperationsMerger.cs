using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class TransactionOperationsMerger : IDisposable
    {
        private readonly DocumentDatabase _parent;
        private readonly CancellationToken _shutdown;
        private bool _runTransactions = true;
        private readonly ConcurrentQueue<QueuedOperation> _operations = new ConcurrentQueue<QueuedOperation>();
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        private ExceptionDispatchInfo _edi;
        private readonly Logger _log;
        private Thread _txMergingThread;

        public TransactionOperationsMerger(DocumentDatabase parent, CancellationToken shutdown)
        {
            _parent = parent;
            _log = _parent.LoggerSetup.GetLogger<TransactionOperationsMerger>(_parent.Name);
            _shutdown = shutdown;
        }

        public void Start()
        {
            _txMergingThread = new Thread(MergeOperationThreadProc)
            {
                IsBackground = true,
                Name = _parent.Name + " transaction merging thread"
            };
            _txMergingThread.Start();
        }

        private class QueuedOperation
        {
            public string Key;
            public long? ExpectedEtag;
            public BlittableJsonReaderObject Document;
            public PutResult PutResult;
            public TaskCompletionSource<PutResult> Task;

        }

        public Task<PutResult> EnqueuePut(string key, long? expectedEtag, BlittableJsonReaderObject document)
        {
            _edi?.Throw();


            var op = new QueuedOperation
            {
                Document = document,
                ExpectedEtag = expectedEtag,
                Key = key,
                Task = new TaskCompletionSource<PutResult>()
            };
            _operations.Enqueue(op);
            _waitHandle.Set();

            return op.Task.Task;
        }

        private void MergeOperationThreadProc()
        {
            try
            {
                while (_runTransactions)
                {
                    _waitHandle.Wait(_shutdown);
                    _waitHandle.Reset();

                    var pending = new List<QueuedOperation>();
                    QueuedOperation op;
                    if (_operations.TryDequeue(out op) == false)
                        continue;
                    pending.Add(op);
                    try
                    {
                        DocumentsOperationContext context;
                        using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                        {
                            using (var tx = context.OpenWriteTransaction())
                            {
                                var sp = Stopwatch.StartNew();
                                do
                                {
                                    _parent.Metrics.DocPutsPerSecond.Mark();
                                    op.PutResult = _parent.DocumentsStorage.Put(context, op.Key, op.ExpectedEtag, op.Document);
                                } while (
                                    sp.ElapsedMilliseconds < 150 &&
                                    _operations.TryDequeue(out op));
                                tx.Commit();
                            }
                        }
                        Task.Factory.StartNew(state =>
                        {
                            foreach (var completedOp in ((List<QueuedOperation>)state))
                            {
                                completedOp.Task.TrySetResult(completedOp.PutResult);
                            }
                        },pending, _shutdown);
                    }
                    catch(Exception e)
                    {
                        Task.Factory.StartNew(state =>
                        {
                            foreach (var completedOp in ((List<QueuedOperation>)state))
                            {
                                completedOp.Task.TrySetException(e);
                            }
                        }, pending, _shutdown);
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                {
                    _log.Operations(
                        "Serious failure in transaction merging thread, the database must be restarted!",
                        e);
                }
                Interlocked.Exchange(ref _edi, ExceptionDispatchInfo.Capture(e));
            }
        }


        public void Dispose()
        {
            _runTransactions = false;
            _waitHandle.Set();
            _txMergingThread?.Join();
        }
    }
}