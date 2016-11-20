using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class TransactionOperationsMerger : IDisposable
    {
        private readonly DocumentDatabase _parent;
        private readonly CancellationToken _shutdown;
        private bool _runTransactions = true;
        private readonly ConcurrentQueue<MergedTransactionCommand> _operations = new ConcurrentQueue<MergedTransactionCommand>();
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        private ExceptionDispatchInfo _edi;
        private readonly Logger _log;
        private Thread _txMergingThread;

        public TransactionOperationsMerger(DocumentDatabase parent, CancellationToken shutdown)
        {
            _parent = parent;
            _log = LoggingSource.Instance.GetLogger<TransactionOperationsMerger>(_parent.Name);
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

        public abstract class MergedTransactionCommand
        {
            public abstract void Execute(DocumentsOperationContext context, RavenTransaction tx);
            public TaskCompletionSource<object> TaskCompletionSource = new TaskCompletionSource<object>();
            public Exception Exception;
        }

        public Task Enqueue(MergedTransactionCommand cmd)
        {
            _edi?.Throw();

            _operations.Enqueue(cmd);
            _waitHandle.Set();

            return cmd.TaskCompletionSource.Task;
        }

        private void MergeOperationThreadProc()
        {
            try
            {
                var pendingOps = new List<MergedTransactionCommand>();
                while (_runTransactions)
                {
                    if (_operations.Count == 0)
                    {
                        _waitHandle.Wait(_shutdown);
                        _waitHandle.Reset();
                    }

                    try
                    {
                        if (MergeTransactionsOnce(pendingOps))
                        {
                            foreach (var op in pendingOps)
                            {
                                NotifyOnThreadPool(op);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        foreach (var op in pendingOps)
                        {
                            op.Exception = e;
                            NotifyOnThreadPool(op);
                        }
                    }
                    finally
                    {
                        pendingOps.Clear();
                    }
                }

            }
            catch (OperationCanceledException)
            {
                // clean shutdown, nothing to do
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

        private void DoCommandNotification(object op)
        {
            var cmd = (MergedTransactionCommand)op;
            if (cmd.Exception != null)
            {
                cmd.TaskCompletionSource.TrySetException(cmd.Exception);
            }
            else
            {
                cmd.TaskCompletionSource.TrySetResult(null);
            }
        }

        private bool MergeTransactionsOnce(List<MergedTransactionCommand> pendingOps)
        {
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
                            MergedTransactionCommand op;
                            if (_operations.TryDequeue(out op) == false)
                                break;
                            pendingOps.Add(op);
                            op.Execute(context, tx);

                            if (pendingOps.Count % 128 != 0)
                                continue;
                            if (sp.ElapsedMilliseconds < 150)
                                break;
                        } while (true);
                        tx.Commit();
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                if (pendingOps.Count == 1)
                {
                    pendingOps[0].Exception = e;
                    NotifyOnThreadPool(pendingOps[0]);
                    return false;
                }
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Error when merging {0} transactions, will try running independently", e);
                }
                RunEachOperationIndependently(pendingOps);
                return false;
            }
        }

        private void NotifyOnThreadPool(MergedTransactionCommand cmd)
        {
            if (ThreadPool.QueueUserWorkItem(DoCommandNotification, cmd) == false)
            {
                // if we can't schedule it, run it inline
                DoCommandNotification(cmd);
            }
        }

        private void RunEachOperationIndependently(List<MergedTransactionCommand> pendingOps)
        {
            foreach (var op in pendingOps)
            {
                try
                {
                    DocumentsOperationContext context;
                    using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            op.Execute(context, tx);
                            tx.Commit();
                        }
                    }
                    DoCommandNotification(op);
                }
                catch (Exception e)
                {
                    op.Exception = e;
                    NotifyOnThreadPool(op);
                }
            }
        }

        public void Dispose()
        {
            _runTransactions = false;
            _waitHandle.Set();
            _txMergingThread?.Join();
            _waitHandle.Dispose();
        }

    }
}