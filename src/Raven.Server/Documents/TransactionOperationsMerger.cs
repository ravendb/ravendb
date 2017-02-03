using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    /// <summary>
    /// Merges multiple commands into a single transaction. Any commands that implement IDisposable
    /// will be disposed after the command is executed and transaction is committed
    /// </summary>
    public class TransactionOperationsMerger : IDisposable
    {
        private readonly DocumentDatabase _parent;
        private readonly CancellationToken _shutdown;
        private bool _runTransactions = true;
        private readonly ConcurrentQueue<MergedTransactionCommand> _operations = new ConcurrentQueue<MergedTransactionCommand>();

        private readonly ConcurrentQueue<List<MergedTransactionCommand>> _opsBuffers = new ConcurrentQueue<List<MergedTransactionCommand>>();
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
            public abstract void Execute(DocumentsOperationContext context);
            public TaskCompletionSource<object> TaskCompletionSource = new TaskCompletionSource<object>();
            public Exception Exception;
        }

        /// <summary>
        /// Enqueue the command to be eventually executed. If the command implements
        ///  IDisposable, the command will be disposed after it is run and a tx is committed.
        /// </summary>
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
                while (_runTransactions)
                {
                    if (_operations.Count == 0)
                    {
                        _waitHandle.Wait(_shutdown);
                        _waitHandle.Reset();
                    }

                    MergeTransactionsOnce();
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
                // cautionary, we make sure that stuff that is waiting on the 
                // queue is notified about this catasropic error and we wait
                // just a bit more to verify that nothing racy can still get 
                // there
                for (int i = 0; i < 3; i++)
                {
                    MergedTransactionCommand result;
                    while (_operations.TryDequeue(out result))
                    {
                        result.Exception = e;
                        NotifyOnThreadPool(result);
                    }
                    _waitHandle.Wait(50, _shutdown);
                    _waitHandle.Reset();
                }
            }
        }

        private List<MergedTransactionCommand> GetBufferForPendingOps()
        {
            List<MergedTransactionCommand> pendingOps;
            if (_opsBuffers.TryDequeue(out pendingOps) == false)
            {
                return new List<MergedTransactionCommand>();
            }
            return pendingOps;
        }

        private void DoCommandsNotification(object cmds)
        {
            var pendingOperations = (List<MergedTransactionCommand>)cmds;
            foreach (var op in pendingOperations)
            {
                DoCommandNotification(op);
            }
            pendingOperations.Clear();
            _opsBuffers.Enqueue(pendingOperations);
        }

        private void DoCommandNotification(object op)
        {
            DoCommandNotification((MergedTransactionCommand)op);
        }

        private void DoCommandNotification(MergedTransactionCommand cmd)
        {
            if (cmd.Exception != null)
            {
                cmd.TaskCompletionSource.TrySetException(cmd.Exception);
            }
            else
            {
                cmd.TaskCompletionSource.TrySetResult(null);
            }

        }

        private void MergeTransactionsOnce()
        {
            var pendingOps = GetBufferForPendingOps();
            DocumentsOperationContext context;
            using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                DocumentsTransaction tx = null; ;
                try
                {
                    try
                    {
                        tx = context.OpenWriteTransaction();
                    }
                    catch (Exception e)
                    {
                        MergedTransactionCommand command;
                        if (_operations.TryDequeue(out command))
                        {
                            command.Exception = e;
                            DoCommandNotification(command);
                        }
                        return;
                    }
                    PendingOperations result;
                    try
                    {
                        result = ExecutePendingOperationsInTransaction(pendingOps, context, null);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info(
                                $"Failed to run merged transaction with {pendingOps.Count:#,#}, will retry independently",
                                e);
                        }
                        tx.Dispose();
                        NotifyTransactionFailureAndRerunIndependently(pendingOps, e);
                        return;
                    }

                    switch (result)
                    {
                        case PendingOperations.CompletedAll:
                        case PendingOperations.ModifiedsSystemDocuments:
                            try
                            {
                                tx.Commit();
                                tx.Dispose();
                            }
                            catch (Exception e)
                            {
                                foreach (var op in pendingOps)
                                {
                                    op.Exception = e;
                                }
                            }
                            finally
                            {
                                NotifyOnThreadPool(pendingOps);
                            }
                            return;
                        case PendingOperations.HasMore:
                            MergeTransactionsWithAsycnCommit(context, pendingOps);
                            return;
                        default:
                            Debug.Assert(false, "Should never happen");
                            return;
                    }
                }
                finally
                {
                    tx?.Dispose();
                }
            }
        }

        private void NotifyTransactionFailureAndRerunIndependently(List<MergedTransactionCommand> pendingOps, Exception e)
        {
            if (pendingOps.Count == 1)
            {
                pendingOps[0].Exception = e;
                NotifyOnThreadPool(pendingOps);
                return;
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Error when merging {0} transactions, will try running independently", e);
            }
            RunEachOperationIndependently(pendingOps);
        }

        private void MergeTransactionsWithAsycnCommit(
            DocumentsOperationContext context, 
            List<MergedTransactionCommand> previousPendingOps)
        {
            var previous = context.Transaction;
            try
            {
                while (true)
                {
                    if(_log.IsInfoEnabled)
                        _log.Info($"More pending operations than can handle quickly, started async commit and proceeding concurrently, has {_operations.Count} additional operations");
                    try
                    {
                        context.Transaction = previous.BeginAsyncCommitAndStartNewTransaction();
                    }
                    catch (Exception e)
                    {
                        foreach (var op in previousPendingOps)
                        {
                            op.Exception = e;
                        }
                        NotifyOnThreadPool(previousPendingOps);
                        return;
                    }
                    try
                    {
                        var currentPendingOps = GetBufferForPendingOps();
                        PendingOperations result;
                        try
                        {
                            result = ExecutePendingOperationsInTransaction(
                                currentPendingOps, context,
                                previous.InnerTransaction.LowLevelTransaction.AsyncCommit);
                            CompletePreviousTransction(previous, previousPendingOps, throwOnError: true);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(
                                    $"Failed to run merged transaction with {currentPendingOps.Count:#,#} operations in async manner, will retry independently",
                                    e);
                            }
                            CompletePreviousTransction(previous, previousPendingOps,
                                // if this previous threw, it won't throw again
                                throwOnError: false);
                            previous.Dispose();
                            context.Transaction.Dispose();
                            NotifyTransactionFailureAndRerunIndependently(currentPendingOps, e);
                            return;
                        }
                        previous.Dispose();

                        switch (result)
                        {
                            case PendingOperations.CompletedAll:
                            case PendingOperations.ModifiedsSystemDocuments:
                                try
                                {
                                    context.Transaction.Commit();
                                    context.Transaction.Dispose();
                                }
                                catch (Exception e)
                                {
                                    foreach (var op in currentPendingOps)
                                    {
                                        op.Exception = e;
                                    }
                                }
                                NotifyOnThreadPool(currentPendingOps);
                                return;
                            case PendingOperations.HasMore:
                                previousPendingOps = currentPendingOps;
                                previous = context.Transaction;
                                context.Transaction = null;
                                break;
                            default:
                                Debug.Assert(false);
                                return;
                        }

                    }
                    finally
                    {
                        context.Transaction?.Dispose();
                    }
                }
            }
            finally
            {
                previous.Dispose();
            }
        }

        private void CompletePreviousTransction(
            RavenTransaction previous, 
            List<MergedTransactionCommand> previousPendingOps,
            bool throwOnError)
        {
            try
            {
                previous.EndAsyncCommit();
                NotifyOnThreadPool(previousPendingOps);
            }
            catch (Exception e)
            {
                foreach (var op in previousPendingOps)
                {
                    op.Exception = e;
                }
                NotifyOnThreadPool(previousPendingOps);
                if (throwOnError)
                    throw;
            }
        }

        private enum PendingOperations
        {
            CompletedAll,
            ModifiedsSystemDocuments,
            HasMore
        }
        int _maxTimeToWait = 1000;

        private PendingOperations ExecutePendingOperationsInTransaction(
            List<MergedTransactionCommand> pendingOps, 
            DocumentsOperationContext context, 
            Task previousOperation)
        {
            var sp = Stopwatch.StartNew();
            do
            {
                MergedTransactionCommand op;
                if (_operations.TryDequeue(out op) == false)
                    break;
                pendingOps.Add(op);
                op.Execute(context);

                if (sp.ElapsedMilliseconds > _maxTimeToWait)
                {
                    if (previousOperation != null)
                    {
                        _maxTimeToWait += 10;
                        if (previousOperation.IsCompleted)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info($"Stopping merged operations because previous transaction async commit completed. Took {sp.Elapsed} with {pendingOps.Count} operations and {_operations.Count} remaining operations");
                            }
                            _maxTimeToWait -= 10;
                            return GetPendingOperationsStatus(context);
                        }

                        continue;
                    }
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Stopping merged operations because {sp.Elapsed} passed {pendingOps.Count} operations and {_operations.Count} remaining operations");
                    }
                    return GetPendingOperationsStatus(context);
                }
            } while (true);
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Merged {pendingOps.Count} operations in {sp.Elapsed} and there is no more work");
            }
            return PendingOperations.CompletedAll;
        }

        private  PendingOperations GetPendingOperationsStatus(DocumentsOperationContext context)
        {
            if(context.Transaction.ModifiedSystemDocuments)
                // a transaction that modified system documents may cause us to 
                // do certain actions (for example, initialize trees for versioning)
                // which we can't realy do if we are starting another transaction
                // immediately. This way, we skip this optimization for this
                // kind of work
                return PendingOperations.ModifiedsSystemDocuments;

            return _operations.Count == 0
                ? PendingOperations.CompletedAll
                : PendingOperations.HasMore;
        }

        private void NotifyOnThreadPool(MergedTransactionCommand cmd)
        {
            if (ThreadPool.QueueUserWorkItem(DoCommandNotification, cmd) == false)
            {
                // if we can't schedule it, run it inline
                DoCommandNotification(cmd);
            }
        }


        private void NotifyOnThreadPool(List<MergedTransactionCommand> cmds)
        {
            if (ThreadPool.QueueUserWorkItem(DoCommandsNotification, cmds) == false)
            {
                // if we can't schedule it, run it inline
                DoCommandsNotification(cmds);
            }
        }


        private void RunEachOperationIndependently(List<MergedTransactionCommand> pendingOps)
        {
            try
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
                                op.Execute(context);
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
            finally
            {
                pendingOps.Clear();
                _opsBuffers.Enqueue(pendingOps);
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