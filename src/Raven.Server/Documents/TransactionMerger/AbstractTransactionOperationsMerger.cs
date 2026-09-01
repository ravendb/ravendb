using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Server.Logging;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Meters;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Journal;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.TransactionMerger
{
    /// <summary>
    /// Merges multiple commands into a single transaction. Any commands that implement IDisposable
    /// will be disposed after the command is executed and transaction is committed
    /// </summary>
    public abstract partial class AbstractTransactionOperationsMerger<TOperationContext, TTransaction> : IDisposable
        where TOperationContext : TransactionOperationContext<TTransaction>
        where TTransaction : RavenTransaction
    {
        private readonly string _resourceName;
        private JsonContextPoolBase<TOperationContext> _contextPool;
        private readonly RavenConfiguration _configuration;
        private readonly SystemTime _time;
        private readonly CancellationToken _shutdown;
        private bool _runTransactions = true;
        private readonly ConcurrentQueue<MergedTransactionCommand<TOperationContext, TTransaction>> _operations = new();
        private readonly CountdownEvent _concurrentOperations = new(1);
        private StorageEnvironment _env;

        private readonly ConcurrentQueue<List<MergedTransactionCommand<TOperationContext, TTransaction>>> _opsBuffers = new();

        private sealed class AsyncCommittedTransaction
        {
            public TOperationContext Context;
            public IDisposable ReturnContext;
            public List<MergedTransactionCommand<TOperationContext, TTransaction>> PendingOps;
        }

        private sealed class AsyncCommitCompletionPump(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> parent)
        {
            private readonly Queue<AsyncCommittedTransaction> _inFlight = new();
            private ExceptionDispatchInfo _failure;

            public bool HasPendingCompletions => _inFlight.Count > 0;

            private Task _headDurableCommit;

            public bool OldestTransactionIsDurable => _headDurableCommit is { IsCompleted: true };

            private void UpdateHeadDurableCommit()
            {
                _headDurableCommit = _inFlight.TryPeek(out var head)
                    ? head.Context.Transaction.InnerTransaction.LowLevelTransaction.DurableCommit
                    : null;
            }

            public void Release(AsyncCommittedTransaction entry)
            {
                _inFlight.Enqueue(entry);
                if (_inFlight.Count == 1)
                    UpdateHeadDurableCommit();

                if (entry.Context.Transaction.InnerTransaction.LowLevelTransaction.DurableCommit is not { IsCompleted: false })
                    DrainCompleted(); // can be completed inline, if the durable commit is already done, otherwise we wait for the completion
            }

            public void DrainCompleted()
            {
                while (_inFlight.TryPeek(out var head)) // in commit order, so no tx can be raised before its predecessor is completed
                {
                    if (_failure == null &&
                        head.Context.Transaction.InnerTransaction.LowLevelTransaction.DurableCommit is { IsCompleted: false })
                        break; // its ack wakes us again

                    Complete(_inFlight.Dequeue());
                }

                UpdateHeadDurableCommit();
            }

            public void DrainAll()
            {
                while (_inFlight.Count > 0)
                    Complete(_inFlight.Dequeue());

                _headDurableCommit = null;
            }

            private void Complete(AsyncCommittedTransaction entry)
            {
                // once one of them failed, all the following ones failed, a journal write failure will throw on EndAsyncCommit
                var error = _failure?.SourceException;

                try
                {
                    parent._recording.State?.TryRecord(entry.Context, TxInstruction.EndAsyncCommit);
                    entry.Context.Transaction.EndAsyncCommit();

                    if (parent._log.IsDebugEnabled)
                        parent._log.Debug($"EndAsyncCommit on {entry.Context.Transaction.InnerTransaction.LowLevelTransaction.Id}");

                    parent._recording.State?.TryRecord(entry.Context, TxInstruction.DisposePrevTx, entry.Context.Disposed == false);
                }
                catch (Exception e)
                {
                    error ??= e;
                    _failure ??= ExceptionDispatchInfo.Capture(e);
                }

                try
                {
                    entry.Context.Transaction.Dispose();
                    entry.ReturnContext?.Dispose();
                }
                catch (Exception e)
                {
                    error ??= e;
                    _failure ??= ExceptionDispatchInfo.Capture(e);
                }

                if (error != null)
                {
                    foreach (var op in entry.PendingOps)
                    {
                        op.Exception = error;
                    }
                }

                try
                {
                    parent.NotifyOnThreadPool(entry.PendingOps);
                }
                catch (Exception e)
                {
                    _failure ??= ExceptionDispatchInfo.Capture(e);
                }
            }

            public void ThrowOnFailure()
            {
                _failure?.Throw();
            }
        }

        private readonly AsyncCommitCompletionPump _completionPump;

        private readonly ManualResetEventSlim _waitHandle = new(false);
        private ExceptionDispatchInfo _edi;
        private readonly RavenLogger _log;
        private PoolOfThreads.LongRunningWork _txLongRunningOperation;

        private readonly double _maxTimeToWaitForPreviousTxInMs;
        private readonly long _maxTxSizeInBytes;

        private double GetBatchingWindowDurationInMs()
        {
            if (_is32Bits)
                return _maxTimeToWaitForPreviousTxInMs;

            return _env.WriteFlow.GetBatchingWindowDurationInMs(_maxTimeToWaitForPreviousTxInMs);
        }
        private readonly double _maxTimeToWaitForPreviousTxBeforeRejectingInMs;

        private bool _isEncrypted;
        private bool _is32Bits;

        private bool _initialized;

        protected AbstractTransactionOperationsMerger(
            [NotNull] string resourceName,
            [NotNull] RavenConfiguration configuration,
            [NotNull] SystemTime time,
            [NotNull] RavenLogger logger,
            CancellationToken shutdown)
        {
            _resourceName = resourceName ?? throw new ArgumentNullException(nameof(resourceName));
            _log = logger ?? throw new ArgumentNullException(nameof(logger));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _time = time ?? throw new ArgumentNullException(nameof(time));
            _shutdown = shutdown;

            _maxTimeToWaitForPreviousTxInMs = configuration.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTx.AsTimeSpan.TotalMilliseconds;
            _maxTxSizeInBytes = configuration.TransactionMergerConfiguration.MaxTxSize.GetValue(SizeUnit.Bytes);
            _maxTimeToWaitForPreviousTxBeforeRejectingInMs = configuration.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTxBeforeRejecting.AsTimeSpan.TotalMilliseconds;
            _timeToCheckHighDirtyMemory = configuration.Memory.TemporaryDirtyMemoryChecksPeriod;
            _lastHighDirtyMemCheck = time.GetUtcNow();
            _completionPump = new AsyncCommitCompletionPump(this);
        }
        
        public void Initialize([NotNull] JsonContextPoolBase<TOperationContext> contextPool, bool isEncrypted, bool is32Bits)
        {
            _contextPool = contextPool ?? throw new ArgumentNullException(nameof(contextPool));
            _isEncrypted = isEncrypted;
            _is32Bits = is32Bits;
            _env = GetStorageEnvironment(contextPool);
            _env.DurableCommitAcknowledged = _waitHandle.Set; // raise the event when the journal write is durable, so we can drain the async commit queue
            _initialized = true;
        }

        protected abstract StorageEnvironment GetStorageEnvironment(JsonContextPoolBase<TOperationContext> contextPool);

        internal abstract TTransaction BeginAsyncCommitAndStartNewTransaction(TTransaction previousTransaction, TOperationContext currentContext);

        public DatabasePerformanceMetrics GeneralWaitPerformanceMetrics = new(DatabasePerformanceMetrics.MetricType.GeneralWait, 256, 1);
        public DatabasePerformanceMetrics TransactionPerformanceMetrics = new(DatabasePerformanceMetrics.MetricType.Transaction, 256, 8);

        public int NumberOfQueuedOperations => _operations.Count;

        private string TransactionMergerThreadName => $"'{_resourceName}' TxMT";

        public void Start()
        {
            _txLongRunningOperation = PoolOfThreads.GlobalRavenThreadPool.LongRunning(o => MergeOperationThreadProc(), null, ThreadNames.ForTransactionMerging(TransactionMergerThreadName, _resourceName));
        }

        /// <summary>
        /// Enqueue the command to be eventually executed.
        /// </summary>
        public async Task Enqueue(MergedTransactionCommand<TOperationContext, TTransaction> cmd)
        {
            Debug.Assert(cmd.TaskCompletionSource.Task.IsCompleted == false, $"{cmd.GetType()} is already completed");
            Debug.Assert(Thread.CurrentThread.ManagedThreadId != _txLongRunningOperation.ManagedThreadId, $"Cannot Enqueue \"{cmd.GetType()}\" into the TxMerger from its dedicated thread.");

            if (_initialized == false)
                throw new InvalidOperationException($"Tx Merger for '{_resourceName}' is not initialized.");

            _edi?.Throw();
            _operations.Enqueue(cmd);
            _waitHandle.Set();

            if (_concurrentOperations.TryAddCount() == false)
                ThrowTxMergerWasDisposed();

            try
            {
                await cmd.TaskCompletionSource.Task.ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    _concurrentOperations.Signal(); // done with this
                }
                catch (InvalidOperationException)
                {
                    // Expected: "Invalid attempt made to decrement the event's count below zero."
                }
            }
        }

        public void EnqueueSync(MergedTransactionCommand<TOperationContext, TTransaction> cmd)
        {
            Enqueue(cmd).GetAwaiter().GetResult();
        }

        [DoesNotReturn]
        private static void ThrowTxMergerWasDisposed()
        {
            throw new ObjectDisposedException("Transaction Merger");
        }

        private void MergeOperationThreadProc()
        {
            ThreadHelper.TrySetThreadPriority(ThreadPriority.AboveNormal, TransactionMergerThreadName, _log);

            var oomTimer = new Stopwatch();// this is allocated here to avoid OOM when using it
            
            while (true) // this is actually only executed once, except if we are trying to recover from OOM errors
            {
                NativeMemory.EnsureRegistered();
                try
                {
                    while (_runTransactions)
                    {
                        _recording.State?.Prepare(ref _recording.State);

                        if (_operations.IsEmpty && _env.Journal.HasBranchCommits is false)
                        {
                            if (_isEncrypted)
                            {
                                using (_contextPool.AllocateOperationContext(out TOperationContext ctx))
                                {
                                    using (ctx.OpenWriteTransaction())
                                    {
                                        var llt = ctx.Transaction.InnerTransaction.LowLevelTransaction;
                                        var waj = ctx.Environment.Options.Encryption.WriteAheadJournal;

                                        waj.ZeroCompressionBuffer(ref llt.PagerTransactionState);
                                    }
                                }
                            }

                            using (var generalMeter = GeneralWaitPerformanceMetrics.MeterPerformanceRate())
                            {
                                generalMeter.IncrementCounter(1);
                                _waitHandle.Wait(_shutdown);
                            }
                            _waitHandle.Reset();
                        }

                        MergeTransactionsOnce();
                    }
                    return;
                }
                catch (OperationCanceledException)
                {
                    // clean shutdown, nothing to do
                }
                catch (Exception e) when (e is EarlyOutOfMemoryException || e is OutOfMemoryException)
                {
                    // this catch block is meant to handle potentially transient errors
                    // in particular, an OOM error is something that we want to recover
                    // we'll handle this by throwing out all the pending transactions,
                    // waiting for 3 seconds and then resuming normal operations
                    if (_log.IsWarnEnabled)
                    {
                        try
                        {
                            _log.Warn(
                                "OutOfMemoryException happened in the transaction merger, will abort all transactions for the next 3 seconds and then resume operations",
                                e);
                        }
                        catch
                        {
                            // under these conditions, this may throw, but we don't care, we wanna survive (cue music)
                        }
                    }
                    ClearQueueWithException(e);
                    oomTimer.Restart();
                    while (_runTransactions)
                    {
                        try
                        {
                            var timeSpan = TimeSpan.FromSeconds(3) - oomTimer.Elapsed;
                            if (timeSpan <= TimeSpan.Zero || _waitHandle.Wait(timeSpan, _shutdown) == false)
                                break;
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                        ClearQueueWithException(e);
                    }
                    oomTimer.Stop();
                    // and now we return to the top of the loop and restart
                }
                catch (Exception e)
                {
                    if (_log.IsFatalEnabled)
                    {
                        _log.Fatal(
                            "Serious failure in transaction merging thread, the database must be restarted!",
                            e);
                    }
                    Interlocked.Exchange(ref _edi, ExceptionDispatchInfo.Capture(e));
                    // cautionary, we make sure that stuff that is waiting on the
                    // queue is notified about this catastrophic3 error and we wait
                    // just a bit more to verify that nothing racy can still get
                    // there
                    while (_runTransactions)
                    {
                        ClearQueueWithException(e);
                        try
                        {
                            _waitHandle.Wait(_shutdown);
                            _waitHandle.Reset();
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
            }

            void ClearQueueWithException(Exception e)
            {
                while (_operations.TryDequeue(out MergedTransactionCommand<TOperationContext, TTransaction> result))
                {
                    result.Exception = e;
                    NotifyOnThreadPool(result);
                }
            }
        }

        private List<MergedTransactionCommand<TOperationContext, TTransaction>> GetBufferForPendingOps()
        {
            if (_opsBuffers.TryDequeue(out var pendingOps) == false)
            {
                return new List<MergedTransactionCommand<TOperationContext, TTransaction>>();
            }
            return pendingOps;
        }

        private void DoCommandsNotification(object cmds)
        {
            var pendingOperations = (List<MergedTransactionCommand<TOperationContext, TTransaction>>)cmds;
            foreach (var op in pendingOperations)
            {
                DoCommandNotification(op);
            }

            pendingOperations.Clear();
            _opsBuffers.Enqueue(pendingOperations);
        }

        private void DoCommandNotification(object op)
        {
            DoCommandNotification((MergedTransactionCommand<TOperationContext, TTransaction>)op);
        }

        private static void DoCommandNotification(MergedTransactionCommand<TOperationContext, TTransaction> cmd)
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
            TOperationContext context = null;
            IDisposable returnContext = null;
            TTransaction tx = null;
            try
            {
                var pendingOps = GetBufferForPendingOps();
                returnContext = _contextPool.AllocateOperationContext(out context);
                {
                    try
                    {
                        _recording.State?.TryRecord(context, TxInstruction.BeginTx);
                        tx = context.OpenWriteTransaction();
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            if (_operations.TryDequeue(out MergedTransactionCommand<TOperationContext, TTransaction> command))
                            {
                                command.Exception = e;
                                DoCommandNotification(command);
                            }
                            return;
                        }
                        finally
                        {
                            if (tx != null)
                            {
                                _recording.State?.TryRecord(context, TxInstruction.DisposeTx, tx.Disposed == false);
                                tx.Dispose();
                            }
                        }
                    }
                    PendingOperations result;
                    try
                    {
                        var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                        try
                        {
                            result = ExecutePendingOperationsInTransaction(pendingOps, context, null, ref transactionMeter);
                            UpdateGlobalReplicationInfoBeforeCommit(context);
                        }
                        finally
                        {
                            transactionMeter.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        // need to dispose here since we are going to open new tx for each operation
                        if (tx != null)
                        {
                            _recording.State?.TryRecord(context, TxInstruction.DisposeTx, tx.Disposed == false);
                            tx.Dispose();
                        }

                        if (e is HighDirtyMemoryException highDirtyMemoryException)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                var errorMessage = $"{pendingOps.Count:#,#0} operations were cancelled because of high dirty memory, details: {highDirtyMemoryException.Message}";
                                _log.Info(errorMessage, highDirtyMemoryException);
                            }

                            NotifyHighDirtyMemoryFailure(pendingOps, highDirtyMemoryException);
                        }
                        else
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info($"Failed to run merged transaction with {pendingOps.Count:#,#0}, will retry independently", e);
                            }

                            NotifyTransactionFailureAndRerunIndependently(pendingOps, e);
                        }

                        return;
                    }

                    switch (result)
                    {
                        case PendingOperations.CompletedAll:
                            try
                            {
                                _recording.State?.TryRecord(context, TxInstruction.Commit);
                                tx.Commit();

                                _recording.State?.TryRecord(context, TxInstruction.DisposeTx, tx.Disposed == false);
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
                            MergeTransactionsWithAsyncCommit(ref context, ref returnContext, pendingOps);
                            return;

                        default:
                            Debug.Assert(false, "Should never happen");
                            return;
                    }
                }
            }
            finally
            {
                using (returnContext)
                using (context?.Transaction)
                {
                    _recording.State?.TryRecord(context, TxInstruction.DisposeTx, context?.Transaction != null && context.Transaction.Disposed == false);
                }
            }
        }

        private void NotifyHighDirtyMemoryFailure(List<MergedTransactionCommand<TOperationContext, TTransaction>> pendingOps, HighDirtyMemoryException exception)
        {
            // set all pending ops exception
            foreach (var pendingOp in pendingOps)
                pendingOp.Exception = exception;

            NotifyOnThreadPool(pendingOps);

            // set operations that are waiting in queue
            var rejectedBuffer = GetBufferForPendingOps();
            while (_operations.TryDequeue(out var operationToReject))
            {
                operationToReject.Exception = exception;
                rejectedBuffer.Add(operationToReject);
            }

            NotifyOnThreadPool(rejectedBuffer);
        }

        internal abstract void UpdateGlobalReplicationInfoBeforeCommit(TOperationContext context);

        private void NotifyTransactionFailureAndRerunIndependently(List<MergedTransactionCommand<TOperationContext, TTransaction>> pendingOps, Exception e)
        {
            if (pendingOps.Count == 1 && pendingOps[0].RetryOnError == false)
            {
                pendingOps[0].Exception = e;
                NotifyOnThreadPool(pendingOps);
                return;
            }

            if (_log.IsInfoEnabled)
            {
                _log.Info($"Error when merging {pendingOps.Count} transactions, will try running independently", e);
            }

            RunEachOperationIndependently(pendingOps);
        }

        private void MergeTransactionsWithAsyncCommit(
            ref TOperationContext previous,
            ref IDisposable returnPreviousContext,
            List<MergedTransactionCommand<TOperationContext, TTransaction>> previousPendingOps)
        {
            TOperationContext current = null;
            IDisposable currentReturnContext = null;

            Debug.Assert(_completionPump.HasPendingCompletions == false, "every async committed transaction of the previous chain must have been completed");

            try
            {
                while (true)
                {
                    if (_log.IsDebugEnabled)
                        _log.Debug($"BeginAsyncCommit on {previous.Transaction.InnerTransaction.LowLevelTransaction.Id} with {_operations.Count} additional operations pending");

                    currentReturnContext = _contextPool.AllocateOperationContext(out current);

                    try
                    {
                        _recording.State?.TryRecord(current, TxInstruction.BeginAsyncCommitAndStartNewTransaction);
                        current.Transaction = BeginAsyncCommitAndStartNewTransaction(previous.Transaction, current);
                    }
                    catch (Exception e)
                    {
                        using (currentReturnContext)
                        {
                            foreach (var op in previousPendingOps)
                            {
                                op.Exception = e;
                            }

                            // complete whatever the chain already has in flight
                            _completionPump.DrainAll();

                            try
                            {
                                // already throwing, complete the previous tx, whose own async commit may have started before the failure
                                // leaving it open would keep the write lock held forever
                                CompletePreviousTransaction(previous, returnPreviousContext, ref previousPendingOps, throwOnError: false);
                            }
                            finally
                            {
                                current.Transaction?.Dispose();
                            }
                        }

                        return;
                    }

                    var previousInFlight = previous.Transaction.InnerTransaction.LowLevelTransaction;
                    Task batchingWindow = previousInFlight.DurableCommit ?? previousInFlight.AsyncCommit;

                    _completionPump.Release(new AsyncCommittedTransaction
                    {
                        Context = previous,
                        ReturnContext = returnPreviousContext, // the pump owns returning the context now
                        PendingOps = previousPendingOps
                    });
                    returnPreviousContext = null;

                    var currentPendingOps = GetBufferForPendingOps();
                    PendingOperations result;
                    bool startedCompletingInFlight = false;
                    try
                    {
                        var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                        try
                        {
                            // accumulate operations into the current transaction while the previous one is in flight
                            result = ExecutePendingOperationsInTransaction(
                                currentPendingOps, current,
                                batchingWindow, ref transactionMeter);
                            UpdateGlobalReplicationInfoBeforeCommit(current);
                        }
                        finally
                        {
                            transactionMeter.Dispose();
                        }
                        startedCompletingInFlight = true;
                        _completionPump.DrainCompleted();
                        _completionPump.ThrowOnFailure();
                    }
                    catch (Exception e)
                    {
                        using (current.Transaction)
                        using (currentReturnContext)
                        {
                            if (startedCompletingInFlight == false)
                            {
                                // the current transaction failed, but the previous ones should be completed
                                _completionPump.DrainAll();
                            }
                            else
                            {
                                // a previous transaction failed, therefore _we_ failed, and need to report this to our callers
                                foreach (var op in currentPendingOps)
                                {
                                    op.Exception = e;
                                }
                                NotifyOnThreadPool(currentPendingOps);

                                throw;
                            }
                        }

                        if (e is HighDirtyMemoryException highDirtyMemoryException)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                var errorMessage = $"{currentPendingOps.Count:#,#0} operations were cancelled because of high dirty memory, details: {highDirtyMemoryException.Message}";
                                _log.Info(errorMessage, highDirtyMemoryException);
                            }

                            NotifyHighDirtyMemoryFailure(currentPendingOps, highDirtyMemoryException);
                        }
                        else
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info($"Failed to run merged transaction with {currentPendingOps.Count:#,#0} operations in async manner, will retry independently", e);
                            }

                            NotifyTransactionFailureAndRerunIndependently(currentPendingOps, e);
                        }

                        return;
                    }

                    previous = current;
                    returnPreviousContext = currentReturnContext;

                    switch (result)
                    {
                        case PendingOperations.CompletedAll:
                            try
                            {
                                _completionPump.DrainAll(); // everything in flight must be completed first
                                _completionPump.ThrowOnFailure();

                                _recording.State?.TryRecord(current, TxInstruction.Commit);
                                previous.Transaction.Commit();
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
                            break;

                        default:
                            Debug.Assert(false);
                            return;
                    }
                }
            }
            catch
            {
                _completionPump.DrainAll();

                if (current?.Transaction != null)
                {
                    _recording.State?.TryRecord(current, TxInstruction.DisposeTx, current.Transaction.Disposed == false);
                    current.Transaction.Dispose();
                }
                currentReturnContext?.Dispose();
                throw;
            }
        }

        private void CompletePreviousTransaction(
            TOperationContext previous,
            IDisposable returnPreviousContext,
            ref List<MergedTransactionCommand<TOperationContext, TTransaction>> previousPendingOps,
            bool throwOnError)
        {
            try
            {
                _recording.State?.TryRecord(previous, TxInstruction.EndAsyncCommit);
                previous.Transaction.EndAsyncCommit();

                if (_log.IsDebugEnabled)
                    _log.Debug($"EndAsyncCommit on {previous.Transaction.InnerTransaction.LowLevelTransaction.Id}");
                
                _recording.State?.TryRecord(previous, TxInstruction.DisposePrevTx, previous.Disposed == false);
                
                previous.Transaction.Dispose();
                returnPreviousContext.Dispose();
                
                NotifyOnThreadPool(previousPendingOps);
            }
            catch (Exception e)
            {
                foreach (var op in previousPendingOps)
                {
                    op.Exception = e;
                }

                // it's safe to call this twice
                previous.Transaction.Dispose();
                returnPreviousContext.Dispose();
                
                NotifyOnThreadPool(previousPendingOps);
                previousPendingOps = null; // RavenDB-7417
                if (throwOnError)
                    throw;
            }
        }

        private enum PendingOperations
        {
            CompletedAll,
            HasMore
        }

        private bool _alreadyListeningToPreviousOperationEnd;

        private struct BatchShaper(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> merger)
        {
            public readonly Stopwatch Timer = Stopwatch.StartNew();

            public WriteFlowPolicy.BatchCloseReason CloseReason = WriteFlowPolicy.BatchCloseReason.QueueEmpty;

            private long _queueDepthSnapshot = -1;

            // called when the _previous_ tx is done, and we *can* close, we check whether we _should_ 
            // if we keep the tx open longer, we get better batch and better throughput (min latency cost)
            public bool ShouldCloseBatch(int batchOperations, long modifiedSize)
            {
                var windowMs = merger.GetBatchingWindowDurationInMs();

                if (merger._operations.IsEmpty)
                {
                    // we have no more operations to process, we can close the batch and return results to the callers
                    CloseReason = WriteFlowPolicy.BatchCloseReason.QueueEmpty;
                    return true;
                }

                if (Timer.ElapsedMilliseconds > windowMs)
                {
                    // a batch this small closing as soon as it may is arrival-capped, treat it as such
                    CloseReason = batchOperations < WriteFlowPolicy.TinyBatchOperations
                        ? WriteFlowPolicy.BatchCloseReason.QueueEmpty
                        : WriteFlowPolicy.BatchCloseReason.MaxBatchTimeReached;
                    return true; // too much time
                }

                if (ShouldYieldPendingOpsToNextTransaction(batchOperations))
                {
                    CloseReason = WriteFlowPolicy.BatchCloseReason.YieldedQueueTail;
                    return true;
                }

                if (modifiedSize > merger._maxTxSizeInBytes)
                {
                    CloseReason = WriteFlowPolicy.BatchCloseReason.SizeReached;
                    return true; // transaction is too big, let's clean it
                }

                // even though we can close the tx, we choose to keep it a bit longer:
                // keep processing operations until the queue clears or time / size limits hit
                return false;
            }

            // counter-intuitive: we leave a small number of operations to the *next* transaction, 
            // so that it can start on them immediately. That allow to interleave operations from clients, 
            // to max out over all concurrency. Otherwise, we may "sync up" everything, and then we have 
            // high peaks and completely idle periods. Better to leave work to the next transaction instead.
            private bool ShouldYieldPendingOpsToNextTransaction(int currentBatchSize)
            {
                int minimumQueueDepthToAbsorb = merger._env.WriteFlow.MinQueueDepthToKeepAbsorbing;
                
                // No consolidation or batch too small to benefit from leaving work to the next tx
                if (minimumQueueDepthToAbsorb == 0 || currentBatchSize < WriteFlowPolicy.TinyBatchOperations)
                    return false;

                if (_queueDepthSnapshot > 0)
                    _queueDepthSnapshot--; // each call absorbed one op since the snapshot was taken

                int requiredQueueDepthToContinue = Math.Max(minimumQueueDepthToAbsorb, currentBatchSize);

                if (_queueDepthSnapshot >= requiredQueueDepthToContinue)
                    return false;

                // Slow path: refresh snapshot (acquires lock) before finalizing the decision to stop absorbing ops
                _queueDepthSnapshot = merger._operations.Count;

                return _queueDepthSnapshot < requiredQueueDepthToContinue;
            }

            public void ClosedBySizeRejection() => CloseReason = WriteFlowPolicy.BatchCloseReason.SizeReached;

            public readonly void Publish(int operations, long modifiedBytes) =>
                merger._env.WriteFlow.RecordBatchClosed(CloseReason, operations, modifiedBytes);
        }

        private PendingOperations ExecutePendingOperationsInTransaction(
            List<MergedTransactionCommand<TOperationContext, TTransaction>> executedOps,
            TOperationContext context,
            Task previousOperation, ref PerformanceMetrics.DurationMeasurement meter)
        {
            _alreadyListeningToPreviousOperationEnd = false;
            context.TransactionMarkerOffset = 1;  // ensure that we are consistent here and don't use old values
            var shaper = new BatchShaper(this);
            long modifiedSize = 0;

            do
            {
                // RavenDB-7732 - Even if we merged multiple separate operations into
                // a single transaction in Voron, we're still going to have a separate
                // tx marker for them for the purpose of replication, to avoid creating
                // overly large replication batches.
                context.TransactionMarkerOffset++;

                if (TryGetNextOperation(previousOperation, out MergedTransactionCommand<TOperationContext, TTransaction> op, ref meter) == false)
                    break;

                executedOps.Add(op);

                var llt = context.Transaction.InnerTransaction.LowLevelTransaction;

                var dirtyMemoryState = LowMemoryNotification.Instance.DirtyMemoryState;
                if (dirtyMemoryState.IsHighDirty)
                {
                    var now = _time.GetUtcNow();
                    if (now - _lastHighDirtyMemCheck > _timeToCheckHighDirtyMemory.AsTimeSpan)
                    {
                        // we need to ask for a flush here
                        GlobalFlushingBehavior.GlobalFlusher.Value?.MaybeFlushEnvironment(context.Environment);
                        _lastHighDirtyMemCheck = now;
                    }

                    throw new HighDirtyMemoryException(
                        $"Operation was cancelled by the transaction merger for transaction #{llt.Id} due to high dirty memory in scratch files." +
                        $" This might be caused by a slow IO storage. Current memory usage: " +
                        $"Total Physical Memory: {MemoryInformation.TotalPhysicalMemory}, " +
                        $"Total Scratch Allocated Memory: {dirtyMemoryState.TotalDirty} " +
                        $"(which is above {_configuration.Memory.TemporaryDirtyMemoryAllowedPercentage * 100}%)");
                }

                meter.IncrementCounter(1);
                meter.IncrementCommands(op.Execute(context, _recording.State));
                if (op.UpdateAccessTime)
                    UpdateLastAccessTime(_time.GetUtcNow());

                modifiedSize = llt.NumberOfModifiedPages * Constants.Storage.PageSize + llt.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);


                var canCloseCurrentTx = previousOperation == null || previousOperation.IsCompleted;
                if (canCloseCurrentTx || _is32Bits)
                {
                    if (shaper.ShouldCloseBatch(executedOps.Count, modifiedSize))
                        break;

                    continue;
                }

                // if I can't close the tx, this means that the previous async operation is still in progress
                // and there are incoming requests coming in. We'll accept them, to a certain limit
                if (modifiedSize < _maxTxSizeInBytes)
                    continue; // we can still process requests at this time, so let's do that...

                UnlikelyRejectOperations(previousOperation, shaper.Timer, llt, modifiedSize);
                shaper.ClosedBySizeRejection();
                break;
            } while (true);

            shaper.Publish(executedOps.Count, modifiedSize);

            var status = GetPendingOperationsStatus(context, executedOps.Count is 0);
            if (_log.IsDebugEnabled)
            {
                var opType = previousOperation == null ? string.Empty : "(async) ";
                _log.Debug($"Merged {executedOps.Count:#,#;;0} operations in {shaper.Timer.Elapsed} {opType}with {_operations.Count:#,#;;0} operations remaining. Status: {status}");
            }
            return status;
        }

        protected abstract void UpdateLastAccessTime(DateTime time);

        private void UnlikelyRejectOperations(IAsyncResult previousOperation, Stopwatch sp, LowLevelTransaction llt, long modifiedSize)
        {
            Debug.Assert(previousOperation != null);

            // we have now reached the point were we are consuming too much memory, and we cannot
            // proceed with accepting a new request, it is time to start rejecting requests
            WaitHandle[] waitHandles =
            {
                _waitHandle.WaitHandle,
                previousOperation.AsyncWaitHandle
            };

            while (previousOperation.IsCompleted == false)
            {
                _waitHandle.Reset();

                var timeToWait = (int)_maxTimeToWaitForPreviousTxBeforeRejectingInMs - (int)sp.ElapsedMilliseconds;
                // we still give the queued operation in the queue time to respond, but if the total
                // time we are waiting exceed the configured limit, we need to wait until either there
                // is a new operation to reject or the previous tx has completed
                if (timeToWait < 0)
                    timeToWait = Timeout.Infinite;

                var waitAny = WaitHandle.WaitAny(waitHandles, timeToWait);
                if (waitAny == 1) // previous operation completed, can carry on, yeah!
                    break;

                if (sp.ElapsedMilliseconds < _maxTimeToWaitForPreviousTxBeforeRejectingInMs)
                    continue;

                if (_operations.IsEmpty)
                    continue;

                var timeout = new TimeoutException(
                    $"Operation was cancelled by the transaction merger because a transaction #{llt.Id} has been waiting for the previous " +
                    $"transaction to complete for {sp.Elapsed} and has reached size of {new Size(modifiedSize, SizeUnit.Bytes)}, which is over the limit." +
                    Environment.NewLine +
                    "In order to protect the database from out of memory errors, transactions are now rejected until the current work is completed");

                var rejectedBuffer = GetBufferForPendingOps();
                while (_operations.TryDequeue(out var operationToReject))
                {
                    operationToReject.Exception = timeout;
                    rejectedBuffer.Add(operationToReject);
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"Rejecting {rejectedBuffer.Count} to avoid OOM error", timeout);

                NotifyOnThreadPool(rejectedBuffer);
            }
        }

        private bool TryGetNextOperation(Task previousOperation, out MergedTransactionCommand<TOperationContext, TTransaction> op, ref PerformanceMetrics.DurationMeasurement meter)
        {
            if (_completionPump.OldestTransactionIsDurable)
                _completionPump.DrainCompleted();

            if (_operations.TryDequeue(out op))
                return true;

            if (previousOperation == null || previousOperation.IsCompleted)
                return false;

            return UnlikelyWaitForNextOperationOrPreviousTransactionComplete(previousOperation, out op, ref meter);
        }

        private bool UnlikelyWaitForNextOperationOrPreviousTransactionComplete(Task previousOperation,
            out MergedTransactionCommand<TOperationContext, TTransaction> op, ref PerformanceMetrics.DurationMeasurement meter)
        {
            if (_alreadyListeningToPreviousOperationEnd == false)
            {
                _alreadyListeningToPreviousOperationEnd = true;
                if (previousOperation.IsCompleted)
                    _waitHandle.Set();
                else
                    previousOperation.ContinueWith(_ => _waitHandle.Set(), _shutdown);
            }
            while (true)
            {
                try
                {
                    meter.MarkInternalWindowStart();
                    _waitHandle.Wait(_shutdown);
                    _waitHandle.Reset();

                    _completionPump.DrainCompleted(); // complete any completed txs, and notify their waiters

                    if (previousOperation.IsCompleted)
                    {
                        op = null;
                        return false;
                    }
                    if (_operations.TryDequeue(out op))
                        return true;
                }
                finally
                {
                    meter.MarkInternalWindowEnd();
                }
            }
        }

        private PendingOperations GetPendingOperationsStatus(TOperationContext context, bool forceCompletion = false)
        {
            // this optimization is disabled for 32 bits
            if (_is32Bits)
                return PendingOperations.CompletedAll;

            // This optimization is disabled when encryption is on
            if (_isEncrypted)
                return PendingOperations.CompletedAll;

            if (forceCompletion)
                return PendingOperations.CompletedAll;

            return PendingOperations.HasMore;
        }

        private void NotifyOnThreadPool(MergedTransactionCommand<TOperationContext, TTransaction> command)
        {
            TaskExecutor.Execute(DoCommandNotification, command);
        }

        private void NotifyOnThreadPool(List<MergedTransactionCommand<TOperationContext, TTransaction>> commands)
        {
            if (commands == null)
                return;

            var n = commands.Count;
            var cores = Environment.ProcessorCount;
            var shardSize = Math.Min(2 * cores, (n + cores - 1) / cores);
            if (n <= shardSize)
            {
                TaskExecutor.Execute(DoCommandsNotification, commands);
                return;
            }

            var sharded = new ShardedNotification(this, commands, n, shardSize);
            var workers = (n + shardSize - 1) / shardSize;

            TaskExecutor.Execute(static s => ((ShardedNotification)s).Run(), sharded);
            for (var i = 1; i < workers; i++)
                ThreadPool.UnsafeQueueUserWorkItem(static s => s.Run(), sharded, preferLocal: false);
        }

        private sealed class ShardedNotification(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> parent, List<MergedTransactionCommand<TOperationContext, TTransaction>> commands, int total, int shardSize)
        {
            private int _next;
            private int _completed;

            public void Run()
            {
                while (true)
                {
                    var start = Interlocked.Add(ref _next, shardSize) - shardSize;
                    if (start >= total)
                        return;

                    var end = Math.Min(start + shardSize, total);
                    for (var i = start; i < end; i++)
                        DoCommandNotification(commands[i]);

                    if (Interlocked.Add(ref _completed, end - start) == total)
                    {
                        commands.Clear();
                        parent._opsBuffers.Enqueue(commands);
                        return;
                    }
                }
            }
        }

        private void RunEachOperationIndependently(List<MergedTransactionCommand<TOperationContext, TTransaction>> pendingOps)
        {
            try
            {
                foreach (var op in pendingOps)
                {
                    bool alreadyRetried = false;
                    while (true)
                    {
                        try
                        {
                            using (_contextPool.AllocateOperationContext(out TOperationContext context))
                            {
                                _recording.State?.TryRecord(context, TxInstruction.BeginTx);
                                using (var tx = context.OpenWriteTransaction())
                                {
                                    op.RetryOnError = false;

                                    op.Execute(context, _recording.State);

                                    _recording.State?.TryRecord(context, TxInstruction.Commit);
                                    tx.Commit();
                                }
                            }

                            NotifyOnThreadPool(op);
                        }
                        catch (Exception e)
                        {
                            if (alreadyRetried == false && op.RetryOnError)
                            {
                                alreadyRetried = true;
                                continue;
                            }

                            op.Exception = e;
                            NotifyOnThreadPool(op);
                        }
                        break;
                    }
                }
            }
            finally
            {
                pendingOps.Clear();
                _opsBuffers.Enqueue(pendingOps);
            }
        }

        public virtual void Dispose()
        {
            StopRecording();

            _runTransactions = false;

            // once all the concurrent transactions are done, this will signal the event, preventing
            // it from adding additional operations
            var done = _concurrentOperations.Signal();

            _waitHandle.Set();
            _txLongRunningOperation?.Join(int.MaxValue);

            // the merger thread released everything before exiting; finish ending those transactions
            // before the environment they belong to goes away
            _completionPump.DrainAll();

            _waitHandle.Dispose();
            _recording.State?.Dispose();
            _recording.Stream?.Dispose();

            // make sure that the queue is empty and there are no pending
            // transactions waiting.
            // this is probably a bit more aggressive that what is needed, but it is better
            // to be cautious and slower on rare dispose than hang

            while (done == false)
            {
                try
                {
                    done = _concurrentOperations.Signal();
                }
                catch (InvalidOperationException)
                {
                    break;
                }
            }

            while (_operations.TryDequeue(out MergedTransactionCommand<TOperationContext, TTransaction> result))
            {
                TaskExecutor.Execute(static s => ((MergedTransactionCommand<TOperationContext, TTransaction>)s).TaskCompletionSource.TrySetCanceled(), result);
            }
        }

        private DateTime _lastHighDirtyMemCheck;
        private readonly TimeSetting _timeToCheckHighDirtyMemory;
    }
}
