using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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

        private readonly Queue<(TOperationContext Context, List<MergedTransactionCommand<TOperationContext, TTransaction>> PendingOps)> _asyncCommittedTransactions = new();
        private int _maxConcurrentJournalWrites = 1;
        
        private readonly ManualResetEventSlim _waitHandle = new(false);
        private ExceptionDispatchInfo _edi;
        private readonly RavenLogger _log;
        private PoolOfThreads.LongRunningWork _txLongRunningOperation;

        private readonly double _maxTimeToWaitForPreviousTxInMs;
        private readonly long _maxTxSizeInBytes;

        // On a slow device the group-commit equilibrium can collapse into tiny batches: short writes release few clients, next requests dribbles and we have small batches
        // result: volume IOPS bound at a fraction of its bandwidth. We intentionally increase transaction time here, to batch more
        private const long EnterBatchConsolidationAtLatencyTicks = 8 * TimeSpan.TicksPerMillisecond;   // the device is queuing under the write traffic
        private const long ExitBatchConsolidationAtLatencyTicks = 4 * TimeSpan.TicksPerMillisecond;    // consolidation grows the writes, and must not turn itself off too quickly
        private const long BatchConsolidationWindowLatencyFactor = 2;                                  // hold for up to this many write durations...
        private const long MaxBatchConsolidationWindowInMs = 50;                                     // ...but never longer than this
        private const long MaxBatchConsolidationSizeInBytes = 128 * Constants.Size.Megabyte;
        private int _consecutiveEmptyConsolidationWaits;

        // two empty one-millisecond waits in a row mean the arrivals dried up - every writer is already
        // aboard this batch, and holding it any longer is pure added latency for them
        private const int MaxConsecutiveEmptyConsolidationWaits = 2;

        private bool _consolidatingBatches;

        // Is the batch is still worth waiting for more operations to arrive?
        // Even though we have async commits, we want to keep the *current* transaction alive for a bit longer
        //
        // This is an issue because gp3 drives, for example, has both IOPS and bandwidth limits. If we have a lot of 
        // small writes, we burned through that budget, and start queuing. And we end in a meta-stable slow state.
        // 
        // Batch size is whatever arrives during one write, so tiny writes -> the device goes IOPS bound at a fraction of its bandwidth 
        // -> queuing inflates every op's latency -> clients cycle slowly -> few arrivals per write -> tiny writes again. 
        // 
        // Holding the batch open for a couple of write-durations flips it to few large writes: the device becomes bandwidth
        // // bound, the queuing disappears, and per-op latency drops even though we waited (measured on gp3: 8.4k -> 12.9k ops/s at 1,531 clients, p50 down). 
        // 
        // The hold only engages when both symptoms are present (writes queuing AND batches collapsed), exits within ~2ms once every writer is already 
        // aboard, and never engages on a fast device - a flat hold measurably hurts healthy workloads
        private bool TryWaitForMoreOperationsToConsolidate(long modifiedSize, long consolidationSize, Stopwatch sp, double consolidationWindowMs)
        {
            if (_consolidatingBatches == false || // not relevant unless in consolidation regine
                modifiedSize >= consolidationSize ||
                sp.ElapsedMilliseconds >= consolidationWindowMs)
            {
                _consecutiveEmptyConsolidationWaits = 0;
                return false;
            }

            _waitHandle.Reset();

            if (_operations.IsEmpty == false || // an operation arrived between the queue check and the reset
                _waitHandle.Wait(millisecondsTimeout: 1, _shutdown)) // or was enqueued while we waited
            {
                _consecutiveEmptyConsolidationWaits = 0;
                return true;
            }

            if (++_consecutiveEmptyConsolidationWaits >= MaxConsecutiveEmptyConsolidationWaits)
            {
                _consecutiveEmptyConsolidationWaits = 0;
                return false;
            }

            return true;
        }

        private double GetBatchingWindowDurationInMs()
        {
            if (_is32Bits)
                return _maxTimeToWaitForPreviousTxInMs;

            var writeLatencyTicks = _env.Journal.WriteLatencyEwmaTicks;

            _consolidatingBatches = _consolidatingBatches
                ? writeLatencyTicks >= ExitBatchConsolidationAtLatencyTicks
                : writeLatencyTicks >= EnterBatchConsolidationAtLatencyTicks && _env.Journal.IsCommitLatencyBound;
            _env.BatchConsolidationActive = _consolidatingBatches;

            if (_consolidatingBatches == false)
                return _maxTimeToWaitForPreviousTxInMs;

            var windowMs = BatchConsolidationWindowLatencyFactor * writeLatencyTicks / TimeSpan.TicksPerMillisecond;
            return Math.Max(_maxTimeToWaitForPreviousTxInMs, Math.Min(MaxBatchConsolidationWindowInMs, windowMs));
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
        }
        
        public void Initialize([NotNull] JsonContextPoolBase<TOperationContext> contextPool, bool isEncrypted, bool is32Bits)
        {
            _contextPool = contextPool ?? throw new ArgumentNullException(nameof(contextPool));
            _isEncrypted = isEncrypted;
            _is32Bits = is32Bits;
            _env = GetStorageEnvironment(contextPool);
            _maxConcurrentJournalWrites = Math.Max(1, _env.Journal.MaxConcurrentJournalWrites);
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

            Debug.Assert(_asyncCommittedTransactions.Count == 0, "every async committed transaction of the previous chain must have been completed");

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
                            CompleteAsyncCommittedTransactions(keep: 0, throwOnError: false);

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

                    _asyncCommittedTransactions.Enqueue((previous, previousPendingOps));

                    var currentPendingOps = GetBufferForPendingOps();
                    PendingOperations result;
                    bool startedCompletingInFlight = false;
                    try
                    {
                        var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                        try
                        {
                            // accumulate operations into the current transaction while the previous one is in flight
                            var previousInFlight = previous.Transaction.InnerTransaction.LowLevelTransaction;
                            Task batchingWindow = previousInFlight.DurableCommit ?? previousInFlight.AsyncCommit;

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
                        // depending on the size of journal writes, we may want to keep writes to the jounral in flight (pipelined).
                        // that doesn't make sense if we have large journal writes, since then we are bandwidth bound anyway
                        var keepInFlight = _env.Journal.ShouldPipelineJournalNow ? _maxConcurrentJournalWrites - 1 : 0;
                        CompleteAsyncCommittedTransactions(keep: keepInFlight, throwOnError: true);
                    }
                    catch (Exception e)
                    {
                        using (current.Transaction)
                        using (currentReturnContext)
                        {
                            if (startedCompletingInFlight == false)
                            {
                                // the current transaction failed, but previous ones should be completed (CompleteAsyncCommittedTransactions not called yet)
                                CompleteAsyncCommittedTransactions(keep: 0, throwOnError: false);
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
                                CompleteAsyncCommittedTransactions(keep: 0, throwOnError: true);

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
                CompleteAsyncCommittedTransactions(keep: 0, throwOnError: false);

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

        private void CompleteAsyncCommittedTransactions(int keep, bool throwOnError)
        {
            ExceptionDispatchInfo failure = null;

            // once one of them failed, all the following ones are also failed
            while (_asyncCommittedTransactions.Count > (failure == null ? keep : 0))
            {
                var (context, pendingOps) = _asyncCommittedTransactions.Dequeue();

                var error = failure?.SourceException;

                try
                {
                    _recording.State?.TryRecord(context, TxInstruction.EndAsyncCommit);
                    context.Transaction.EndAsyncCommit();

                    if (_log.IsDebugEnabled)
                        _log.Debug($"EndAsyncCommit on {context.Transaction.InnerTransaction.LowLevelTransaction.Id}");

                    _recording.State?.TryRecord(context, TxInstruction.DisposePrevTx, context.Disposed == false);
                }
                catch (Exception e)
                {
                    error ??= e; // propagate the first error to all future transactions & operations
                    failure ??= ExceptionDispatchInfo.Capture(e);
                }

                try
                {
                    context.Transaction.Dispose();
                }
                catch (Exception e)
                {
                    error ??= e;
                    failure ??= ExceptionDispatchInfo.Capture(e);
                }

                if (error != null)
                {
                    foreach (var op in pendingOps)
                    {
                        op.Exception = error;
                    }
                }

                try
                {
                    NotifyOnThreadPool(pendingOps);
                }
                catch (Exception e)
                {
                    failure ??= ExceptionDispatchInfo.Capture(e);
                }
            }

            if (throwOnError)
                failure?.Throw();
        }

        private enum PendingOperations
        {
            CompletedAll,
            HasMore
        }

        private bool _alreadyListeningToPreviousOperationEnd;

        private PendingOperations ExecutePendingOperationsInTransaction(
            List<MergedTransactionCommand<TOperationContext, TTransaction>> executedOps,
            TOperationContext context,
            Task previousOperation, ref PerformanceMetrics.DurationMeasurement meter)
        {
            _alreadyListeningToPreviousOperationEnd = false;
            context.TransactionMarkerOffset = 1;  // ensure that we are consistent here and don't use old values
            var sp = Stopwatch.StartNew();

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

                var modifiedSize = llt.NumberOfModifiedPages * Constants.Storage.PageSize;

                modifiedSize += llt.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

                var canCloseCurrentTx = previousOperation == null || previousOperation.IsCompleted;
                if (canCloseCurrentTx || _is32Bits)
                {
                    var consolidationWindowMs = GetBatchingWindowDurationInMs();
                    var consolidationSize = Math.Min(_maxTxSizeInBytes, MaxBatchConsolidationSizeInBytes);

                    if (_operations.IsEmpty && 
                        TryWaitForMoreOperationsToConsolidate(modifiedSize, consolidationSize, sp, consolidationWindowMs) is false)
                    {
                        break; // nothing remaining to do, let's us close this work
                    }

                    if (sp.ElapsedMilliseconds > consolidationWindowMs)
                        break; // too much time

                    if (modifiedSize > _maxTxSizeInBytes)
                        break; // transaction is too big, let's clean it

                    // even though we can close the tx, we choose to keep it a bit longer
                    // we want to keep processing operations until we clear the queue, time / size
                    // limits are reached
                    continue;
                }

                // if I can't close the tx, this means that the previous async operation is still in progress
                // and there are incoming requests coming in. We'll accept them, to a certain limit
                if (modifiedSize < _maxTxSizeInBytes)
                    continue; // we can still process requests at this time, so let's do that...

                UnlikelyRejectOperations(previousOperation, sp, llt, modifiedSize);
                break;
            } while (true);

            var status = GetPendingOperationsStatus(context, executedOps.Count is 0);
            if (_log.IsDebugEnabled)
            {
                var opType = previousOperation == null ? string.Empty : "(async) ";
                _log.Debug($"Merged {executedOps.Count:#,#;;0} operations in {sp.Elapsed} {opType}with {_operations.Count:#,#;;0} operations remaining. Status: {status}");
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
