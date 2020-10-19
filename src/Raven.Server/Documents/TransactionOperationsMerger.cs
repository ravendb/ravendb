using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Json;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Journal;

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
        private readonly CountdownEvent _concurrentOperations = new CountdownEvent(1);

        private readonly ConcurrentQueue<List<MergedTransactionCommand>> _opsBuffers = new ConcurrentQueue<List<MergedTransactionCommand>>();
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        private ExceptionDispatchInfo _edi;
        private readonly Logger _log;
        private PoolOfThreads.LongRunningWork _txLongRunningOperation;

        private readonly double _maxTimeToWaitForPreviousTxInMs;
        private readonly long _maxTxSizeInBytes;
        private readonly double _maxTimeToWaitForPreviousTxBeforeRejectingInMs;

        public TransactionOperationsMerger(DocumentDatabase parent, CancellationToken shutdown)
        {
            _parent = parent;
            _log = LoggingSource.Instance.GetLogger<TransactionOperationsMerger>(_parent.Name);
            _shutdown = shutdown;

            _maxTimeToWaitForPreviousTxInMs = _parent.Configuration.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTx.AsTimeSpan.TotalMilliseconds;
            _maxTxSizeInBytes = _parent.Configuration.TransactionMergerConfiguration.MaxTxSize.GetValue(SizeUnit.Bytes);
            _maxTimeToWaitForPreviousTxBeforeRejectingInMs = _parent.Configuration.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTxBeforeRejecting.AsTimeSpan.TotalMilliseconds;
            _timeToCheckHighDirtyMemory = _parent.Configuration.Memory.TemporaryDirtyMemoryChecksPeriod;
            _lastHighDirtyMemCheck = _parent.Time.GetUtcNow();
        }

        public DatabasePerformanceMetrics GeneralWaitPerformanceMetrics = new DatabasePerformanceMetrics(DatabasePerformanceMetrics.MetricType.GeneralWait, 256, 1);
        public DatabasePerformanceMetrics TransactionPerformanceMetrics = new DatabasePerformanceMetrics(DatabasePerformanceMetrics.MetricType.Transaction, 256, 8);

        public int NumberOfQueuedOperations => _operations.Count;

        private string TransactionMergerThreadName => $"'{_parent.Name}' Transaction Merging Thread";

        public void Start()
        {
            _txLongRunningOperation = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => MergeOperationThreadProc(), null, TransactionMergerThreadName);
        }

        public interface IRecordableCommand
        {
            IReplayableCommandDto<MergedTransactionCommand> ToDto(JsonOperationContext context);
        }

        public interface IReplayableCommandDto<out T> where T : MergedTransactionCommand
        {
            T ToCommand(DocumentsOperationContext context, DocumentDatabase database);
        }

        public abstract class MergedTransactionCommand : IRecordableCommand
        {
            public bool UpdateAccessTime = true;

            protected abstract long ExecuteCmd(DocumentsOperationContext context);

            internal long ExecuteDirectly(DocumentsOperationContext context)
            {
                return ExecuteCmd(context);
            }

            public virtual long Execute(DocumentsOperationContext context, RecordingState recordingState)
            {
                recordingState?.Record(context, this);

                return ExecuteCmd(context);
            }

            public abstract IReplayableCommandDto<MergedTransactionCommand> ToDto(JsonOperationContext context);

            [JsonIgnore]
            public readonly TaskCompletionSource<object> TaskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            public Exception Exception;

            public bool RetryOnError = false;
        }

        /// <summary>
        /// Enqueue the command to be eventually executed. If the command implements
        ///  IDisposable, the command will be disposed after it is run and a tx is committed.
        /// </summary>
        public async Task Enqueue(MergedTransactionCommand cmd)
        {
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

        private static void ThrowTxMergerWasDisposed()
        {
            throw new ObjectDisposedException("Transaction Merger");
        }

        public abstract class RecordingState : IDisposable
        {
            public abstract void Record(DocumentsOperationContext context, MergedTransactionCommand cmd);

            internal abstract void Record(DocumentsOperationContext context, TxInstruction tx, bool doRecord = true);

            public abstract void Prepare(ref RecordingState state);

            private static BlittableJsonReaderObject SerializeRecordingCommandDetails(
                JsonOperationContext context,
                RecordingDetails commandDetails)
            {
                using (var writer = new BlittableJsonWriter(context))
                {
                    var jsonSerializer = ReplayTxCommandHelper.GetJsonSerializer();

                    jsonSerializer.Serialize(writer, commandDetails);
                    writer.FinalizeDocument();

                    return writer.CreateReader();
                }
            }

            protected class EnabledRecordingState : RecordingState
            {
                private readonly TransactionOperationsMerger _txOpMerger;
                private int _isDisposed = 0;

                public EnabledRecordingState(TransactionOperationsMerger txOpMerger)
                {
                    _txOpMerger = txOpMerger;
                }

                public override void Record(DocumentsOperationContext context, MergedTransactionCommand operation)
                {
                    var obj = new RecordingCommandDetails(operation.GetType().Name)
                    {
                        Command = operation.ToDto(context)
                    };

                    Record(obj, context);
                }

                internal override void Record(DocumentsOperationContext ctx, TxInstruction tx, bool doRecord = true)
                {
                    if (doRecord == false)
                    {
                        return;
                    }

                    var commandDetails = new RecordingDetails(tx.ToString());

                    Record(commandDetails, ctx);
                }

                private void Record(RecordingDetails commandDetails, JsonOperationContext context)
                {
                    using (var commandDetailsReader = SerializeRecordingCommandDetails(context, commandDetails))
                    using (var writer = new BlittableJsonTextWriter(context, _txOpMerger._recording.Stream))
                    {
                        writer.WriteComma();
                        context.Write(writer, commandDetailsReader);
                    }
                }

                public override void Prepare(ref RecordingState state)
                {
                    if (IsShutdown == false)
                        return;

                    state = null;
                    Dispose();

                    _txOpMerger._recording.Stream?.Dispose();
                    _txOpMerger._recording.Stream = null;
                }

                public override void Dispose()
                {
                    if (1 == Interlocked.CompareExchange(ref _isDisposed, 1, 0))
                    {
                        return;
                    }

                    using (_txOpMerger._parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, _txOpMerger._recording.Stream))
                        {
                            writer.WriteEndArray();
                        }
                    }
                }
            }

            public class BeforeEnabledRecordingState : RecordingState
            {
                private readonly TransactionOperationsMerger _txOpMerger;

                public BeforeEnabledRecordingState(TransactionOperationsMerger txOpMerger)
                {
                    _txOpMerger = txOpMerger;
                }

                public override void Record(DocumentsOperationContext context, MergedTransactionCommand cmd)
                {
                }

                internal override void Record(DocumentsOperationContext context, TxInstruction tx, bool doRecord = true)
                {
                }

                public override void Prepare(ref RecordingState state)
                {
                    if (IsShutdown)
                    {
                        state = null;
                        return;
                    }

                    using (_txOpMerger._parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var writer = new BlittableJsonTextWriter(context, _txOpMerger._recording.Stream))
                    {
                        writer.WriteStartArray();

                        var commandDetails = new StartRecordingDetails();
                        var commandDetailsReader = SerializeRecordingCommandDetails(context, commandDetails);

                        context.Write(writer, commandDetailsReader);
                    }

                    state = new EnabledRecordingState(_txOpMerger);
                }

                public override void Dispose()
                {
                }
            }

            protected bool IsShutdown { private set; get; }

            public void Shutdown()
            {
                IsShutdown = true;
            }

            public abstract void Dispose();
        }

        private void MergeOperationThreadProc()
        {
            try
            {
                Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Unable to elevate the transaction merger thread for " + _parent.Name, e);
                }
            }

            var oomTimer = new Stopwatch();// this is allocated here to avoid OOM when using it

            while (true) // this is actually only executed once, except if we are trying to recover from OOM errors
            {
                NativeMemory.EnsureRegistered();
                try
                {
                    while (_runTransactions)
                    {
                        _recording.State?.Prepare(ref _recording.State);

                        if (_operations.IsEmpty)
                        {
                            if (_parent.IsEncrypted)
                            {
                                using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                                {
                                    using (ctx.OpenWriteTransaction())
                                    {
                                        ctx.Environment.Options.Encryption.JournalCompressionBufferHandler.ZeroCompressionBuffer(ctx.Transaction.InnerTransaction.LowLevelTransaction);
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
                    if (_log.IsOperationsEnabled)
                    {
                        try
                        {
                            _log.Operations(
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
                    if (_log.IsOperationsEnabled)
                    {
                        _log.Operations(
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
                while (_operations.TryDequeue(out MergedTransactionCommand result))
                {
                    result.Exception = e;
                    NotifyOnThreadPool(result);
                }
            }
        }

        private List<MergedTransactionCommand> GetBufferForPendingOps()
        {
            if (_opsBuffers.TryDequeue(out var pendingOps) == false)
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

        private static void DoCommandNotification(MergedTransactionCommand cmd)
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
            DocumentsOperationContext context = null;
            IDisposable returnContext = null;
            DocumentsTransaction tx = null;
            try
            {
                var pendingOps = GetBufferForPendingOps();
                returnContext = _parent.DocumentsStorage.ContextPool.AllocateOperationContext(out context);
                {
                    try
                    {
                        _recording.State?.Record(context, TxInstruction.BeginTx);
                        tx = context.OpenWriteTransaction();
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            if (_operations.TryDequeue(out MergedTransactionCommand command))
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
                                _recording.State?.Record(context, TxInstruction.DisposeTx, tx.Disposed == false);
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
                            _recording.State?.Record(context, TxInstruction.DisposeTx, tx.Disposed == false);
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
                                tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out var stats);

                                _recording.State?.Record(context, TxInstruction.Commit);
                                tx.Commit();

                                SlowWriteNotification.Notify(stats, _parent);
                                _recording.State?.Record(context, TxInstruction.DisposeTx, tx.Disposed == false);
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
                if (context?.Transaction != null)
                {
                    using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    {
                        _recording.State?.Record(ctx, TxInstruction.DisposeTx, context.Transaction.Disposed == false);
                    }
                    context.Transaction.Dispose();
                }
                returnContext?.Dispose();
            }
        }

        private void NotifyHighDirtyMemoryFailure(List<MergedTransactionCommand> pendingOps, HighDirtyMemoryException exception)
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

        internal void UpdateGlobalReplicationInfoBeforeCommit(DocumentsOperationContext context)
        {
            if (string.IsNullOrEmpty(context.LastDatabaseChangeVector) == false)
            {
                _parent.DocumentsStorage.SetDatabaseChangeVector(context, context.LastDatabaseChangeVector);
            }

            if (context.LastReplicationEtagFrom != null)
            {
                foreach (var repEtag in context.LastReplicationEtagFrom)
                {
                    DocumentsStorage.SetLastReplicatedEtagFrom(context, repEtag.Key, repEtag.Value);
                }
            }
        }

        private void NotifyTransactionFailureAndRerunIndependently(List<MergedTransactionCommand> pendingOps, Exception e)
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
            ref DocumentsOperationContext previous,
            ref IDisposable returnPreviousContext,
            List<MergedTransactionCommand> previousPendingOps)
        {
            DocumentsOperationContext current = null;
            IDisposable currentReturnContext = null;
            try
            {
                while (true)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"BeginAsyncCommit on {previous.Transaction.InnerTransaction.LowLevelTransaction.Id} with {_operations.Count} additional operations pending");

                    currentReturnContext = _parent.DocumentsStorage.ContextPool.AllocateOperationContext(out current);
                    CommitStats commitStats = null;
                    try
                    {
                        previous.Transaction.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out commitStats);
                        _recording.State?.Record(current, TxInstruction.BeginAsyncCommitAndStartNewTransaction);
                        current.Transaction = previous.Transaction.BeginAsyncCommitAndStartNewTransaction(current);
                    }
                    catch (Exception e)
                    {
                        foreach (var op in previousPendingOps)
                        {
                            op.Exception = e;
                        }
                        NotifyOnThreadPool(previousPendingOps);

                        if (e is OutOfMemoryException)
                        {
                            try
                            {
                                //already throwing, attempt to complete previous tx
                                CompletePreviousTransaction(previous, previous.Transaction, commitStats, ref previousPendingOps, throwOnError: false);
                            }
                            finally
                            {
                                current.Transaction?.Dispose();
                                currentReturnContext.Dispose();
                            }
                        }

                        return;
                    }

                    var currentPendingOps = GetBufferForPendingOps();
                    PendingOperations result;
                    bool calledCompletePreviousTx = false;
                    try
                    {
                        var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                        try
                        {
                            result = ExecutePendingOperationsInTransaction(
                                currentPendingOps, current,
                                previous.Transaction.InnerTransaction.LowLevelTransaction.AsyncCommit, ref transactionMeter);
                            UpdateGlobalReplicationInfoBeforeCommit(current);
                        }
                        finally
                        {
                            transactionMeter.Dispose();
                        }
                        calledCompletePreviousTx = true;
                        CompletePreviousTransaction(previous, previous.Transaction, commitStats, ref previousPendingOps, throwOnError: true);
                    }
                    catch (Exception e)
                    {
                        using (current.Transaction)
                        using (currentReturnContext)
                        {
                            if (calledCompletePreviousTx == false)
                            {
                                CompletePreviousTransaction(
                                    previous,
                                    previous.Transaction,
                                    commitStats,
                                    ref previousPendingOps,
                                    // if this previous threw, it won't throw again
                                    throwOnError: false);
                            }
                            else
                            {
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

                    _recording.State?.Record(previous, TxInstruction.DisposePrevTx, previous.Transaction.Disposed == false);

                    previous.Transaction.Dispose();
                    returnPreviousContext.Dispose();

                    previous = current;
                    returnPreviousContext = currentReturnContext;

                    switch (result)
                    {
                        case PendingOperations.CompletedAll:
                            try
                            {
                                previous.Transaction.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out var stats);
                                _recording.State?.Record(current, TxInstruction.Commit);
                                previous.Transaction.Commit();

                                SlowWriteNotification.Notify(stats, _parent);
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
                if (current.Transaction != null)
                {
                    _recording.State?.Record(current, TxInstruction.DisposeTx, current.Transaction.Disposed == false);
                    current.Transaction.Dispose();
                }
                currentReturnContext?.Dispose();
                throw;
            }
        }

        private void CompletePreviousTransaction(
            DocumentsOperationContext context,
            RavenTransaction previous,
            CommitStats commitStats,
            ref List<MergedTransactionCommand> previousPendingOps,
            bool throwOnError)
        {
            try
            {
                _recording.State?.Record(context, TxInstruction.EndAsyncCommit);
                previous.EndAsyncCommit();

                //not sure about this 'if'
                if (commitStats != null)
                {
                    SlowWriteNotification.Notify(commitStats, _parent);
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"EndAsyncCommit on {previous.InnerTransaction.LowLevelTransaction.Id}");
                NotifyOnThreadPool(previousPendingOps);
            }
            catch (Exception e)
            {
                foreach (var op in previousPendingOps)
                {
                    op.Exception = e;
                }
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

        private PendingOperations ExecutePendingOperationsInTransaction(
            List<MergedTransactionCommand> executedOps,
            DocumentsOperationContext context,
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

                if (TryGetNextOperation(previousOperation, out MergedTransactionCommand op, ref meter) == false)
                    break;

                executedOps.Add(op);

                var llt = context.Transaction.InnerTransaction.LowLevelTransaction;

                var dirtyMemoryState = LowMemoryNotification.Instance.DirtyMemoryState;
                if (dirtyMemoryState.IsHighDirty)
                {
                    var now = _parent.Time.GetUtcNow();
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
                        $"Total Scratch Allocated Memory: {new Size(dirtyMemoryState.TotalDirtyInBytes, SizeUnit.Bytes)} " +
                        $"(which is above {_parent.Configuration.Memory.TemporaryDirtyMemoryAllowedPercentage * 100}%)");
                }

                meter.IncrementCounter(1);
                meter.IncrementCommands(op.Execute(context, _recording.State));
                if (op.UpdateAccessTime)
                    _parent.LastAccessTime = _parent.Time.GetUtcNow();

                var modifiedSize = llt.NumberOfModifiedPages * Constants.Storage.PageSize;

                modifiedSize += llt.TotalEncryptionBufferSize.GetValue(SizeUnit.Bytes);

                var canCloseCurrentTx = previousOperation == null || previousOperation.IsCompleted;
                if (canCloseCurrentTx || _parent.Is32Bits)
                {
                    if (_operations.IsEmpty)
                        break; // nothing remaining to do, let's us close this work

                    if (sp.ElapsedMilliseconds > _maxTimeToWaitForPreviousTxInMs)
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

            var currentOperationsCount = _operations.Count;
            var status = GetPendingOperationsStatus(context, currentOperationsCount == 0);
            if (_log.IsInfoEnabled)
            {
                var opType = previousOperation == null ? string.Empty : "(async) ";
                _log.Info($"Merged {executedOps.Count:#,#;;0} operations in {sp.Elapsed} {opType}with {currentOperationsCount:#,#;;0} operations remaining. Status: {status}");
            }
            return status;
        }

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

        private bool TryGetNextOperation(Task previousOperation, out MergedTransactionCommand op, ref PerformanceMetrics.DurationMeasurement meter)
        {
            if (_operations.TryDequeue(out op))
                return true;

            if (previousOperation == null || previousOperation.IsCompleted)
                return false;

            return UnlikelyWaitForNextOperationOrPreviousTransactionComplete(previousOperation, out op, ref meter);
        }

        private bool UnlikelyWaitForNextOperationOrPreviousTransactionComplete(Task previousOperation,
            out MergedTransactionCommand op, ref PerformanceMetrics.DurationMeasurement meter)
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

        private PendingOperations GetPendingOperationsStatus(DocumentsOperationContext context, bool forceCompletion = false)
        {
            // this optimization is disabled for 32 bits
            if (_parent.Is32Bits)
                return PendingOperations.CompletedAll;

            // This optimization is disabled when encryption is on
            if (context.Environment.Options.Encryption.IsEnabled)
                return PendingOperations.CompletedAll;

            if (forceCompletion)
                return PendingOperations.CompletedAll;

            return PendingOperations.HasMore;
        }

        private void NotifyOnThreadPool(MergedTransactionCommand cmd)
        {
            TaskExecutor.Execute(DoCommandNotification, cmd);
        }

        private void NotifyOnThreadPool(List<MergedTransactionCommand> cmds)
        {
            if (cmds == null)
                return;

            TaskExecutor.Execute(DoCommandsNotification, cmds);
        }

        private void RunEachOperationIndependently(List<MergedTransactionCommand> pendingOps)
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
                            using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                            {
                                _recording.State?.Record(context, TxInstruction.BeginTx);
                                using (var tx = context.OpenWriteTransaction())
                                {
                                    op.RetryOnError = false;

                                    op.Execute(context, _recording.State);

                                    tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out var stats);

                                    _recording.State?.Record(context, TxInstruction.Commit);
                                    tx.Commit();
                                    SlowWriteNotification.Notify(stats, _parent);
                                }
                            }
                            DoCommandNotification(op);
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

        public void Dispose()
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

            while (_operations.TryDequeue(out MergedTransactionCommand result))
            {
                result.TaskCompletionSource.TrySetCanceled();
            }
        }

        private RecordingTx _recording = default;
        private DateTime _lastHighDirtyMemCheck;
        private readonly TimeSetting _timeToCheckHighDirtyMemory;

        private struct RecordingTx
        {
            public RecordingState State;
            public Stream Stream;
            public Action StopAction;
        }

        public void StartRecording(string filePath, Action stopAction)
        {
            var recordingFileStream = new FileStream(filePath, FileMode.Create);
            if (null != Interlocked.CompareExchange(ref _recording.State, new RecordingState.BeforeEnabledRecordingState(this), null))
            {
                recordingFileStream.Dispose();
                File.Delete(filePath);
            }
            _recording.Stream = new GZipStream(recordingFileStream, CompressionMode.Compress);
            _recording.StopAction = stopAction;
        }

        public bool RecordingEnabled => _recording.State != null;

        public void StopRecording()
        {
            var recordingState = _recording.State;
            if (recordingState != null)
            {
                recordingState.Shutdown();
                _waitHandle.Set();
                _recording.StopAction?.Invoke();
                _recording.StopAction = null;
            }
        }
    }
}
