using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices.ComTypes;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Properties;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Json.Converters;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using static Sparrow.DatabasePerformanceMetrics;

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
        }

        public DatabasePerformanceMetrics GeneralWaitPerformanceMetrics = new DatabasePerformanceMetrics(MetricType.GeneralWait, 256, 1);
        public DatabasePerformanceMetrics TransactionPerformanceMetrics = new DatabasePerformanceMetrics(MetricType.Transaction, 256, 8);

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
            protected abstract int ExecuteCmd(DocumentsOperationContext context);

            internal int ExecuteDirectly(DocumentsOperationContext context)
            {
                return ExecuteCmd(context);
            }

            public virtual int Execute(DocumentsOperationContext context, RecordingState recordingState)
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
                await cmd.TaskCompletionSource.Task;
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

            public abstract void Record(DocumentsOperationContext context, TxInstruction tx, bool doRecord = true);

            public abstract void Prepare(ref RecordingState state);

            private static BlittableJsonReaderObject SerializeRecordingCommandDetails(
                JsonOperationContext context,
                RecordingDetails commandDetails)
            {
                using (var writer = new BlittableJsonWriter(context))
                {
                    var jsonSerializer = GetJsonSerializer();

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

                public override void Record(DocumentsOperationContext ctx, TxInstruction tx, bool doRecord = true)
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

                public override void Record(DocumentsOperationContext context, TxInstruction tx, bool doRecord = true)
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
                catch (Exception e) when (e is OutOfMemoryException)
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
            DocumentsTransaction tx = null;
            try
            {
                var pendingOps = GetBufferForPendingOps();
                using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info(
                                $"Failed to run merged transaction with {pendingOps.Count:#,#0}, will retry independently",
                                e);
                        }

                        // need to dispose here since we are going to open new tx for each operation
                        if (tx != null)
                        {
                            _recording.State?.Record(context, TxInstruction.DisposeTx, tx.Disposed == false);
                            tx.Dispose();
                        }

                        NotifyTransactionFailureAndRerunIndependently(pendingOps, e);
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
                            MergeTransactionsWithAsyncCommit(context, pendingOps);
                            return;
                        default:
                            Debug.Assert(false, "Should never happen");
                            return;
                    }
                }
            }
            finally
            {
                if (tx != null)
                {
                    using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        _recording.State?.Record(context, TxInstruction.DisposeTx, tx.Disposed == false);
                    }
                    tx.Dispose();
                }
            }
        }



        private static void UpdateGlobalReplicationInfoBeforeCommit(DocumentsOperationContext context)
        {
            if (string.IsNullOrEmpty(context.LastDatabaseChangeVector) == false)
            {
                DocumentsStorage.SetDatabaseChangeVector(context, context.LastDatabaseChangeVector);
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
                _log.Info($"Error when merging {0} transactions, will try running independently", e);
            }
            RunEachOperationIndependently(pendingOps);
        }

        private void MergeTransactionsWithAsyncCommit(
            DocumentsOperationContext context,
            List<MergedTransactionCommand> previousPendingOps)
        {
            var previous = context.Transaction;
            try
            {
                while (true)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"BeginAsyncCommit on {previous.InnerTransaction.LowLevelTransaction.Id} with {_operations.Count} additional operations pending");

                    CommitStats commitStats = null;
                    try
                    {
                        previous.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out commitStats);

                        _recording.State?.Record(context, TxInstruction.BeginAsyncCommitAndStartNewTransaction);
                        context.Transaction = previous.BeginAsyncCommitAndStartNewTransaction();
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
                                CompletePreviousTransaction(context, previous, commitStats, ref previousPendingOps, throwOnError: false);
                            }
                            finally
                            {
                                context.Transaction?.Dispose();
                            }
                        }

                        return;
                    }
                    try
                    {
                        var currentPendingOps = GetBufferForPendingOps();
                        PendingOperations result;
                        bool calledCompletePreviousTx = false;
                        try
                        {
                            var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                            try
                            {

                                result = ExecutePendingOperationsInTransaction(
                                    currentPendingOps, context,
                                    previous.InnerTransaction.LowLevelTransaction.AsyncCommit, ref transactionMeter);
                                UpdateGlobalReplicationInfoBeforeCommit(context);
                            }
                            finally
                            {
                                transactionMeter.Dispose();
                            }
                            calledCompletePreviousTx = true;
                            CompletePreviousTransaction(context, previous, commitStats, ref previousPendingOps, throwOnError: true);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(
                                    $"Failed to run merged transaction with {currentPendingOps.Count:#,#0} operations in async manner, will retry independently",
                                    e);
                            }

                            using (context.Transaction)
                            using (previous)
                            {
                                if (calledCompletePreviousTx == false)
                                {
                                    CompletePreviousTransaction(
                                        context,
                                        previous,
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
                            NotifyTransactionFailureAndRerunIndependently(currentPendingOps, e);
                            return;
                        }

                        _recording.State?.Record(context, TxInstruction.DisposePrevTx, previous.Disposed == false);
                        previous.Dispose();

                        switch (result)
                        {
                            case PendingOperations.CompletedAll:
                                try
                                {
                                    context.Transaction.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out var stats);

                                    _recording.State?.Record(context, TxInstruction.Commit);
                                    context.Transaction.Commit();

                                    SlowWriteNotification.Notify(stats, _parent);

                                    _recording.State?.Record(context, TxInstruction.DisposeTx, context.Transaction.Disposed == false);
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
                        if (context.Transaction != null)
                        {
                            _recording.State?.Record(context, TxInstruction.DisposeTx, context.Transaction.Disposed == false);
                            context.Transaction.Dispose();
                        }
                    }
                }
            }
            finally
            {
                _recording.State?.Record(context, TxInstruction.DisposeTx, previous.Disposed == false);
                previous.Dispose();
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
            List<MergedTransactionCommand> pendingOps,
            DocumentsOperationContext context,
            Task previousOperation, ref PerformanceMetrics.DurationMeasurement meter)
        {
            _alreadyListeningToPreviousOperationEnd = false;
            context.TransactionMarkerOffset = 1;  // ensure that we are consistent here and don't use old values
            var sp = Stopwatch.StartNew();
            do
            {
                // RavenDB-7732 - Even if we merged multiple seprate operations into 
                // a single transaction in Voron, we're still going to have a separate
                // tx marker for them for the purpose of replication, to avoid creating
                // overly large replication batches.
                context.TransactionMarkerOffset++;

                if (TryGetNextOperation(previousOperation, out MergedTransactionCommand op, ref meter) == false)
                    break;

                pendingOps.Add(op);
                meter.IncrementCounter(1);

                meter.IncrementCommands(op.Execute(context, _recording.State));

                var llt = context.Transaction.InnerTransaction.LowLevelTransaction;
                var modifiedSize = llt.NumberOfModifiedPages * Constants.Storage.PageSize;

                var canCloseCurrentTx = previousOperation == null || previousOperation.IsCompleted;
                if (canCloseCurrentTx)
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

                // if I can't close the tx, this means that the previous async operation is stil in progress
                // and there are incoming requests coming in. We'll accept them, to a certain limit
                if (modifiedSize < _maxTxSizeInBytes)
                    continue; // we can still process requests at this time, so let's do that...

                UnlikelyRejectOperations(previousOperation, sp, llt, modifiedSize);

                break;

            } while (true);

            var status = GetPendingOperationsStatus(context, pendingOps.Count == 0);
            if (_log.IsInfoEnabled)
            {
                var opType = previousOperation == null ? string.Empty : "(async)";
                _log.Info($"Merged {pendingOps.Count:#,#;;0} operations in {sp.Elapsed} {opType} with {_operations.Count:#,#;;0} operations remaining. Status: {status}");
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
            if (sizeof(int) == IntPtr.Size || _parent.Configuration.Storage.ForceUsing32BitsPager)
                return PendingOperations.CompletedAll;

            // This optimization is disabled when encryption is on	
            if (context.Environment.Options.EncryptionEnabled)
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
            // this is probably a bit more aggresive that what is needed, but it is better 
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


        public enum TxInstruction
        {
            BeginTx,
            Commit,
            Rollback,
            DisposeTx,
            BeginAsyncCommitAndStartNewTransaction,
            EndAsyncCommit,
            DisposePrevTx
        }

        private RecordingTx _recording = default;

        private struct RecordingTx
        {
            public RecordingState State;
            public Stream Stream;
            public object LockTaken;
        }

        public void StartRecording(string filePath)
        {
            var recordingFileStream = new FileStream(filePath, FileMode.Create);
            if (null != Interlocked.CompareExchange(ref _recording.State, new RecordingState.BeforeEnabledRecordingState(this), null))
            {
                recordingFileStream.Dispose();
                File.Delete(filePath);
            }
            _recording.Stream = new GZipStream(recordingFileStream, CompressionMode.Compress);
        }

        public void StopRecording()
        {
            var recordingState = _recording.State;
            if (recordingState != null)
            {
                recordingState.Shutdown();
                _waitHandle.Set();
            }
        }

        public IEnumerable<ReplayProgress> Replay(Stream replayStream)
        {
            DocumentsOperationContext txCtx = null;
            IDisposable txDisposable = null;
            DocumentsTransaction previousTx = null;

            using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.GetManagedBuffer(out var buffer))
            using (var gZipStream = new GZipStream(replayStream, CompressionMode.Decompress, leaveOpen: true))
            {
                var peepingTomStream = new PeepingTomStream(gZipStream, context);
                var state = new JsonParserState();
                var parser = new UnmanagedJsonParser(context, state, "file");

                var commandsProgress = 0;
                var readers = UnmanagedJsonParserHelper.ReadArrayToMemory(context, peepingTomStream, parser, state, buffer);
                using (var readersItr = readers.GetEnumerator())
                {
                    ReadStartRecordingDetails(readersItr, context, peepingTomStream);
                    while (readersItr.MoveNext())
                    {
                        using (readersItr.Current)
                        {
                            if (readersItr.Current.TryGet(nameof(RecordingDetails.Type), out string strType) == false)
                            {
                                throw new ReplayTransactionsException($"Can't read {nameof(RecordingDetails.Type)} of replay detail", peepingTomStream);
                            }

                            if (Enum.TryParse<TxInstruction>(strType, true, out var type))
                            {
                                switch (type)
                                {
                                    case TxInstruction.BeginTx:
                                        txDisposable = _parent.DocumentsStorage.ContextPool.AllocateOperationContext(out txCtx);
                                        txCtx.OpenWriteTransaction();
                                        break;
                                    case TxInstruction.Commit:
                                        txCtx.Transaction.Commit();
                                        break;
                                    case TxInstruction.DisposeTx:
                                        txDisposable.Dispose();
                                        break;
                                    case TxInstruction.BeginAsyncCommitAndStartNewTransaction:
                                        previousTx = txCtx.Transaction;
                                        txCtx.Transaction = txCtx.Transaction.BeginAsyncCommitAndStartNewTransaction();
                                        txDisposable = txCtx.Transaction;
                                        break;
                                    case TxInstruction.EndAsyncCommit:
                                        previousTx.EndAsyncCommit();
                                        break;
                                    case TxInstruction.DisposePrevTx:
                                        previousTx.Dispose();
                                        break;
                                }
                                continue;
                            }

                            try
                            {
                                var cmd = DeserializeCommand(strType, readersItr.Current, context, peepingTomStream);
                                commandsProgress += cmd.ExecuteDirectly(txCtx);
                                UpdateGlobalReplicationInfoBeforeCommit(txCtx);
                            }
                            catch (Exception)
                            {
                                //TODO To accept exceptions that was thrown while recording
                                txDisposable.Dispose();
                                throw;
                            }

                            yield return new ReplayProgress
                            {
                                CommandsProgress = commandsProgress
                            };
                        }
                    }
                }
            }
        }

        private static void ReadStartRecordingDetails(IEnumerator<BlittableJsonReaderObject> iterator, DocumentsOperationContext context, PeepingTomStream peepingTomStream)
        {
            if (false == iterator.MoveNext())
            {
                throw new ReplayTransactionsException("Replay stream is empty", peepingTomStream);
            }
            using (iterator.Current)
            {
                var jsonSerializer = GetJsonSerializer();
                StartRecordingDetails startDetail;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(iterator.Current);
                    startDetail = jsonSerializer.Deserialize<StartRecordingDetails>(reader);
                }

                if (string.IsNullOrEmpty(startDetail.Type))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(RecordingDetails.Type)} of replay detail", peepingTomStream);
                }

                if (string.IsNullOrEmpty(startDetail.Type))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(StartRecordingDetails.Version)} of replay instructions", peepingTomStream);
                }

                if (startDetail.Version != RavenVersionAttribute.Instance.Build)
                {
                    throw new ReplayTransactionsException($"Can't replay transaction instructions of different server version - Current version({ServerVersion.FullVersion}), Record version({startDetail.Version})", peepingTomStream);
                }
            }
        }

        public class ReplayProgress
        {
            public long CommandsProgress;
        }

        private MergedTransactionCommand DeserializeCommand(string type, BlittableJsonReaderObject wrapCmdReader, DocumentsOperationContext context,
            PeepingTomStream peepingTomStream)
        {
            if (!wrapCmdReader.TryGet(nameof(RecordingCommandDetails.Command), out BlittableJsonReaderObject commandReader))
            {
                throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }

            var jsonSerializer = GetJsonSerializer();
            using (var reader = new BlittableJsonReader(context))
            {
                reader.Init(commandReader);
                var dto = DeserializeCommandDto(type, jsonSerializer, reader, peepingTomStream);
                return dto.ToCommand(context, _parent);
            }
        }

        private IReplayableCommandDto<MergedTransactionCommand> DeserializeCommandDto(string type, JsonSerializer jsonSerializer, BlittableJsonReader reader, PeepingTomStream peepingTomStream)
        {
            switch (type)
            {
                case nameof(BatchHandler.MergedBatchCommand):
                    return jsonSerializer.Deserialize<MergedBatchCommandDto>(reader);
                case nameof(DeleteDocumentCommand):
                    return jsonSerializer.Deserialize<DeleteDocumentCommandDto>(reader);
                case nameof(PatchDocumentCommand):
                    return jsonSerializer.Deserialize<PatchDocumentCommandDto>(reader);
                case nameof(DatabaseDestination.MergedBatchPutCommand):
                    return jsonSerializer.Deserialize<DatabaseDestination.MergedBatchPutCommandDto>(reader);
                case nameof(MergedPutCommand):
                    return jsonSerializer.Deserialize<MergedPutCommand.MergedPutCommandDto>(reader);
                case nameof(BulkInsertHandler.MergedInsertBulkCommand):
                    return jsonSerializer.Deserialize<MergedInsertBulkCommandDto>(reader);
                case nameof(AttachmentHandler.MergedPutAttachmentCommand):
                    return jsonSerializer.Deserialize<MergedPutAttachmentCommandDto>(reader);
                case nameof(AttachmentHandler.MergedDeleteAttachmentCommand):
                    return jsonSerializer.Deserialize<MergedDeleteAttachmentCommandDto>(reader);
                case nameof(ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand):
                    return jsonSerializer.Deserialize<PutResolvedConflictsCommandDto>(reader);
                case nameof(HiLoHandler.MergedNextHiLoCommand):
                    return jsonSerializer.Deserialize<MergedNextHiLoCommandDto>(reader);
                case nameof(HiLoHandler.MergedHiLoReturnCommand):
                    return jsonSerializer.Deserialize<MergedHiLoReturnCommandDto>(reader);
                case nameof(IncomingReplicationHandler.MergedDocumentReplicationCommand):
                    return jsonSerializer.Deserialize<MergedDocumentReplicationCommandDto>(reader);
                case nameof(ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand):
                    return jsonSerializer.Deserialize<DeleteExpiredDocumentsCommandDto>(reader);
                case nameof(OutgoingReplicationHandler.UpdateSiblingCurrentEtag):
                    return jsonSerializer.Deserialize<UpdateSiblingCurrentEtagDto>(reader);
                case nameof(IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommand):
                    return jsonSerializer.Deserialize<MergedUpdateDatabaseChangeVectorCommandDto>(reader);
                case nameof(AdminRevisionsHandler.DeleteRevisionsCommand):
                    return jsonSerializer.Deserialize<DeleteRevisionsCommandDto>(reader);
                case nameof(RevisionsOperations.DeleteRevisionsBeforeCommand):
                    throw new ReplayTransactionsException(
                        "Because this command is deleting according to revisions' date & the revisions that created by replaying have different date an in place decision needed to be made",
                        peepingTomStream);
                case nameof(TombstoneCleaner.DeleteTombstonesCommand):
                    return jsonSerializer.Deserialize<DeleteTombstonesCommandDto>(reader);
                case nameof(OutputReduceIndexWriteOperation.OutputReduceToCollectionCommand):
                    return jsonSerializer.Deserialize<OutputReduceToCollectionCommandDto>(reader);
                default:
                    throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }
        }

        private static JsonSerializer GetJsonSerializer()
        {
            var jsonSerializer = DocumentConventions.Default.CreateSerializer();
            jsonSerializer.Converters.Add(SliceJsonConverter.Instance);
            jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);
            jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
            jsonSerializer.Converters.Add(StreamConverter.Instance);
            return jsonSerializer;
        }

        public class ReplayTransactionsException : Exception
        {
            public string Context { get; }

            public ReplayTransactionsException(string message, PeepingTomStream peepingTomStream)
            : base(message)
            {
                try
                {
                    Context = Encodings.Utf8.GetString(peepingTomStream.PeepInReadStream());
                }
                catch (Exception e)
                {
                    Context = "Failed to generated peepedWindow: " + e;
                }
            }

            public override string Message => base.Message + ";" + Environment.NewLine + "Context" + Environment.NewLine + Context;
        }

        private class RecordingDetails
        {
            public string Type { get; }
            public DateTime DateTime { get; }

            public RecordingDetails(string type)
            {
                Type = type;
                DateTime = DateTime.Now;
            }
        }

        private class StartRecordingDetails : RecordingDetails
        {
            private const string DetailsType = "StartRecording";
            public string Version { get; }

            public StartRecordingDetails()
            : base(DetailsType)
            {
                Version = RavenVersionAttribute.Instance.Build;
            }
        }

        private class RecordingCommandDetails : RecordingDetails
        {
            public IReplayableCommandDto<MergedTransactionCommand> Command;

            public RecordingCommandDetails(string type) : base(type)
            {
            }
        }
    }
}
