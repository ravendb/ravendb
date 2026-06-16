using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Jint.Native;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Sparrow.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;


namespace Raven.Server.Documents.CdcSink;

public abstract class CdcSinkProcess : IDisposable, ILowMemoryHandler
{
    internal const string Tag = "CDC Sink";

    private CancellationTokenSource _cts;
    private PoolOfThreads.LongRunningWork _longRunningWork;

    private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();

    // Set when the process is stopped/disposed. HandleDatabaseRecordChange can dispose a process
    // (disposing _cts) before swapping it out of the loader's _processes array, so an in-flight
    // ongoing-tasks request may still call GetConnectionStatus() on it. Reading _cts.Token after
    // disposal throws ObjectDisposedException - guard against that with this flag.
    private readonly MultipleUseFlag _disposed = new MultipleUseFlag();

    protected readonly RavenLogger Logger;

    private CdcSinkStatsAggregator _lastStats;

    private int _statsId;

    private readonly ConcurrentQueue<CdcSinkStatsAggregator> _lastCdcSinkStats = new();

    protected readonly CdcSinkDocumentProcessor DocumentProcessor;

    protected CdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database, string defaultSchema)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        Logger = database.Loggers.GetLogger(GetType());
        Database = database;
        Configuration = configuration;
        Name = Configuration.Name;
        Statistics = new CdcSinkProcessStatistics(Tag, Name, Database.NotificationCenter);
        DocumentProcessor = new CdcSinkDocumentProcessor(configuration, defaultSchema) { Logger = Logger };
    }

    protected CancellationToken CancellationToken => _cts.Token;

    /// <summary>
    /// Reads the cancellation state without throwing if <see cref="_cts"/> has already been
    /// disposed by a concurrent Stop/Dispose. A disposed token source is treated as cancelled.
    /// </summary>
    private bool IsCancellationRequestedSafe()
    {
        try
        {
            return _cts.IsCancellationRequested;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    public DocumentDatabase Database { get; }

    /// <summary>
    /// Completed when the initial load phase finishes. Created at construction time
    /// so tests can await it without racing against the process start.
    /// </summary>
    internal Task InitialLoadCompleted => _initialLoadTcs.Task;
    protected TaskCompletionSource _initialLoadTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Raised each time RunInternalAsync fails with an exception (before entering fallback mode).
    /// Useful for tests that need to observe process errors without polling the notification center.
    /// </summary>
    internal event Action<Exception> ProcessError;

    /// <summary>
    /// The last exception thrown by RunInternalAsync (null if the process hasn't failed).
    /// </summary>
    internal Exception LastProcessException { get; private set; }

    /// <summary>
    /// True once the process hit a permanent configuration/schema error (a <see cref="CdcSinkFaultedException"/>)
    /// and stopped retrying. Correcting the configuration recreates the process, which clears this.
    /// </summary>
    public bool IsFaulted { get; private set; }

    public CdcSinkProcessStatistics Statistics { get; }

    public long TaskId => Configuration.TaskId;

    public string Name { get; }

    public CdcSinkConfiguration Configuration { get; }

    public TimeSpan? FallbackTime { get; internal set; }

    /// <summary>
    /// UTC time of the last successfully completed batch. Null if no batch has completed yet.
    /// Used by the dashboard to compute replication lag.
    /// </summary>
    public DateTime? LastBatchTime { get; private set; }

    /// <summary>
    /// The last successfully persisted checkpoint (LSN/GTID string).
    /// Null before the first batch completes.
    /// </summary>
    public string LastCheckpoint { get; private set; }

    /// <summary>
    /// UTC time of the last activity from the source — poll iteration (SQL Server),
    /// replication message/keepalive (PostgreSQL), or binlog event/heartbeat (MySQL).
    /// Distinguishes "no changes at source" (LastActivityTime recent, LastBatchTime old)
    /// from "connection silently dead" (LastActivityTime stale).
    /// </summary>
    public DateTime? LastActivityTime { get; protected set; }

    /// <summary>
    /// Checks whether the CDC Sink process is healthy. Returns true if healthy,
    /// false with a diagnostic message explaining the problem.
    /// Each provider implements its own logic based on its communication model.
    /// </summary>
    public abstract bool IsHealthy(out string issue);

    public OngoingTaskConnectionStatus GetConnectionStatus()
    {
        // The process may be disposed concurrently (reconfiguration) while still referenced by an
        // in-flight ongoing-tasks request - reading CancellationToken (_cts.Token) after disposal
        // would throw. Treat a disposed process as not active.
        if (_disposed.IsRaised() || Configuration.Disabled || IsCancellationRequestedSafe())
            return OngoingTaskConnectionStatus.NotActive;

        if (FallbackTime != null)
            return OngoingTaskConnectionStatus.Reconnect;

        if (Statistics.WasLatestConsumeSuccessful || Statistics.ConsumeErrors == 0)
            return OngoingTaskConnectionStatus.Active;

        return OngoingTaskConnectionStatus.NotActive;
    }

    public static CdcSinkProcessState GetProcessState(DocumentDatabase database, string configurationName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var stateBlittable = database.ServerStore.Cluster.Read(context,
                CdcSinkProcessState.GenerateItemName(database.Name, configurationName));

            if (stateBlittable != null)
            {
                return JsonDeserializationClient.CdcSinkProcessState(stateBlittable);
            }

            return new CdcSinkProcessState();
        }
    }

    /// <summary>
    /// The actual work for the CDC setup, initial load, streaming.
    /// Throwing exits the current attempt; the retry loop in <see cref="Run"/>
    /// will enter fallback mode and call this again after the backoff period.
    /// <see cref="OperationCanceledException"/> exits the process cleanly.
    /// </summary>
    protected abstract Task RunInternalAsync(CancellationToken ct);

    private async Task RunWithRetryAsync(CancellationToken ct)
    {
        // Record which node owns this CDC Sink so the mentor-node resolution keeps it sticky to this
        // node across topology changes. Once per start is enough - unlike ETL/QueueSink we don't persist
        // progress in this state (the LSN/GTID checkpoint lives in the CdcSinkTaskState document), so
        // there is nothing to refresh per batch.
        await TryUpdateProcessStateForStickiness();

        while (true)
        {
            // Reading the token (IsCancellationRequested / WaitHandle) can throw ObjectDisposedException
            // if Stop/Dispose disposed _cts concurrently - treat that as a cancellation and exit.
            try
            {
                if (ct.IsCancellationRequested)
                    return;

                if (FallbackTime != null)
                {
                    if (ct.WaitHandle.WaitOne(FallbackTime.Value))
                        return;

                    FallbackTime = null;
                }
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            try
            {
                await RunInternalAsync(ct);
            }
            catch (Exception e) when (IsPermanentFault(e))
            {
                // A permanent configuration/schema error (e.g. a configured table that doesn't resolve in the
                // processor mapping). Retrying can't fix it, so move to a faulted state and STOP the loop instead
                // of falling back and hammering the source forever. Correcting the configuration recreates the
                // process (CdcSinkLoader.HandleDatabaseRecordChange) for a fresh, un-faulted start.
                IsFaulted = true;
                LastProcessException = e;

                // Unlike the transient path below, a faulted process will not retry - so initial-load waiters
                // must observe the failure here rather than hang forever.
                _initialLoadTcs.TrySetException(e);
                ProcessError?.Invoke(e);

                if (Logger.IsErrorEnabled)
                    Logger.Error($"[{Name}] CDC Sink process faulted; it will not retry until the configuration is corrected.", e);

                Database.NotificationCenter.Add(AlertRaised.Create(
                    Database.Name, Tag,
                    $"[{Name}] CDC Sink process faulted (configuration error): {e.Message}",
                    AlertReason.CdcSink_Error,
                    NotificationSeverity.Error,
                    key: $"{Tag}/{Name}",
                    details: new ExceptionDetails(e)));

                Statistics.RecordConsumeError(e.ToString());
                return;
            }
            catch (OperationCanceledException)
            {
                _initialLoadTcs.TrySetCanceled();
                return;
            }
            catch (Exception e)
            {
                //  _initialLoadTcs.TrySetException(e);
                // Intentionally DOES NOT fault the _initialLoadTcs here — the process will retry after fallback.
                // If we set TrySetException, a subsequent successful retry can never signal completion.

                LastProcessException = e;
                ProcessError?.Invoke(e);

                if (Logger.IsErrorEnabled)
                    Logger.Error($"[{Name}] CDC Sink process failed.", e);
                var alert = AlertRaised.Create(
                    Database.Name, Tag,
                    $"[{Name}] CDC Sink process failed: {e.Message}",
                    AlertReason.CdcSink_Error,
                    NotificationSeverity.Error,
                    key: $"{Tag}/{Name}",
                    details: new ExceptionDetails(e));

                Database.NotificationCenter.Add(alert);

                // Read the error time BEFORE recording the new error.
                EnterFallbackMode();
                Statistics.RecordConsumeError(e.ToString());
            }
        }
    }

    // A CdcSinkFaultedException anywhere in the exception chain marks a permanent configuration/schema error
    // that retrying cannot fix; everything else is treated as transient and retried after fallback.
    private static bool IsPermanentFault(Exception e)
    {
        for (var current = e; current != null; current = current.InnerException)
        {
            if (current is CdcSinkFaultedException)
                return true;
        }

        return false;
    }

    // Records which node currently owns this CDC Sink so the mentor-node resolution keeps the task
    // sticky to it (UpdateCdcSinkProcessStateCommand's apply rejects writes from a node that isn't the
    // responsible one). Best-effort: a transient failure must not stop the task - on failover the new
    // owner records its own state on start.
    private async Task TryUpdateProcessStateForStickiness()
    {
        try
        {
            var state = new CdcSinkProcessState
            {
                ConfigurationName = Configuration.Name,
                NodeTag = Database.ServerStore.NodeTag
            };

            var command = new UpdateCdcSinkProcessStateCommand(
                Database.Name, state, Database.ServerStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());

            var (index, _) = await Database.ServerStore.SendToLeaderAsync(command);
            await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, Database.ServerStore.Engine.OperationTimeout);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Could not record CDC Sink process state for node stickiness; the task still runs. {e}");
        }
    }

    protected void AddPerformanceStats(CdcSinkStatsAggregator stats)
    {
        _lastStats = stats;

        _lastCdcSinkStats.Enqueue(stats);

        while (_lastCdcSinkStats.Count > 25)
            _lastCdcSinkStats.TryDequeue(out _);
    }

    public void Start()
    {
        if (_longRunningWork != null)
            return;

        if (Configuration.Disabled)
            return;

        _cts?.Dispose();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);
        _disposed.Lower();

        // Callers should re-read the InitialLoadCompleted property after this Start() returns.
        _initialLoadTcs.TrySetCanceled();
        _initialLoadTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

        var threadName = $"{Tag} process: {Name}";
        _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
        {
            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                NativeMemory.EnsureRegistered();
                AsyncHelpers.RunSync(() => RunWithRetryAsync(_cts.Token));
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to run CDC Sink {Name}", e);
            }
        }, null, ThreadNames.ForCdcSinkProcess(threadName, Tag, Name));

        if (Logger.IsInfoEnabled)
            Logger.Info($"Starting {Tag} process: '{Name}'.");
    }

    public void Stop(string reason)
    {
        if (_longRunningWork == null)
            return;

        string msg = $"Stopping {Tag} process: '{Name}'. Reason: {reason}";

        if (Logger.IsInfoEnabled)
        {
            Logger.Info(msg);
        }

        _cts.SafeCancel(Logger, $"{Tag} process '{Name}'");

        var longRunningWork = _longRunningWork;
        _longRunningWork = null;

        if (longRunningWork != PoolOfThreads.LongRunningWork.Current)
            longRunningWork.Join(int.MaxValue);

        // Raise BEFORE disposing _cts so a concurrent GetConnectionStatus() sees the process as
        // not-active rather than reading the disposed token source.
        _disposed.Raise();
        _cts.Dispose();
    }

    protected void EnterFallbackMode()
    {
        if (Statistics.LastConsumeErrorTime is DateTime lastErrorTime)
        {
            var secondsSinceLastError =
                (Database.Time.GetUtcNow() - lastErrorTime).TotalSeconds;

            var maxSeconds = Database.Configuration.CdcSink.MaxFallbackTime.AsTimeSpan.TotalSeconds;
            // Jitter: add up to 10% random variation to avoid synchronized retries
            // across multiple CDC Sink processes when a shared source goes down.
            var baseSeconds = Math.Min(maxSeconds, Math.Max(5, secondsSinceLastError * 2));
            var jitter = baseSeconds * Random.Shared.NextDouble() * 0.1;
            FallbackTime = TimeSpan.FromSeconds(baseSeconds + jitter);
        }
        else
        {
            FallbackTime = TimeSpan.FromSeconds(5);
        }
    }

    protected async Task<(string Checkpoint, int Rows)> SubmitBatch(List<CdcSinkDocumentOp> ops, string checkpoint = null,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates = null,
        Commands.CdcSinkBatchCommand.DocumentGrouper grouper = null)
    {
        // Compact nulls — streaming paths add null entries for rows from unconfigured tables
        int pos = 0;
        for (int i = 0; i < ops.Count; i++)
        {
            ops[pos] = ops[i];
            pos += (ops[i] != null).ToInt32();
        }
        ops.RemoveRange(pos, ops.Count - pos);

        // We *intentionally* submit the command here, even if we have no entries
        // to update the checkpoint and table load state in a timely manner

        // Per-batch stats aggregator, mirroring the ETL/QueueSink stats lifecycle: chain the
        // start time off the previous batch, open a scope the batch command records into,
        // start the timer and register it as the latest (and in the ring buffer), then
        // Complete() once the batch finishes. CreateScope() returns an unstarted scope, so
        // Start() it here to begin timing at batch submission rather than at construction.
        var statsAggregator = new CdcSinkStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);
        var stats = statsAggregator.CreateScope();

        statsAggregator.Start();
        stats.Start();
        AddPerformanceStats(statsAggregator);

        try
        {
            var command = new Commands.CdcSinkBatchCommand(
                Database, ops, Configuration.Name, checkpoint,
                tableLoadUpdates: tableLoadUpdates,
                patchRequest: DocumentProcessor.CombinedPatchRequest,
                statsScope: stats, statistics: Statistics, logger: Logger,
                grouper: grouper, defaultSchema: DefaultSchema);

            var start = Stopwatch.GetTimestamp();
            await Database.TxMerger.Enqueue(command);

            LastBatchTime = Database.Time.GetUtcNow();
            if (checkpoint != null)
                LastCheckpoint = checkpoint;

            if (Logger.IsDebugEnabled)
                Logger.Debug($"[{Name}] SubmitBatch: {command.ProcessedSuccessfully} ops persisted in {Stopwatch.GetElapsedTime(start).TotalMilliseconds:#,#} ms, checkpoint={checkpoint ?? "(none)"}");
        }
        finally
        {
            stats.Dispose();
            statsAggregator.Complete();
        }

        Database.CdcSinkLoader.OnBatchCompleted(Configuration.Name, Name, Statistics);
        return (checkpoint, ops.Count);
    }


    protected enum CdcEventType { Upsert, Delete, TransactionCommit }

    protected readonly record struct CdcEvent(CdcEventType Type, CdcSinkDocumentOp Op, string Checkpoint);

    /// <summary>
    /// Builds the <see cref="CdcEvent"/>(s) for an UPDATE on an embedded table.
    /// When the join column changed (reparenting), returns a Delete against the old parent
    /// plus an Upsert against the new parent. Otherwise <c>Delete</c> is null and only the
    /// Upsert event is returned. The caller is responsible for returning <paramref name="oldValues"/>
    /// to the processor pool when <c>Delete</c> is null (when it is non-null, the values array
    /// lives on as <c>deleteOp.RawValues</c> and is released with the batch).
    /// Precondition: <paramref name="newOp"/>.Processor.IsRoot must be false.
    /// </summary>
    /// <param name="newOp">The op produced by <see cref="CdcSinkDocumentProcessor.ProcessRow(CdcSinkTableProcessor, CdcSinkOperation, object[], JsonOperationContext)"/> for the new row.</param>
    /// <param name="oldValues">Decoded column values from the row BEFORE the update (same positional layout as <see cref="CdcSinkTableProcessor.SourceColumnNames"/>).</param>
    protected (CdcEvent? Delete, CdcEvent Upsert) CreateEmbeddedUpdateEvents(
        CdcSinkDocumentOp newOp, object[] oldValues)
    {
        var upsert = new CdcEvent(CdcEventType.Upsert, newOp, null);
        var proc = newOp.Processor;

        var oldParentId = proc.GetParentDocumentId(oldValues);
        if (string.Equals(oldParentId, newOp.DocumentId, StringComparison.Ordinal))
            return (null, upsert);

        // Reparent: build the delete op with MappedData derived from the OLD row so that
        // $old in OnDelete patches (and any PK-based array/map matching) reflects the
        // actual item being removed from the old parent.
        var oldMappedData = proc.MapColumns(oldValues, StreamingJsonContext);
        proc.ApplyLinks(oldMappedData, oldValues);

        var deleteOp = new CdcSinkDocumentOp
        {
            Type = CdcSinkDocumentOpType.EmbeddedModify,
            DocumentId = oldParentId,
            Processor = proc,
            MappedData = oldMappedData,
            RawValues = oldValues,
            Operation = CdcSinkOperation.Delete,
        };
        return (new CdcEvent(CdcEventType.Delete, deleteOp, null), upsert);
    }

    /// <summary>
    /// Returns an async stream of CDC events from the source database.
    /// Each subclass converts provider-specific events into <see cref="CdcEvent"/>:
    /// <list type="bullet">
    /// <item>Insert/Update → <see cref="CdcEventType.Upsert"/> with the decoded op</item>
    /// <item>Delete → <see cref="CdcEventType.Delete"/> with the decoded op</item>
    /// <item>Transaction boundary → <see cref="CdcEventType.TransactionCommit"/> with checkpoint string</item>
    /// </list>
    /// </summary>
    protected abstract IAsyncEnumerable<CdcEvent> GetCdcEvents(CancellationToken ct);

    /// <summary>
    /// Called after each batch is submitted to the TxMerger. Subclasses can override to
    /// perform provider-specific acknowledgment (e.g. PostgreSQL WAL status update).
    /// </summary>
    protected virtual Task OnBatchFlushed(string checkpoint, int rows) => Task.CompletedTask;

    /// <summary>
    /// The current JSON parsing context for CDC streaming. Subclasses use this in
    /// <see cref="GetCdcEvents"/> when decoding rows. Rotated on each batch flush
    /// so that blittable objects from the previous batch stay alive until the TxMerger
    /// finishes writing them.
    /// </summary>
    protected DocumentsOperationContext StreamingJsonContext { get; private set; }

    /// <summary>
    /// Common streaming loop shared by all providers. Uses MoveNextAsync + Task.WhenAny
    /// to overlap event reading with batch processing — when the TxMerger finishes a batch
    /// while we're waiting for the next event, we immediately flush any accumulated ops
    /// without waiting for another event to arrive.
    /// </summary>
    protected async Task ProcessCdcStream(CancellationToken ct)
    {
        var batch = new List<CdcSinkDocumentOp>();
        var pending = new List<CdcSinkDocumentOp>();
        var emptyTask = Task.FromResult<(string, int)>((null, 0));
        Task<(string, int)> lastBatch = emptyTask;
        List<CdcSinkDocumentOp> lastBatchOps = null;
        // Two groupers for the two concurrent batches in the pipeline (current + previous).
        var groupers = new[] { new Commands.CdcSinkBatchCommand.DocumentGrouper(), new Commands.CdcSinkBatchCommand.DocumentGrouper() };
        int grouperIndex = 0;
        string lastCheckpoint = null;

        // Context lifetime. Blittables produced by the decode (ProcessRow/DecodeRow inside
        // GetCdcEvents) reference the memory of whatever context was StreamingJsonContext at decode
        // time; they live on in `pending`/`batch` until the TxMerger finishes writing the batch they
        // end up in. We therefore allocate a fresh streaming context (rotate) ONLY at points where
        // the current context has no live reference that would outlive the rotation:
        //   * no row is being decoded right now - a MoveNextAsync in flight (the Task.WhenAny flush
        //     path) is actively writing into the current context, and
        //   * `pending` is empty - uncommitted ops still reference the current context and will be
        //     submitted in a later batch.
        // When either holds we keep the same context and defer the rotation to the next safe flush.
        // A retired context (previousCtx) backs every batch submitted since the last rotation; since
        // batches are awaited in submit order, it is safe to dispose once the most recent batch that
        // used it (lastBatch) has completed - which is exactly the next FlushBatch's `await lastBatch`.
        IDisposable previousCtx = null;
        var currentCtx = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);
        StreamingJsonContext = ctx;

        async Task FlushBatch(bool decodeInFlight)
        {
            (string completedCheckpoint, int rows) = await lastBatch;
            if (completedCheckpoint is not null)
                await OnBatchFlushed(completedCheckpoint, rows);

            // Return values from the completed batch back to per-table pools.
            // Done here (after await) rather than inside SubmitBatch so that pool
            // access stays on the caller's flow and never races with RentValues.
            if (lastBatchOps != null)
            {
                DocumentProcessor.ReturnBatchValues(lastBatchOps);
                lastBatchOps = null;
            }

            // previousCtx (if any) backed batches that are all guaranteed complete now - we just
            // awaited the most recent one - and it is never the live StreamingJsonContext, so it is
            // safe to dispose. Null it out so a deferred rotation doesn't double-dispose.
            if (previousCtx != null)
            {
                previousCtx.Dispose();
                previousCtx = null;
            }

            if (batch.Count is not 0)
            {
                lastBatchOps = batch;
                lastBatch = SubmitBatch(batch, lastCheckpoint, grouper: groupers[grouperIndex]);
                grouperIndex ^= 1; // alternate between the two groupers
                batch = [];
            }
            else
            {
                // drop reference to the last batch, in case it holds any resources / memory
                lastBatch = emptyTask;
            }

            // Rotate only when the current context has no reference that would outlive it: no decode
            // is mid-flight and no uncommitted ops remain. Otherwise keep it and rotate at the next
            // safe flush; the just-submitted batch then shares the context with that deferred work,
            // and the context is retired (and later disposed) only once we can prove no one reads it.
            if (decodeInFlight == false && pending.Count == 0)
            {
                previousCtx = currentCtx;
                currentCtx = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out ctx);
                StreamingJsonContext = ctx;
            }
        }

        try
        {
            await using var enumerator = GetCdcEvents(ct).GetAsyncEnumerator(ct);
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var moveNext = enumerator.MoveNextAsync();

                bool moveNextResult = false;

                if (moveNext.IsCompleted == false)
                {
                    var moveTask = moveNext.AsTask();

                    try
                    {
                        // Race: wait for either the next event or the previous batch to complete.
                        // This allows flushing accumulated ops while the source is idle or slow.
                        await Task.WhenAny(moveTask, lastBatch);

                        // A decode (moveTask) is in flight here writing into StreamingJsonContext,
                        // so FlushBatch must NOT rotate/free that context out from under it.
                        if (lastBatch.IsCompleted || ShouldFlushBatch(batch.Count))
                            await FlushBatch(decodeInFlight: true);

                        if (moveTask.IsCompleted == false)
                        {
                            // Source is idle - clear array contents immediately to release
                            // references for GC, but keep the arrays in the pool for reuse.
                            DocumentProcessor.ClearValuePoolArrays();

                            // If still idle after 1 minute, release the pooled arrays entirely.
                            var completed = await moveTask.WaitFor(TimeSpan.FromMinutes(1), ct);
                            if (completed != moveTask)
                                DocumentProcessor.ClearValuePools();
                        }

                        moveNextResult = await moveTask;
                    }
                    catch
                    {
                        // The decode may still be running (e.g. FlushBatch above threw). Drain it
                        // before unwinding so the async enumerator can be disposed and so the
                        // streaming context is never freed while a decode is still writing into it.
                        await DrainSafely(moveTask);
                        throw;
                    }
                }
                else
                {
                    moveNextResult = await moveNext;
                }

                if (moveNextResult is false)
                    break; // enumerator is completed (cancelled, probably)

                var evt = enumerator.Current;
                LastActivityTime = Database.Time.GetUtcNow();

                switch (evt.Type)
                {
                    case CdcEventType.Upsert or CdcEventType.Delete:
                        pending.Add(evt.Op);
                        break;

                    case CdcEventType.TransactionCommit:
                        batch.AddRange(pending);
                        pending.Clear();
                        lastCheckpoint = evt.Checkpoint;
                        break;
                }

                // Bound memory: when events arrive fast enough that MoveNextAsync keeps completing
                // synchronously, the Task.WhenAny flush path above is skipped, so `batch` would grow
                // without bound until the source goes idle. Flush completed transactions here once the
                // batch reaches the size limit. (A single transaction is never split; this only bounds
                // cross-transaction accumulation. This is also a race-free flush point - no decode is
                // in flight here, unlike the WhenAny path.)
                if (ShouldFlushBatch(batch.Count))
                    await FlushBatch(decodeInFlight: false);
            }

            if (pending.Count > 0 && Logger.IsDebugEnabled)
                Logger.Debug($"[{Name}] Discarding {pending.Count} pending op(s) from incomplete transaction at stream end.");

            // Stream ended — flush remaining ops, then wait for the final batch to complete
            await FlushBatch(decodeInFlight: false);
            (string finalCheckpoint, int _) = await lastBatch;
            if (lastBatchOps != null)
            {
                DocumentProcessor.ReturnBatchValues(lastBatchOps);
                lastBatchOps = null;
            }
            if (finalCheckpoint is not null) // force a flush, ensure the last LSN is acknowledged for PostgreSQL and similar providers.
                await OnBatchFlushed(finalCheckpoint, Database.Configuration.CdcSink.MaxBatchSize + 1);
        }
        finally
        {
            // The in-flight TxMerger batch reads previousCtx/currentCtx; wait for it to finish
            // before freeing them so the exception/cancellation path never disposes a context that
            // is still being written. (Any in-flight decode was already drained inside the loop.)
            await DrainSafely(lastBatch);

            previousCtx?.Dispose();
            currentCtx?.Dispose();
            StreamingJsonContext = null;
        }
    }

    /// <summary>
    /// Awaits a possibly-in-flight task, swallowing any fault/cancellation. Used on disposal/teardown
    /// paths to ensure a batch or decode has finished touching a context before it is freed, without
    /// masking the exception that triggered the teardown.
    /// </summary>
    private static async Task DrainSafely(Task task)
    {
        if (task is null)
            return;
        try
        {
            await task;
        }
        catch
        {
            // The task already faulted or was cancelled; disposal must still proceed.
        }
    }

    /// <summary>
    /// Returns the default schema name for this database provider (e.g. "public" for PostgreSQL,
    /// "dbo" for SQL Server, database name for MySQL).
    /// </summary>
    protected abstract string GetDefaultSchema();

    public string DefaultSchema => GetDefaultSchema();

    /// <summary>
    /// Opens a database connection for the initial load phase. Called once before all tables
    /// are loaded. The connection is passed to <see cref="ReadOneBatch"/> and disposed after
    /// the load completes.
    /// </summary>
    protected abstract Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct);


    /// <summary>
    /// Builds the SELECT used by initial-load keyset pagination. Default implementation is
    /// PostgreSQL/MySQL syntax (LIMIT + row-value comparison `(c1,c2) > (@k0,@k1)`).
    /// SQL Server overrides this because it does not support row-value comparison.
    /// </summary>
    protected virtual string BuildBatchQuery(
        CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize)
    {
        var table = $"{CommandBuilder.QuoteIdentifier(tableInfo.Schema)}.{CommandBuilder.QuoteIdentifier(tableInfo.TableName)}";
        var pkCols = string.Join(", ", pkColumns.Select(c => CommandBuilder.QuoteIdentifier(c)));
        var where = lastKeys != null
            ? $" WHERE ({pkCols}) > ({string.Join(", ", pkColumns.Select((_, i) => $"@k{i}"))})"
            : "";

        return $"SELECT * FROM {table} {where} ORDER BY {pkCols} LIMIT {maxBatchSize}";
    }

    /// <summary>
    /// Binds keyset pagination parameters to the command. Postgres and SQL Server
    /// convert string keys to typed values; MySQL passes raw strings.
    /// Only called when <paramref name="lastKeys"/> is not null.
    /// </summary>
    protected abstract Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct);

    /// <summary>
    /// Converts a single column value from the initial load DbDataReader to a
    /// normalized CLR type for the document processor pipeline. Receives the source
    /// table identity so providers can consult their own per-column metadata
    /// (e.g. MySQL's MySqlColumnCategory) for type-aware coercions that the raw
    /// DbDataReader value alone can't drive.
    /// </summary>
    protected abstract object ConvertInitialLoadValue(DbDataReader reader, int ordinal, CdcSinkConfiguration.TableInfo tableInfo);

    /// <summary>
    /// Result of a single initial-load batch read: the decoded ops, the keyset-pagination resume
    /// keys, and the context whose memory the ops' blittables reference (the caller disposes it once
    /// the batch has been written).
    /// </summary>
    private readonly record struct InitialLoadBatch(List<CdcSinkDocumentOp> Ops, string[] LastKeys, IDisposable Context);

    private async Task<InitialLoadBatch> ReadOneBatch(
        DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize, CancellationToken ct)
    {
        var query = BuildBatchQuery(tableInfo, pkColumns, lastKeys, maxBatchSize);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = query;

        if (lastKeys != null)
            await BindKeysetParameters(cmd, tableInfo, pkColumns, lastKeys, ct);

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        // Context must outlive this method — blittable objects created by ProcessRow
        // (for CdcColumnType.Json columns) reference the context's memory. The caller
        // is responsible for disposing it after the batch has been processed.
        var ctxHolder = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingCtx);

        try
        {
            var ops = new List<CdcSinkDocumentOp>();
            CdcSinkTableProcessor processor;
            try
            {
                processor = DocumentProcessor.GetProcessor(tableInfo.Schema, tableInfo.TableName);
            }
            catch (InvalidOperationException e)
            {
                // A configured table that doesn't resolve in the mapping is a permanent configuration error,
                // not a transient failure - fault the process so it stops retrying until the config is fixed,
                // rather than re-running the initial load forever against a mapping that can never match.
                throw new CdcSinkFaultedException(
                    $"Initial load cannot start for table '{tableInfo.Schema}.{tableInfo.TableName}': it does not resolve " +
                    "in the task's table mapping. Fix the table configuration; the task will restart.", e);
            }

            // On first batch for this table, set source column names from the reader if not already set
            bool columnNamesSet = processor.SourceColumnNames != null;

            while (await reader.ReadAsync(ct))
            {
                if (columnNamesSet == false)
                {
                    var columnNames = new string[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++)
                        columnNames[i] = reader.GetName(i);
                    processor.SetSourceColumnNames(columnNames);
                    columnNamesSet = true;
                }
                else
                {
                    string[] sourceColumnNames = processor.SourceColumnNames;
                    if (sourceColumnNames != null && reader.FieldCount != sourceColumnNames.Length)
                    {
                        throw new InvalidOperationException(
                            $"Column count mismatch for table {tableInfo.FullName}: " +
                            $"expected {sourceColumnNames.Length} columns but SELECT * returned {reader.FieldCount}. " +
                            "The table schema may have changed. The process will retry with updated column metadata.");
                    }
                }

                var values = processor.RentValues();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    values[i] = reader.IsDBNull(i) ? null : ConvertInitialLoadValue(reader, i, tableInfo);
                }

                ops.Add(DocumentProcessor.ProcessRow(processor, CdcSinkOperation.Upsert, values, jsonParsingCtx));
            }

            // Extract last keys from the last non-null op's RawValues for keyset pagination resume.
            string[] newLastKeys = null;
            for (int j = ops.Count - 1; j >= 0; j--)
            {
                if (ops[j]?.RawValues == null)
                    continue;
                var lastValues = ops[j].RawValues;
                var pkIndices = ops[j].Processor.PrimaryKeyIndices;
                newLastKeys = new string[pkColumns.Count];
                for (int i = 0; i < pkColumns.Count; i++)
                    newLastKeys[i] = lastValues[pkIndices[i]]?.ToString() ?? "";
                break;
            }

            return new InitialLoadBatch(ops, newLastKeys, ctxHolder);
        }
        catch
        {
            ctxHolder.Dispose();
            throw;
        }
    }


    /// <summary>
    /// Returns the provider's current streaming position (e.g., GTID, WAL LSN, CDC max LSN).
    /// Called by HandleInitialLoad when no checkpoint exists yet, to persist a starting point
    /// so that a restart after initial load streams from the correct position.
    /// </summary>
    protected virtual Task<string> ReadCurrentCheckpointAsync(CancellationToken ct)
        => Task.FromResult<string>(null);

    protected async Task HandleInitialLoad(CancellationToken ct)
    {
        CdcSinkTaskState state;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            state = LoadState(context);
        }

        // If no streaming checkpoint exists yet, capture the current position from the source
        // database and persist it. This ensures that on restart, streaming resumes from where
        // we were when initial load began — not from wherever the server is at restart time.
        if (string.IsNullOrEmpty(state.LastLsn))
        {
            var checkpoint = await ReadCurrentCheckpointAsync(ct);
            if (string.IsNullOrEmpty(checkpoint) == false)
            {
                await SubmitBatch([], checkpoint: checkpoint);
            }
        }

        var allTables = Configuration.CollectAllTablesFlat(GetDefaultSchema());

        if (Configuration.SkipInitialLoad)
        {
            var updates = new Dictionary<string, CdcSinkTableLoadState>();
            foreach (var tableInfo in allTables)
            {
                var tableKey = tableInfo.FullName;
                if (state.Tables.TryGetValue(tableKey, out var ts) && ts.InitialLoadCompleted)
                    continue;
                updates[tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true };
            }

            if (updates.Count > 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] SkipInitialLoad is set — marking {updates.Count} table(s) as complete without scanning.");
                await SubmitBatch([], tableLoadUpdates: updates);
            }

            return;
        }

        // Single connection for the entire initial load — reused across all tables
        await using var conn = await OpenInitialLoadConnection(ct);

        foreach (var tableInfo in allTables)
        {
            var tableKey = tableInfo.FullName;

            if (state.Tables.TryGetValue(tableKey, out var tableState) && tableState.InitialLoadCompleted)
                continue;

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Starting initial load for table {tableInfo.FullName}");

            await ProcessTableInitialLoad(conn, tableInfo, tableKey, tableState, ct);

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Completed initial load for table {tableInfo.FullName}");
        }
    }

    private async Task ProcessTableInitialLoad(
        DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, string tableKey,
        CdcSinkTableLoadState resumeState, CancellationToken ct)
    {
        var pkColumns = tableInfo.PrimaryKeyColumns;
        var maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

        string[] lastKeys = null;
        if (resumeState?.LastKeyValues != null && resumeState.LastKeyValues.Count == pkColumns.Count)
        {
            lastKeys = new string[resumeState.LastKeyValues.Count];
            for (int i = 0; i < resumeState.LastKeyValues.Count; i++)
                lastKeys[i] = resumeState.LastKeyValues[i];
        }

        var lastBatch = Task.CompletedTask;
        List<CdcSinkDocumentOp> lastBatchOps = null;
        IDisposable previousBatchCtx = null;
        IDisposable currentBatchCtx = null;

        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                var (ops, newLastKeys, batchCtx) = await ReadOneBatch(conn, tableInfo, pkColumns, lastKeys, maxBatchSize, ct);
                currentBatchCtx = batchCtx;

                if (ops.Count == 0)
                {
                    currentBatchCtx?.Dispose();
                    currentBatchCtx = null;

                    await lastBatch;
                    if (lastBatchOps != null)
                    {
                        DocumentProcessor.ReturnBatchValues(lastBatchOps);
                    }
                    previousBatchCtx?.Dispose();
                    previousBatchCtx = null;

                    var finalUpdate = new Dictionary<string, CdcSinkTableLoadState>
                    {
                        [tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true }
                    };
                    await SubmitBatch(new List<CdcSinkDocumentOp>(), tableLoadUpdates: finalUpdate);
                    return;
                }

                await lastBatch;
                if (lastBatchOps != null)
                {
                    DocumentProcessor.ReturnBatchValues(lastBatchOps);
                    lastBatchOps = null;
                }
                previousBatchCtx?.Dispose();

                var tableLoadUpdate = new Dictionary<string, CdcSinkTableLoadState>
                {
                    [tableKey] = new CdcSinkTableLoadState { LastKeyValues = new List<string>(newLastKeys) }
                };

                lastBatchOps = ops;
                lastBatch = SubmitBatch(ops, tableLoadUpdates: tableLoadUpdate);
                previousBatchCtx = currentBatchCtx;
                currentBatchCtx = null;
                lastKeys = newLastKeys;
            }
        }
        finally
        {
            // The in-flight submitted batch reads previousBatchCtx; wait for it to finish before
            // disposing so the exception/cancellation path never frees a context the TxMerger is
            // still writing. (The initial-load decode in ReadOneBatch is fully awaited before each
            // submit, so unlike the streaming loop there is never a decode in flight here.)
            await DrainSafely(lastBatch);
            previousBatchCtx?.Dispose();
            currentBatchCtx?.Dispose();
        }
    }

    public CdcSinkPerformanceStats[] GetPerformanceStats()
    {
        var lastStats = _lastStats;
        List<CdcSinkPerformanceStats> result = [];

        foreach (var stats in _lastCdcSinkStats)
        {
            result.Add(stats == lastStats
                ? stats.ToPerformanceLiveStatsWithDetails()
                : stats.ToPerformanceStats());
        }

        return result.ToArray();
    }

    public CdcSinkStatsAggregator GetLatestPerformanceStats()
    {
        return _lastStats;
    }

    /// <summary>
    /// Lightweight batch-size check for the streaming loops in derived classes.
    /// Returns false when the batch should be flushed (size limit, low memory, or CPU credits).
    /// </summary>
    protected bool ShouldFlushBatch(int batchSize)
    {
        if (batchSize >= Database.Configuration.CdcSink.MaxBatchSize)
            return true;

        // we are always called with a full transaction, so no point in 
        // trying to fill a whole batch in low mem state
        return _lowMemoryFlag.IsRaised() ||
            Database.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised();
    }


    protected CdcSinkTaskState LoadState(DocumentsOperationContext context)
    {
        var stateDocId = CdcSinkTaskState.GetDocumentId(Configuration.Name);
        var doc = Database.DocumentsStorage.Get(context, stateDocId);

        if (doc == null)
            return new CdcSinkTaskState { ConfigurationName = Configuration.Name };

        return JsonDeserializationServer.CdcSinkTaskState(doc.Data);
    }

    public virtual void Dispose()
    {
        // Mark disposed up front so a concurrent GetConnectionStatus() bails out before touching _cts,
        // even on the never-started path where Stop() returns early but _cts is still disposed below.
        _disposed.Raise();

        var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

        exceptionAggregator.Execute(() => Stop("Dispose"));

        exceptionAggregator.Execute(() => _cts.Dispose());

        exceptionAggregator.Execute(() => CommandBuilder.Dispose());

        exceptionAggregator.ThrowIfNeeded();
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        _lowMemoryFlag.Raise();
    }

    public void LowMemoryOver()
    {
        _lowMemoryFlag.Lower();
    }

    internal static void AddParameter(DbCommand cmd, string name, object value)
    {
        var param = cmd.CreateParameter();
        param.ParameterName = name;
        param.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(param);
    }

    /// <summary>
    /// Provider-specific command builder for safe identifier quoting via
    /// <see cref="DbCommandBuilder.QuoteIdentifier"/>. Disposed with the process.
    /// </summary>
    protected abstract DbCommandBuilder CommandBuilder { get; }
}
