using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Converters;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlProcess : IDisposable, ITombstoneAware
    {
        public string Tag { get; protected set; }

        public EtlProcessStatistics Statistics { get; protected set; }

        public EtlMetricsCountersManager Metrics { get; protected set; }

        public string Name { get; protected set; }

        public string ConfigurationName { get; protected set; }

        public string TransformationName { get; protected set; }

        public TimeSpan? FallbackTime { get; protected set; }

        public abstract void Start();

        public abstract void Stop();

        public abstract void Dispose();

        public abstract void Reset();

        public abstract void Reset(DocumentChange change);

        public abstract EtlPerformanceStats[] GetPerformanceStats();

        public abstract Dictionary<string, long> GetLastProcessedTombstonesPerCollection();

        public abstract OngoingTaskConnectionStatus GetConnectionStatus();

        public static EtlProcessState GetProcessState(DocumentDatabase database, string configurationName, string transformationName)
        {
            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var stateBlittable = database.ServerStore.Cluster.Read(context, EtlProcessState.GenerateItemName(database.Name, configurationName, transformationName));

                if (stateBlittable != null)
                {
                    return JsonDeserializationClient.EtlProcessState(stateBlittable);
                }

                return new EtlProcessState();
            }
        }
    }

    public abstract class EtlProcess<TExtracted, TTransformed, TConfiguration, TConnectionString> : EtlProcess where TExtracted : ExtractedItem where TConfiguration : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
    {
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim();
        private readonly CancellationTokenSource _cts;
        private readonly HashSet<string> _collections;

        private readonly ConcurrentQueue<EtlStatsAggregator> _lastEtlStats =
            new ConcurrentQueue<EtlStatsAggregator>();

        private Size _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
        private NativeMemory.ThreadStats _threadAllocations;
        private PoolOfThreads.LongRunningWork _longRunningWork;
        private EtlStatsAggregator _lastStats;
        private int _statsId;

        protected readonly Transformation Transformation;
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;
        private readonly ServerStore _serverStore;

        public readonly TConfiguration Configuration;

        protected EtlProcess(Transformation transformation, TConfiguration configuration, DocumentDatabase database, ServerStore serverStore, string tag)
        {
            Transformation = transformation;
            Configuration = configuration;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
            Tag = tag;
            ConfigurationName = Configuration.Name;
            TransformationName = Transformation.Name;
            Name = $"{Configuration.Name}/{Transformation.Name}";
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            Database = database;
            _serverStore = serverStore;
            Statistics = new EtlProcessStatistics(Tag, Name, Database.NotificationCenter);

            if (transformation.ApplyToAllDocuments == false)
                _collections = new HashSet<string>(Transformation.Collections, StringComparer.OrdinalIgnoreCase);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        protected abstract IEnumerator<TExtracted> ConvertDocsEnumerator(IEnumerator<Document> docs, string collection);

        protected abstract IEnumerator<TExtracted> ConvertTombstonesEnumerator(IEnumerator<Tombstone> tombstones, string collection);

        protected abstract bool ShouldTrackAttachmentTombstones();

        public virtual IEnumerable<TExtracted> Extract(DocumentsOperationContext context, long fromEtag, EtlStatsScope stats)
        {
            using (var scope = new DisposableScope())
            {
                var enumerators = new List<(IEnumerator<Document> Docs, IEnumerator<Tombstone> Tombstones, string Collection)>(Transformation.Collections.Count);

                if (Transformation.ApplyToAllDocuments)
                {
                    var docs = Database.DocumentsStorage.GetDocumentsFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();
                    scope.EnsureDispose(docs);

                    var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();
                    scope.EnsureDispose(tombstones);
                    tombstones = new FilterTombstonesEnumerator(tombstones, stats, Tombstone.TombstoneType.Document, context);

                    enumerators.Add((docs, tombstones, null));
                }
                else
                {
                    foreach (var collection in Transformation.Collections)
                    {
                        var docs = Database.DocumentsStorage.GetDocumentsFrom(context, collection, fromEtag, 0, int.MaxValue).GetEnumerator();
                        scope.EnsureDispose(docs);

                        var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, collection, fromEtag, 0, int.MaxValue).GetEnumerator();
                        scope.EnsureDispose(tombstones);

                        enumerators.Add((docs, tombstones, collection));
                    }
                }

                using (var merged = new ExtractedItemsEnumerator<TExtracted>(stats))
                {
                    foreach (var en in enumerators)
                    {
                        merged.AddEnumerator(ConvertDocsEnumerator(en.Docs, en.Collection));
                        merged.AddEnumerator(ConvertTombstonesEnumerator(en.Tombstones, en.Collection));
                    }

                    if (ShouldTrackAttachmentTombstones())
                    {
                        var attachmentTombstones = Database.DocumentsStorage
                            .GetTombstonesFrom(context, AttachmentsStorage.AttachmentsTombstones, fromEtag, 0, int.MaxValue).GetEnumerator();
                        scope.EnsureDispose(attachmentTombstones);

                        attachmentTombstones = new FilterTombstonesEnumerator(attachmentTombstones, stats, Tombstone.TombstoneType.Attachment, context,
                            fromCollections: Transformation.ApplyToAllDocuments ? null : Transformation.Collections);

                        merged.AddEnumerator(ConvertTombstonesEnumerator(attachmentTombstones, null));
                    }

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }
        }

        protected abstract EtlTransformer<TExtracted, TTransformed> GetTransformer(DocumentsOperationContext context);

        public IEnumerable<TTransformed> Transform(IEnumerable<TExtracted> items, DocumentsOperationContext context, EtlStatsScope stats, EtlProcessState state)
        {
            using (var transformer = GetTransformer(context))
            {
                transformer.Initalize();

                foreach (var item in items)
                {
                    if (AlreadyLoadedByDifferentNode(item, state))
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(stats.LastTransformedEtag);

                        continue;
                    }

                    if (Transformation.ApplyToAllDocuments &&
                        CollectionName.IsHiLoCollection(item.CollectionFromMetadata) &&
                        ShouldFilterOutHiLoDocument())
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(stats.LastTransformedEtag);

                        continue;
                    }

                    using (stats.For(EtlOperations.Transform))
                    {
                        CancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            transformer.Transform(item);

                            Statistics.TransformationSuccess();

                            stats.RecordTransformedItem();
                            stats.RecordLastTransformedEtag(item.Etag);
                            stats.RecordChangeVector(item.ChangeVector);

                            if (CanContinueBatch(stats, context) == false)
                                break;
                        }
                        catch (JavaScriptParseException e)
                        {
                            var message = $"[{Name}] Could not parse transformation script. Stopping ETL process.";

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations(message, e);

                            var alert = AlertRaised.Create(
                                Database.Name,
                                Tag,
                                message,
                                AlertType.Etl_InvalidScript,
                                NotificationSeverity.Error,
                                key: Name,
                                details: new ExceptionDetails(e));

                            Database.NotificationCenter.Add(alert);

                            Stop();

                            break;
                        }
                        catch (Exception e)
                        {
                            Statistics.RecordTransformationError(e, item.DocumentId);

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"Could not process SQL ETL script for '{Name}', skipping document: {item.DocumentId}", e);
                        }
                    }
                }

                return transformer.GetTransformedResults();
            }
        }

        public void Load(IEnumerable<TTransformed> items, JsonOperationContext context, EtlStatsScope stats)
        {
            using (stats.For(EtlOperations.Load))
            {
                try
                {
                    LoadInternal(items, context);

                    stats.RecordLastLoadedEtag(stats.LastTransformedEtag);

                    Statistics.LoadSuccess(stats.NumberOfTransformedItems);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to load transformed data for '{Name}'", e);

                    EnterFallbackMode();

                    Statistics.RecordLoadError(e.ToString(), documentId: null, count: stats.NumberOfExtractedItems);
                }
            }
        }

        private void EnterFallbackMode()
        {
            if (Statistics.LastLoadErrorTime == null)
                FallbackTime = TimeSpan.FromSeconds(5);
            else
            {
                // double the fallback time (but don't cross Etl.MaxFallbackTime)
                var secondsSinceLastError = (Database.Time.GetUtcNow() - Statistics.LastLoadErrorTime.Value).TotalSeconds;

                FallbackTime = TimeSpan.FromSeconds(Math.Min(Database.Configuration.Etl.MaxFallbackTime.AsTimeSpan.TotalSeconds, Math.Max(5, secondsSinceLastError * 2)));
            }
        }

        protected abstract void LoadInternal(IEnumerable<TTransformed> items, JsonOperationContext context);

        public bool CanContinueBatch(EtlStatsScope stats, JsonOperationContext ctx)
        {
            if (stats.NumberOfExtractedItems >= Database.Configuration.Etl.MaxNumberOfExtractedDocuments)
            {
                var reason = $"Stopping the batch because it has already processed {stats.NumberOfExtractedItems} items";

                if (Logger.IsInfoEnabled)
                    Logger.Info(reason);

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (stats.Duration >= Database.Configuration.Etl.ExtractAndTransformTimeout.AsTimeSpan)
            {
                var reason = $"Stopping the batch after {stats.Duration} due to extract and transform processing timeout";

                if (Logger.IsInfoEnabled)
                    Logger.Info(reason);

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            var currentlyInUse = new Size(_threadAllocations.TotalAllocated, SizeUnit.Bytes);
            if (currentlyInUse > _currentMaximumAllowedMemory)
            {
                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                        currentlyInUse,
                        Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Logger, out ProcessMemoryUsage memoryUsage) == false)
                {
                    var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {new Size(_threadAllocations.TotalAllocated, SizeUnit.Bytes)}. Current memory usage: " +
                                 $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                                 $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(reason);

                    stats.RecordBatchCompleteReason(reason);

                    ctx.DoNotReuse = true;

                    return false;
                }
            }

            return true;
        }

        protected void UpdateMetrics(DateTime startTime, EtlStatsScope stats)
        {
            Metrics.BatchSizeMeter.Mark(stats.NumberOfExtractedItems);
        }

        public override void Reset()
        {
            Statistics.Reset();

            if (_longRunningWork == null)
                return;

            _waitForChanges.Set();
        }

        public override void Reset(DocumentChange change)
        {
            if (Transformation.ApplyToAllDocuments || _collections.Contains(change.CollectionName))
                _waitForChanges.Set();
        }

        public override void Start()
        {
            if (_longRunningWork != null)
                return;

            if (Transformation.Disabled)
                return;

            var threadName = $"{Tag} process: {Name}";
            _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
            {
                try
                {
                    // This has lower priority than request processing, so we let the OS
                    // schedule this appropriately
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                    NativeMemory.EnsureRegistered();
                    Run();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to run ETL {Name}", e);
                }
            }, null, threadName);

            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting {Tag} process: '{Name}'.");

        }

        public override void Stop()
        {
            if (_longRunningWork == null)
                return;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping {Tag} process: '{Name}'.");

            _cts.Cancel();

            var longRunningWork = _longRunningWork;
            _longRunningWork = null;

            if (longRunningWork != PoolOfThreads.LongRunningWork.Current) // prevent a deadlock
                longRunningWork.Join(int.MaxValue);
        }

        public void Run()
        {
            while (true)
            {
                try
                {
                    if (CancellationToken.IsCancellationRequested)
                        return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                try
                {
                    _waitForChanges.Reset();

                    var startTime = Database.Time.GetUtcNow();

                    var didWork = false;

                    var state = GetProcessState(Database, Configuration.Name, Transformation.Name);

                    var loadLastProcessedEtag = state.GetLastProcessedEtagForNode(_serverStore.NodeTag);

                    using (Statistics.NewBatch())
                    using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var statsAggregator = _lastStats = new EtlStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);
                        AddPerformanceStats(statsAggregator);

                        using (var stats = statsAggregator.CreateScope())
                        {
                            try
                            {
                                EnsureThreadAllocationStats();

                                using (context.OpenReadTransaction())
                                {
                                    var extracted = Extract(context, loadLastProcessedEtag + 1, stats);

                                    var transformed = Transform(extracted, context, stats, state);

                                    Load(transformed, context, stats);

                                    var lastProcessed = Math.Max(stats.LastLoadedEtag, stats.LastFilteredOutEtag);

                                    if (lastProcessed > Statistics.LastProcessedEtag)
                                    {
                                        didWork = true;
                                        Statistics.LastProcessedEtag = lastProcessed;
                                        Statistics.LastChangeVector = stats.ChangeVector;
                                    }

                                    if (didWork)
                                    {
                                        UpdateMetrics(startTime, stats);

                                        if (Logger.IsInfoEnabled)
                                            LogSuccessfulBatchInfo(stats);
                                    }
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                var message = $"Exception in ETL process '{Name}'";

                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"{Tag} {message}", e);
                            }
                        }

                        statsAggregator.Complete();
                    }

                    if (didWork)
                    {
                        var command = new UpdateEtlProcessStateCommand(Database.Name, Configuration.Name, Transformation.Name, Statistics.LastProcessedEtag,
                            ChangeVectorUtils.MergeVectors(Statistics.LastChangeVector, state.ChangeVector), _serverStore.NodeTag, _serverStore.LicenseManager.HasHighlyAvailableTasks());

                        var sendToLeaderTask = _serverStore.SendToLeaderAsync(command);
                        sendToLeaderTask.Wait(CancellationToken);
                        var (etag, _) = sendToLeaderTask.Result;

                        try
                        {
                            Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout).Wait(CancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }

                        if (CancellationToken.IsCancellationRequested == false)
                        {
                            var batchCompleted = Database.EtlLoader.BatchCompleted;
                            batchCompleted?.Invoke(Name, Statistics);
                        }

                        continue;
                    }

                    try
                    {
                        if (FallbackTime == null)
                        {
                            _waitForChanges.Wait(CancellationToken);
                        }
                        else
                        {
                            var sp = Stopwatch.StartNew();

                            if (_waitForChanges.Wait(FallbackTime.Value, CancellationToken))
                            {
                                // we are in the fallback mode but got new docs to process
                                // let's wait full time and retry the process then

                                var timeLeftToWait = FallbackTime.Value - sp.Elapsed;

                                if (timeLeftToWait > TimeSpan.Zero)
                                {
                                    Thread.Sleep(timeLeftToWait);
                                }
                            }

                            FallbackTime = null;
                        }
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
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Unexpected error in {Tag} process: '{Name}'", e);
                }
            }
        }

        protected abstract bool ShouldFilterOutHiLoDocument();

        private static bool AlreadyLoadedByDifferentNode(ExtractedItem item, EtlProcessState state)
        {
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: item.ChangeVector,
                localAsString: state.ChangeVector);

            return conflictStatus == ConflictStatus.AlreadyMerged;
        }

        protected void EnsureThreadAllocationStats()
        {
            _threadAllocations = NativeMemory.CurrentThreadStats;
        }

        public override Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
        {
            var lastProcessedEtag = GetProcessState(Database, Configuration.Name, Transformation.Name).GetLastProcessedEtagForNode(_serverStore.NodeTag);

            if (Transformation.ApplyToAllDocuments)
            {
                return new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
                {
                    [Constants.Documents.Collections.AllDocumentsCollection] = lastProcessedEtag
                };
            }

            var lastProcessedTombstones = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

            foreach (var collection in Transformation.Collections)
            {
                lastProcessedTombstones[collection] = lastProcessedEtag;
            }

            if (ShouldTrackAttachmentTombstones())
                lastProcessedTombstones[AttachmentsStorage.AttachmentsTombstones] = lastProcessedEtag;

            return lastProcessedTombstones;
        }

        private void AddPerformanceStats(EtlStatsAggregator stats)
        {
            _lastEtlStats.Enqueue(stats);

            while (_lastEtlStats.Count > 25)
                _lastEtlStats.TryDequeue(out stats);
        }

        public override EtlPerformanceStats[] GetPerformanceStats()
        {
            //var lastStats = _lastStats;

            return _lastEtlStats
                // .Select(x => x == lastStats ? x.ToEtlPerformanceStats().ToIndexingPerformanceLiveStatsWithDetails() : x.ToIndexingPerformanceStats())
                .Select(x => x.ToPerformanceStats())
                .ToArray();
        }

        private void LogSuccessfulBatchInfo(EtlStatsScope stats)
        {
            var message = new StringBuilder();

            message.Append(
                $"{Tag} process '{Name}' extracted {stats.NumberOfExtractedItems} docs, transformed and loaded {stats.NumberOfTransformedItems} docs in {stats.Duration}. ");

            message.Append($"{nameof(stats.LastTransformedEtag)}: {stats.LastTransformedEtag}. ");
            message.Append($"{nameof(stats.LastLoadedEtag)}: {stats.LastLoadedEtag}. ");

            if (stats.LastFilteredOutEtag > 0)
                message.Append($"{nameof(stats.LastFilteredOutEtag)}: {stats.LastFilteredOutEtag}. ");

            if (stats.BatchCompleteReason != null)
                message.Append($"Batch completion reason: {stats.BatchCompleteReason}");

            Logger.Info(message.ToString());
        }

        public override OngoingTaskConnectionStatus GetConnectionStatus()
        {
            if (Configuration.Disabled)
                return OngoingTaskConnectionStatus.NotActive;

            if (FallbackTime != null)
                return OngoingTaskConnectionStatus.Reconnect;

            if (Statistics.WasLatestLoadSuccessful || Statistics.LoadErrors == 0)
                return OngoingTaskConnectionStatus.Active;

            return OngoingTaskConnectionStatus.NotActive;
        }

        public override void Dispose()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

            exceptionAggregator.Execute(Stop);

            exceptionAggregator.Execute(() => _cts.Dispose());
            exceptionAggregator.Execute(() => _waitForChanges.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
