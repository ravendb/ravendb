using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlProcess : IDisposable
    {
        public string Tag { get; protected set; }

        public abstract EtlType EtlType { get; }
        
        public abstract long TaskId { get; }

        public EtlProcessStatistics Statistics { get; protected set; }

        public EtlMetricsCountersManager Metrics { get; protected set; }

        public string Name { get; protected set; }

        public string ConfigurationName { get; protected set; }

        public string TransformationName { get; protected set; }

        public TimeSpan? FallbackTime { get; protected set; }

        public abstract void Start();

        public abstract void Stop(string reason);

        public abstract void Dispose();

        public abstract void Reset();

        public abstract void NotifyAboutWork(DatabaseChange change);

        public abstract bool ShouldTrackCounters();
        
        public abstract bool ShouldTrackTimeSeries();

        public abstract EtlPerformanceStats[] GetPerformanceStats();

        public abstract IEtlStatsAggregator GetLatestPerformanceStats();

        public abstract OngoingTaskConnectionStatus GetConnectionStatus();

        public abstract EtlProcessProgress GetProgress(DocumentsOperationContext documentsContext);

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

    public abstract class EtlProcess<TExtracted, TTransformed, TConfiguration, TConnectionString, TStatsScope, TEtlPerformanceOperation> : EtlProcess, ILowMemoryHandler where TExtracted : ExtractedItem
        where TConfiguration : EtlConfiguration<TConnectionString>
        where TConnectionString : ConnectionString
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);
        internal const int MinBatchSize = 64;

        protected readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim();
        private CancellationTokenSource _cts;
        private readonly HashSet<string> _collections;

        private readonly ConcurrentQueue<IEtlStatsAggregator> _lastEtlStats =
            new ConcurrentQueue<IEtlStatsAggregator>();

        private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        private NativeMemory.ThreadStats _threadAllocations;
        private PoolOfThreads.LongRunningWork _longRunningWork;
        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
        private IEtlStatsAggregator _lastStats;
        private int _statsId;

        private TestMode _testMode;

        protected readonly Transformation Transformation;
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;
        protected EtlProcessState LastProcessState;

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

            LastProcessState = GetProcessState(Database, Configuration.Name, Transformation.Name);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        protected abstract IEnumerator<TExtracted> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection);

        protected abstract IEnumerator<TExtracted> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments);

        protected abstract IEnumerator<TExtracted> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections);

        protected abstract IEnumerator<TExtracted> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection);
        
        protected abstract IEnumerator<TExtracted> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection);
        
        protected abstract IEnumerator<TExtracted> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection);

        protected abstract bool ShouldTrackAttachmentTombstones();

        public override long TaskId => Configuration.TaskId;

        private void Extract(DocumentsOperationContext context, ExtractedItemsEnumerator<TExtracted, TStatsScope, TEtlPerformanceOperation> merged, long fromEtag, EtlItemType type, AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> stats, DisposableScope scope)
        {
            switch (type)
            {
                case EtlItemType.Document:
                    ExtractDocuments(context, merged, fromEtag, stats, scope);
                    break;
                case EtlItemType.CounterGroup:
                    ExtractCounters(context, merged, fromEtag, stats, scope);
                    break;
                case EtlItemType.TimeSeries:
                    ExtractTimeSeries(context, merged, fromEtag, stats, scope);
                    break;
                default:
                    throw new NotSupportedException($"Invalid ETL item type: {type}");
            }
        }
        
        private void ExtractDocuments(
            DocumentsOperationContext context, 
            ExtractedItemsEnumerator<TExtracted, TStatsScope, TEtlPerformanceOperation> merged, 
            long fromEtag, 
            AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> stats,
            DisposableScope scope)
        {

            if (Transformation.ApplyToAllDocuments)
            {
                var docs = Database.DocumentsStorage.GetDocumentsFrom(context, fromEtag, 0, long.MaxValue).GetEnumerator();
                scope.EnsureDispose(docs);
                merged.AddEnumerator(ConvertDocsEnumerator(context, docs, null));

                // when collection isn't specified this will get the tombstones for docs, attachments & revisions in a single enumerator
                // otherwise we will handle attachment and documents in a dedicated enumerator

                var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, fromEtag, 0, long.MaxValue).GetEnumerator();
                scope.EnsureDispose(tombstones);
                merged.AddEnumerator(ConvertTombstonesEnumerator(context, tombstones, null, trackAttachments: ShouldTrackAttachmentTombstones()));
            }
            else
            {
                foreach (var collection in Transformation.Collections)
                {
                    var docs = Database.DocumentsStorage.GetDocumentsFrom(context, collection, fromEtag, 0, long.MaxValue).GetEnumerator();
                    scope.EnsureDispose(docs);
                    merged.AddEnumerator(ConvertDocsEnumerator(context, docs, collection));

                    var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, collection, fromEtag, 0, long.MaxValue).GetEnumerator();
                    scope.EnsureDispose(tombstones);
                    merged.AddEnumerator(ConvertTombstonesEnumerator(context, tombstones, collection, trackAttachments: false));
                }

                if (ShouldTrackAttachmentTombstones())
                {
                    var attachmentTombstones = Database.DocumentsStorage.GetAttachmentTombstonesFrom(context, fromEtag, 0, long.MaxValue).GetEnumerator();
                    scope.EnsureDispose(attachmentTombstones);
                    merged.AddEnumerator(ConvertAttachmentTombstonesEnumerator(context, attachmentTombstones, Transformation.Collections));
                }
            }
        }
                
        private void ExtractCounters(DocumentsOperationContext context,
            ExtractedItemsEnumerator<TExtracted, TStatsScope, TEtlPerformanceOperation> merged,
            long fromEtag,
            AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> stats,
            DisposableScope scope)
        {
            if (Transformation.ApplyToAllDocuments)
            {
                var counters = Database.DocumentsStorage.CountersStorage.GetCountersFrom(context, fromEtag, 0, long.MaxValue).GetEnumerator();
                scope.EnsureDispose(counters);
                merged.AddEnumerator(ConvertCountersEnumerator(context, counters, null));
            }
            else
            {
                foreach (var collection in Transformation.Collections)
                {
                    var counters = Database.DocumentsStorage.CountersStorage.GetCountersFrom(context, collection, fromEtag, 0, long.MaxValue).GetEnumerator();
                    scope.EnsureDispose(counters);
                    merged.AddEnumerator(ConvertCountersEnumerator(context, counters, collection));
                }
            }
        }
        
        private void ExtractTimeSeries(DocumentsOperationContext context,
            ExtractedItemsEnumerator<TExtracted, TStatsScope, TEtlPerformanceOperation> merged,
            long fromEtag,
            AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> stats,
            DisposableScope scope)
        {
            if (Transformation.ApplyToAllDocuments)
            {
                var timeSeries = Database.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(context, fromEtag, long.MaxValue).GetEnumerator();
                scope.EnsureDispose(timeSeries);
                merged.AddEnumerator(ConvertTimeSeriesEnumerator(context, timeSeries, null));
                
                var deletedRanges = Database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(context, fromEtag).GetEnumerator();
                scope.EnsureDispose(deletedRanges);
                merged.AddEnumerator(ConvertTimeSeriesDeletedRangeEnumerator(context, deletedRanges, null));
            }
            else
            {
                foreach (var collection in Transformation.Collections)
                {
                    var timeSeries = Database.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(context, collection, fromEtag, long.MaxValue).GetEnumerator();
                    scope.EnsureDispose(timeSeries);
                    merged.AddEnumerator(ConvertTimeSeriesEnumerator(context, timeSeries, collection));
                    
                    var deletedRanges = Database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(context, collection, fromEtag).GetEnumerator();
                    scope.EnsureDispose(deletedRanges);
                    merged.AddEnumerator(ConvertTimeSeriesDeletedRangeEnumerator(context, deletedRanges, collection));
                }
            }
        }

        protected abstract EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> GetTransformer(DocumentsOperationContext context);

        public IEnumerable<TTransformed> Transform(IEnumerable<TExtracted> items, DocumentsOperationContext context, TStatsScope stats, EtlProcessState state)
        {
            using (var transformer = GetTransformer(context))
            {
                transformer.Initialize(debugMode: _testMode != null);

                var batchSize = 0;

                var batchStopped = false;

                foreach (var item in items)
                {
                    if (item.Filtered)
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(item.Etag, item.Type);
                        continue;
                    }

                    stats.RecordLastExtractedEtag(item.Etag, item.Type);

                    if (AlreadyLoadedByDifferentNode(item, state))
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(item.Etag, item.Type);

                        continue;
                    }

                    if (Transformation.ApplyToAllDocuments &&
                        item.Type == EtlItemType.Document &&
                        CollectionName.IsHiLoCollection(item.CollectionFromMetadata) &&
                        ShouldFilterOutHiLoDocument())
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(item.Etag, item.Type);

                        continue;
                    }

                    using (stats.For(EtlOperations.Transform))
                    {
                        CancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            if (CanContinueBatch(stats, item, batchSize, context) == false)
                            {
                                batchStopped = true;
                                break;
                            }

                            transformer.Transform(item, stats, state);

                            Statistics.TransformationSuccess();

                            stats.RecordTransformedItem(item.Type, item.IsDelete);
                            stats.RecordLastTransformedEtag(item.Etag, item.Type);
                            stats.RecordChangeVector(item.ChangeVector);

                            batchSize++;
                            
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

                            stats.RecordBatchCompleteReason(message);

                            Stop(reason: message);

                            break;
                        }
                        catch (Exception e)
                        {
                            Statistics.RecordTransformationError(e, item.DocumentId);

                            stats.RecordTransformationError();

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"Could not process SQL ETL script for '{Name}', skipping document: {item.DocumentId}", e);
                        }
                    }
                }

                if (batchStopped == false && stats.HasBatchCompleteReason() == false)
                {
                    stats.RecordBatchCompleteReason("No items to process");
                }

                _testMode?.DebugOutput.AddRange(transformer.GetDebugOutput());

                return transformer.GetTransformedResults();
            }
        }

        public bool Load(IEnumerable<TTransformed> items, DocumentsOperationContext context, TStatsScope stats)
        {
            using (var loadScope = stats.For(EtlOperations.Load))
            {
                try
                {
                    var count = LoadInternal(items, context, loadScope);

                    stats.RecordLastLoadedEtag(stats.LastTransformedEtags.Values.Max());

                    Statistics.LoadSuccess(count);

                    stats.RecordLoadSuccess(count);

                    return true;
                }
                catch (Exception e)
                {
                    if (CancellationToken.IsCancellationRequested == false)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Failed to load transformed data for '{Name}'", e);

                        stats.RecordLoadFailure();

                        EnterFallbackMode();

                        Statistics.RecordLoadError(e.ToString(), documentId: null, count: stats.NumberOfExtractedItems.Sum(x => x.Value));
                    }

                    return false;
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

        protected abstract int LoadInternal(IEnumerable<TTransformed> items, DocumentsOperationContext context, TStatsScope scope);
    
        private bool CanContinueBatch(TStatsScope stats, TExtracted currentItem, int batchSize, DocumentsOperationContext ctx)
        {
            if (_serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
            {
                var reason = $"Stopping the batch after {stats.Duration} because the CPU credits balance is almost completely used";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (currentItem is ToOlapItem)
            {
                if (stats.NumberOfExtractedItems[EtlItemType.Document] > Database.Configuration.Etl.OlapMaxNumberOfExtractedDocuments)
                {
                    var reason = $"Stopping the batch because it has already processed max number of extracted documents : {stats.NumberOfExtractedItems[EtlItemType.Document]}";

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"[{Name}] {reason}");

                    stats.RecordBatchCompleteReason(reason);

                    return false;
                }
            }

            else
            {
                if (stats.NumberOfExtractedItems[EtlItemType.Document] > Database.Configuration.Etl.MaxNumberOfExtractedDocuments ||
                    stats.NumberOfExtractedItems.Sum(x => x.Value) > Database.Configuration.Etl.MaxNumberOfExtractedItems)
                {
                    var reason = $"Stopping the batch because it has already processed max number of items ({string.Join(',', stats.NumberOfExtractedItems.Select(x => $"{x.Key} - {x.Value}"))})";

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"[{Name}] {reason}");

                    stats.RecordBatchCompleteReason(reason);

                    return false;
                }
            }

            if (stats.Duration >= Database.Configuration.Etl.ExtractAndTransformTimeout.AsTimeSpan)
            {
                var reason = $"Stopping the batch after {stats.Duration} due to extract and transform processing timeout";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
            {
                var reason = $"The batch was stopped after processing {batchSize:#,#;;0} items because of low memory";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);
                return false;
            }

            var totalAllocated = new Size(_threadAllocations.TotalAllocated + ctx.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes), SizeUnit.Bytes);
            _threadAllocations.CurrentlyAllocatedForProcessing = totalAllocated.GetValue(SizeUnit.Bytes);

            stats.RecordCurrentlyAllocated(totalAllocated.GetValue(SizeUnit.Bytes) + GC.GetAllocatedBytesForCurrentThread());

            if (totalAllocated > _currentMaximumAllowedMemory)
            {
                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                        totalAllocated,
                        Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Logger, out var memoryUsage) == false)
                {
                    var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {totalAllocated}.";
                    if (memoryUsage != null)
                    {
                        reason += " Current memory usage: " +
                                   $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                                   $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                    }

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"[{Name}] {reason}");

                    stats.RecordBatchCompleteReason(reason);

                    ctx.DoNotReuse = true;

                    return false;
                }
            }

            var maxBatchSize = Database.Configuration.Etl.MaxBatchSize;

            if (maxBatchSize != null && stats.BatchSize >= maxBatchSize)
            {
                var reason = $"Stopping the batch because maximum batch size limit was reached ({stats.BatchSize})";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            return true;
        }
        protected void UpdateMetrics(DateTime startTime, TStatsScope stats)
        {
            var batchSize = stats.NumberOfExtractedItems.Sum(x => x.Value);

            Metrics.BatchSizeMeter.MarkSingleThreaded(batchSize);
            Metrics.UpdateProcessedPerSecondRate(batchSize, stats.Duration);
        }

        public override void Reset()
        {
            Statistics.Reset();

            if (_longRunningWork == null)
                return;

            _waitForChanges.Set();
        }

        public override void NotifyAboutWork(DatabaseChange change)
        {
            bool shouldNotify = change switch
            {
                DocumentChange documentChange => Transformation.ApplyToAllDocuments || _collections.Contains(documentChange.CollectionName),
                CounterChange _ => ShouldTrackCounters(),
                TimeSeriesChange _ => ShouldTrackTimeSeries(),
                _ => throw new InvalidOperationException($"Notify about {change.GetType()} is not supported")
            };
            if(shouldNotify)
                _waitForChanges.Set();
        }

        public override void Start()
        {
            if (_longRunningWork != null)
                return;

            if (Transformation.Disabled || Configuration.Disabled)
                return;

            _cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);

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
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to run ETL {Name}", e);
                }
            }, null, threadName);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Starting {Tag} process: '{Name}'.");

        }

        public override void Stop(string reason)
        {
            if (_longRunningWork == null)
                return;

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Stopping {Tag} process: '{Name}'. Reason: {reason}");

            _cts.Cancel();

            var longRunningWork = _longRunningWork;
            _longRunningWork = null;

            if (longRunningWork != PoolOfThreads.LongRunningWork.Current) // prevent a deadlock
                longRunningWork.Join(int.MaxValue);
        }

        protected abstract TStatsScope CreateScope(EtlRunStats stats);

        public void Run()
        {
            var runStart = Database.Time.GetUtcNow();

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

                    var state  = LastProcessState = GetProcessState(Database, Configuration.Name, Transformation.Name);

                    var loadLastProcessedEtag = state.GetLastProcessedEtagForNode(_serverStore.NodeTag);

                    using (Statistics.NewBatch())
                    using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var statsAggregator = new EtlStatsAggregator<TStatsScope, TEtlPerformanceOperation>(Interlocked.Increment(ref _statsId), CreateScope, _lastStats);
                        _lastStats = statsAggregator;

                        AddPerformanceStats(statsAggregator);

                        using (var stats = statsAggregator.CreateScope())
                        {
                            try
                            {
                                EnsureThreadAllocationStats();

                                using (context.OpenReadTransaction())
                                using (var scope = new DisposableScope())
                                using (var merged = new ExtractedItemsEnumerator<TExtracted, TStatsScope, TEtlPerformanceOperation>(stats))
                                {
                                    var nextEtag = loadLastProcessedEtag + 1;

                                    Extract(context, merged, nextEtag, EtlItemType.Document, stats, scope);

                                    if (ShouldTrackCounters())
                                        Extract(context, merged, nextEtag, EtlItemType.CounterGroup, stats, scope);
                                    
                                    if (ShouldTrackTimeSeries())
                                        Extract(context, merged, nextEtag, EtlItemType.TimeSeries, stats, scope);

                                    var transformations = Transform(merged, context, stats, state);

                                    var noFailures = Load(transformations, context, stats);

                                    var lastProcessed = Math.Max(stats.LastLoadedEtag, stats.LastFilteredOutEtags.Values.Max());

                                    if (lastProcessed > Statistics.LastProcessedEtag && noFailures)
                                    {
                                        didWork = true;
                                        
                                        Statistics.LastProcessedEtag = lastProcessed;
                                        Statistics.LastChangeVector = stats.ChangeVector;
                                    
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

                                if (Logger.IsOperationsEnabled)
                                    Logger.Operations($"{Tag} {message}", e);
                            }
                        }

                        statsAggregator.Complete();
                    }

                    if (didWork)
                    {
                        try
                        {
                            UpdateEtlProcessState(state);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }

                        if (CancellationToken.IsCancellationRequested == false)
                        {
                            Database.EtlLoader.OnBatchCompleted(ConfigurationName, TransformationName, Statistics);
                        }

                        continue;
                    }
                    try
                    {
                        AfterAllBatchesCompleted(runStart);

                        PauseIfCpuCreditsBalanceIsTooLow();

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

                        runStart = Database.Time.GetUtcNow();
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
                finally
                {
                    _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                    _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
                }
            }
        }

        protected void UpdateEtlProcessState(EtlProcessState state, DateTime? lastBatchTime = null)
        {
            var command = new UpdateEtlProcessStateCommand(Database.Name, Configuration.Name, Transformation.Name, Statistics.LastProcessedEtag,
                ChangeVectorUtils.MergeVectors(Statistics.LastChangeVector, state.ChangeVector), _serverStore.NodeTag,
                _serverStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId(), state.SkippedTimeSeriesDocs, lastBatchTime);

            var sendToLeaderTask = _serverStore.SendToLeaderAsync(command);

            sendToLeaderTask.Wait(CancellationToken);
            var (etag, _) = sendToLeaderTask.Result;

            Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout).Wait(CancellationToken);
        }

        private void PauseIfCpuCreditsBalanceIsTooLow()
        {
            AlertRaised alert = null;
            int numberOfTimesSlept = 0;
            while (_serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() &&
                Database.DatabaseShutdown.IsCancellationRequested == false)
            {
                // give us a bit more than a measuring cycle to gain more CPU credits
                Thread.Sleep(1250);
                if (alert == null && numberOfTimesSlept++ > 5)
                {
                    alert = AlertRaised.Create(
                       Database.Name,
                       Tag,
                       "Etl process paused because the CPU credits balance is almost completely used, will be resumed when there are enough CPU credits to use.",
                       AlertType.Throttling_CpuCreditsBalance,
                       NotificationSeverity.Warning,
                       key: Name);
                    Database.NotificationCenter.Add(alert);
                }
            }
            if (alert != null)
            {
                Database.NotificationCenter.Dismiss(alert.Id);
            }
        }

        protected abstract bool ShouldFilterOutHiLoDocument();

        protected virtual void AfterAllBatchesCompleted(DateTime lastBatchTime)
        {
        }
        
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

        private void AddPerformanceStats(IEtlStatsAggregator stats)
        {
            _lastEtlStats.Enqueue(stats);

            while (_lastEtlStats.Count > 25)
                _lastEtlStats.TryDequeue(out stats);
        }

        public override EtlPerformanceStats[] GetPerformanceStats()
        {
            var lastStats = _lastStats;

            return _lastEtlStats
                .Select(x => x == lastStats ? x.ToPerformanceLiveStatsWithDetails() : x.ToPerformanceStats())
                .ToArray();
        }

        public override IEtlStatsAggregator GetLatestPerformanceStats()
        {
            return _lastStats;
        }

        private void LogSuccessfulBatchInfo(AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> stats)
        {
            var message = new StringBuilder();

            message.Append(
                $"{Tag} process '{Name}' processed the following number of items: ");

            foreach (var extracted in stats.NumberOfExtractedItems)
            {
                if (extracted.Value > 0)
                    message.Append($"{extracted.Key} - {extracted.Value} items, last transformed etag: {stats.LastTransformedEtags[extracted.Key]}");

                if (stats.LastFilteredOutEtags[extracted.Key] > 0)
                    message.Append($", last filtered etag: {stats.LastFilteredOutEtags[extracted.Key]}");
            }

            message.Append($" in {stats.Duration} (last loaded etag: {stats.LastLoadedEtag})");

            if (stats.BatchCompleteReason != null)
                message.Append($" Batch completion reason: {stats.BatchCompleteReason}");

            Logger.Info(message.ToString());
        }

        public override OngoingTaskConnectionStatus GetConnectionStatus()
        {
            if (Configuration.Disabled || _cts.IsCancellationRequested)
                return OngoingTaskConnectionStatus.NotActive;

            if (FallbackTime != null)
                return OngoingTaskConnectionStatus.Reconnect;

            if (Statistics.WasLatestLoadSuccessful || Statistics.LoadErrors == 0)
                return OngoingTaskConnectionStatus.Active;

            return OngoingTaskConnectionStatus.NotActive;
        }

        public static TestEtlScriptResult TestScript(TestEtlScript<TConfiguration, TConnectionString> testScript, DocumentDatabase database, ServerStore serverStore,
            DocumentsOperationContext context)
        {
            using (testScript.IsDelete ? context.OpenWriteTransaction() : context.OpenReadTransaction()) // we open write tx to test deletion but we won't commit it
            {
                var document = database.DocumentsStorage.Get(context, testScript.DocumentId);

                if (document == null)
                    throw new InvalidOperationException($"Document {testScript.DocumentId} does not exist");

                TConnectionString connection = null;

                var sqlTestScript = testScript as TestSqlEtlScript;

                if (sqlTestScript != null)
                {
                    // we need to have connection string when testing SQL ETL because we need to have the factory name
                    // and if PerformRolledBackTransaction = true is specified then we need make a connection to SQL

                    var csErrors = new List<string>();

                    if (sqlTestScript.Connection != null)
                    {
                        if (sqlTestScript.Connection.Validate(ref csErrors) == false)
                            throw new InvalidOperationException($"Invalid connection string due to {string.Join(";", csErrors)}");

                        connection = sqlTestScript.Connection as TConnectionString;
                    }
                    else
                    {
                        Dictionary<string, SqlConnectionString> sqlConnectionStrings;
                        using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(ctx, database.Name))
                        {
                            sqlConnectionStrings = rawRecord.SqlConnectionStrings;
                            if (sqlConnectionStrings == null)
                                throw new InvalidOperationException($"{nameof(DatabaseRecord.SqlConnectionStrings)} was not found in the database record");
                        }

                        if (sqlConnectionStrings.TryGetValue(testScript.Configuration.ConnectionStringName, out var sqlConnection) == false)
                        {
                            throw new InvalidOperationException(
                                $"Connection string named '{testScript.Configuration.ConnectionStringName}' was not found in the database record");
                        }

                        if (sqlConnection.Validate(ref csErrors) == false)
                            throw new InvalidOperationException(
                                $"Invalid '{testScript.Configuration.ConnectionStringName}' connection string due to {string.Join(";", csErrors)}");

                        connection = sqlConnection as TConnectionString;
                    }
                }

                testScript.Configuration.Initialize(connection);

                testScript.Configuration.TestMode = true;

                if (testScript.Configuration.Validate(out List<string> errors) == false)
                {
                    throw new InvalidOperationException($"Invalid ETL configuration for '{testScript.Configuration.Name}'. " +
                                                        $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.");
                }

                if (testScript.Configuration.Transforms.Count != 1)
                {
                    throw new InvalidOperationException($"Invalid number of transformations. You have provided {testScript.Configuration.Transforms.Count} " +
                                                        "while ETL test expects to get exactly 1 transformation script");
                }

                var docCollection = database.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;

                if (testScript.Configuration.Transforms[0].ApplyToAllDocuments == false &&
                    testScript.Configuration.Transforms[0].Collections.Contains(docCollection, StringComparer.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException($"Document '{document.Id}' belongs to {docCollection} collection " +
                                                        $"while tested ETL script works on the following collections: {string.Join(", ", testScript.Configuration.Transforms[0].Collections)}");
                }

                if (testScript.Configuration.Transforms[0].ApplyToAllDocuments)
                {
                    // when ETL script has ApplyToAllDocuments then it extracts docs without
                    // providing collection name to ExtractedItem
                    // it is retrieved from metadata then
                    // let's do the same to ensure we have the same behavior in test mode

                    docCollection = null;
                }

                Tombstone tombstone = null;

                if (testScript.IsDelete)
                {
                    var deleteResult = database.DocumentsStorage.Delete(context, testScript.DocumentId, null);

                    tombstone = database.DocumentsStorage.GetTombstoneByEtag(context, deleteResult.Value.Etag);
                }

                List<string> debugOutput;

                switch (testScript.Configuration.EtlType)
                {
                    case EtlType.Sql:
                        using (var sqlEtl = new SqlEtl(testScript.Configuration.Transforms[0], testScript.Configuration as SqlEtlConfiguration, database, database.ServerStore))
                        using (sqlEtl.EnterTestMode(out debugOutput))
                        {
                            sqlEtl.EnsureThreadAllocationStats();

                            var sqlItem = testScript.IsDelete ? new ToSqlItem(tombstone, docCollection) : new ToSqlItem(document, docCollection);

                            var transformed = sqlEtl.Transform(new[] {sqlItem}, context, new EtlStatsScope(new EtlRunStats()),
                                new EtlProcessState());

                            Debug.Assert(sqlTestScript != null);

                            var result = sqlEtl.RunTest(context, transformed, sqlTestScript.PerformRolledBackTransaction);
                            result.DebugOutput = debugOutput;

                            return result;
                        }
                    case EtlType.Raven:
                        using (var ravenEtl = new RavenEtl(testScript.Configuration.Transforms[0], testScript.Configuration as RavenEtlConfiguration, database, database.ServerStore))
                        using (ravenEtl.EnterTestMode(out debugOutput))
                        {
                            ravenEtl.EnsureThreadAllocationStats();

                            var ravenEtlItem = testScript.IsDelete ? new RavenEtlItem(tombstone, docCollection, EtlItemType.Document) : new RavenEtlItem(document, docCollection);

                            var results = ravenEtl.Transform(new[] {ravenEtlItem}, context, new EtlStatsScope(new EtlRunStats()),
                                new EtlProcessState{SkippedTimeSeriesDocs = new HashSet<string> {testScript.DocumentId}});

                            return new RavenEtlTestScriptResult
                            {
                                TransformationErrors = ravenEtl.Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                                Commands = results.ToList(),
                                DebugOutput = debugOutput
                            };
                        }
                    case EtlType.Olap:
                        var olapTestScriptConfiguration = testScript.Configuration as OlapEtlConfiguration;

                        if (olapTestScriptConfiguration == null)
                            throw new InvalidOperationException(
                                $"Configuration must be of type '{nameof(OlapEtlConfiguration)}' while it got {testScript.Configuration?.GetType()}");

                        olapTestScriptConfiguration.Connection = new OlapConnectionString();

                        using (var olapElt = new OlapEtl(testScript.Configuration.Transforms[0], olapTestScriptConfiguration, database, database.ServerStore))
                        using (olapElt.EnterTestMode(out debugOutput))
                        {
                            olapElt.EnsureThreadAllocationStats();

                            if (testScript.IsDelete)
                                throw new InvalidOperationException("OLAP ETL doesn't deal with deletions. It's append only process");

                            var olapEtlItem = new ToOlapItem(document, docCollection);

                            var results = olapElt.Transform(new[] { olapEtlItem }, context, new OlapEtlStatsScope(new EtlRunStats()),
                                new EtlProcessState { SkippedTimeSeriesDocs = new HashSet<string> { testScript.DocumentId } });

                            var itemsByPartition = new List<OlapEtlTestScriptResult.PartitionItems>();

                            foreach (OlapTransformedItems olapItem in results)
                            {
                                switch (olapItem)
                                {
                                    case ParquetTransformedItems parquetItem:

                                        parquetItem.AddMandatoryFields();

                                        var partitionItems = new OlapEtlTestScriptResult.PartitionItems();

                                        partitionItems.Key = parquetItem.Key;

                                        foreach (var columnData in parquetItem.RowGroup.Data)
                                        {
                                            if (parquetItem.Fields.TryGetValue(columnData.Key, out var field) == false)
                                                continue;

                                            partitionItems.Columns.Add(new OlapEtlTestScriptResult.PartitionColumn
                                            {
                                                Name = field.Name,
                                                Type = field.DataType.ToString(),
                                                Values = columnData.Value
                                            });
                                        }

                                        itemsByPartition.Add(partitionItems);

                                        break;
                                    default:
                                        throw new NotSupportedException("Unknown transform type: " + olapItem.GetType());
                                }
                            }

                            return new OlapEtlTestScriptResult
                            {
                                TransformationErrors = olapElt.Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                                ItemsByPartition = itemsByPartition,
                                DebugOutput = debugOutput
                            };

                        }
                    default:
                        throw new NotSupportedException($"Unknown ETL type in script test: {testScript.Configuration.EtlType}");
                }
            }
        }

        private IDisposable EnterTestMode(out List<string> debugOutput)
        {
            _testMode = new TestMode();
            var disableAlerts = Statistics.PreventFromAddingAlertsToNotificationCenter();

            debugOutput = _testMode.DebugOutput;

            return new DisposableAction(() =>
            {
                _testMode = null;
                disableAlerts.Dispose();
            });
        }

        public override EtlProcessProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            var result = new EtlProcessProgress
            {
                TransformationName = TransformationName,
                Disabled = Transformation.Disabled || Configuration.Disabled,
                AverageProcessedPerSecond = Metrics.GetProcessedPerSecondRate() ?? 0.0
            };

            var collections = Transformation.ApplyToAllDocuments 
                ? Database.DocumentsStorage.GetCollections(documentsContext).Select(x => x.Name).ToList() 
                : Transformation.Collections;

            var lastProcessedEtag = LastProcessState.GetLastProcessedEtagForNode(_serverStore.NodeTag);

            foreach (var collection in collections)
            {
                result.NumberOfDocumentsToProcess += Database.DocumentsStorage.GetNumberOfDocumentsToProcess(documentsContext, collection, lastProcessedEtag, out var total);
                result.TotalNumberOfDocuments += total;

                result.NumberOfDocumentTombstonesToProcess += Database.DocumentsStorage.GetNumberOfTombstonesToProcess(documentsContext, collection, lastProcessedEtag, out total);
                result.TotalNumberOfDocumentTombstones += total;

                if (ShouldTrackCounters())
                {
                    result.NumberOfCounterGroupsToProcess += Database.DocumentsStorage.CountersStorage.GetNumberOfCounterGroupsToProcess(documentsContext, collection, lastProcessedEtag, out total);
                    result.TotalNumberOfCounterGroups += total;
                }
                
                if (ShouldTrackTimeSeries())
                {
                    result.NumberOfTimeSeriesSegmentsToProcess += Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegmentsToProcess(documentsContext, collection, lastProcessedEtag, out total);
                    result.TotalNumberOfTimeSeriesSegments += total;
                    
                    result.NumberOfTimeSeriesDeletedRangesToProcess += Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(documentsContext, collection, lastProcessedEtag, out total);
                    result.TotalNumberOfTimeSeriesDeletedRanges += total;
                }
            }

            result.Completed = (result.NumberOfDocumentsToProcess > 0 
                                && result.NumberOfDocumentTombstonesToProcess > 0 
                                && result.NumberOfCounterGroupsToProcess > 0 
                                && result.NumberOfTimeSeriesSegmentsToProcess > 0 
                                && result.NumberOfTimeSeriesDeletedRangesToProcess > 0) == false;

            var performance = _lastStats?.ToPerformanceLiveStats();

            if (performance != null && performance.DurationInMs > 0 &&
                performance.SuccessfullyLoaded != false && FallbackTime != null)
            {
                var processedPerSecondInCurrentBatch = performance.NumberOfExtractedItems.Sum(x => x.Value) / (performance.DurationInMs / 1000);

                result.AverageProcessedPerSecond = (result.AverageProcessedPerSecond + processedPerSecondInCurrentBatch) / 2;

                if (result.NumberOfDocumentsToProcess > 0)
                    result.NumberOfDocumentsToProcess -= performance.NumberOfTransformedItems[EtlItemType.Document];

                if (result.NumberOfDocumentTombstonesToProcess > 0)
                    result.NumberOfDocumentTombstonesToProcess -= performance.NumberOfTransformedTombstones[EtlItemType.Document];

                if (result.NumberOfCounterGroupsToProcess > 0)
                    result.NumberOfCounterGroupsToProcess -= performance.NumberOfTransformedItems[EtlItemType.CounterGroup];
                
                if (result.NumberOfTimeSeriesSegmentsToProcess > 0)
                    result.NumberOfTimeSeriesSegmentsToProcess -= performance.NumberOfTransformedItems[EtlItemType.TimeSeries];

                if (result.NumberOfTimeSeriesDeletedRangesToProcess > 0)
                    result.NumberOfTimeSeriesDeletedRangesToProcess -= performance.NumberOfTransformedTombstones[EtlItemType.TimeSeries];
                
                result.Completed = (result.NumberOfDocumentsToProcess > 0 
                                   && result.NumberOfDocumentTombstonesToProcess > 0 
                                   && result.NumberOfCounterGroupsToProcess > 0 
                                   && result.NumberOfTimeSeriesSegmentsToProcess > 0 
                                   && result.NumberOfTimeSeriesDeletedRangesToProcess > 0) == false;

                if (result.Completed && performance.Completed == null)
                {
                    // note the above calculations of items to process subtract _transformed_ items in current batch, they aren't loaded yet
                    // in order to indicate that load phase is still in progress we're marking that it isn't completed yet

                    result.Completed = performance.SuccessfullyLoaded ?? false;
                }
            }

            return result;
        }

        public override void Dispose()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

            exceptionAggregator.Execute(() => Stop("Dispose"));

            exceptionAggregator.Execute(() => _cts.Dispose());
            exceptionAggregator.Execute(() => _waitForChanges.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }

        private class TestMode
        {
            public readonly List<string> DebugOutput = new List<string>();
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }
    }
}
