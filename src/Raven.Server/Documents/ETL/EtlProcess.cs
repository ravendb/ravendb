using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlProcess : IDisposable, IDocumentTombstoneAware
    {
        public string Tag { get; protected set; }

        public EtlProcessStatistics Statistics { get; protected set; }

        public EtlMetricsCountersManager Metrics { get; protected set; }

        public abstract string Name { get; }

        public abstract void Start();

        public abstract void Stop();

        public abstract void Dispose();

        public abstract void NotifyAboutWork(DocumentChange change);

        public abstract EtlPerformanceStats[] GetPerformanceStats();

        public abstract Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection();
    }

    public abstract class EtlProcess<TExtracted, TTransformed, TDestination> : EtlProcess where TExtracted : ExtractedItem where TDestination : EtlDestination
    {
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim();
        private readonly CancellationTokenSource _cts;
        private readonly EtlStorage _storage;
        private readonly HashSet<string> _collections;

        private readonly ConcurrentQueue<EtlStatsAggregator> _lastEtlStats =
            new ConcurrentQueue<EtlStatsAggregator>();

        private Size _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
        private NativeMemory.ThreadStats _threadAllocations;
        private Thread _thread;
        private EtlStatsAggregator _lastStats;
        private int _statsId;

        protected readonly Transformation Transformation;
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;
        protected TimeSpan? FallbackTime;

        public readonly TDestination Destination;

        protected EtlProcess(Transformation transformation, TDestination destination, DocumentDatabase database, string tag)
        {
            Transformation = transformation;
            Destination = destination;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
            Tag = tag;
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            Database = database;
            _storage = Database.ConfigurationStorage.EtlStorage;
            Statistics = new EtlProcessStatistics(tag, Transformation.Name, Database.NotificationCenter);

            if (transformation.ApplyToAllDocuments == false)
                _collections = new HashSet<string>(Transformation.Collections, StringComparer.OrdinalIgnoreCase);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        public override string Name => Transformation.Name;

        protected abstract IEnumerator<TExtracted> ConvertDocsEnumerator(IEnumerator<Document> docs, string collection);

        protected abstract IEnumerator<TExtracted> ConvertTombstonesEnumerator(IEnumerator<DocumentTombstone> tombstones, string collection);

        public virtual IEnumerable<TExtracted> Extract(DocumentsOperationContext context, long fromEtag, EtlStatsScope stats)
        {
            using (var scope = new DisposeableScope())
            {
                var enumerators = new List<(IEnumerator<Document> Docs, IEnumerator<DocumentTombstone> Tombstones, string Collection)>(Transformation.Collections.Count);

                if (Transformation.ApplyToAllDocuments)
                {
                    var docs = Database.DocumentsStorage.GetDocumentsFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();

                    scope.EnsureDispose(docs);

                    var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();

                    scope.EnsureDispose(tombstones);

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

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }
        }

        protected abstract EtlTransformer<TExtracted, TTransformed> GetTransformer(DocumentsOperationContext context);

        public unsafe IEnumerable<TTransformed> Transform(IEnumerable<TExtracted> items, DocumentsOperationContext context, EtlStatsScope stats)
        {
            var transformer = GetTransformer(context);

            foreach (var item in items)
            {
                if (Transformation.ApplyToAllDocuments && CollectionName.IsSystemDocument(item.DocumentKey.Buffer, item.DocumentKey.Size, out var isHilo))
                {
                    if (ShouldFilterOutSystemDocument(isHilo))
                    {
                        stats.RecordLastTransformedEtag(item.Etag);
                        continue;
                    }
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

                        if (CanContinueBatch(stats) == false)
                            break;
                    }
                    catch (JavaScriptParseException e)
                    {
                        var message = $"[{Name}] Could not parse transformation script. Stopping ETL process.";

                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(message, e);

                        var alert = AlertRaised.Create(
                            Tag,
                            message,
                            AlertType.Etl_TransformationError,
                            NotificationSeverity.Error,
                            key: Name,
                            details: new ExceptionDetails(e));

                        Database.NotificationCenter.Add(alert);

                        Stop();

                        break;
                    }
                    catch (Exception e)
                    {
                        Statistics.RecordTransformationError(e);

                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Could not process SQL ETL script for '{Name}', skipping document: {item.DocumentKey}", e);
                    }
                }
            }

            return transformer.GetTransformedResults();
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

                    HandleFallback();

                    Statistics.RecordLoadError(e, stats.NumberOfExtractedItems);
                }
            }
        }

        protected virtual void HandleFallback()
        {
        }

        protected abstract void LoadInternal(IEnumerable<TTransformed> items, JsonOperationContext context);

        public bool CanContinueBatch(EtlStatsScope stats)
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

            if (_threadAllocations.Allocations > _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes))
            {
                ProcessMemoryUsage memoryUsage;
                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                        Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Logger, out memoryUsage) == false)
                {
                    var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {_threadAllocations.Allocations}. Current memory usage: " +
                                 $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                                 $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(reason);

                    stats.RecordBatchCompleteReason(reason);

                    return false;
                }
            }

            return true;
        }

        protected void UpdateMetrics(DateTime startTime, EtlStatsScope stats)
        {
            Metrics.BatchSizeMeter.Mark(stats.NumberOfExtractedItems);
        }

        public override void NotifyAboutWork(DocumentChange change)
        {
            if (Transformation.ApplyToAllDocuments || _collections.Contains(change.CollectionName))
                _waitForChanges.Set();
        }

        public override void Start()
        {
            if (_thread != null)
                return;

            if (Transformation.Disabled)
                return;

            var threadName = $"{Tag} process: {Name}";
            _thread = new Thread(() =>
            {
                // This has lower priority than request processing, so we let the OS
                // schedule this appropriately
                Threading.TrySettingCurrentThreadPriority(ThreadPriority.BelowNormal);
                NativeMemory.EnsureRegistered();
                Run();
            })
            {
                Name = threadName,
                IsBackground = true
            };

            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting {Tag} process: '{Name}'.");

            _thread.Start();
        }

        public override void Stop()
        {
            if (_thread == null)
                return;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping {Tag} process: '{Name}'.");

            _cts.Cancel();

            var thread = _thread;
            _thread = null;

            if (Thread.CurrentThread != thread) // prevent a deadlock
                thread.Join();
        }

        public void Run()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                try
                {
                    _waitForChanges.Reset();

                    var startTime = Database.Time.GetUtcNow();

                    if (FallbackTime != null)
                    {
                        Thread.Sleep(FallbackTime.Value);
                        FallbackTime = null;
                    }
                    var didWork = false;

                    DocumentsOperationContext context;
                    using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    {
                        var statsAggregator = _lastStats = new EtlStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);

                        AddPerformanceStats(statsAggregator);

                        using (var stats = statsAggregator.CreateScope())
                        {
                            try
                            {
                                EnsureThreadAllocationStats();

                                var loadLastProcessedEtag = Database.ConfigurationStorage.EtlStorage.GetLastProcessedEtag(Destination, Name);

                                using (context.OpenReadTransaction())
                                {
                                    var extracted = Extract(context, loadLastProcessedEtag + 1, stats);

                                    var transformed = Transform(extracted, context, stats);

                                    Load(transformed, context, stats);

                                    if (stats.LastLoadedEtag > Statistics.LastProcessedEtag)
                                    {
                                        didWork = true;
                                        Statistics.LastProcessedEtag = stats.LastLoadedEtag;
                                    }

                                    if (didWork)
                                        UpdateMetrics(startTime, stats);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                var message = $"Exception in ETL process named '{Name}'";

                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"{Tag} {message}", e);
                            }
                        }

                        statsAggregator.Complete();
                    }

                    if (didWork)
                    {
                        _storage.StoreLastProcessedEtag(Destination, Name, Statistics.LastProcessedEtag);

                        if (CancellationToken.IsCancellationRequested == false)
                        {
                            var batchCompleted = Database.EtlLoader.BatchCompleted;
                            batchCompleted?.Invoke(Name, Statistics);
                        }

                        continue;
                    }


                    try
                    {
                        _waitForChanges.Wait(CancellationToken);
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

        protected abstract bool ShouldFilterOutSystemDocument(bool isHiLo);

        protected void EnsureThreadAllocationStats()
        {
            _threadAllocations = NativeMemory.ThreadAllocations.Value;
        }

        public override Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            var lastProcessedEtag = _storage.GetLastProcessedEtag(Destination, Name);

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