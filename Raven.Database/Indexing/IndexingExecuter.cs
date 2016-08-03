//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using Lucene.Net.Index;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Sparrow.Collections;

namespace Raven.Database.Indexing
{
    public class IndexingExecuter : AbstractIndexingExecuter
    {
        private readonly ConcurrentSet<PrefetchingBehavior> prefetchingBehaviors = new ConcurrentSet<PrefetchingBehavior>();
        private readonly Prefetcher prefetcher;
        private readonly PrefetchingBehavior defaultPrefetchingBehavior;
        private readonly Dictionary<IComparable, int> tasksFailureCount = new Dictionary<IComparable, int>();

        public IndexingExecuter(WorkContext context, Prefetcher prefetcher, IndexReplacer indexReplacer)
            : base(context, indexReplacer)
        {
            autoTuner = new IndexBatchSizeAutoTuner(context);
            this.prefetcher = prefetcher;
            defaultPrefetchingBehavior = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner, "Default Prefetching behavior", true);
            defaultPrefetchingBehavior.ShouldHandleUnusedDocumentsAddedAfterCommit = true;
            prefetchingBehaviors.TryAdd(defaultPrefetchingBehavior);
        }

        public List<PrefetchingBehavior> PrefetchingBehaviors
        {
            get { return prefetchingBehaviors.ToList(); }
        }

        protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
        {
            var isStale = actions.Staleness.IsMapStale(indexesStat.Id);
            var indexingPriority = indexesStat.Priority;
            if (isStale == false)
                return false;

            if (indexingPriority == IndexingPriority.None)
                return true;


            if ((indexingPriority & IndexingPriority.Normal) == IndexingPriority.Normal)
            {
                onlyFoundIdleWork.Value = false;
                return true;
            }

            if ((indexingPriority & (IndexingPriority.Disabled | IndexingPriority.Error)) != IndexingPriority.None)
                return false;

            if (isIdle == false)
                return false; // everything else is only valid on idle runs

            if ((indexingPriority & IndexingPriority.Idle) == IndexingPriority.Idle)
                return true;

            if ((indexingPriority & IndexingPriority.Abandoned) == IndexingPriority.Abandoned)
            {
                var timeSinceLastIndexing = (SystemTime.UtcNow - indexesStat.LastIndexingTime);

                return (timeSinceLastIndexing > context.Configuration.TimeToWaitBeforeRunningAbandonedIndexes);
            }

            throw new InvalidOperationException("Unknown indexing priority for index " + indexesStat.Id + ": " + indexesStat.Priority);
        }


        protected override void UpdateStalenessMetrics(int staleCount)
        {
            context.MetricsCounters.StaleIndexMaps.Update(staleCount);
        }

        protected override bool ShouldSkipIndex(Index index)
        {
            return index.IsTestIndex ||
                   index.IsMapIndexingInProgress; // precomputed? slow? it is already running, nothing to do with it for now;
        }

        protected override bool ExecuteTasks()
        {
            try
            {
                var result = ExecuteTasksInternal();
                if (result == false)
                {
                    //we can cleanup the tasks failure count if have no more tasks
                    tasksFailureCount.Clear();
                }

                return result;
            }
            catch (Exception e)
            {
                Log.WarnException("Failed to execute tasks", e);
                throw;
            }
        }

        private bool ExecuteTasksInternal()
        {
            // we want to drain all of the pending tasks before the next run
            // but we don't want to halt indexing completely
            var sp = Stopwatch.StartNew();
            var count = 0;
            var indexIds = new HashSet<int>();
            var totalProcessedKeys = 0;

            var alreadySeen = new HashSet<IComparable>();
            transactionalStorage.Batch(actions =>
            {
                while (context.RunIndexing && sp.Elapsed.TotalMinutes < 1)
                {
                    var processedKeys = ExecuteTask(indexIds, alreadySeen);
                    if (processedKeys == 0)
                        break;

                    totalProcessedKeys += processedKeys;
                    actions.General.MaybePulseTransaction(
                        addToPulseCount: processedKeys,
                        beforePulseTransaction: () =>
                        {
                            // if we need to PulseTransaction, we are going to delete
                            // all the completed tasks, so we need to flush 
                            // all the changes made to the indexes to disk before that
                            context.IndexStorage.FlushIndexes(indexIds, onlyAddIndexError: true);
                            actions.Tasks.DeleteTasks(alreadySeen);
                            indexIds.Clear();
                            alreadySeen.Clear();
                        });

                    count++;
                }

                // need to flush all the changes
                context.IndexStorage.FlushIndexes(indexIds, onlyAddIndexError: true);
                actions.Tasks.DeleteTasks(alreadySeen);
            });

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Executed {0} tasks, processed documents: {1:#,#;;0}, took {2:#,#;;0}ms",
                    count, totalProcessedKeys, sp.ElapsedMilliseconds);
            }

            return count != 0;
        }

        private int ExecuteTask(HashSet<int> indexIds, HashSet<IComparable> alreadySeen)
        {
            var processedKeys = 0;

            transactionalStorage.Batch(actions =>
            {
                var task = GetApplicableTask(actions, alreadySeen);
                if (task == null)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("No tasks to execute were found!");

                    return;
                }
                
                context.UpdateFoundWork();

                var indexName = GetIndexName(task.Index);
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Executing {0} for index: {1} (id: {2}, task id: {3}), details: {4}",
                        task.GetType().Name, indexName, task.Index, task.Id, task);
                }

                processedKeys = task.NumberOfKeys;

                context.CancellationToken.ThrowIfCancellationRequested();

                var sp = Stopwatch.StartNew();
                try
                {
                    task.Execute(context);
                    indexIds.Add(task.Index);
                }
                catch (Exception e)
                {
                    Log.WarnException(
                        string.Format("{0} for index: {1} (index id: {2}, task id: {3}) has failed, details: {4}",
                            task.GetType().Name, indexName, task.Index, task.Id, task), e);
                    
                    if (e is CorruptIndexException)
                    {
                        Log.WarnException(string.Format("Index name: {0}, id: {1} is corrupted and needs to be reset", 
                            indexName, task.Index), e);

                        //the index is corrupted, we couldn't write to the index
                        //we can delete this task and issue an alert and set the index to errored
                        var index = context.IndexStorage.GetIndexInstance(task.Index);
                        if (index != null)
                            index.AddIndexCorruptError(e);

                        return;
                    }

                    int failureCount = 0;
                    tasksFailureCount.TryGetValue(task.Id, out failureCount);
                    failureCount++;
                    tasksFailureCount[task.Id] = failureCount;
                    if (failureCount >= 3)
                    {
                        //if we failed to execute the task for more than 3 times,
                        //we can issue an alert and delete the task
                        context.Database.AddAlert(new Alert
                        {
                            AlertLevel = AlertLevel.Error,
                            CreatedAt = SystemTime.UtcNow,
                            Message = string.Format("For index: {0} (index id: {1}, task id: {2}), details: {3}, exception: {4}",
                                indexName, task.Index, task.Id, task, e),
                            Title = string.Format("{0} failed to execute for {1} times", task.GetType().Name, failureCount),
                            UniqueKey = string.Format("Task failed for index: {0} (index id: {1}, task id: {2}) has failed ({3} times)",
                                indexName, task.Index, task.Id, failureCount),
                        });

                        return;
                    }

                    throw;
                }

                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Task for index: {0} (id: {1}, task id: {2}) has finished, took {3:#,#;;0}ms",
                        indexName, task.Index, task.Id, sp.ElapsedMilliseconds);
                }
            });

            return processedKeys;
        }

        private DatabaseTask GetApplicableTask(IStorageActionsAccessor actions, HashSet<IComparable> alreadySeen)
        {
            var disabledIndexIds = context.IndexStorage.GetDisabledIndexIds();

            var removeFromIndexTasks = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                disabledIndexIds, context.IndexStorage.Indexes, alreadySeen);
            if (removeFromIndexTasks != null)
                return removeFromIndexTasks;

            return actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(
                disabledIndexIds, context.IndexStorage.Indexes, alreadySeen);
        }

        protected override void FlushAllIndexes()
        {
            context.IndexStorage.FlushMapIndexes();
        }

        protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
        {
            return new IndexToWorkOn
            {
                IndexId = indexesStat.Id,
                LastIndexedEtag = indexesStat.LastIndexedEtag,
                LastIndexedTimestamp = indexesStat.LastIndexedTimestamp
            };
        }

        private class IndexingGroup : IDisposable
        {
            public Etag LastIndexedEtag;
            public DateTime? LastQueryTime;

            public List<IndexToWorkOn> Indexes; 
            public PrefetchingBehavior PrefetchingBehavior;
            public List<JsonDocument> JsonDocs;
            private IDisposable prefetchDisposable;
            public IndexingBatchInfo BatchInfo { get; set; }
            private int disposed = 0;
            private int indexedAmount = 0;

            public event Action<IndexingGroup> IndexingGroupProcessingFinished;


            public void SignalIndexingComplete()
            {
                if (Interlocked.Increment(ref indexedAmount) == Indexes.Count && IndexingGroupProcessingFinished != null)
                {
                    IndexingGroupProcessingFinished(this);
                }
            }

            public void ReleaseIndexingGroupFinished()
            {
                IndexingGroupProcessingFinished = null;
            }

            public void PrefetchDocuments()
            {
                /*prefetchDisposable = new WeakReference<IDisposable>(
                    PrefetchingBehavior.DocumentBatchFrom(LastIndexedEtag, out JsonDocs));*/

                prefetchDisposable =
                    PrefetchingBehavior.DocumentBatchFrom(LastIndexedEtag, out JsonDocs);
            }
            
            ~IndexingGroup()
            {
                if (Thread.VolatileRead(ref disposed) == 0)
                {
                    Dispose();
                }
            }

            public void Dispose()
            {
                prefetchDisposable?.Dispose();
                Interlocked.Increment(ref disposed);
            }
        }

        public class IndexingBatchOperation
        {
            public IndexingBatchForIndex IndexingBatch { get; set; }
            public Etag LastEtag { get; set; }
            public DateTime LastModified { get; set; }
            public IndexingBatchInfo IndexingBatchInfo { get; set; }
        }

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexes)
        {
            var currentlyRunning = currentlyProcessedIndexes.Keys;
            //we filter the indexes that are already running
            var indexesToWorkOn = indexes.Where(x => currentlyRunning.Contains(x.IndexId) == false).ToList();

            ConcurrentSet<PrefetchingBehavior> usedPrefetchers;
            List<IndexingGroup> groupedIndexes;
            var completedGroups = 0;

            if (GenerateIndexingGroupsByEtagRanges(indexesToWorkOn, out usedPrefetchers, out groupedIndexes))
            {
                return;
            }

            foreach (var indexToWorkOn in indexesToWorkOn)
            {
                indexToWorkOn.Index.IsMapIndexingInProgress = true;
            }

            var indexingAutoTunerContext = ((IndexBatchSizeAutoTuner) autoTuner).ConsiderLimitingNumberOfItemsToProcessForThisBatch(
                groupedIndexes.Max(x => x.Indexes.Max(y => y.Index.MaxIndexOutputsPerDocument)),
                groupedIndexes.Any(x => x.Indexes.Any(y => y.Index.IsMapReduce)));

            using (indexingAutoTunerContext)
            {
                var indexBatchOperations = new ConcurrentDictionary<IndexingBatchOperation, object>();

                var operationWasCancelled = GenerateIndexingBatchesAndPrefetchDocuments(groupedIndexes, indexBatchOperations);

                var executionStopwatch = Stopwatch.StartNew();

                foreach (var indexingGroup in groupedIndexes)
                {
                    indexingGroup.IndexingGroupProcessingFinished += x =>
                    {
                        if (operationWasCancelled == false)
                        {
                            ReleasePrefetchersAndUpdateStatistics(x, executionStopwatch.Elapsed);
                        }

                        if (Interlocked.Increment(ref completedGroups) == groupedIndexes.Count)
                        {
                            RemoveUnusedPrefetchers(usedPrefetchers);
                        }
                    };
                }

                if (operationWasCancelled == false)
                    operationWasCancelled = PerformIndexingOnIndexBatches(indexBatchOperations);

                context.CancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void SetPrefetcherForIndexingGroup(IndexingGroup groupIndex, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            groupIndex.PrefetchingBehavior = TryGetPrefetcherFor(groupIndex.LastIndexedEtag, usedPrefetchers) ??
                                      TryGetDefaultPrefetcher(groupIndex.LastIndexedEtag, usedPrefetchers) ??
                                      GetPrefetcherFor(groupIndex.LastIndexedEtag, usedPrefetchers);

            groupIndex.PrefetchingBehavior.Indexes = groupIndex.Indexes;
            groupIndex.PrefetchingBehavior.LastIndexedEtag = groupIndex.LastIndexedEtag;
        }

        private PrefetchingBehavior TryGetPrefetcherFor(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                if (prefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag) &&
                    usedPrefetchers.TryAdd(prefetchingBehavior))
                {
                    return prefetchingBehavior;
                }
            }

            return null;
        }

        private void ReleasePrefetchersAndUpdateStatistics(IndexingGroup indexingGroup, TimeSpan elapsedTimeSpan)
        {
            if (indexingGroup.JsonDocs != null && indexingGroup.JsonDocs.Count > 0)
            {
                indexingGroup.PrefetchingBehavior.CleanupDocuments(indexingGroup.LastIndexedEtag);
                indexingGroup.PrefetchingBehavior.UpdateAutoThrottler(indexingGroup.JsonDocs, elapsedTimeSpan);
                indexingGroup.PrefetchingBehavior.BatchProcessingComplete();
                context.ReportIndexingBatchCompleted(indexingGroup.BatchInfo);
            }
            indexingGroup.ReleaseIndexingGroupFinished();
        }

        private PrefetchingBehavior TryGetDefaultPrefetcher(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            if (defaultPrefetchingBehavior.CanUseDefaultPrefetcher(fromEtag) &&
                usedPrefetchers.TryAdd(defaultPrefetchingBehavior))
            {
                return defaultPrefetchingBehavior;
            }

            return null;
        }

        private bool PerformIndexingOnIndexBatches(ConcurrentDictionary<IndexingBatchOperation, object> indexBatchOperations)
        {
            var operationWasCancelled = false;

            try
            {
                context.MetricsCounters.IndexedPerSecond.Mark(indexBatchOperations.Keys.Count);

                long executedPartially = 0;
                context.Database.MappingThreadPool.ExecuteBatch(indexBatchOperations.Keys.ToList(),
                    indexBatchOperation =>
                    {
                        context.CancellationToken.ThrowIfCancellationRequested();
                        using (LogContext.WithResource(context.DatabaseName))
                        {
                            var performance = HandleIndexingFor(indexBatchOperation.IndexingBatch, indexBatchOperation.LastEtag, indexBatchOperation.LastModified, CancellationToken.None);

                            if (performance != null)
                                indexBatchOperation.IndexingBatchInfo.PerformanceStats.TryAdd(indexBatchOperation.IndexingBatch.Index.PublicName, performance);

                            if (Interlocked.Read(ref executedPartially) == 1)
                            {
                                context.NotifyAboutWork();
                            }
                        }
                    }, allowPartialBatchResumption: MemoryStatistics.AvailableMemoryInMb > 1.5*context.Configuration.MemoryLimitForProcessingInMb,
                    description: $"Performing indexing on index batches for a total of {indexBatchOperations.Count} indexes");

                Interlocked.Increment(ref executedPartially);
            }
            catch (OperationCanceledException)
            {
                operationWasCancelled = true;
            }
            catch (AggregateException ae)
            {
                operationWasCancelled = IsOperationCanceledException(ae);
                if (operationWasCancelled == false)
                    throw;
            }

            return operationWasCancelled;
        }

        private bool GenerateIndexingBatchesAndPrefetchDocuments(List<IndexingGroup> groupedIndexes, ConcurrentDictionary<IndexingBatchOperation, object> indexBatchOperations)
        {
            bool operationWasCancelled = false;

            context.Database.MappingThreadPool.ExecuteBatch(groupedIndexes,
                indexingGroup =>
                {
                    bool operationAdded = false;
                    try
                    {
                        indexingGroup.PrefetchDocuments();
                        var curGroupJsonDocs = indexingGroup.JsonDocs;
                        if (Log.IsDebugEnabled)
                        {
                            Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
                            curGroupJsonDocs.Count, indexingGroup.LastIndexedEtag, string.Join(", ", curGroupJsonDocs.Select(x => x.Key)));
                        }

                        indexingGroup.BatchInfo =
                            context.ReportIndexingBatchStarted(curGroupJsonDocs.Count,
                                curGroupJsonDocs.Sum(x => x.SerializedSizeOnDisk),
                                indexingGroup.Indexes.Select(x => x.Index.PublicName).ToList());

                        context.CancellationToken.ThrowIfCancellationRequested();
                        var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(curGroupJsonDocs);
                        var lastModified = lastByEtag.LastModified.Value;
                        var lastEtag = lastByEtag.Etag;
                        List<IndexToWorkOn> filteredOutIndexes;
                        var indexBatches = FilterIndexes(indexingGroup.Indexes, curGroupJsonDocs, lastEtag, out filteredOutIndexes).OrderByDescending(x => x.Index.LastQueryTime).ToList();

                        foreach (var filteredOutIndex in filteredOutIndexes)
                        {
                            indexingGroup.SignalIndexingComplete();
                            filteredOutIndex.Index.IsMapIndexingInProgress = false;
                        }

                        foreach (var indexBatch in indexBatches)
                        {
                            var indexingBatchOperation = new IndexingBatchOperation
                            {
                                IndexingBatch = indexBatch,
                                LastEtag = lastEtag,
                                LastModified = lastModified,
                                IndexingBatchInfo = indexingGroup.BatchInfo
                            };

                            if (indexBatchOperations.TryAdd(indexingBatchOperation, new object()))
                            {
                                indexingBatchOperation.IndexingBatch.OnIndexingComplete += indexingGroup.SignalIndexingComplete;
                                operationAdded = true;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        operationWasCancelled = true;
                    }
                    catch (Exception e)
                    {
                        //this is a precaution, no exception should happen at this point
                        var indexes = indexingGroup.Indexes.Select(x => x.IndexId).ToList();
                        var indexesString = string.Join(", ", indexes);
                        var message = $"Unexpected exception happened during execution of indexing... " +
                                      $"This is not supposed to happen. Reason: {e}. " +
                                      $"Currently indexing: {currentlyProcessedIndexes.Keys}, " +
                                      $"indexing group that failed: {indexesString}";

                        Log.ErrorException(message, e);

                        //rethrow because we do not want to interrupt the existing exception flow
                        throw;
                    }
                    finally
                    {
                        if (operationAdded == false)
                        {
                            indexingGroup.Indexes.ForEach(x =>
                            {
                                indexingGroup.SignalIndexingComplete();
                                x.Index.IsMapIndexingInProgress = false;
                            });
                        }
                    }
                }, description: $"Prefetching index groups for {groupedIndexes.Count} groups");

            return operationWasCancelled;
        }

        private bool GenerateIndexingGroupsByEtagRanges(IList<IndexToWorkOn> indexes, out ConcurrentSet<PrefetchingBehavior> usedPrefetchers, out List<IndexingGroup> indexingGroups)
        {
            indexingGroups = new List<IndexingGroup>();
            usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();

            var groupedIndexesByEtagRange = context.Configuration.IndexingClassifier.GroupMapIndexes(indexes);
            if (groupedIndexesByEtagRange.Count == 0)
                return true;

            groupedIndexesByEtagRange = groupedIndexesByEtagRange.OrderByDescending(x => x.Key).ToDictionary(x => x.Key, x => x.Value);

            foreach (var indexingGroup in groupedIndexesByEtagRange)
            {
                var result = new IndexingGroup
                {
                    Indexes = indexingGroup.Value,
                    LastIndexedEtag = indexingGroup.Key,
                    LastQueryTime = indexingGroup.Value.Max(y => y.Index.LastQueryTime)
                };

                SetPrefetcherForIndexingGroup(result, usedPrefetchers);

                indexingGroups.Add(result);
            }
            indexingGroups = indexingGroups.OrderByDescending(x => x.LastQueryTime).ToList();
            return false;
        }

        private PrefetchingBehavior GetPrefetcherFor(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                // at this point we've already verified that we can't use the default prefetcher
                // if it's empty, we don't need to use it
                if (prefetchingBehavior.IsDefault == false && prefetchingBehavior.IsEmpty() && usedPrefetchers.TryAdd(prefetchingBehavior))
                    return prefetchingBehavior;
            }

            var newPrefetcher = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner, string.Format("Etags from: {0}", fromEtag));

            prefetchingBehaviors.Add(newPrefetcher);
            usedPrefetchers.Add(newPrefetcher);

            return newPrefetcher;
        }

        private void RemoveUnusedPrefetchers(IEnumerable<PrefetchingBehavior> usedPrefetchingBehaviors)
        {
            var unused = prefetchingBehaviors.Except(usedPrefetchingBehaviors.Union(new[]
            {
                defaultPrefetchingBehavior
            })).ToList();


            if (unused.Count == 0)
                return;

            foreach (var unusedPrefetcher in unused)
            {
                prefetchingBehaviors.TryRemove(unusedPrefetcher);
                prefetcher.RemovePrefetchingBehavior(unusedPrefetcher);
            }
        }

        public override bool ShouldRun
        {
            get { return context.RunIndexing; }
        }

        protected override void CleanupPrefetchers()
        {
            RemoveUnusedPrefetchers(Enumerable.Empty<PrefetchingBehavior>());
        }


        private static IDisposable MapIndexingInProgress(IList<Index> indexesToWorkOn)
        {
            indexesToWorkOn.ForEach(x => x.IsMapIndexingInProgress = true);

            return new DisposableAction(() => 
                indexesToWorkOn.ForEach(x => x.IsMapIndexingInProgress = false));
        }

        public void IndexPrecomputedBatch(PrecomputedIndexingBatch precomputedBatch, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            context.MetricsCounters.IndexedPerSecond.Mark(precomputedBatch.Documents.Count);
            var indexToWorkOn = new IndexToWorkOn
            {
                Index = precomputedBatch.Index,
                IndexId = precomputedBatch.Index.indexId,
                LastIndexedEtag = Etag.Empty
            };

            using (LogContext.WithResource(context.DatabaseName))
            using (MapIndexingInProgress(new List<Index> { indexToWorkOn.Index }))
            {
                IndexingBatchForIndex indexingBatchForIndex;
                if (precomputedBatch.Documents.Count > 0)
                {
                    List<IndexToWorkOn> filteredOutIndexes;
                    indexingBatchForIndex = 
                        FilterIndexes(
                                new List<IndexToWorkOn> { indexToWorkOn },
                                precomputedBatch.Documents,
                                precomputedBatch.LastIndexed,
                                out filteredOutIndexes)
                          .FirstOrDefault();
                }
                else
                {
                    indexingBatchForIndex = new IndexingBatchForIndex
                    {
                        Batch = new IndexingBatch(precomputedBatch.LastIndexed),
                        Index = precomputedBatch.Index,
                        IndexId = precomputedBatch.Index.indexId,
                        LastIndexedEtag = precomputedBatch.LastIndexed
                    };
                }

                if (indexingBatchForIndex == null)
                    return;

                IndexingBatchInfo batchInfo = null;

                IndexingPerformanceStats performance = null;
                try
                {
                    batchInfo = context.ReportIndexingBatchStarted(precomputedBatch.Documents.Count, -1, new List<string>
                    {
                        indexToWorkOn.Index.PublicName
                    });


                    batchInfo.BatchType = BatchType.Precomputed;


                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug("Going to index precomputed documents for a new index {0}. Count of precomputed docs {1}",
                            precomputedBatch.Index.PublicName, precomputedBatch.Documents.Count);
                    }

                    context.ReportIndexingBatchCompleted(batchInfo);



                    indexReplacer.ReplaceIndexes(new[] { indexToWorkOn.IndexId });


                    performance = HandleIndexingFor(indexingBatchForIndex, precomputedBatch.LastIndexed, precomputedBatch.LastModified, token);
                }
                finally
                {
                    if (batchInfo != null)
                    {
                        if (performance != null)
                            batchInfo.PerformanceStats.TryAdd(indexingBatchForIndex.Index.PublicName, performance);


                        context.ReportIndexingBatchCompleted(batchInfo);
                    }
                }
            }
        }

        private IndexingPerformanceStats HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified, CancellationToken token)
        {
            if (currentlyProcessedIndexes.TryAdd(batchForIndex.IndexId, batchForIndex.Index) == false)
            {
                Log.Warn("Tried to run indexing for index '{0}' that is already running", batchForIndex.Index.PublicName);
                batchForIndex.SignalIndexingComplete();
                return null;
            }

            IndexingPerformanceStats performanceResult = null;
            var wasOperationCanceled = false;
            try
            {
                transactionalStorage.Batch(actions => { performanceResult = IndexDocuments(actions, batchForIndex, token); });

                // This can be null if IndexDocument fails to execute and the exception is catched.
                if (performanceResult != null)
                    performanceResult.RunCompleted();

                batchForIndex.Index.DecrementIndexingOutOfMemoryErrors();
            }
            catch (IndexDoesNotExistsException)
            {
                //race condition -> index was deleted
                //thus we do not need to update last indexed docs..
                wasOperationCanceled = true;
            }
            catch (InvalidDataException e)
            {
                Log.ErrorException("Failed to index because of data corruption. ", e);
                context.AddError(batchForIndex.IndexId, batchForIndex.Index.PublicName, null, e, $"Failed to index because of data corruption. Reason: {e.Message}");
            }
            catch (Exception e)
            {
                Exception conflictException;
                if (TransactionalStorageHelper.IsWriteConflict(e, out conflictException))
                {
                    Log.Info($"Write conflict encountered for index {batchForIndex.Index.PublicName}, " +
                             $"probably when updating indexing stats. Will retry." +
                             $"Details: {conflictException.Message}");
                    return null;
                }

                if (HandleIfOutOfMemory(e, new OutOfMemoryDetails
                {
                    Index = batchForIndex.Index,
                    FailedItemsToProcessCount = batchForIndex.Batch.Ids.Count
                }))
                {
                    wasOperationCanceled = true;
                    return null;
                }

                if (IsOperationCanceledException(e))
                {
                    wasOperationCanceled = true;
                    throw;
                }

                Log.WarnException($"Failed to index documents for index: {batchForIndex.Index.PublicName}", e);
                context.AddError(batchForIndex.IndexId, batchForIndex.Index.PublicName, null, e);
            }
            finally
            {
                if (performanceResult != null)
                {
                    performanceResult.OnCompleted = null;
                }

                try
                {
                    if (wasOperationCanceled == false)
                    {
                        if (Log.IsDebugEnabled)
                        {
                            Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
                                batchForIndex.Batch.Docs.Count,
                                lastEtag,
                                batchForIndex.Index.PublicName);
                        }

                        var keepTrying = true;
                        for (var i = 0; i < 10 && keepTrying; i++)
                        {
                            keepTrying = false;

                            try
                            {
                                transactionalStorage.Batch(actions =>
                                {
                                    // whatever we succeeded in indexing or not, we have to update this
                                    // because otherwise we keep trying to re-index failed documents
                                    actions.Indexing.UpdateLastIndexed(batchForIndex.IndexId, lastEtag, lastModified);
                                });
                            }
                            catch (IndexDoesNotExistsException)
                            {
                                //we can ignore this, no need to retry
                            }
                            catch (Exception e)
                            {
                                if (TransactionalStorageHelper.IsOutOfMemoryException(e))
                                {
                                    batchForIndex.Index.AddOutOfMemoryDatabaseAlert(e);
                                    //if it's an esent/voron OOME we can keep trying
                                    keepTrying = true;
                                }
                                    
                                Exception conflictException;
                                if (TransactionalStorageHelper.IsWriteConflict(e, out conflictException))
                                {
                                    Log.Info($"Write conflict encountered for index '{batchForIndex.Index.PublicName}' when updating last etag. " +
                                             $"Will retry. Details: {conflictException.Message}");
                                    keepTrying = true;
                                }

                                if (keepTrying == false)
                                {
                                    //unknown error
                                    Log.WarnException($"Failed to update last etag for index '{batchForIndex.Index.PublicName}'", e);
                                    context.AddError(batchForIndex.IndexId, batchForIndex.Index.PublicName, null, e);
                                }
                            }

                            if (keepTrying)
                                Thread.Sleep(11);
                        }
                    }
                }
                finally
                {
                    Index _;
                    currentlyProcessedIndexes.TryRemove(batchForIndex.IndexId, out _);
                    batchForIndex.SignalIndexingComplete();
                    batchForIndex.Index.IsMapIndexingInProgress = false;
                }
            }

            return performanceResult;
        }

        public class IndexingBatchForIndex
        {
            private static int _counter = 0;
            public int BatchId = 0;

            public IndexingBatchForIndex()
            {
                BatchId = Interlocked.Increment(ref _counter);
            }

            public int IndexId { get; set; }

            public Index Index { get; set; }

            public Etag LastIndexedEtag { get; set; }

            public IndexingBatch Batch { get; set; }
            public event Action OnIndexingComplete;

            public void SignalIndexingComplete()
            {
                if (OnIndexingComplete != null)
                {
                    OnIndexingComplete();
                }
            }
        }

        private IEnumerable<IndexingBatchForIndex> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, Etag highestETagInBatch, out List<IndexToWorkOn> filteredOutIndexes)
        {
            var innerFilteredOutIndexes = new ConcurrentStack<IndexToWorkOn>();
            var last = jsonDocs.Last();

            Debug.Assert(last.Etag != null);
            Debug.Assert(last.LastModified != null);

            var lastEtag = last.Etag;
            var lastModified = last.LastModified.Value;

            var documentRetriever = new DocumentRetriever(null, null, context.ReadTriggers);

            var filteredDocs =
                BackgroundTaskExecuter.Instance.Apply(context, jsonDocs, doc =>
                {
                    var filteredDoc = documentRetriever.ExecuteReadTriggers(doc, null, ReadOperation.Index);
                    return filteredDoc == null ? new
                    {
                        Doc = doc,
                        Json = (object)new FilteredDocument(doc)
                    } : new
                    {
                        Doc = filteredDoc,
                        Json = JsonToExpando.Convert(doc.ToJson())
                    };
                });

            if (Log.IsDebugEnabled)
                Log.Debug("After read triggers executed, {0} documents remained", filteredDocs.Count);

            var results = new ConcurrentQueue<IndexingBatchForIndex>();
            var actions = new ConcurrentQueue<Tuple<Action<IStorageActionsAccessor>, IndexToWorkOn>>();
            context.Database.MappingThreadPool.ExecuteBatch(indexesToWorkOn, indexToWorkOn =>
            {
                try
                {
                    var indexName = indexToWorkOn.Index.PublicName;
                    var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
                    if (viewGenerator == null)
                        return; // probably deleted

                    var batch = new IndexingBatch(highestETagInBatch);

                    foreach (var filteredDoc in filteredDocs)
                    {
                        var doc = filteredDoc.Doc;
                        var json = filteredDoc.Json;

                        if (defaultPrefetchingBehavior.FilterDocuments(doc) == false
                            || doc.Etag.CompareTo(indexToWorkOn.LastIndexedEtag) <= 0)
                            continue;

                        // did we already indexed this document in this index?

                        var etag = doc.Etag;
                        if (etag == null)
                            continue;

                        // is the Raven-Entity-Name a match for the things the index executes on?
                        if (viewGenerator.ForEntityNames.Count != 0 &&
                            viewGenerator.ForEntityNames.Contains(doc.Metadata.Value<string>(Constants.RavenEntityName)) == false)
                        {
                            continue;
                        }

                        batch.Add(doc, json, defaultPrefetchingBehavior.ShouldSkipDeleteFromIndex(doc));

                        if (batch.DateTime == null)

                            batch.DateTime = doc.LastModified;
                        else
                            batch.DateTime = batch.DateTime > doc.LastModified
                                ? doc.LastModified
                                : batch.DateTime;
                    }

                    if (batch.Docs.Count == 0)
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("All documents have been filtered for {0}, no indexing will be performed, updating to {1}, {2}", indexName, lastEtag, lastModified);

                        // we use it this way to batch all the updates together
                        if (indexToWorkOn.LastIndexedEtag.CompareTo(lastEtag) < 0)
                            actions.Enqueue(new Tuple<Action<IStorageActionsAccessor>, IndexToWorkOn>(accessor =>
                            {
                                accessor.Indexing.UpdateLastIndexed(indexToWorkOn.Index.indexId, lastEtag, lastModified);
                                accessor.AfterStorageCommit += () =>
                                {
                                    indexToWorkOn.Index.EnsureIndexWriter();
                                    indexToWorkOn.Index.Flush(lastEtag);
                                };
                            }, indexToWorkOn));

                        innerFilteredOutIndexes.Push(indexToWorkOn);
                        context.MarkIndexFilteredOut(indexName);
                        return;
                    }

                    if (Log.IsDebugEnabled)
                        Log.Debug("Going to index {0} documents in {1}: ({2})", batch.Ids.Count, indexToWorkOn, string.Join(", ", batch.Ids));

                    results.Enqueue(new IndexingBatchForIndex
                    {
                        Batch = batch,
                        IndexId = indexToWorkOn.IndexId,
                        Index = indexToWorkOn.Index,
                        LastIndexedEtag = indexToWorkOn.LastIndexedEtag
                    });
                }
                catch (InvalidDataException e)
                {
                    Log.ErrorException("Failed to index because of data corruption. ", e);
                    context.AddError(indexToWorkOn.IndexId, indexToWorkOn.Index.PublicName, null, e, $"Failed to index because of data corruption. Reason: {e.Message}");
                }
            }, description: $"Filtering documents for {indexesToWorkOn.Count} indexes");

            filteredOutIndexes = innerFilteredOutIndexes.ToList();

            foreach (var actionWithIndex in actions)
            {
                var action = actionWithIndex.Item1;
                if (action == null)
                    continue;

                var index = actionWithIndex.Item2.Index;

                var keepTrying = true;
                for (var i = 0; i < 10 && keepTrying; i++)
                {
                    keepTrying = false;

                    try
                    {
                        transactionalStorage.Batch(actionsAccessor =>
                        {
                            action(actionsAccessor);
                        });
                    }
                    catch (IndexDoesNotExistsException)
                    {
                        //we can ignore this, no need to retry
                    }
                    catch (IOException e)
                    {
                        //we failed to create the index writer, we will retry on the next run of the indexing executer
                        Log.WarnException(e.Message, e);
                        context.AddError(index.IndexId, index.PublicName, null, e, $"Failed to save last indexed etag. Reason: {e.Message}");
                        break;
                    }
                    catch (Exception e)
                    {
                        if (TransactionalStorageHelper.IsOutOfMemoryException(e))
                        {
                            index.AddOutOfMemoryDatabaseAlert(e);

                            //we can keep trying if it's an esent/voron OOME
                            //if we fail to save the last indexed (after all the retries),
                            //it will be retried on the next run of the indexing executer
                            keepTrying = true;
                        }

                        Exception conflictException;
                        if (TransactionalStorageHelper.IsWriteConflict(e, out conflictException))
                        {
                            Log.Info($"Write conflict encountered for index {index.PublicName} when updaing last indexed. " +
                                     $"Will retry. Details: {conflictException.Message}");
                            keepTrying = true;
                        }

                        if (keepTrying == false)
                        {
                            Log.WarnException($"Failed to update last etag for index '{index.PublicName}'", e);
                            context.AddError(index.IndexId, index.PublicName, null, e, $"Failed to save last indexed etag. Reason: {e.Message}");
                            break;
                        }
                    }

                    if (keepTrying)
                        Thread.Sleep(11);
                }
            }

            return results.Where(x => x != null);
        }

        protected override bool IsValidIndex(IndexStats indexesStat)
        {
            return true;
        }

        private IndexingPerformanceStats IndexDocuments(IStorageActionsAccessor actions, IndexingBatchForIndex indexingBatchForIndex, CancellationToken token)
        {
            var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexingBatchForIndex.IndexId);
            if (viewGenerator == null)
                return null; // index was deleted, probably

            var batch = indexingBatchForIndex.Batch;

            if (Log.IsDebugEnabled)
            {
                string ids;
                if (batch.Ids.Count < 256)
                    ids = string.Join(",", batch.Ids);
                else
                {
                    ids = string.Join(", ", batch.Ids.Take(128)) + " ... " + string.Join(", ", batch.Ids.Skip(batch.Ids.Count - 128));
                }
                Log.Debug("Indexing {0} documents for index: {1}. ({2})", batch.Docs.Count, indexingBatchForIndex.Index.PublicName, ids);
            }

            token.ThrowIfCancellationRequested();

            return context.IndexStorage.Index(indexingBatchForIndex.IndexId, viewGenerator, batch, context, actions, batch.DateTime ?? DateTime.MinValue, token); ;
        }

        protected override void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(Log, "Could not dispose of IndexingExecuter");

            foreach (var prefetchingBehavior in PrefetchingBehaviors)
            {
                exceptionAggregator.Execute(prefetchingBehavior.Dispose);
            }

            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
