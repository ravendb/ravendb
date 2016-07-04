//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
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

        private class IndexingGroup
        {
            public Etag LastIndexedEtag;
            public DateTime? LastQueryTime;
            public List<IndexToWorkOn> Indexes; 
            public PrefetchingBehavior PrefetchingBehavior;
        }

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexes)
        {
            var indexingGroups = context.Configuration.IndexingClassifier.GroupMapIndexes(indexes);

            indexingGroups = indexingGroups.OrderByDescending(x => x.Key).ToDictionary(x => x.Key, x => x.Value);

            if (indexingGroups.Count == 0)
                return;

            var usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();

            var groupedIndexes = indexingGroups.Select(x =>
            {
                var result = new IndexingGroup
                {
                    LastIndexedEtag = x.Key,
                    Indexes = x.Value,
                    LastQueryTime = x.Value.Max(y => y.Index.LastQueryTime),
                };

                SetPrefetcherForIndexingGroup(result, usedPrefetchers);

                return result;
            }).OrderByDescending(x => x.LastQueryTime).ToList();
            

            var maxIndexOutputsPerDoc = groupedIndexes.Max(x => x.Indexes.Max(y => y.Index.MaxIndexOutputsPerDocument));
            var containsMapReduceIndexes = groupedIndexes.Any(x => x.Indexes.Any(y => y.Index.IsMapReduce));

            var recoverTunerState = ((IndexBatchSizeAutoTuner)autoTuner).ConsiderLimitingNumberOfItemsToProcessForThisBatch(maxIndexOutputsPerDoc, containsMapReduceIndexes);

            BackgroundTaskExecuter.Instance.ExecuteAll(context, groupedIndexes, (indexingGroup, i) =>
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                using (LogContext.WithDatabase(context.DatabaseName))
                {
                    var prefetchingBehavior = indexingGroup.PrefetchingBehavior;
                    var indexesToWorkOn = indexingGroup.Indexes;

                    var operationCanceled = false;
                    TimeSpan indexingDuration = TimeSpan.Zero;
                    var lastEtag = Etag.Empty;

                    IndexingBatchInfo batchInfo = null;

                    try
                    {
                        using (MapIndexingInProgress(indexesToWorkOn))
                        {
                            List<JsonDocument> jsonDocs;
                            using (prefetchingBehavior.DocumentBatchFrom(indexingGroup.LastIndexedEtag, out jsonDocs))
                            {
                                try
                                {
                                    if (Log.IsDebugEnabled)
                                    {
                                        Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
                                            jsonDocs.Count, indexingGroup.LastIndexedEtag, string.Join(", ", jsonDocs.Select(x => x.Key)));
                                    }

                                    batchInfo = context.ReportIndexingBatchStarted(jsonDocs.Count, jsonDocs.Sum(x => x.SerializedSizeOnDisk), indexesToWorkOn.Select(x => x.Index.PublicName).ToList());

                                    context.CancellationToken.ThrowIfCancellationRequested();

                                    if (jsonDocs.Count <= 0)
                                    {
                                        return;
                                    }

                                    var sw = Stopwatch.StartNew();

                                    lastEtag = DoActualIndexing(indexesToWorkOn, jsonDocs, batchInfo);

                                    indexingDuration = sw.Elapsed;
                                }
                                catch (InvalidDataException e)
                                {
                                    Log.ErrorException("Failed to index because of data corruption. ", e);
                                    indexesToWorkOn.ForEach(index =>
                                        context.AddError(index.IndexId, index.Index.PublicName, null, string.Format("Failed to index because of data corruption. Reason: {0}", e.Message)));
                                }
                                catch (OperationCanceledException)
                                {
                                    operationCanceled = true;
                                }
                                catch (AggregateException e)
                                {
                                    var allOperationsCanceled = e
                                        .InnerExceptions
                                        .All(x => x is OperationCanceledException);

                                    if (allOperationsCanceled == false)
                                        throw;

                                    operationCanceled = true;
                                }
                                catch (Exception)
                                {
                                    //we should not clean docs from prefetching behavior because something
                                    //unexpected has thrown
                                    //logging will be done in catch in the outer scope
                                    operationCanceled = true;
                                    // ReSharper disable once ThrowingSystemException
                                    throw;
                                }
                                finally
                                {
                                    if (operationCanceled == false && jsonDocs != null && jsonDocs.Count > 0)
                                    {
                                        prefetchingBehavior.CleanupDocuments(lastEtag);
                                        prefetchingBehavior.UpdateAutoThrottler(jsonDocs, indexingDuration);
                                    }

                                    prefetchingBehavior.BatchProcessingComplete();
                                    if (batchInfo != null)
                                        context.ReportIndexingBatchCompleted(batchInfo);
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        //expected exception here, nothing to do
                    }
                    catch (AggregateException e)
                    {
                        var allOperationsCanceled = e
                            .InnerExceptions
                            .All(x => x is OperationCanceledException);

                        if (allOperationsCanceled == false)
                        {
                            var message = string.Format("Unexpected AggregateException happened during execution of indexing batch...this is not supposed to happen. Reason: {0}", e);
                            Log.Error(message, e);
                            indexesToWorkOn.ForEach(index => context.AddError(index.IndexId, index.Index.PublicName, null, message));
                        }
                    }
                    catch (Exception e)
                    {
                        //this is a precaution, no exception should happen at this point
                        var message = string.Format("Unexpected exception happened during execution of indexing batch...this is not supposed to happen. Reason: {0}", e);
                        Log.Error(message, e);
                        indexesToWorkOn.ForEach(index => context.AddError(index.IndexId, index.Index.PublicName, null, message));

                        //rethrow because we do not want to interrupt the existing exception flow
                        // ReSharper disable once ThrowingSystemException
                        throw;
                    }
                }
            });

            if (recoverTunerState != null)
                recoverTunerState();

            RemoveUnusedPrefetchers(usedPrefetchers);
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

        private PrefetchingBehavior TryGetDefaultPrefetcher(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            if (defaultPrefetchingBehavior.CanUseDefaultPrefetcher(fromEtag) &&
                usedPrefetchers.TryAdd(defaultPrefetchingBehavior))
            {
                return defaultPrefetchingBehavior;
            }

            return null;
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

            if(unused.Count == 0)
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

        private static IDisposable MapIndexingInProgress(IList<IndexToWorkOn> indexesToWorkOn)
        {
            indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = true);

            return new DisposableAction(() => indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = false));
        }

        private Etag DoActualIndexing(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, IndexingBatchInfo indexingBatchInfo)
        {
            var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(jsonDocs);
            var lastModified = lastByEtag.LastModified.Value;
            var lastEtag = lastByEtag.Etag;

            context.MetricsCounters.IndexedPerSecond.Mark(jsonDocs.Count);
            
            var result = FilterIndexes(indexesToWorkOn, jsonDocs, lastEtag).OrderByDescending(x => x.Index.LastQueryTime).ToList();

            BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, result,
                index =>
                {
                    using (LogContext.WithDatabase(context.DatabaseName))
                    {
                        var performance = HandleIndexingFor(index, lastEtag, lastModified, context.CancellationToken);

                        if (performance != null)
                            indexingBatchInfo.PerformanceStats.TryAdd(index.Index.PublicName, performance);
                    }
                });

            return lastEtag;
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

            using (LogContext.WithDatabase(context.DatabaseName))
            using (MapIndexingInProgress(new List<IndexToWorkOn> { indexToWorkOn }))
            {
                IndexingBatchForIndex indexingBatchForIndex;
                if (precomputedBatch.Documents.Count > 0)
                {
                    indexingBatchForIndex = 
                        FilterIndexes(
                                new List<IndexToWorkOn> {indexToWorkOn}, 
                                precomputedBatch.Documents,
                                precomputedBatch.LastIndexed)
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

            indexReplacer.ReplaceIndexes(new []{ indexToWorkOn.IndexId });
        }

        private IndexingPerformanceStats HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified, CancellationToken token)
        {
            if (currentlyProcessedIndexes.TryAdd(batchForIndex.IndexId, batchForIndex.Index) == false)
            {
                Log.Warn("Tried to run indexing for index '{0}' that is already running", batchForIndex.Index.PublicName);
                return null;
            }

            IndexingPerformanceStats performanceResult = null;
            var wasOutOfMemory = false;
            var wasOperationCanceled = false;
            try
            {
                transactionalStorage.Batch(actions =>
                {
                    performanceResult = IndexDocuments(actions, batchForIndex, token);
                });

                // This can be null if IndexDocument fails to execute and the exception is catched.
                if (performanceResult != null)
                    performanceResult.RunCompleted();
            }
            catch (OperationCanceledException)
            {
                wasOperationCanceled = true;
                throw;
            }
            catch (IndexDoesNotExistsException)
            {
                //race condition -> index was deleted
                //thus we do not need to update last indexed docs..
                wasOperationCanceled = true;
                throw;
            }
            catch (Exception e)
            {
                var exception = e;
                var aggregateException = exception as AggregateException;
                if (aggregateException != null)
                    exception = aggregateException.ExtractSingleInnerException();

                if (TransactionalStorageHelper.IsWriteConflict(exception))
                    return null;

                Log.WarnException(string.Format("Failed to index documents for index: {0}", batchForIndex.Index.PublicName), exception);

                wasOutOfMemory = TransactionalStorageHelper.IsOutOfMemoryException(exception);

                if (wasOutOfMemory == false)
                    context.AddError(batchForIndex.IndexId, batchForIndex.Index.PublicName, null, exception);
            }
            finally
            {
                try
                {
                    if (performanceResult != null)
                    {
                        performanceResult.OnCompleted = null;
                    }

                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
                            batchForIndex.Batch.Docs.Count,
                            lastEtag,
                            batchForIndex.Index.PublicName);
                    }

                    if (wasOutOfMemory == false && wasOperationCanceled == false)
                    {
                        transactionalStorage.Batch(actions =>
                        {
                            // whatever we succeeded in indexing or not, we have to update this
                            // because otherwise we keep trying to re-index failed documents
                            actions.Indexing.UpdateLastIndexed(batchForIndex.IndexId, lastEtag, lastModified);
                        });
                    }
                    else if (wasOutOfMemory)
                        HandleOutOfMemory(batchForIndex);
                }
                finally
                {
                    Index _;
                    currentlyProcessedIndexes.TryRemove(batchForIndex.IndexId, out _);
                }
            }

            return performanceResult;
        }

        private void HandleOutOfMemory(IndexingBatchForIndex batchForIndex)
        {
            transactionalStorage.Batch(actions =>
            {
                var instance = context.IndexStorage.GetIndexInstance(batchForIndex.IndexId);
                if (instance == null)
                {
                    return;
                }

                Log.Error("Disabled index '{0}'. Reason: out of memory.", instance.PublicName);

                string configurationKey = null;
                if (string.Equals(context.Database.TransactionalStorage.FriendlyName, InMemoryRavenConfiguration.VoronTypeName, StringComparison.OrdinalIgnoreCase))
                {
                    configurationKey = Constants.Voron.MaxScratchBufferSize;
                }
                else if (string.Equals(context.Database.TransactionalStorage.FriendlyName, InMemoryRavenConfiguration.EsentTypeName, StringComparison.OrdinalIgnoreCase))
                {
                    configurationKey = Constants.Esent.MaxVerPages;
                }

                Debug.Assert(configurationKey != null);

                actions.Indexing.SetIndexPriority(batchForIndex.IndexId, IndexingPriority.Disabled);
                context.Database.AddAlert(new Alert { AlertLevel = AlertLevel.Error, CreatedAt = SystemTime.UtcNow, Title = string.Format("Index '{0}' was disabled", instance.PublicName), UniqueKey = string.Format("Index '{0}' was disabled", instance.IndexId), Message = string.Format("Out of memory exception occured in storage during indexing process for index '{0}'. As a result of this action, index changed state to disabled. Try increasing '{1}' value in configuration.", instance.PublicName, configurationKey) });
                instance.Priority = IndexingPriority.Disabled;
            });
        }

        public class IndexingBatchForIndex
        {
            public int IndexId { get; set; }

            public Index Index { get; set; }

            public Etag LastIndexedEtag { get; set; }

            public IndexingBatch Batch { get; set; }
        }

        private IEnumerable<IndexingBatchForIndex> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, Etag highestETagInBatch)
        {
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

            if ( Log.IsDebugEnabled ) 
                Log.Debug("After read triggers executed, {0} documents remained", filteredDocs.Count);

            var results = new IndexingBatchForIndex[indexesToWorkOn.Count];
            var actions = new Action<IStorageActionsAccessor>[indexesToWorkOn.Count];

            BackgroundTaskExecuter.Instance.ExecuteAll(context, indexesToWorkOn, (indexToWorkOn, i) =>
            {
                var indexName = indexToWorkOn.Index.PublicName;
                var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
                if (viewGenerator == null)
                    return; // probably deleted

                var batch = new IndexingBatch(highestETagInBatch);

                foreach (var item in filteredDocs)
                {
                    if (defaultPrefetchingBehavior.FilterDocuments(item.Doc) == false)
                        continue;

                    // did we already indexed this document in this index?
                    var etag = item.Doc.Etag;
                    if (etag == null)
                        continue;

                    // is the Raven-Entity-Name a match for the things the index executes on?
                    if (viewGenerator.ForEntityNames.Count != 0 &&
                        viewGenerator.ForEntityNames.Contains(item.Doc.Metadata.Value<string>(Constants.RavenEntityName)) == false)
                    {
                        continue;
                    }

                    batch.Add(item.Doc, item.Json, defaultPrefetchingBehavior.ShouldSkipDeleteFromIndex(item.Doc));

                    if (batch.DateTime == null)
                        batch.DateTime = item.Doc.LastModified;
                    else
                        batch.DateTime = batch.DateTime > item.Doc.LastModified
                                             ? item.Doc.LastModified
                                             : batch.DateTime;
                }

                if (batch.Docs.Count == 0)
                {
                    if ( Log.IsDebugEnabled )
                        Log.Debug("All documents have been filtered for {0}, no indexing will be performed, updating to {1}, {2}", indexName, lastEtag, lastModified);

                    // we use it this way to batch all the updates together
                    actions[i] = accessor =>
                    {
                        accessor.Indexing.UpdateLastIndexed(indexToWorkOn.Index.indexId, lastEtag, lastModified);

                        accessor.AfterStorageCommit += () =>
                        {
                            indexToWorkOn.Index.EnsureIndexWriter();
                            indexToWorkOn.Index.Flush(lastEtag);
                        };
                    };

                    return;
                }
                
                if (Log.IsDebugEnabled)
                    Log.Debug("Going to index {0} documents in {1}: ({2})", batch.Ids.Count, indexToWorkOn, string.Join(", ", batch.Ids));
                
                results[i] = new IndexingBatchForIndex
                {
                    Batch = batch,
                    IndexId = indexToWorkOn.IndexId,
                    Index = indexToWorkOn.Index,
                    LastIndexedEtag = indexToWorkOn.LastIndexedEtag
                };

            });

            transactionalStorage.Batch(actionsAccessor =>
            {
                foreach (var action in actions)
                {
                    if (action != null)
                    {
                        try
                        {
                            action(actionsAccessor);
                        }
                        catch (Exception e)
                        {
                            Log.WarnException("Failure running post filter index action for index with no docs to execute", e);
                        }
                    }
                }
            });

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
