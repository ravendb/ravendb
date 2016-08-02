using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
    public abstract class AbstractIndexingExecuter
    {
        protected WorkContext context;

        protected readonly IndexReplacer indexReplacer;

        protected TaskScheduler scheduler;
        protected readonly ILog Log;
        protected ITransactionalStorage transactionalStorage;
        protected int workCounter;
        protected int lastFlushedWorkCounter;
        protected BaseBatchSizeAutoTuner autoTuner;
        protected ConcurrentDictionary<int, Index> currentlyProcessedIndexes = new ConcurrentDictionary<int, Index>();

        protected AbstractIndexingExecuter(WorkContext context, IndexReplacer indexReplacer)
        {
            Log = LogManager.GetLogger(GetType());
            this.transactionalStorage = context.TransactionalStorage;
            this.context = context;
            this.indexReplacer = indexReplacer;
            this.scheduler = context.TaskScheduler;
        }

        public void Execute()
        {
            using (LogContext.WithResource(context.DatabaseName))
            {
                Init();

                var name = GetType().Name;
                var workComment = "WORK BY " + name;

                bool isIdle = false;
                while (ShouldRun)
                {
                    bool foundWork;
                    try
                    {
                        bool onlyFoundIdleWork;
                        foundWork = ExecuteIndexing(isIdle, out onlyFoundIdleWork);
                        if (foundWork && onlyFoundIdleWork == false)
                            isIdle = false;

                        var foundTasksWork = ExecuteTasks();
                        foundWork = foundWork || foundTasksWork;
                    }
                    catch (OutOfMemoryException oome)
                    {
                        foundWork = true;
                        HandleSystemOutOfMemoryException(oome);
                    }
                    catch (OperationCanceledException)
                    {
                        Log.Info("Got rude cancellation of indexing as a result of shutdown, aborting current indexing run");
                        return;
                    }
                    catch (AggregateException ae)
                    {
                        if (IsOperationCanceledException(ae))
                        {
                            Log.Info("Got rude cancellation of indexing as a result of shutdown, aborting current indexing run");
                            return;
                        }

                        foundWork = true;
                        if (HandleIfOutOfMemory(ae, null) == false)
                        {
                            Log.ErrorException("Failed to execute indexing", ae);
                        }
                    }
                    catch (Exception e)
                    {
                        foundWork = true; // we want to keep on trying, anyway, not wait for the timeout or more work
                        if (HandleIfOutOfMemory(e, null) == false)
                        {
                            Log.ErrorException("Failed to execute indexing", e);
                        }
                    }
                    if (foundWork == false && ShouldRun)
                    {
                        isIdle = context.WaitForWork(context.Configuration.TimeToWaitBeforeRunningIdleIndexes, ref workCounter, () =>
                        {
                            try
                            {
                                FlushIndexes();
                            }
                            catch (Exception e)
                            {
                                Log.WarnException("Could not flush indexes properly", e);
                            }

                            try
                            {
                                CleanupPrefetchers();
                            }
                            catch (Exception e)
                            {
                                Log.WarnException("Could not cleanup prefetchers properly", e);
                            }

                            try
                            {
                                CleanupScheduledReductions();
                            }
                            catch (Exception e)
                            {
                                Log.WarnException("Could not cleanup scheduled reductions properly", e);
                            }

                        }, name);
                    }
                    else // notify the tasks executer that it has work to do
                    {                       
                        context.ShouldNotifyAboutWork(() => workComment);
                        context.NotifyAboutWork();
                    }
                }
                Dispose();
            }
        }

        protected static bool IsOperationCanceledException(Exception e)
        {
            var ae = e as AggregateException;
            if (ae == null)
            {
                return e is OperationCanceledException;
            }

            foreach (var innerException in ae.Flatten().InnerExceptions)
            {
                if (innerException is AggregateException &&
                    IsOperationCanceledException(innerException))
                    continue;

                if (innerException is OperationCanceledException == false)
                    return false;
            }

            //return true only if all of the exceptions are operation canceled exceptions
            return true;
        }

        protected bool HandleIfOutOfMemory(Exception exception, OutOfMemoryDetails details)
        {
            var ae = exception as AggregateException;
            if (ae == null)
            {
                if (exception is OutOfMemoryException)
                {
                    HandleSystemOutOfMemoryException(exception);
                    return true;
                }  

                if (TransactionalStorageHelper.IsOutOfMemoryException(exception))
                {
                    HandleRavenOutOfMemoryException(exception, details);
                    return true;
                }
            }

            var isSystemOutOfMemory = false;
            var isRavenOutOfMemoryException = false;
            Exception oome = null;
            Exception ravenOutOfMemoryException = null;

            foreach (var innerException in ae.Flatten().InnerExceptions)
            {
                if (innerException is OutOfMemoryException)
                {
                    isSystemOutOfMemory = true;
                    oome = innerException;
                }

                if (TransactionalStorageHelper.IsOutOfMemoryException(innerException))
                {
                    isRavenOutOfMemoryException = true;
                    ravenOutOfMemoryException = innerException;
                }

                if (isSystemOutOfMemory && isRavenOutOfMemoryException)
                    break;
            }

            if (isSystemOutOfMemory)
                HandleSystemOutOfMemoryException(oome);

            if (isRavenOutOfMemoryException)
                HandleRavenOutOfMemoryException(ravenOutOfMemoryException, details);

            return isRavenOutOfMemoryException;
        }

        public abstract bool ShouldRun { get; }

        protected virtual bool ExecuteTasks()
        {
            return false;
        }

        protected virtual void CleanupPrefetchers() { }

        protected virtual void CleanupScheduledReductions() { }

        protected virtual void Dispose() { }

        protected virtual void Init() { }

        protected string GetIndexName(int indexId)
        {
            var index = context.IndexStorage.GetIndexInstance(indexId);
            return index == null ? $"N/A, index id: {indexId}" : index.PublicName;
        }

        private void HandleSystemOutOfMemoryException(Exception oome)
        {
            Log.WarnException(
                "Failed to execute indexing because of an out of memory exception. " +
                "Will force a full GC cycle and then become more conservative with regards to memory",
                oome);

            // On the face of it, this is stupid, because OOME will not be thrown if the GC could release
            // memory, but we are actually aware that during indexing, the GC couldn't find garbage to clean,
            // but in here, we are AFTER the index was done, so there is likely to be a lot of garbage.
            RavenGC.CollectGarbage(GC.MaxGeneration);
            autoTuner.HandleOutOfMemory();
        }

        public class OutOfMemoryDetails
        {
            public Index Index { get; set; }

            public int FailedItemsToProcessCount { get; set; }

            public bool IsReducing { get; set; }
        }

        protected void HandleRavenOutOfMemoryException(Exception exception, OutOfMemoryDetails details)
        {
            var message = $"Failed to execute indexing because of {context.Database.TransactionalStorage.FriendlyName} " +
                             "out of memory exception. Will try to reduce batch size";

            if (details == null)
            {
                Log.WarnException(message, exception);
                autoTuner.DecreaseBatchSize();
                return;
            }

            var errorCount = details.Index.IncrementOutOfMemoryErrors(details.IsReducing);
            //if the current number of items to process in single batch is more than
            //the initial number to index/reduce, than we can reduce the batch size and try again.
            //If we can't reduce anymore, we'll try to disable the index
            if (autoTuner.NumberOfItemsToProcessInSingleBatch > autoTuner.InitialNumberOfItems)
            {
                //will try to reduce the batch size
                Log.WarnException(message, exception);
                autoTuner.DecreaseBatchSize();
                details.Index.AddOutOfMemoryDatabaseAlert(exception);
                return;
            }

            details.Index.TryDisable(exception, errorCount, details.FailedItemsToProcessCount);
        }

        private void FlushIndexes()
        {
            if (lastFlushedWorkCounter == workCounter || context.DoWork == false)
                return;
            lastFlushedWorkCounter = workCounter;
            FlushAllIndexes();
        }

        protected abstract void FlushAllIndexes();

        protected abstract void UpdateStalenessMetrics(int staleCount);

        protected bool ExecuteIndexing(bool isIdle, out bool onlyFoundIdleWork)
        {
            var indexesToWorkOn = new List<IndexToWorkOn>();
            var localFoundOnlyIdleWork = new Reference<bool> { Value = true };
            transactionalStorage.Batch(actions =>
            {
                foreach (var indexesStat in actions.Indexing.GetIndexesStats().Where(IsValidIndex))
                {
                    var failureRate = actions.Indexing.GetFailureRate(indexesStat.Id);
                    if (failureRate.IsInvalidIndex)
                    {
                        if (Log.IsDebugEnabled)
                        {
                            Log.Debug("Skipped indexing documents for index: {0} because failure rate is too high: {1}",
                                indexesStat.Id,
                                failureRate.FailureRate);
                        }
                        continue;
                    }
                    if (IsIndexStale(indexesStat, actions, isIdle, localFoundOnlyIdleWork) == false)
                        continue;

                    var index = context.IndexStorage.GetIndexInstance(indexesStat.Id);
                    if (index == null) // not there
                        continue;

                    if (ShouldSkipIndex(index))
                        continue;

                    if(context.IndexDefinitionStorage.GetViewGenerator(indexesStat.Id) == null)
                        continue; // an index that is in the process of being added, ignoring it, we'll check again on the next run

                    var indexToWorkOn = GetIndexToWorkOn(indexesStat);
                    indexToWorkOn.Index = index;

                    indexesToWorkOn.Add(indexToWorkOn);
                }
            });

            UpdateStalenessMetrics(indexesToWorkOn.Count);

            onlyFoundIdleWork = localFoundOnlyIdleWork.Value;
            if (indexesToWorkOn.Count == 0)
                return false;
            

            context.UpdateFoundWork();
            context.CancellationToken.ThrowIfCancellationRequested();

            using (context.IndexDefinitionStorage.CurrentlyIndexing())
            {
               ExecuteIndexingWork(indexesToWorkOn);
            }

            indexReplacer.ReplaceIndexes(indexesToWorkOn.Select(x => x.IndexId).ToList());

            return true;
        }

        protected abstract bool ShouldSkipIndex(Index index);

        public Index[] GetCurrentlyProcessingIndexes()
        {
            return currentlyProcessedIndexes.Values.ToArray();
        }

        protected abstract IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat);

        protected abstract bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork);

        protected abstract void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn);

        protected abstract bool IsValidIndex(IndexStats indexesStat);
    }
}
