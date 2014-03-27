using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Server;
using Raven.Database.Storage;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
    public abstract class AbstractIndexingExecuter
    {
        protected WorkContext context;
        protected TaskScheduler scheduler;
        protected static readonly ILog Log = LogManager.GetCurrentClassLogger();
        protected ITransactionalStorage transactionalStorage;
        protected int workCounter;
        protected int lastFlushedWorkCounter;
        protected BaseBatchSizeAutoTuner autoTuner;
        protected ConcurrentDictionary<int, Index> currentlyProcessedIndexes = new ConcurrentDictionary<int, Index>();

        protected AbstractIndexingExecuter(WorkContext context)
        {
            this.transactionalStorage = context.TransactionalStorage;
            this.context = context;
            this.scheduler = context.TaskScheduler;
        }

        public void Execute()
        {
            using (LogContext.WithDatabase(context.DatabaseName))
            {
                Init();
                var name = GetType().Name;
                var workComment = "WORK BY " + name;
                bool isIdle = false;
                while (context.RunIndexing)
                {
                    bool foundWork;
                    try
                    {
                        bool onlyFoundIdleWork;
                        foundWork = ExecuteIndexing(isIdle, out onlyFoundIdleWork);
                        if (foundWork && onlyFoundIdleWork == false)
                            isIdle = false;

                        int runs = 32;

                        // we want to drain all of the pending tasks before the next run
                        // but we don't want to halt indexing completely
                        while (context.RunIndexing && runs-- > 0)
                        {
                            if (ExecuteTasks() == false)
                                break;
                            foundWork = true;
                        }

                    }
                    catch (OutOfMemoryException oome)
                    {
                        foundWork = true;
                        HandleOutOfMemoryException(oome);
                    }
                    catch (AggregateException ae)
                    {
                        foundWork = true;
                        var actual = ae.ExtractSingleInnerException();
                        var oome = actual as OutOfMemoryException;
                        if (oome == null)
                        {
                            if (IsEsentOutOfMemory(actual))
                            {

                                autoTuner.OutOfMemoryExceptionHappened();
                            }
                            Log.ErrorException("Failed to execute indexing", ae);
                        }
                        else
                        {
                            HandleOutOfMemoryException(oome);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Log.Info("Got rude cancellation of indexing as a result of shutdown, aborting current indexing run");
                        return;
                    }
                    catch (Exception e)
                    {
                        foundWork = true; // we want to keep on trying, anyway, not wait for the timeout or more work
#if DEBUG
                        if (Debugger.IsAttached)
                            Debugger.Break();
#endif
                        Log.ErrorException("Failed to execute indexing", e);
                        if (IsEsentOutOfMemory(e))
                        {
                            autoTuner.OutOfMemoryExceptionHappened();
                        }
                    }
                    if (foundWork == false && context.RunIndexing)
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

        private bool IsEsentOutOfMemory(Exception actual)
        {
            var esentErrorException = actual as EsentErrorException;
            if (esentErrorException == null)
                return false;
            switch (esentErrorException.Error)
            {
                case JET_err.OutOfMemory:
                case JET_err.CurrencyStackOutOfMemory:
                case JET_err.SPAvailExtCacheOutOfMemory:
                case JET_err.VersionStoreOutOfMemory:
                case JET_err.VersionStoreOutOfMemoryAndCleanupTimedOut:
                    return true;
            }
            return false;
        }

        protected virtual void Dispose() { }

        protected virtual void Init() { }

        private void HandleOutOfMemoryException(Exception oome)
        {
            Log.WarnException(
                @"Failed to execute indexing because of an out of memory exception. Will force a full GC cycle and then become more conservative with regards to memory",
                oome);

            // On the face of it, this is stupid, because OOME will not be thrown if the GC could release
            // memory, but we are actually aware that during indexing, the GC couldn't find garbage to clean,
            // but in here, we are AFTER the index was done, so there is likely to be a lot of garbage.
            RavenGC.CollectGarbage(GC.MaxGeneration);
            autoTuner.OutOfMemoryExceptionHappened();
        }

        private bool ExecuteTasks()
        {
            bool foundWork = false;
            transactionalStorage.Batch(actions =>
            {
                DatabaseTask task = GetApplicableTask(actions);
                if (task == null)
                    return;

                context.UpdateFoundWork();

                Log.Debug("Executing task: {0}", task);
                foundWork = true;

                context.CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    task.Execute(context);
                }
                catch (Exception e)
                {
                    Log.WarnException(
                        string.Format("Task {0} has failed and was deleted without completing any work", task),
                        e);
                }
            });
            return foundWork;
        }

        protected abstract DatabaseTask GetApplicableTask(IStorageActionsAccessor actions);

        private void FlushIndexes()
        {
            if (lastFlushedWorkCounter == workCounter || context.DoWork == false)
                return;
            lastFlushedWorkCounter = workCounter;
            FlushAllIndexes();
        }

        protected abstract void FlushAllIndexes();

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
                        Log.Info("Skipped indexing documents for index: {0} because failure rate is too high: {1}",
                                   indexesStat.Id,
                                       failureRate.FailureRate);
                        continue;
                    }
                    if (IsIndexStale(indexesStat, actions, isIdle, localFoundOnlyIdleWork) == false)
                        continue;
                    var indexToWorkOn = GetIndexToWorkOn(indexesStat);
                    var index = context.IndexStorage.GetIndexInstance(indexesStat.Id);
                    if (index == null) // not there
                        continue;

                    indexToWorkOn.Index = index;
                    indexesToWorkOn.Add(indexToWorkOn);
                }
            });
            onlyFoundIdleWork = localFoundOnlyIdleWork.Value;
            if (indexesToWorkOn.Count == 0)
                return false;

            context.UpdateFoundWork();
            context.CancellationToken.ThrowIfCancellationRequested();

            using (context.IndexDefinitionStorage.CurrentlyIndexing())
            {
               ExecuteIndexingWork(indexesToWorkOn);
            }

            return true;
        }

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
