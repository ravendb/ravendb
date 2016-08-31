using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Raven.Abstractions;
using Raven.Database.Config;
using System.Linq;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Storage;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
    using Raven.Abstractions.Util;

    public abstract class BaseBatchSizeAutoTuner : ILowMemoryHandler, ITransactionalStorageNotificationHandler
    {
        protected readonly WorkContext context;

        private int currentNumber;

        private DateTime lastIncrease;
        private readonly ConcurrentDictionary<Guid, long> _currentlyUsedBatchSizesInBytes;

        private readonly int maximumSizeAllowedToFetchFromStorageInMb;

        protected BaseBatchSizeAutoTuner(WorkContext context)
        {
            this.context = context;
            FetchingDocumentsFromDiskTimeout = TimeSpan.FromSeconds(context.Configuration.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds);
            maximumSizeAllowedToFetchFromStorageInMb = context.Configuration.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb;
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            NumberOfItemsToProcessInSingleBatch = InitialNumberOfItems;
            MemoryStatistics.RegisterLowMemoryHandler(this);
            context.TransactionalStorage.RegisterTransactionalStorageNotificationHandler(this);
            _currentlyUsedBatchSizesInBytes = new ConcurrentDictionary<Guid, long>();
        }


        public LowMemoryHandlerStatistics HandleLowMemory()
        {
            var prev = NumberOfItemsToProcessInSingleBatch;
            NumberOfItemsToProcessInSingleBatch = CalculateReductionOfItemsInSingleBatch();
            if (prev != NumberOfItemsToProcessInSingleBatch)
            {
                return new LowMemoryHandlerStatistics
                {
                    DatabaseName = context.DatabaseName,
                    Summary = $"Reduced batch size from {prev:#,#} to {NumberOfItemsToProcessInSingleBatch:#,#}"
                }; 
            }
            return new LowMemoryHandlerStatistics();
        }


        public void HandleTransactionalStorageNotification()
        {
            HandleLowMemory();
        }

        public virtual LowMemoryHandlerStatistics GetStats()
        {
            return new LowMemoryHandlerStatistics
            {
                EstimatedUsedMemory = 0,
                Name = Name,
                DatabaseName = context.DatabaseName,
                Metadata = new
                {
                    BatchSize = NumberOfItemsToProcessInSingleBatch
                }
            };
        }

        public int NumberOfItemsToProcessInSingleBatch
        {
            get { return currentNumber; }
            set
            {
                CurrentNumberOfItems = currentNumber = value;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AutoThrottleBatchSize(int amountOfItemsToProcess, long size, TimeSpan processingDuration, bool skipIncreasingBatchSize = false)
        {
            try
            {
                if (ReduceBatchSizeIfCloseToMemoryCeiling())
                    return;
                if (ConsiderDecreasingBatchSize(amountOfItemsToProcess, processingDuration))
                    return;

                if (skipIncreasingBatchSize)
                    return;

                if (ConsiderIncreasingBatchSize(amountOfItemsToProcess, size, processingDuration))
                {
                    lastIncrease = SystemTime.UtcNow;
                }

            }
            finally
            {
                RecordAmountOfItems(amountOfItemsToProcess);
            }
        }

        private bool ConsiderIncreasingBatchSize(int amountOfItemsToProcess, long size, TimeSpan processingDuration)
        {
            //if we are using too much memory for indexing, do not even consider to increase batch size
            if (amountOfItemsToProcess < NumberOfItemsToProcessInSingleBatch || CurrentlyUsedBatchSizesInBytes.Values.Sum() > context.Configuration.DynamicMemoryLimitForProcessing)
            {
                return false;
            }

            if (GetLastAmountOfItems().Any(x => x < NumberOfItemsToProcessInSingleBatch))
            {
                // this is the first time we hit the limit, we will give another go before we increase
                // the batch size
                return false;
            }

            // in the previous run, we also hit the current limit, we need to check if we can increase the max batch size

            // here we make the assumptions that the average size of documents are the same. We check if we doubled the amount of memory
            // that we used for the last batch (note that this is only an estimate number, but should be close enough), would we still be
            // within the limits that governs us

            var sizeInMegabytes = size / 1024 / 1024;

            // we don't actually *know* what the actual cost of indexing, because that depends on many factors (how the index
            // is structured, is it analyzed/default/not analyzed, etc). We just assume for now that it takes 25% of the actual
            // on disk structure per each active index. That should give us a good guesstimate about the value.
            // Because of the way we are executing indexes, only N are running at once, where N is the parallel level, so we take
            // that into account, you may have 10 indexes but only 2 CPUs, so we only consider the cost of executing 2 indexes,
            // not all 10
            var sizedPlusIndexingCost = sizeInMegabytes * (1 + (0.25 * Math.Min(context.IndexDefinitionStorage.IndexesCount, context.Configuration.MaxNumberOfParallelProcessingTasks)));

            var remainingMemoryAfterBatchSizeIncrease = MemoryStatistics.AvailableMemoryInMb - sizedPlusIndexingCost;

            if (remainingMemoryAfterBatchSizeIncrease < context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
                return false;

            // here we assume that the next batch would be 175% as long as the current one
            // and there is no point in trying if we are just going to blow out past our max latency
            var timeSpan = processingDuration.Add(TimeSpan.FromMilliseconds(processingDuration.TotalMilliseconds * 0.75));
            if (timeSpan > context.Configuration.MaxProcessingRunLatency)
                return false;

            // We want to aggressively increase the size while it is small, but once we hit a certain
            // size, we don't want to just double it. It is easy to create major issues that way.
            // as long as we are less than 25% of the max size, we can double it until we hit that limit
            int maybeNewSize = NumberOfItemsToProcessInSingleBatch * 2;
            // if we are more than 25% of the size, we want to grow more slowly, we'll grow my 1/8 of the 
            // max number of items
            //
            // For the details, that means that we'll double the sizes until we get to 32K items, then increase
            // it by 16K each time we need to increase. That is much more gradual and will let us more time to 
            // see if we don't want to increase things.
            if (maybeNewSize > MaxNumberOfItems / 4)
            {
                var stepSize = MaxNumberOfItems / 8;
                if (stepSize < InitialNumberOfItems)
                    stepSize = InitialNumberOfItems;
                maybeNewSize = NumberOfItemsToProcessInSingleBatch + stepSize;
            }


            NumberOfItemsToProcessInSingleBatch = Math.Min(MaxNumberOfItems,
                                                         maybeNewSize);
            return true;
        }

        public TimeSpan FetchingDocumentsFromDiskTimeout { get; private set; }

        public long MaximumSizeAllowedToFetchFromStorageInBytes
        {
            get
            {
                // we take just a bit more to account for indexing costs as well
                var sizeToKeepFree = context.Configuration.AvailableMemoryForRaisingBatchSizeLimit * 1.33;
                // if we just loaded > 256 MB to index, that is big enough for right now
                // remember, this value refer to just the data on disk, not including
                // the memory to do the actual indexing
                double sizeInMb = Math.Min(maximumSizeAllowedToFetchFromStorageInMb, Math.Max(8, MemoryStatistics.AvailableMemoryInMb - sizeToKeepFree));
                return (long)sizeInMb * 1024 * 1024;
            }
        }

        public bool IsProcessingUsingTooMuchMemory
        {
            get
            {
                return _currentlyUsedBatchSizesInBytes.Values.Sum() * 4 > context.Configuration.DynamicMemoryLimitForProcessing;
            }
        }

        private bool ReduceBatchSizeIfCloseToMemoryCeiling(bool forceReducing = false)
        {
            if (MemoryStatistics.AvailableMemoryInMb >= context.Configuration.AvailableMemoryForRaisingBatchSizeLimit && forceReducing == false &&
                IsProcessingUsingTooMuchMemory == false)
            {
                // there is enough memory available for the next indexing run
                return false;
            }

            // we are using too much memory, let us use a less next time...
            // maybe it is us? we generate a lot of garbage when doing indexing, so we ask the GC if it would kindly try to
            // do something about it.
            // Note that this order for this to happen we need:
            // * We had two full run when we were doing nothing but indexing at full throttle
            // * The system is over the configured limit, and there is a strong likelihood that this is us causing this
            // * By forcing a GC, we ensure that we use less memory, and it is not frequent enough to cause perf problems

            RavenGC.ConsiderRunningGC();

            // let us check again after the GC call, do we still need to reduce the batch size?

            if (MemoryStatistics.AvailableMemoryInMb > context.Configuration.AvailableMemoryForRaisingBatchSizeLimit && forceReducing == false)
            {
                // we don't want to try increasing things, we just hit the ceiling, maybe on the next try
                return true;
            }

            // we are still too high, let us reduce the size and see what is going on.
            var prev = NumberOfItemsToProcessInSingleBatch;
            NumberOfItemsToProcessInSingleBatch = CalculateReductionOfItemsInSingleBatch();

            var reason = string.Format("Batch size is close to memory ceiling, NumberOfItemsToProcessInSingleBatch was reduced from {0:##,###;;0} to {1:##,###;;0}", prev, NumberOfItemsToProcessInSingleBatch);
            context.Database.AutoTuningTrace.Enqueue(new AutoTunerDecisionDescription(Name, context.DatabaseName, reason));
            return true;
        }

        private bool ConsiderDecreasingBatchSize(int amountOfItemsToProcess, TimeSpan processingDuration)
        {
            var isIndexingUsingTooMuchMemory = IsProcessingUsingTooMuchMemory;
            if (isIndexingUsingTooMuchMemory == false) //skip all other heuristics if indexing takes too much memory
            {
                if (
                    // we had as much work to do as we are currently capable of handling,
                    // we might need to increase, but certainly not decrease the batch size
                    amountOfItemsToProcess >= NumberOfItemsToProcessInSingleBatch ||
                    // we haven't gone over the max latency limit, no reason to decrease yet
                    processingDuration < context.Configuration.MaxProcessingRunLatency)
                {
                    return false;
                }

                if ((SystemTime.UtcNow - lastIncrease).TotalMinutes < 3)
                    return true;

                // we didn't have a lot of work to do, so let us see if we can reduce the batch size

                // we are at the configured minimum, nothing to do
                if (NumberOfItemsToProcessInSingleBatch == InitialNumberOfItems)
                {
                    return true;
                }


                // we were above the max/2 the last few times, we can't reduce the work load now
                if (GetLastAmountOfItems().Any(x => x > NumberOfItemsToProcessInSingleBatch / 2))
                {
                    return true;
                }

            }

            var old = NumberOfItemsToProcessInSingleBatch;
            NumberOfItemsToProcessInSingleBatch = CalculateReductionOfItemsInSingleBatch();

            // we just reduced the batch size because we have two concurrent runs where we had
            // less to do than the previous runs. That indicate the the busy period is over, maybe we
            // run out of data? Or the rate of data entry into the system was just reduce?
            // At any rate, there is a strong likelihood of having a lot of garbage in the system
            // let us ask the GC nicely to clean it

            // but we only want to do it if the change was significant 
            if (NumberOfItemsToProcessInSingleBatch - old > 4096)
            {
                RavenGC.ConsiderRunningGC();
            }

            return true;
        }

        /// <summary>
        /// This let us know that an esent/voron out of memory exception has happened.
        /// We need to decrease the batch size.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DecreaseBatchSize()
        {
            var prev = NumberOfItemsToProcessInSingleBatch;
            NumberOfItemsToProcessInSingleBatch = CalculateReductionOfItemsInSingleBatch();
            var reason = $"Number of items per batch was reset from {prev} to {NumberOfItemsToProcessInSingleBatch} due to esent/voron OOME";
            context.Database.AutoTuningTrace.Enqueue(new AutoTunerDecisionDescription(Name, context.DatabaseName, reason));
        }

        private int CalculateReductionOfItemsInSingleBatch()
        {
            var minNumberOfItemsToProcess = InitialNumberOfItems;
            if (IsProcessingUsingTooMuchMemory)
            {
                minNumberOfItemsToProcess /= 4;
            }

            // we have had a couple of times were we didn't get to the current max, so we can probably
            // reduce the max again now, this will reduce the memory consumption eventually, and will cause 
            // faster indexing times in case we get a big batch again
            // * if indexing is using too much memory --> probably we have very large documents in the index, so let it reduce to 1 if needed
            return Math.Max(minNumberOfItemsToProcess, NumberOfItemsToProcessInSingleBatch / 2);
        }

        /// <summary>
        /// This let us know that an OOME has happened, and we need to be much more
        /// conservative with regards to how fast we can grow memory.
        /// </summary>
        public void HandleOutOfMemory()
        {
            var newNumberOfItemsToProcess = Math.Min(InitialNumberOfItems, NumberOfItemsToProcessInSingleBatch);
            if (IsProcessingUsingTooMuchMemory) //if using too much memory, decrease number of items in each batch
            {
                var prevNum = newNumberOfItemsToProcess;
                newNumberOfItemsToProcess /= 4;
                var reason = $"Using too much memory, NumberOfItemsToProcess was decreased from {prevNum} to {newNumberOfItemsToProcess}";
                context.Database.AutoTuningTrace.Enqueue(new AutoTunerDecisionDescription(Name, context.DatabaseName, reason));
            }
            else
            {
                var preNum = newNumberOfItemsToProcess;
                newNumberOfItemsToProcess /= 2; // we hit OOME so we should rapidly decrease batch size even when process is not using too much memory
                var reason = $"we hit OOME so we should decrease batch size even when process is not using too much memory. NumberOfItemsToProcess was decreased from {preNum} to {newNumberOfItemsToProcess}";
                context.Database.AutoTuningTrace.Enqueue(new AutoTunerDecisionDescription(Name, context.DatabaseName, reason));
            }

            // first thing to do, reset the number of items per batch
            var prev = NumberOfItemsToProcessInSingleBatch;
            NumberOfItemsToProcessInSingleBatch = newNumberOfItemsToProcess > 0 ? newNumberOfItemsToProcess : 1;
            var res = $"Number of items per batch was reset from {prev} to {NumberOfItemsToProcessInSingleBatch}";
            context.Database.AutoTuningTrace.Enqueue(new AutoTunerDecisionDescription(Name, context.DatabaseName, res));
            // now, we need to be more conservative about how we are increasing memory usage, so instead of increasing
            // every time we hit the limit twice, we will increase every time we hit it three times, then 5, 9, etc

            LastAmountOfItemsToRemember *= 2;
        }

        // The following methods and properties are wrappers around members of the context which are different for the different indexes
        public abstract int InitialNumberOfItems { get; }
        protected abstract int MaxNumberOfItems { get; }
        protected abstract int CurrentNumberOfItems { get; set; }
        protected abstract int LastAmountOfItemsToRemember { get; set; }
        public ConcurrentDictionary<Guid, long> CurrentlyUsedBatchSizesInBytes { get { return _currentlyUsedBatchSizesInBytes; } }
        protected abstract void RecordAmountOfItems(int numberOfItems);
        protected abstract IEnumerable<int> GetLastAmountOfItems();
        protected abstract string Name { get; }
    }
}
