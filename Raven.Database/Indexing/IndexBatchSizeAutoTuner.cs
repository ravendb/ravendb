using System;

namespace Raven.Database.Indexing
{
	public class IndexBatchSizeAutoTuner
	{
		private readonly WorkContext context;
		private int numberOfItemsToIndexInSingleBatch;
		protected int lastAmountOfItemsToIndex;

		public IndexBatchSizeAutoTuner(WorkContext context)
		{
			this.context = context;
			numberOfItemsToIndexInSingleBatch = context.Configuration.InitialNumberOfItemsToIndexInSingleBatch;
		}

		public int NumberOfItemsToIndexInSingleBatch
		{
			get { return numberOfItemsToIndexInSingleBatch; }
			set
			{
				context.CurrentNumberOfItemsToIndexInSingleBatch = numberOfItemsToIndexInSingleBatch = value;
			}
		}

		public void AutoThrottleBatchSize(int amountOfItemsToIndex, int size)
		{
			try
			{
				if (ConsiderDecreasingBatchSize(amountOfItemsToIndex))
					return;
				ConsiderIncreasingBatchSize(amountOfItemsToIndex, size);
			}
			finally
			{
				lastAmountOfItemsToIndex = amountOfItemsToIndex;
			}
		}

		private void ConsiderIncreasingBatchSize(int amountOfItemsToIndex, int size)
		{
			if (amountOfItemsToIndex < NumberOfItemsToIndexInSingleBatch)
			{
				return;
			}

			if (lastAmountOfItemsToIndex < NumberOfItemsToIndexInSingleBatch)
			{
				// this is the first time we hit the limit, we will give another go before we increase
				// the batch size
				return;
			}

			// in the previous run, we also hit the current limit, we need to check if we can increase the max batch size

			// here we make the assumptions that the average size of documents are the same. We check if we doubled the amount of memory
			// that we used for the last batch (note that this is only an estimate number, but should be close enough), would we still be
			// within the limits that governs us

			var sizeInMegabytes = size / 1024 / 1024;

			// we don't actually *know* what the actual cost of indexing, beause that depends on many factors (how the index
			// is structured, is it analyzed/default/not analyzed, etc). We just assume for now that it takes 10% of the actual
			// on disk structure per each active index. That should give us a good guesstimate about the value.
			var sizedPlusIndexingCost = sizeInMegabytes * (1 + (0.1 * context.IndexDefinitionStorage.MapIndexesCount));

			var availablePhysicalMemoryInMegabytes = context.Configuration.AvailablePhysicalMemoryInMegabytes;
			var remainingMemoryAfterBatchSizeIncrease = availablePhysicalMemoryInMegabytes - sizedPlusIndexingCost;

			if (remainingMemoryAfterBatchSizeIncrease >= context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
			{
				NumberOfItemsToIndexInSingleBatch = Math.Min(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch,
															 NumberOfItemsToIndexInSingleBatch * 2);
				return;
			}

			if (availablePhysicalMemoryInMegabytes >= context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
			{
				// there is enough memory available for the next indexing run
				return;
			}

			// we are using too much memory, let us use a less next time...
			// maybe it is us? we generate a lot of garbage when doing indexing, so we ask the GC if it would kindly try to
			// do something about it.
			// Note that this order for this to happen we need:
			// * We had two full run when we were doing nothing but indexing at full throttle
			// * The system is over the configured limit, and there is a strong likelihood that this is us causing this
			// * By forcing a GC, we ensure that we use less memory, and it is not frequent enough to cause perf problems

			GC.Collect(0, GCCollectionMode.Optimized);

			// let us check again after the GC call

			if (context.Configuration.AvailablePhysicalMemoryInMegabytes > context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				return;

			NumberOfItemsToIndexInSingleBatch = Math.Max(context.Configuration.InitialNumberOfItemsToIndexInSingleBatch,
														 NumberOfItemsToIndexInSingleBatch / 2);
		}

		private bool ConsiderDecreasingBatchSize(int amountOfItemsToIndex)
		{
			if (amountOfItemsToIndex >= NumberOfItemsToIndexInSingleBatch)
			{
				// we had as much work to do as we are currently capable of handling
				// there isn't nothing that we need to do here...
				return false;
			}

			// we didn't have a lot of work to do, so let us see if we can reduce the batch size

			// we are at the configured minimum, nothing to do
			if (NumberOfItemsToIndexInSingleBatch == context.Configuration.InitialNumberOfItemsToIndexInSingleBatch)
				return true;

			// we were above the max the last time, we can't reduce the work load now
			if (lastAmountOfItemsToIndex > NumberOfItemsToIndexInSingleBatch)
				return true;

			// we have had a couple of times were we didn't get to the current max, so we can probably
			// reduce the max again now, this will reduce the memory consumption eventually, and will cause 
			// faster indexing times in case we get a big batch again
			NumberOfItemsToIndexInSingleBatch = Math.Max(context.Configuration.InitialNumberOfItemsToIndexInSingleBatch,
														 NumberOfItemsToIndexInSingleBatch / 2);

			// we just reduced the batch size because we have two concurrent runs where we had
			// less to do than the previous runs. That indicate the the busy period is over, maybe we
			// run out of data? Or the rate of data entry into the system was just reduce?
			// At any rate, there is a strong likelyhood of having a lot of garbage in the system
			// let us ask the GC nicely to clean it

			GC.Collect(0, GCCollectionMode.Optimized);

			return true;
		}
	}
}