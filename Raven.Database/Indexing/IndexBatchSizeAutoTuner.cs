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
			var lastTime = lastAmountOfItemsToIndex;

			lastAmountOfItemsToIndex = amountOfItemsToIndex;
			if (amountOfItemsToIndex < NumberOfItemsToIndexInSingleBatch) // we didn't have a lot of work to do
			{
				// we are at the configured default, nothing to do
				if (NumberOfItemsToIndexInSingleBatch == context.Configuration.InitialNumberOfItemsToIndexInSingleBatch)
					return;

				// we were above the max the last time, we can't reduce the work load now
				if (lastTime > NumberOfItemsToIndexInSingleBatch)
					return;

				// we have had a couple of times were we didn't get to the current max, so we can probably
				// reduce the max again now.

				NumberOfItemsToIndexInSingleBatch = Math.Max(context.Configuration.InitialNumberOfItemsToIndexInSingleBatch,
																NumberOfItemsToIndexInSingleBatch / 2);
			}
			// in the previous run, we also hit the current limit, we need to check if we can increase the max batch size
			else if (lastTime >= NumberOfItemsToIndexInSingleBatch)
			{
				// here we make the assumptions that the average size of documents are the same. We check if we doubled the amount of memory
				// that we used for the last batch (note that this is only an estimate number, but should be close enough), would we still be
				// within the limits that governs us

				var sizeInMegabytes = size / 1024 / 1024;
				var availablePhysicalMemoryInMegabytes = context.Configuration.AvailablePhysicalMemoryInMegabytes;
				var remainingMemoryAfterBatchSizeIncrease = availablePhysicalMemoryInMegabytes - sizeInMegabytes;
				if (remainingMemoryAfterBatchSizeIncrease >= context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				{
					NumberOfItemsToIndexInSingleBatch = Math.Min(context.Configuration.MaxNumberOfItemsToIndexInSingleBatch,
																 NumberOfItemsToIndexInSingleBatch * 2);
				}
				// we are using too much memory, let us use a little less next time...
				if (availablePhysicalMemoryInMegabytes < context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				{
					// maybe it is us? we generate a lot of garbage when doing indexing, so we ask the GC if it would kindly try to do something
					// about it.
					// Note that this order for this to happen we need:
					// * We had two full run when we were doing nothing but indexing at full throttle
					// * The system is over the configured limit, and there is a strong likelihood that this is us
					// * By forcing a GC, we ensure that we use less memory, and it is not frequent enough to cause perf problems

					GC.Collect(0, GCCollectionMode.Optimized);

					// let us check again after the GC call

					if (context.Configuration.AvailablePhysicalMemoryInMegabytes > context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
						return;

					NumberOfItemsToIndexInSingleBatch = Math.Max(context.Configuration.InitialNumberOfItemsToIndexInSingleBatch,
															NumberOfItemsToIndexInSingleBatch / 2);
				}
			}
		}
	}
}