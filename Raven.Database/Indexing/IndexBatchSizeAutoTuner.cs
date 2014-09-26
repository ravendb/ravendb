using System;
using System.Linq;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class IndexBatchSizeAutoTuner : BaseBatchSizeAutoTuner
	{
		public IndexBatchSizeAutoTuner(WorkContext context)
			: base(context)
		{
			LastAmountOfItemsToRemember = 1;
            InstallGauges();
		}

        private void InstallGauges()
        {
            var metricCounters = context.MetricsCounters;
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "InitialNumberOfItems", () => InitialNumberOfItems);
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "MaxNumberOfItems", () => MaxNumberOfItems);
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "CurrentNumberOfItems", () => CurrentNumberOfItems);
        }

		protected override int InitialNumberOfItems
		{
			get { return context.Configuration.InitialNumberOfItemsToProcessInSingleBatch; }
		}

		protected override int MaxNumberOfItems
		{
			get { return context.Configuration.MaxNumberOfItemsToProcessInSingleBatch; }
		}

		protected override int CurrentNumberOfItems
		{
			get { return context.CurrentNumberOfItemsToIndexInSingleBatch; }
			set { context.CurrentNumberOfItemsToIndexInSingleBatch = value; }
		}
		
		protected override sealed int LastAmountOfItemsToRemember { get; set; }

		private List<int> lastAmountOfItemsToIndex = new List<int>();

		protected override void RecordAmountOfItems(int numberOfItems)
		{
			var currentLastAmountOfItemsToIndex = lastAmountOfItemsToIndex;

			var amountToTake = currentLastAmountOfItemsToIndex.Count;
			
			if (amountToTake + 1 >= LastAmountOfItemsToRemember)
				amountToTake = currentLastAmountOfItemsToIndex.Count - 1;

			lastAmountOfItemsToIndex = new List<int>(currentLastAmountOfItemsToIndex.Take(amountToTake))
										{
											numberOfItems
										};
		}

		protected override IEnumerable<int> GetLastAmountOfItems()
		{
			return lastAmountOfItemsToIndex;
		}

		public Action ConsiderLimitingNumberOfItemsToProcessForThisBatch(int? maxIndexOutputsPerDoc, bool containsMapReduceIndexes)
		{
			if (maxIndexOutputsPerDoc == null || maxIndexOutputsPerDoc <= (containsMapReduceIndexes ? context.Configuration.MaxMapReduceIndexOutputsPerDocument : context.Configuration.MaxSimpleIndexOutputsPerDocument))
				return null;

			var oldValue = NumberOfItemsToProcessInSingleBatch;

			var newValue = Math.Max(NumberOfItemsToProcessInSingleBatch / (maxIndexOutputsPerDoc.Value / 2), InitialNumberOfItems);

			if (oldValue == newValue)
				return null;

			NumberOfItemsToProcessInSingleBatch = newValue;

			return () => NumberOfItemsToProcessInSingleBatch = oldValue;
		}
	}
}
