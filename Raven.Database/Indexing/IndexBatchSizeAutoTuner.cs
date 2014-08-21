using System;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using System.Linq;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class IndexBatchSizeAutoTuner : BaseBatchSizeAutoTuner
	{
		public IndexBatchSizeAutoTuner(WorkContext context)
			: base(context)
		{
            this.InstallGauges();
		}

        private void InstallGauges()
        {
            var metricCounters = this.context.MetricsCounters;
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

		protected override int LastAmountOfItemsToRemember
		{
			get { return context.Configuration.IndexingScheduler.LastAmountOfItemsToIndexToRemember; }
			set { context.Configuration.IndexingScheduler.LastAmountOfItemsToIndexToRemember = value; }
		}

		protected override void RecordAmountOfItems(int numberOfItems)
		{
			context.Configuration.IndexingScheduler.RecordAmountOfItemsToIndex(numberOfItems);
		}

		protected override IEnumerable<int> GetLastAmountOfItems()
		{
			return context.Configuration.IndexingScheduler.GetLastAmountOfItemsToIndex();
		}

		public Action ConsiderLimitingNumberOfItemsToProcessForThisBatch(int? maxIndexOutputsPerDoc)
		{
			if (maxIndexOutputsPerDoc == null || maxIndexOutputsPerDoc <= context.Configuration.MaxIndexOutputsPerDocument)
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
