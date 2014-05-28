using System;
using Raven.Database.Config;
using System.Linq;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class ReduceBatchSizeAutoTuner : BaseBatchSizeAutoTuner
	{
		public ReduceBatchSizeAutoTuner(WorkContext context)
			: base(context)
		{
            InstallGauges();
		}

        private void InstallGauges()
        {
            var metricCounters = this.context.MetricsCounters;
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "InitialNumberOfItems", () => InitialNumberOfItems);
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "MaxNumberOfItems", () => MaxNumberOfItems);
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "CurrentNumberOfItems", () => CurrentNumberOfItems);
        }

		protected override int InitialNumberOfItems
		{
			get { return context.Configuration.InitialNumberOfItemsToReduceInSingleBatch; }
		}

		protected override int MaxNumberOfItems
		{
			get { return context.Configuration.MaxNumberOfItemsToReduceInSingleBatch; }
		}

		protected override int CurrentNumberOfItems
		{
			get { return context.CurrentNumberOfItemsToReduceInSingleBatch; }
			set { context.CurrentNumberOfItemsToReduceInSingleBatch = value; }
		}

		protected override int LastAmountOfItemsToRemember
		{
			get { return context.Configuration.IndexingScheduler.LastAmountOfItemsToReduceToRemember; }
			set { context.Configuration.IndexingScheduler.LastAmountOfItemsToReduceToRemember = value; }
		}

		protected override void RecordAmountOfItems(int numberOfItems)
		{
			context.Configuration.IndexingScheduler.RecordAmountOfItemsToReduce(numberOfItems);
		}

		protected override IEnumerable<int> GetLastAmountOfItems()
		{
			return context.Configuration.IndexingScheduler.GetLastAmountOfItemsToReduce();
		}
	}
}
