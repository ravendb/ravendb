using System;
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
		}

		protected override int InitialNumberOfItems
		{
			get { return context.Configuration.InitialNumberOfItemsToIndexInSingleBatch; }
		}

		protected override int MaxNumberOfItems
		{
			get { return context.Configuration.MaxNumberOfItemsToIndexInSingleBatch; }
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
	}
}
