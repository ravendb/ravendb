using System;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class IndependentBatchSizeAutoTuner : BaseBatchSizeAutoTuner
	{
		public IndependentBatchSizeAutoTuner(WorkContext context) : base(context)
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

		protected override int CurrentNumberOfItems { get; set; }
		protected override int LastAmountOfItemsToRemember { get; set; }

		private int lastAmount;

		protected override void RecordAmountOfItems(int numberOfItems)
		{
			lastAmount = numberOfItems;
		}

		protected override IEnumerable<int> GetLastAmountOfItems()
		{
			yield return lastAmount;
		}
	}
}