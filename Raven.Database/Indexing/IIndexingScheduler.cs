using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public interface IIndexingScheduler
	{
		IList<IndexToWorkOn> FilterMapIndexes(IList<IndexToWorkOn> indexes);
		void RecordAmountOfItemsToIndex(int value);
		void RecordAmountOfItemsToReduce(int value);
		IEnumerable<int> GetLastAmountOfItemsToIndex();
		IEnumerable<int> GetLastAmountOfItemsToReduce();
		int LastAmountOfItemsToIndexToRemember { get; set; }
		int LastAmountOfItemsToReduceToRemember { get; set; }
	}
}