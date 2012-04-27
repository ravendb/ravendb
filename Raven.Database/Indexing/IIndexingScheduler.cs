using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public interface IIndexingScheduler
	{
		IList<IndexToWorkOn> FilterMapIndexes(IList<IndexToWorkOn> indexes);
		void RecordAmountOfItemsToIndex(int value);
		IEnumerable<int> GetLastAmountOfItemsToIndex();
		int LastAmountOfItemsToIndexToRemember { get; set; }
	}
}