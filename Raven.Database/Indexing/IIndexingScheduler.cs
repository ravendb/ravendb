using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public interface IIndexingScheduler
	{
		IList<IndexToWorkOn> FilterMapIndexes(IList<IndexToWorkOn> indexes);
		int LastAmountOfItemsToIndex { get; set; }
	}
}