using Raven.Abstractions.Data;

namespace Raven.Studio.Models
{
	public class IndexListItem
	{
	    public string Name { get; set; }
	}

	public class IndexGroupHeader : IndexListItem
	{
	}

	public class IndexItem : IndexListItem
	{
		public IndexStats IndexStats { get; set; }
		public string ModifiedName{get
		{
			if (IndexStats.Priority.HasFlag(IndexingPriority.Abandoned))
				return Name + " (abandoned)";

			if (IndexStats.Priority.HasFlag(IndexingPriority.Idle))
				return Name + " (idle)";

			if (IndexStats.Priority.HasFlag(IndexingPriority.Disabled))
				return Name + " (disabled)";

			return Name;
		}}
	}
}