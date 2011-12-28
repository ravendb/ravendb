namespace Raven.Studio.Models
{
	public class IndexListItem
	{
	}

	public class IndexGroupHeader : IndexListItem
	{
		public string Name { get; set; }
	}

	public class IndexItem : IndexListItem
	{
		public string IndexName { get; set; }
	}
}