namespace Raven.Database.Data
{
	public class IndexStats
	{
		public string Name { get; set; }
		public int IndexingAttempts { get; set; }
		public int IndexingSuccesses { get; set; }
		public int IndexingErrors { get; set; }
	}
}