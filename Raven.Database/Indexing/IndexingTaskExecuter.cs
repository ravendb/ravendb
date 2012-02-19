using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
{
	public class IndexingTaskExecuter
	{
		public static IIndexingTaskExecuter Instance = new DefaultIndexingTaskExecuter();
	}
}