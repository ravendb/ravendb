using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
{
	public class BackgroundTaskExecuter
	{
		public static IBackgroundTaskExecuter Instance = new DefaultBackgroundTaskExecuter();
	}
}