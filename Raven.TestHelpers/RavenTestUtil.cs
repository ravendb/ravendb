using System.Threading;
using Raven.Client;

namespace Raven.TestHelpers
{
	public static class RavenTestUtil
	{
		public static void WaitForIndexing(IDocumentStore store)
		{
			while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}
	}
}