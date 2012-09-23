using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryOptimizerOnStaticIndex : RemoteClientTest
	{
		public class GameServers_ByName : AbstractIndexCreationTask<DynamicQuerySorting.GameServer>
		{
			public GameServers_ByName()
			{
				Map = gameServers => from gameServer in gameServers
									 select new { gameServer.Name };
			}
		}

		[Fact]
		public void WillSelectTheStaticField()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(GameServers_ByName))), store);

				RavenQueryStatistics stats;
				using (var session = store.OpenSession())
				{
					session.Query<DynamicQuerySorting.GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}
				Assert.Equal("GameServers/ByName", stats.IndexName);
			}
		}

	}
}