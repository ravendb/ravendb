using System;
using System.ComponentModel.Composition.Hosting;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.CompiledIndexes
{
	public class UsingNetworkEventsToNetworkTemp : LocalClientTest
	{
		[Fact]
		public void CanGetGoodResults()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new NetworkList
					{
						Network = "abc",
					});
					s.SaveChanges();
				}


				using(var s = store.OpenSession())
				{
					var list = s.Advanced.LuceneQuery<dynamic>("Aggregates/NetworkTest")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(@"\/Date(1306623600000)\/", list[0].NetworkTimeStamp);
				}
			}
		}

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof (NetworkEventsToNetworkTemp)));
		}
	}
}