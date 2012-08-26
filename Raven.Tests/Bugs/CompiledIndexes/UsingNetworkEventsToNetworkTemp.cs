using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Database.Config;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.CompiledIndexes
{
	public class UsingNetworkEventsToNetworkTemp : RavenTest
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
						.WaitForNonStaleResults(TimeSpan.FromSeconds(3))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					var expected = new DateTime(2011,5,29).ToUniversalTime();
					Assert.Equal(expected, list[0].NetworkTimeStamp);
				}
			}
		}

		protected override void ModifyConfiguration(RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof (NetworkEventsToNetworkTemp)));
		}
	}
}
