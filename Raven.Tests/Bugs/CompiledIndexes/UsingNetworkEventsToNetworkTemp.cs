using System;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Storage.Managed;
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


				var persistenceSource = ((TransactionalStorage)store.DocumentDatabase.TransactionalStorage).PersistenceSource;
				var persistentDictionaryStates = persistenceSource.DictionariesStates;

				//for (int i = 0; i < 500; i++)
				//{
				//    bool cont = false;
				//    store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				//    {
				//        var indexFailureInformation = accessor.Indexing.GetFailureRate("Aggregates/NetworkTest");

				//        cont = indexFailureInformation.ReduceSuccesses == 1;
				//    });
				//    if (cont == false)
				//        Thread.Sleep(100);
				//    else
				//        break;
				//}

				using(var s = store.OpenSession())
				{
					var list = s.Advanced.LuceneQuery<dynamic>("Aggregates/NetworkTest")
						.WaitForNonStaleResults(TimeSpan.FromSeconds(3))
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					var expected = new DateTime(2011,5,29).ToUniversalTime();
					Assert.Equal(expected, list[0].NetworkTimeStamp.UtcDateTime);
				}
			}
		}

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof (NetworkEventsToNetworkTemp)));
		}
	}
}