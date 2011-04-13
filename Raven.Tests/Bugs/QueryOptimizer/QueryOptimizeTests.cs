using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.QueryOptimizer
{
	public class QueryOptimizeTests : LocalClientTest
	{
		[Fact]
		public void CanUseExistingDynamicIndex()
		{
			using(var store = NewDocumentStore())
			{
				var queryResult = store.DatabaseCommands.Query("dynamic",
				                                               new IndexQuery
				                                               {
				                                               	Query = "Name:Ayende AND Age:3"
				                                               },
				                                               new string[0]);

				Assert.Equal("Temp/AllDocs/ByAgeAndName", queryResult.IndexName);

				queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende"
															   },
															   new string[0]);

				Assert.Equal("Temp/AllDocs/ByAgeAndName", queryResult.IndexName);
			}
		}

		[Fact]
		public void CanUseExistingExistingManualIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
													Map = "from doc in docs select new { doc.Name, doc.Age }"
				                                });

				var queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende AND Age:3"
															   },
															   new string[0]);

				Assert.Equal("test", queryResult.IndexName);

				queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende"
															   },
															   new string[0]);

				Assert.Equal("test", queryResult.IndexName);
			}
		}

		[Fact]
		public void WillUseWiderIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name, doc.Age }"
												});


				store.DatabaseCommands.PutIndex("test2",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});
				var queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende AND Age:3"
															   },
															   new string[0]);

				Assert.Equal("test", queryResult.IndexName);

				queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende"
															   },
															   new string[0]);

				Assert.Equal("test", queryResult.IndexName);
			}
		}

		[Fact]
		public void WillAlwaysUseSpecifiedIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name, doc.Age }"
												});


				store.DatabaseCommands.PutIndex("test2",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});
				var queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Name:Ayende AND Age:3"
															   },
															   new string[0]);

				Assert.Equal("test", queryResult.IndexName);

				queryResult = store.DatabaseCommands.Query("test2",
															   new IndexQuery
															   {
																   Query = "Name:Ayende"
															   },
															   new string[0]);

				Assert.Equal("test2", queryResult.IndexName);
			}
		}
	}
}