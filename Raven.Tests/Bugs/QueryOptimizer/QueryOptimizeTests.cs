using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.QueryOptimizer
{
	public class QueryOptimizeTests : LocalClientTest
	{
		[Fact]
		public void WillNotError()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{

					var blogPosts = from post in session.Query<BlogPost>()
					                where post.Tags.Any(tag => tag == "RavenDB")
					                select post;


					Console.WriteLine(blogPosts);
					session.Query<User>()
						.Where(x => x.Email == "ayende@ayende.com")
						.ToList();

					session.Query<User>()
						.OrderBy(x=>x.Name)
						.ToList();
				}
			}
		}

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

	public class BlogPost
	{
		public string[] Tags { get; set; }
	}
}