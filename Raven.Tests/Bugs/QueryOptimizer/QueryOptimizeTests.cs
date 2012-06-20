using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;
using System.Linq;
using RavenQueryStatistics = Raven.Client.Linq.RavenQueryStatistics;

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

		[Fact]
		public void WillNotSelectExistingIndexIfFieldAnalyzedSettingsDontMatch()
		{
			//https://groups.google.com/forum/#!topic/ravendb/DYjvNjNIiho/discussion
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Title, doc.BodyText }",
													Indexes = { { "Title", FieldIndexing.Analyzed } }
												});

				var queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "Title:Matt"
															   },
															   new string[0]);

				//Because the "test" index has a field set to Analyzed (and the default is Non-Analysed), 
				//it should NOT be considered a match by the query optimiser!
				Assert.NotEqual("test", queryResult.IndexName);

				queryResult = store.DatabaseCommands.Query("dynamic",
															   new IndexQuery
															   {
																   Query = "BodyText:Matt"
															   },
															   new string[0]);
				//This query CAN use the existing index because "BodyText" is NOT set to analyzed
				Assert.Equal("test", queryResult.IndexName);
			}
		}

		public class SomeObject
		{
			public string StringField { get; set; }
			public int IntField { get; set; }
		}

		[Fact]
		public void WithRangeQuery()
		{
			using (var _documentStore = NewDocumentStore())
			{
				_documentStore.DatabaseCommands.PutIndex("SomeObjects/BasicStuff"
										 , new IndexDefinition
										 {
											 Map = "from doc in docs.SomeObjects\r\nselect new { IntField = (int)doc.IntField, StringField = doc.StringField }",
											 SortOptions = new Dictionary<string, SortOptions> { { "IntField", SortOptions.Int } },
										 });

				using (IDocumentSession session = _documentStore.OpenSession())
				{
					DateTime startedAt = DateTime.UtcNow;
					for (int i = 0; i < 40; i++)
					{
						var p = new SomeObject
						{
							IntField = i,
							StringField = "user " + i,
						};
						session.Store(p);
					}
					session.SaveChanges();
				}

				WaitForIndexing(_documentStore);

				using (IDocumentSession session = _documentStore.OpenSession())
				{
					RavenQueryStatistics stats;
					var list = session.Query<SomeObject>()
						.Statistics(out stats)
						.Where(p => p.StringField == "user 1")
						.ToList();

					Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
				}

				using (IDocumentSession session = _documentStore.OpenSession())
				{
					RavenQueryStatistics stats;
					var list = session.Query<SomeObject>()
						.Statistics(out stats)
						.Where(p => p.IntField > 150000 && p.IntField < 300000)
						.ToList();

					Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
				}

				using (IDocumentSession session = _documentStore.OpenSession())
				{
					RavenQueryStatistics stats;
					var list = session.Query<SomeObject>()
						.Statistics(out stats)
						.Where(p => p.StringField == "user 1" && p.IntField > 150000 && p.IntField < 300000)
						.ToList();

					Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
				}
			}
		}
	}

	public class BlogPost
	{
		public string[] Tags { get; set; }
		public string Title { get; set; }
		public string BodyText { get; set; }
	}
}