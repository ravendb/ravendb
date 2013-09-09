using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class Reindexing : RavenTest
	{
		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			
		}

		[Fact]
		public void ShouldNotReindexAlreadyIndexedDocs()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }"
				});
			    var test = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("test").IndexId;


				using(var session = store.OpenSession())
				{
					session.Store(new { Name = "oren"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x=>x.Id == test).IndexingAttempts);

				store.DocumentDatabase.StopBackgroundWorkers();

				store.DatabaseCommands.PutIndex("test1", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }"
				});
			    var test1 = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("test1").IndexId;

				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "ayende" });
					session.SaveChanges();
				}

				store.DocumentDatabase.SpinBackgroundWorkers();

				using (var session = store.OpenSession())
				{
					session.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(2, store.DocumentDatabase.Statistics.Indexes.First(x => x.Id == test1).IndexingAttempts);
			}
		}


		[Fact]
		public void IndexOnUsersShouldNotIndexPosts()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Users select new { doc.Name }"
				});
			    var test = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("test").IndexId;

				using (var session = store.OpenSession())
				{
					session.Store(new User() { Name = "oren" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x => x.Id == test).IndexingAttempts);

				using (var session = store.OpenSession())
				{
					session.Store(new Patching.Post { });
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test2", new IndexDefinition
				{
					Map = "from doc in docs.Users select new { doc.Name }"
				});
			    var test2 = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("test2").IndexId;

				using (var session = store.OpenSession())
				{
					session.Query<object>("test2").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x => x.Id == test2).IndexingAttempts);


				using (var session = store.OpenSession())
				{
					session.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x => x.Id == test).IndexingAttempts);

				using (var session = store.OpenSession())
				{
					session.Query<object>("test2").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x => x.Id == test2).IndexingAttempts);
			}
		}
	}
}