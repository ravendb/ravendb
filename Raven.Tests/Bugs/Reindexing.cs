using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class Reindexing : RavenTest
	{
		protected override void CreateDefaultIndexes(Client.Embedded.EmbeddableDocumentStore documentStore)
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

				using(var session = store.OpenSession())
				{
					session.Store(new { Name = "oren"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				Assert.Equal(1, store.DocumentDatabase.Statistics.Indexes.First(x=>x.Name == "test").IndexingAttempts);

				store.DocumentDatabase.StopBackgroundWokers();


				store.DatabaseCommands.PutIndex("test1", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name }"
				});

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

				Assert.Equal(2, store.DocumentDatabase.Statistics.Indexes.First(x => x.Name == "test").IndexingAttempts);
			}
		}
	}
}