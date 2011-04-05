using Raven.Client.Document;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class QueryingOnValueWithMinus : LocalClientTest
	{
		[Fact]
		public void CanQueryOnValuesContainingMinus()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new{Name = "Bruce-Lee"});
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var list = session.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Name","Bruce-Lee")
						.ToList();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}

	public class QueryingOnValueWithMinusAnalyzed : LocalClientTest
	{
		[Fact]
		public void CanQueryOnValuesContainingMinus()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs select new {doc.Name}",
													Indexes = {{"Name",FieldIndexing.Analyzed}}
				                                });
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "Bruce-Lee" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var list = session.Advanced.LuceneQuery<dynamic>("test")
						.WaitForNonStaleResults()
						.WhereEquals("Name", "Bruce-Lee")
						.ToList();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}

	public class QueryingOnValueWithMinusRemote : RemoteClientTest
	{
		[Fact]
		public void CanQueryOnValuesContainingMinus()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "Bruce-Lee" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var list = session.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Name", "Bruce-Lee")
						.ToList();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}
}