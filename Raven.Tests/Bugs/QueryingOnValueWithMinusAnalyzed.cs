using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingOnValueWithMinusAnalyzed : RavenTest
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
					var list = session.Advanced.LuceneQuery<object>("test")
						.WaitForNonStaleResults()
						.WhereEquals("Name", "Bruce-Lee")
						.ToList<object>();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}
}
