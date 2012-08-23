using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class FilterOnMissingProperty : RavenTest
	{
		[Fact]
		public void CanFilter()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                	{
				                                		Map = "from doc in docs where doc.Valid select new { doc.Name }"
				                                	});

				using(var sesion = store.OpenSession())
				{
					sesion.Store(new { Valid = true, Name = "Oren"});

					sesion.Store(new { Name = "Ayende "});

					sesion.SaveChanges();
				}

				using (var sesion = store.OpenSession())
				{
					sesion.Advanced.LuceneQuery<dynamic>("test").WaitForNonStaleResults().ToArray();
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}