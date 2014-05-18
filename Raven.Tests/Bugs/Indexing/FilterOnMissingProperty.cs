using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Tests.Common;

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

				using(var session = store.OpenSession())
				{
					session.Store(new { Valid = true, Name = "Oren"});

					session.Store(new { Name = "Ayende "});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    session.Advanced.DocumentQuery<dynamic>("test").WaitForNonStaleResults().ToArray();
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}