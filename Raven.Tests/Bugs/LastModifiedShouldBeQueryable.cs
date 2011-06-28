using System;
using System.Linq;
using Raven.Abstractions.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class LastModifiedShouldBeQueryable : LocalClientTest
	{
		[Fact]
		public void SelectFieldsFromIndex()
		{
			using(var store = NewDocumentStore())
			{
				new RavenDocumentsByEntityName().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "John Doe"} );
					session.SaveChanges();

					var dateTime = DateTools.DateToString(DateTime.Now, DateTools.Resolution.SECOND);

					var results = session.Advanced.LuceneQuery<object>(new RavenDocumentsByEntityName().IndexName)
						.Where("LastModified:[* TO " + dateTime + "]")
						.WaitForNonStaleResults()
						.ToArray();

					Assert.NotEqual(0, results.Count());
				}
			}
		}
	}
}