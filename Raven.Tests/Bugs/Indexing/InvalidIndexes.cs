using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class InvalidIndexes : RavenTest
	{
		[Fact]
		public void CannotCreateIndexesUsingDateTimeNow()
		{
			using (var store = NewDocumentStore())
			{
				var ioe = Assert.Throws<InvalidOperationException>(() =>
				                                                   store.DatabaseCommands.PutIndex("test",
				                                                                                   new IndexDefinition
				                                                                                   {
				                                                                                   	Map =
				                                                                                   		@"from user in docs.Users 
where user.LastLogin > DateTime.Now.AddDays(-10) 
select new { user.Name}"
				                                                                                   }));

				Assert.Equal(@"Cannot use DateTime.Now during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using DateTime.Now invalidate that premise, and is not allowed", ioe.Message);
			}
		}

		[Fact]
		public void CannotCreateIndexWithOrderBy()
		{
			using(var store = NewDocumentStore())
			{
				var ioe = Assert.Throws<InvalidOperationException>(() =>
				                                                                         store.DatabaseCommands.PutIndex("test",
				                                                                                                         new IndexDefinition
				                                                                                                         {
				                                                                                                         	Map =
				                                                                                                         		"from user in docs.Users orderby user.Id select new { user.Name}"
				                                                                                                         }));

				Assert.Equal(@"OrderBy calls are not valid during map or reduce phase, but the following was found:
orderby user.Id
OrderBy calls modify the indexing output, but doesn't actually impact the order of results returned from the database.
You should be calling OrderBy on the QUERY, not on the index, if you want to specify ordering.", ioe.Message);
			}
		}
	}
}