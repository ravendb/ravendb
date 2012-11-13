using System;
using Raven.Abstractions.Indexing;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB651 : RavenTest
	{
		[Fact]
		public void CanWorkWithDateTimeOffset()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = @"from doc in docs select new { Time = doc[""@metadata""].time }"
				});

				using(var session = store.OpenSession())
				{
					var entity = new User();
					session.Store(entity);
					session.Advanced.GetMetadataFor(entity)["time"] = new DateTimeOffset(2012, 11, 08, 11, 20, 0, TimeSpan.FromHours(2));
					session.SaveChanges();
				}

				WaitForIndexing(store);
				WaitForUserToContinueTheTest(store);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}