using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	using Xunit.Extensions;

	public class ManyDocumentBeingIndexed : RavenTest
	{
		public class TestDocument
		{
			public int Id { get; set; }
		}

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.MaxPageSize = 10000;
		}

		[Theory]
		[InlineData("esent")]
		[InlineData("voron")]
		public void WouldBeIndexedProperly(string requestedStorage)
		{
			using (var store = NewDocumentStore(requestedStorage: requestedStorage))
			{
				using (var session = store.OpenSession())
				{
					// Create the temp index before we populate the db.
					session.Query<TestDocument>()
					       .Customize(x => x.WaitForNonStaleResultsAsOfNow())
					       .Count();
				}

				const int expectedCount = 5000;
				var ids = new ConcurrentQueue<string>();
				for (int i = 0; i < expectedCount; i++)
				{
					{
						using (var session = store.OpenSession())
						{
							var testDocument = new TestDocument();
							session.Store(testDocument);
							ids.Enqueue(session.Advanced.GetDocumentId(testDocument));
							session.SaveChanges();
						}
					}
				}
				
				using (var session = store.OpenSession())
				{
					var items = session.Query<TestDocument>()
					                   .Customize(x => x.WaitForNonStaleResults())
					                   .Take(5005)
					                   .ToList();

					var missing = new List<int>();
					for (int i = 0; i < 5000; i++)
					{
						if (items.Any(x => x.Id == i + 1) == false)
							missing.Add(i);
					}

					WaitForUserToContinueTheTest(store);
					Assert.Equal(expectedCount, items.Count);
				}
			}
		}
	}
}