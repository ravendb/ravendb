using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

using Raven.Tests.Common;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.SlowTests.Bugs
{
	public class ManyDocumentsViaDTC : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.MaxPageSize = 10000;
		}

		[Fact]
		public void WouldBeIndexedProperly()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);

				using (var session = store.OpenSession())
				{
					// Create the temp index before we populate the db.
					session.Query<TestDocument>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Count();
				}

				var tasks = new List<Task>();
				const int expectedCount = 5000;
				var ids = new ConcurrentQueue<string>();
				for (int i = 0; i < expectedCount; i++)
				{
					tasks.Add(Task.Factory.StartNew(() =>
					{
						using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew))
						{
							// Promote the transaction

							Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id, new DummyEnlistmentNotification(), EnlistmentOptions.None);

							using (var session = store.OpenSession())
							{
								var testDocument = new TestDocument();
								session.Store(testDocument);
								ids.Enqueue(session.Advanced.GetDocumentId(testDocument));
								session.SaveChanges();
							}

							scope.Complete();
						}
					}));
				}
				Task.WaitAll(tasks.ToArray());
				foreach (var id in ids)
				{
					using(var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						Assert.NotNull(session.Load<TestDocument>(id));
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

					Assert.Equal(expectedCount, items.Count);
				}
			}
		}


		public class TestDocument
		{
			public int Id { get; set; }
		}
	}
}
