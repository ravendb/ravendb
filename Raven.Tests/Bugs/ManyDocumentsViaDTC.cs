using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ManyDocumentsViaDTC : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.MaxPageSize = 10000;
		}

		[Fact]
		public void WouldBeIndexedProperly()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					// Create the temp index before we populate the db.
					session.Query<TestDocument>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Count();
				}

				var tasks = new List<Task>();
				const int expectedCount = 5000;
				for (int i = 0; i < expectedCount; i++)
				{
					tasks.Add(Task.Factory.StartNew(() =>
					{
						using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew))
						{
							// Promote the transaction

							System.Transactions.Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id, new DummyEnlistmentNotification(), EnlistmentOptions.None);

							using (var session = store.OpenSession())
							{
								session.Store(new TestDocument());
								session.SaveChanges();
							}

							scope.Complete();
						}
					}));
				}
				Task.WaitAll(tasks.ToArray());

				using (var session = store.OpenSession())
				{
					var items = session.Query<TestDocument>()
						.Customize(x => x.WaitForNonStaleResults())
						.Take(5005)
						.ToList();

					var missing = new List<int>();
					for (int i = 0; i < 5000; i++)
					{
						if(items.Any(x=>x.Id == i+1) == false)
							missing.Add(i);
					}


					Assert.Equal(expectedCount, items.Count);
				}
			}
		}

		public class DummyEnlistmentNotification : IEnlistmentNotification
		{
			public static readonly Guid Id = Guid.NewGuid();

		    public bool WasCommitted { get; set; }
			public void Prepare(PreparingEnlistment preparingEnlistment)
			{
				preparingEnlistment.Prepared();
			}

			public void Commit(Enlistment enlistment)
			{
			    WasCommitted = true;
				enlistment.Done();
			}

			public void Rollback(Enlistment enlistment)
			{
				enlistment.Done();
			}

			public void InDoubt(Enlistment enlistment)
			{
				enlistment.Done();
			}
		}

		public class TestDocument
		{
			public int Id { get; set; }
		}
	}
}
