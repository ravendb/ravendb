using System.Threading;
using System.Transactions;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Track
{
	public class RavenDB17 : RavenTest
	{
		[Fact]
		public void CacheRespectInFlightTransaction()
		{
            using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
                if(store.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				// Session #1
				using (var scope = new TransactionScope())
				using (var session = store.OpenSession())
				{
					Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
					                                  new DummyEnlistmentNotification(),
					                                  EnlistmentOptions.None);

					session.Advanced.UseOptimisticConcurrency = true;
					session.Advanced.AllowNonAuthoritativeInformation = false;

					session.Store(new SomeDocument {Id = 1, Data = "Data1"});

					session.SaveChanges();
					scope.Complete();
				}

				// Session #2
				using (var scope = new TransactionScope())
				using (var session = store.OpenSession())
				{
					Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
					                                  new DummyEnlistmentNotification(),
					                                  EnlistmentOptions.None);

					session.Advanced.UseOptimisticConcurrency = true;
					session.Advanced.AllowNonAuthoritativeInformation = false;

					var doc = session.Load<SomeDocument>(1);
					Assert.Equal("Data1", doc.Data);
					
					doc.Data = "Data2";

					session.SaveChanges();
					scope.Complete();
				}

				Thread.Sleep(1000); // wait a bit here because a commit operation is done in async manner

				// Session #3
				using (var scope = new TransactionScope())
				using (var session = store.OpenSession())
				{
					Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
					                                  new DummyEnlistmentNotification(),
					                                  EnlistmentOptions.None);

					session.Advanced.UseOptimisticConcurrency = true;
					session.Advanced.AllowNonAuthoritativeInformation = false;

					var doc = session.Load<SomeDocument>(1);
					Assert.Equal("Data2", doc.Data);

					session.SaveChanges();
					scope.Complete();
				}
			}
		}

		public class SomeDocument
		{
			public string Data { get; set; }
			public int Id { get; set; }
		}
	}
}