using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Exceptions;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class ParallelTxDelete : RavenTest
	{
		[Fact]
		public void ParallelDelete()
		{
			using (var store = NewDocumentStore())
			{
				var ids = new List<string>();

				for (int i = 0; i < 500; i++)
				{
					using (var tx = new TransactionScope(TransactionScopeOption.Required))
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						session.Advanced.UseOptimisticConcurrency = true;

						var t1 = new MyData();
						session.Store(t1);
						session.SaveChanges();
						tx.Complete();

						ids.Add(t1.Id);
					}
				}

				//Wait to ensure all documents are saved to db because of DTC being async
				using (var s = store.OpenSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					s.Load<MyData>(ids.Last());
				}

				var alreadyDeleted = new ConcurrentDictionary<string, int>();

				Parallel.ForEach(ids, id =>
				{
					using (var tx = new TransactionScope(TransactionScopeOption.Required))
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						session.Advanced.UseOptimisticConcurrency = true;

						var myData = session.Load<MyData>(id);

						if (myData == null)
						{
							alreadyDeleted.TryAdd(id, 0);

						}
						else
						{
							session.Delete(myData);
							session.SaveChanges();
						}

						tx.Complete();
					}
				});

				Assert.Empty(alreadyDeleted);
			}
		}

		public class MyData
		{
			public string Id { get; set; }
		}
	}


}