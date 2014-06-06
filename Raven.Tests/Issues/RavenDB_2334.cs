using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.MoreLikeThis;
using Raven.Tests.Helpers;
using Xunit;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Tests.Issues
{
	public class RavenDB_2334 : RavenTestBase
	{
	    [Fact]
	    public void ConcurrentDtcTransactions()
	    {
	        using (var store = NewDocumentStore(requestedStorage: "esent"))
	        {
	            var documentDatabase = store.DocumentDatabase;

	            var tx1 = new TransactionInformation
	            {
	                Id = "tx1",
	                Timeout =TimeSpan.FromHours(1)
	            };
	            var tx2 = new TransactionInformation
	            {
	                Id = "tx2",
	                Timeout =TimeSpan.FromHours(1)
	            };

	            documentDatabase.Put("test/1", null, new RavenJObject(), new RavenJObject(), tx1);
                documentDatabase.Put("test/2", null, new RavenJObject(), new RavenJObject(), tx1);

                documentDatabase.Put("test/3", null, new RavenJObject(), new RavenJObject(), tx2);
                documentDatabase.Put("test/4", null, new RavenJObject(), new RavenJObject(), tx2);
                
                documentDatabase.PrepareTransaction("tx1");
                documentDatabase.PrepareTransaction("tx2");

                documentDatabase.Commit("tx2");

                WaitForIndexing(documentDatabase);

                documentDatabase.Commit("tx1");

                WaitForIndexing(documentDatabase);

	            var queryResult = store.DatabaseCommands.Query("Raven/DocumentsByEntityName", new IndexQuery(), null);

                Assert.Equal(4, queryResult.TotalResults);
	        }
	    }

		[Fact]
		public void Many_concurrent_inserts_from_different_DocumentStore_within_TransactionScope_should_not_result_in_missing_documents_in_index()
		{
			const int ParallelThreadCount = 100;
			const int DocsPerThread = 10;
			const int DocumentStoreCount = 5;

			int idCounter = 0;
			using (var server = GetNewServer(requestedStorage: "esent"))
			{
				Parallel.For(0, DocumentStoreCount, storeIndex =>
				{
// ReSharper disable once AccessToDisposedClosure
					using (var store = new DocumentStore
					{
						Url = server.Database.ServerUrl,
						DefaultDatabase = "TestDB"
					})
					{
						store.Initialize();
						Parallel.For(0, ParallelThreadCount, i =>
						{
// ReSharper disable once AccessToDisposedClosure
							using (var session = store.OpenSession())
							using (var transaction = new TransactionScope(TransactionScopeOption.RequiresNew))
							{
								for (int j = 0; j < DocsPerThread; j++)
								{
									string id = "Foo/" + Interlocked.Increment(ref idCounter);
									Trace.WriteLine("PUT " + id + ", store #" + storeIndex);
									session.Store(new { Foo = "Bar" }, id);
								}
								session.SaveChanges();
								transaction.Complete();
							}
						});

						

					}
				});

				using (var store = new DocumentStore
				{
					Url = server.Database.ServerUrl,
					DefaultDatabase = "TestDB"
				})
				{
					store.Initialize();

					WaitForIndexing(store);

					using (var session = store.OpenSession())
						Assert.Equal(DocumentStoreCount * ParallelThreadCount * DocsPerThread, session.Query<dynamic>().Count());
				}
			}
		}
	}
}
