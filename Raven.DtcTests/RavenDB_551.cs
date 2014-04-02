using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Tests.Issues
{
	public class RavenDB_551 : RavenTest
	{
		[Fact]
		public void Non_parallel_delete_with_optimistic_concurrency_should_not_throw_concurrency_exceptions()
		{
            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
			{
				string id;
				using (var session = documentStore.OpenSession())
				{
					var data = new MyData();
					session.Store(data);
					session.SaveChanges();

					id = data.Id;
				}

				IDocumentSession session1 = null;
				IDocumentSession session2 = null;
				try
				{
					session1 = documentStore.OpenSession();
					session1.Advanced.UseOptimisticConcurrency = true;

					session2 = documentStore.OpenSession();
					session2.Advanced.UseOptimisticConcurrency = true;

					var dataFromSession1 = session1.Load<MyData>(id);
					var dataFromSession2 = session2.Load<MyData>(id);
					Assert.NotSame(dataFromSession1,dataFromSession2);

					session1.Delete(dataFromSession1);
					session2.Delete(dataFromSession2);
					
					Assert.DoesNotThrow(() => session1.SaveChanges());
					Assert.DoesNotThrow(() => session2.SaveChanges());
				}
				finally
				{
					if (session1 != null) session1.Dispose();
					if (session2 != null) session2.Dispose();
				}
			}
		}

		[Fact]
		public void ManyConcurrentDeleteForSameId()
		{
			using(var store = NewRemoteDocumentStore(requestedStorage: "esent", configureStore: documentStore =>
			{
				documentStore.EnlistInDistributedTransactions = true;
				documentStore.ResourceManagerId = new Guid("5402132f-32b5-423e-8b3c-b6e27c5e00fa");
								
			}))
			{
                if (store.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				string id;
				int concurrentExceptionsThrown = 0;
				int concurrentDeleted = 0;

				using (
					var tx = new TransactionScope(TransactionScopeOption.RequiresNew,
												  new TransactionOptions
												  {
													  Timeout = TimeSpan.FromSeconds(30),
													  IsolationLevel = IsolationLevel.ReadCommitted
												  }))
				{
					using (var session = store.OpenSession())
					{
						var t1 = new MyData();
						session.Store(t1);
						session.SaveChanges();

						id = t1.Id;
					}

					tx.Complete();
				}

				Thread.Sleep(2000); //Waiting for transaction to commit

				Parallel.For(0, 10, new ParallelOptions { MaxDegreeOfParallelism = 10 }, i =>
				{
					try
					{
						using (
							var tx = new TransactionScope(TransactionScopeOption.RequiresNew,
														  new TransactionOptions
														  {
															  Timeout = TimeSpan.FromSeconds(30),
															  IsolationLevel = IsolationLevel.ReadCommitted
														  }))
						{
							using (var session = store.OpenSession())
							{
								session.Advanced.UseOptimisticConcurrency = true;

								var myData = session.Load<MyData>(id);

								Thread.Sleep(1000);

								session.Delete(myData);
								session.SaveChanges();
							}

							tx.Complete();
						}

						Interlocked.Increment(ref concurrentDeleted);

					}
					catch (Exception)
					{
						Interlocked.Increment(ref concurrentExceptionsThrown);
					}
				});

				Assert.Equal(1, concurrentDeleted);
				Assert.Equal(9, concurrentExceptionsThrown);
			}
		}

		public class MyData
		{
			public string Id { get; set; }
		}
		[Fact]
	 	public void CanGetErrorOnOptimisticDeleteInTransaction()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
					{
                        Id = Guid.NewGuid().ToString()
					};
				Assert.Throws<ConcurrencyException>(() => 
					store.DocumentDatabase.Documents.Delete("items/1", Etag.InvalidEtag, tx));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenModifiedInTransaction()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
				{
                    Id = Guid.NewGuid().ToString()
				};
				store.DocumentDatabase.Documents.Put("items/1", null, new RavenJObject(), new RavenJObject(), tx);
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Documents.Delete("items/1", Etag.InvalidEtag, tx));
			}
		}


		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInTransaction()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
				{
                    Id = Guid.NewGuid().ToString()
				};
				store.DocumentDatabase.Documents.Delete("items/1", null, tx);
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Documents.Delete("items/1", Etag.InvalidEtag, tx));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInTransactionUsingOldEtag()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var jsonDocument = store.DocumentDatabase.Documents.Get("items/1", null);
				store.DocumentDatabase.Documents.Delete("items/1", null, new TransactionInformation
					{
                        Id = Guid.NewGuid().ToString(),
						Timeout = TimeSpan.FromSeconds(2)
					});
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Documents.Delete("items/1", jsonDocument.Etag, new TransactionInformation
						{
                            Id = Guid.NewGuid().ToString()
						}));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInAnotherTransaction()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				store.DocumentDatabase.Documents.Delete("items/1", null, new TransactionInformation
					{
                        Id = Guid.NewGuid().ToString()
					});
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Documents.Delete("items/1", Etag.InvalidEtag, new TransactionInformation
						{
                            Id = Guid.NewGuid().ToString()
						}));
			}
		}

		public class Item { }
	}
}
