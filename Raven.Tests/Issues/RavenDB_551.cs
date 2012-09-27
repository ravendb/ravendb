using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Tests.Issues
{
	public class RavenDB_551 : RavenTest
	{
		[Fact]
		public void ManyConcurrentDeleteForSameId()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
				{
					Url = "http://localhost:8079"
				})
			{
				store.EnlistInDistributedTransactions = true;
				store.ResourceManagerId = new Guid("5402132f-32b5-423e-8b3c-b6e27c5e00fa");
				store.Initialize();

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
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
					{
						Id = Guid.NewGuid()
					};
				Assert.Throws<ConcurrencyException>(() => 
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), tx));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenModifiedInTransaction()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
				{
					Id = Guid.NewGuid()
				};
				store.DocumentDatabase.Put("items/1", null, new RavenJObject(), new RavenJObject(), tx);
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), tx));
			}
		}


		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInTransaction()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
				{
					Id = Guid.NewGuid()
				};
				store.DocumentDatabase.Delete("items/1", null, tx);
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), tx));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInTransactionUsingOldEtag()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var jsonDocument = store.DocumentDatabase.Get("items/1", null);
				store.DocumentDatabase.Delete("items/1", null, new TransactionInformation
					{
						Id = Guid.NewGuid(),
						Timeout = TimeSpan.FromSeconds(2)
					});
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Delete("items/1", jsonDocument.Etag, new TransactionInformation
						{
							Id = Guid.NewGuid()
						}));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenDeletedInAnotherTransaction()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				store.DocumentDatabase.Delete("items/1", null, new TransactionInformation
					{
						Id = Guid.NewGuid()
					});
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), new TransactionInformation
						{
							Id = Guid.NewGuid()
						}));
			}
		}

		public class Item { }
	}
}
