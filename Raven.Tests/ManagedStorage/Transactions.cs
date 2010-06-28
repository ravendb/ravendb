using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Transactions : TxStorageTest
	{
		[Fact]
		public void AddingDocInTxCannotBeReadOutside()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				tx.Read(viewer => 
					Assert.Null(viewer.Documents.DocumentByKey("Ayende", null)));
			}
		}

		[Fact]
		public void CanModifyTxId()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				var txInfo2 = new TransactionInformation
				{
					Id = Guid.NewGuid(),
					Timeout = TimeSpan.FromDays(1)
				};

				tx.Write(mutator => mutator.Transactions.ModifyTransactionId(transactionInformation.Id, txInfo2.Id, txInfo2.Timeout));


				tx.Read(viewer =>
					Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", txInfo2)));
			}
		}

		[Fact]
		public void AfterCommittingCanSeeChangesWithoutTx()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				tx.Write(mutator => mutator.Transactions.CompleteTransaction(transactionInformation.Id, data =>
				{
					if (data.Delete)
					{
						JObject metadata;
						mutator.Documents.DeleteDocument(data.Key, null, out metadata);
					}
					else
						mutator.Documents.AddDocument(data.Key, null, data.Data, data.Metadata);
				}));
				tx.Read(viewer =>
					Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", null)));
			}
		}

		[Fact]
		public void AfterRollbackCannotSeeChangesEvenInSameTxId()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				tx.Read(viewer =>
					Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", transactionInformation)));
			
				tx.Write(mutator => mutator.Transactions.RollbackTransaction(transactionInformation.Id));

				tx.Read(viewer =>
					Assert.Null(viewer.Documents.DocumentByKey("Ayende", transactionInformation)));
			
			}
		}


		[Fact]
		public void AddingDocInTxAndReadingItInSameTx()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				tx.Read(viewer =>
					Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", transactionInformation)));
			}
		}

		[Fact]
		public void AddingDocInTxWithDifferentTxIdThrows()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
				                                                               transactionInformation));
				tx.Write(mutator =>
				         	Assert.Throws<ConcurrencyException>(
				         		() =>
									mutator.Transactions.AddDocumentInTransaction("Ayende", Guid.NewGuid(),
				         			                                           JObject.FromObject(new {Name = "Rahien"}),
				         			                                           new JObject(),
				         			                                           transactionInformation)));
			}
		}

		[Fact]
		public void AddingDocInTxWhenItWasAddedInAnotherWillFail()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
					transactionInformation));

				Assert.Throws<ConcurrencyException>(
					() =>
						tx.Write(
							mutator =>
								mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }),
								                                           new JObject(),
								                                           new TransactionInformation
								                                           {
								                                           	Id = Guid.NewGuid(),
								                                           	Timeout = TimeSpan.FromDays(7)
								                                           })));
			}
		}

		[Fact]
		public void AddingDocInTxWillReadOldValueOutsideIt()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{

				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));

				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien2" }), new JObject(),
					transactionInformation));

				tx.Read(viewer =>
				{
					var doc = viewer.Documents.DocumentByKey("Ayende", null);
					Assert.Equal("Rahien", doc.DataAsJson.Value<string>("Name"));
				});
			}
		}

		[Fact]
		public void AddingDocumentInTxThenAddingWithoutTxThrows()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject(),
																			   transactionInformation));
				tx.Write(mutator =>
							Assert.Throws<ConcurrencyException>(
								() =>
									mutator.Documents.AddDocument("Ayende", Guid.NewGuid(),
																			   JObject.FromObject(new { Name = "Rahien" }),
																			   new JObject())));
			}
		}

		[Fact]
		public void CanDeleteDocumentInTransaction()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", null, JObject.FromObject(new { Name = "Rahien" }), new JObject()));
				tx.Write(mutator => mutator.Transactions.DeleteDocumentInTransaction(transactionInformation, "Ayende", null));
				tx.Read(viewer =>
				{
					Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", null));
					Assert.Null(viewer.Documents.DocumentByKey("Ayende", transactionInformation));
				});
			}
		}


		[Fact]
		public void AddingDocumentInTxThenAddingWithoutTxAfterTxExpiredWorks()
		{
			var transactionInformation = new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = TimeSpan.FromDays(-7)
			};

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, JObject.FromObject(new { Name = "Rahien1" }), new JObject(),
																			   transactionInformation));
				tx.Write(mutator => mutator.Documents.AddDocument("Ayende", Guid.NewGuid(),
																			   JObject.FromObject(new { Name = "Rahien2" }),
																			   new JObject()));

				tx.Read(viewer =>
				{
					var doc  = viewer.Documents.DocumentByKey("Ayende", transactionInformation);
					Assert.Equal("Rahien2", doc.DataAsJson.Value<string>("Name"));
				});

			}
		}


		[Fact]
		public void CanGetTxIdValues()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				var txId = Guid.NewGuid();
				tx.Write(mutator => mutator.Transactions.AddDocumentInTransaction("Ayende", null, new JObject(), new JObject(), new TransactionInformation
				{
					Id = txId,
					Timeout = TimeSpan.FromDays(7)
				}));


				tx.Read(viewer =>
					Assert.Equal(new[] { txId }, viewer.Transactions.GetTransactionIds().ToArray()));
			}
		}
	}
}