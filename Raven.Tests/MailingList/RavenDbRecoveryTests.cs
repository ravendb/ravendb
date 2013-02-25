// -----------------------------------------------------------------------
//  <copyright file="RavenDbRecoveryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Transactions;
using Raven.Abstractions;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenDbRecoveryTests : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.DefaultStorageTypeName = "esent";
		}

		public override void Dispose()
		{
			SystemTime.UtcDateTime = null;
			base.Dispose();
		}

		[Fact]
		public void CanRunWithDTCTxAndRestart()
		{
			var store = NewRemoteDocumentStore(runInMemory: false, deleteDirectoryAfter: false, deleteDirectoryBefore: true);
			using (store)
			{
				// Define ids for 5 testdocuments
				var documentIds = new string[] { "1", "2", "3", "4", "5" };

				// First we add the documents to the store
				foreach (var id in documentIds)
				{
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						session.Advanced.UseOptimisticConcurrency = true;
						session.Store(new TestDocument() { Id = id, Description = "Test" });
						session.SaveChanges();

					}
				}

				// We then load and update the documents in the same transaction and batch
				// but before we commit we stop the RavenDb service in order to simulate
				// a restart of the service
				using (var tx = new TransactionScope())
				{
					Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
													  new ManyDocumentsViaDTC.DummyEnlistmentNotification(), EnlistmentOptions.None);

					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						session.Advanced.UseOptimisticConcurrency = true;

						foreach (var id in documentIds)
						{
							var document = session.Load<TestDocument>(id);
							document.Description = "Updated description";
						}
						session.SaveChanges();
					}


					store.Dispose();
					tx.Complete();
					Assert.Throws<TransactionAbortedException>(() => tx.Dispose());
				}


				// We simulate that the server is restarting. The delay is important as this will make
				// the transaction timeout before we start the service again
				SystemTime.UtcDateTime = () => DateTime.UtcNow.AddDays(1);

				using (var store2 = NewRemoteDocumentStore(runInMemory: false, deleteDirectoryBefore: false, deleteDirectoryAfter: true)) //restart
				{
					foreach (var id in documentIds)
					{
						using (var session = store2.OpenSession())
						{
							session.Advanced.AllowNonAuthoritativeInformation = false;
							session.Advanced.UseOptimisticConcurrency = true;

							var testMessage = session.Load<TestDocument>(id);
							testMessage.Description = "Updated again";

							session.SaveChanges();
						}
					}
				}
			}
		}

		/// <summary>
		/// Test document
		/// </summary>
		public class TestDocument
		{
			/// <summary>
			/// Id
			/// </summary>
			public string Id { get; set; }

			/// <summary>
			/// Desription
			/// </summary>
			public string Description { get; set; }
		}



	}
}