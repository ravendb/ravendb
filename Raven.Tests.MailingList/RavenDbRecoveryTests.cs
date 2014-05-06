// -----------------------------------------------------------------------
//  <copyright file="RavenDbRecoveryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Transactions;
using Raven.Abstractions;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;

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
			// Define ids for 5 testdocuments
			var documentIds = new[] { "1", "2", "3", "4", "5" };

			string dataDir = NewDataPath();
            using (var store = NewRemoteDocumentStore(runInMemory: false, dataDirectory: dataDir, requestedStorage: "esent"))
			{
                if(store.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				// First we add the documents to the store
				foreach (var id in documentIds)
				{
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						session.Advanced.UseOptimisticConcurrency = true;
						session.Store(new TestDocument {Id = id, Description = "Test"});
						session.SaveChanges();
					}
				}

				// We then load and update the documents in the same transaction and batch
				// but before we commit we stop the RavenDb service in order to simulate
				// a restart of the service
				using (var tx = new TransactionScope())
				{
					Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
						new DummyEnlistmentNotification(), EnlistmentOptions.None);

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
			}

			// We simulate that the server is restarting. The delay is important as this will make
			// the transaction timeout before we start the service again
			SystemTime.UtcDateTime = () => DateTime.UtcNow.AddDays(1);

			using (var store2 = NewRemoteDocumentStore(runInMemory: false, dataDirectory: dataDir)) //restart
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

		public class TestDocument
		{
			public string Id { get; set; }
			public string Description { get; set; }
		}
	}
}