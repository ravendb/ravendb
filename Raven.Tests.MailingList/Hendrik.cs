// -----------------------------------------------------------------------
//  <copyright file="Hendrik.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Hendrik : RavenTest
	{
		[Fact]
		public void TransactionTest_forceDistributedTransaction()
		{
			var id = new Guid("9e15c907-68da-44a0-8197-81c0ef1d88c9");

			var barrier = new Barrier(2);

            using (GetNewServer(requestedStorage: "esent"))
			using (var documentStore = new DocumentStore() { Url = "http://localhost:8079", EnlistInDistributedTransactions = true }.Initialize())
			{
                if(documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				using (var session = documentStore.OpenSession())
				{
					session.Store(new LocationView{Id = id});

					session.SaveChanges();
				}
				// Delete location (savechanges immediately) but rollback efter sleeping 10 seconds
				var task = new Task(() =>
				{
					using (var tx = new TransactionScope())
					{
						ForceDistributedTransaction();

						using (var session = documentStore.OpenSession())
						{
							session.Advanced.AllowNonAuthoritativeInformation = false;
							var locationView = session.Load<LocationView>(id);
							session.Delete(locationView);

							session.SaveChanges();
						}
						barrier.SignalAndWait();
						barrier.SignalAndWait();
						// We don't complete so the deletion will rollback
					}
				});
				task.Start();

				barrier.SignalAndWait();

				Assert.True(CanReadLocation(documentStore, id, forceDistributedTransaction: true));

				barrier.SignalAndWait();
				task.Wait();
				Assert.True(CanReadLocation(documentStore, id, forceDistributedTransaction: true));
			}
		}


		[Fact]
		public void TransactionTest_local_tx()
		{
			var id = new Guid("9e15c907-68da-44a0-8197-81c0ef1d88c9");

			var barrier = new Barrier(2);

            using (GetNewServer(requestedStorage: "esent"))
			using (var documentStore = new DocumentStore() { Url = "http://localhost:8079", EnlistInDistributedTransactions = true }.Initialize())
			{
                if(documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				using (var session = documentStore.OpenSession())
				{
					session.Store(new LocationView { Id = id });

					session.SaveChanges();
				}

				new Task(() =>
				{
					using (var tx = new TransactionScope())
					{
						using (var session = documentStore.OpenSession())
						{
							session.Advanced.AllowNonAuthoritativeInformation = false;
							var locationView = session.Load<LocationView>(id);
							session.Delete(locationView);

							session.SaveChanges();
						}
						barrier.SignalAndWait();
						barrier.SignalAndWait();
						// We don't complete so the deletion will rollback
					}
				}).Start();

				barrier.SignalAndWait();

				Assert.True(CanReadLocation(documentStore, id, forceDistributedTransaction: false));

				barrier.SignalAndWait();
				Assert.True(CanReadLocation(documentStore, id, forceDistributedTransaction: false));
			}
		}

		private bool CanReadLocation(IDocumentStore documentStore, Guid locationId, bool forceDistributedTransaction)
		{
			using (var tx = new TransactionScope())
			{
				if (forceDistributedTransaction) ForceDistributedTransaction();

				using (var session = documentStore.OpenSession())
				{
					session.Advanced.AllowNonAuthoritativeInformation = true;
					var locationView = session.Load<LocationView>(locationId);
					return locationView != null;
				}
			}
		}

		public class LocationView
		{
			public Guid Id { get; set; }
		}

		/// <summary>
		/// Force using distributed transaction by sending message to messagequeue
		/// </summary>
		private void ForceDistributedTransaction()
		{
			Transaction.Current.EnlistDurable(Guid.NewGuid(), new DummyEnlistmentNotification(),
											  EnlistmentOptions.None);
		}
	}
}