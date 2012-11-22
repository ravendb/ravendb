//-----------------------------------------------------------------------
// <copyright file="DocumentStoreServerTests_DifferentProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Util;
using Xunit;
using Transaction = System.Transactions.Transaction;

namespace Raven.Tests.Document
{
	public class DocumentStoreServerTests_DifferentProcess
	{
		[Fact]
		public void Can_promote_transactions()
		{
			using (var driver = new RavenDBDriver("HelloShard", new DocumentConvention()))
			{
				driver.Start();

				WaitForNetwork(driver.Url);

				using (var documentStore = new DocumentStore { Url = driver.Url  }.Initialize())
				{
					var company = new Company {Name = "Company Name"};
					var durableEnlistment = new ManyDocumentsViaDTC.DummyEnlistmentNotification();
					using (var tx = new TransactionScope())
					{
						var session = documentStore.OpenSession();
						session.Store(company);
						session.SaveChanges();

						Assert.Equal(Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier);

						Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
						                                  durableEnlistment, EnlistmentOptions.None);

						Assert.NotEqual(Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier);


						tx.Complete();
					}


					for (int i = 0; i < 15; i++) // wait for commit
					{
						using (var session2 = documentStore.OpenSession())
							if (session2.Load<Company>(company.Id) != null)
								break;
						Thread.Sleep(100);
					}
					using (var session2 = documentStore.OpenSession())
						Assert.NotNull((session2.Load<Company>(company.Id)));

					for (int i = 0; i < 15; i++) // we have to wait to be notified, too
					{
						if (durableEnlistment.WasCommitted == false)
							Thread.Sleep(100);
					}

					Assert.True(durableEnlistment.WasCommitted);
				}
			}
		}

		private static void WaitForNetwork(string url)
		{
			for (int i = 0; i < 15; i++)
			{
				try
				{
					var request = WebRequest.Create(url);
					request.GetResponse().Close();
					break;
				}
				catch (Exception)
				{
					Thread.Sleep(100);
				}
			}
		}
	}
}