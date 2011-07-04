//-----------------------------------------------------------------------
// <copyright file="DocumentStoreServerTests_DifferentProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Transactions;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Bugs;
using Xunit;
using Transaction = System.Transactions.Transaction;

namespace Raven.Tests.Document
{
	public class DocumentStoreServerTests_DifferentProcess
	{
		[Fact]
		public void Can_promote_transactions()
		{
			var process = Process.Start(typeof (Program).Assembly.Location, "/ram");
			try
			{
				WaitForNetwork("http://localhost:8080");

				var documentStore = new DocumentStore { Url = "http://localhost:8080" };
				documentStore.Initialize();

				var company = new Company { Name = "Company Name" };
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


				for (int i = 0; i < 15; i++)// wait for commit
				{
					using (var session2 = documentStore.OpenSession())
						if (session2.Load<Company>(company.Id) != null)
							break;
					Thread.Sleep(100);
				}
				using (var session2 = documentStore.OpenSession())
					Assert.NotNull((session2.Load<Company>(company.Id)));

				Assert.True(durableEnlistment.WasCommitted);

			}
			finally
			{
				process.Kill();
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
