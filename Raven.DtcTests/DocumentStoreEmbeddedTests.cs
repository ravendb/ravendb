// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Transactions;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Raven.Tests.Document;
using Xunit;

namespace Raven.DtcTests
{
	public class DocumentStoreEmbeddedTests : RavenTest
	{
		private readonly EmbeddableDocumentStore documentStore;

		public DocumentStoreEmbeddedTests()
		{
			documentStore = NewDocumentStore(requestedStorage: "esent");
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			base.Dispose();
		}

		[Fact]
		public void WillProcessAllDifferentDocumentsEnlistedInATransaction()
		{
			EnsureDtcIsSupported(documentStore);

			using (var tx = new TransactionScope())
			{
				using (var session = documentStore.OpenSession())
				{
					// Remark: Don't change the order of the stored classes!
					// This test will only fail if the classes are not
					// stored in their alphabetical order!
					session.Store(new Contact { FirstName = "Contact" });
					session.Store(new Company { Name = "Company" });
					session.SaveChanges();
				}
				tx.Complete();
			}
			Thread.Sleep(500);
			using (var session = documentStore.OpenSession())
			{
				Assert.NotNull(session.Load<Contact>("contacts/1"));
				Assert.NotNull(session.Load<Company>("companies/1"));
				session.SaveChanges();
			}
		}



		[Fact]
		public void CanUseTransactionsToIsolateSaves()
		{
			EnsureDtcIsSupported(documentStore);
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					session.Store(company);

					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var session2 = documentStore.OpenSession())
							Assert.Null(session2.Load<Company>(company.Id));

						tx.Complete();
					}
				}
				Assert.NotNull(session.Load<Company>(company.Id));
			}
		}

	

		[Fact]
		public void CanUseTransactionsToIsolateDelete()
		{
			EnsureDtcIsSupported(documentStore);
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();

				using (var tx = new TransactionScope())
				{
					session.Delete(company);
					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var session2 = documentStore.OpenSession())
							Assert.NotNull(session2.Load<Company>(company.Id));
					}

					tx.Complete();
				}
				for (int i = 0; i < 15; i++) // wait for commit
				{
					using (var session2 = documentStore.OpenSession())
						if (session2.Load<Company>(company.Id) == null)
							break;
					Thread.Sleep(100);
				}
				using (var session2 = documentStore.OpenSession())
					Assert.Null(session2.Load<Company>(company.Id));
			}
		}


	}
}