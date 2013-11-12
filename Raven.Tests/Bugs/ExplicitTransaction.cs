//-----------------------------------------------------------------------
// <copyright file="ExplicitTransaction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Client.Connection;
using Raven.Tests.Document;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class ExplicitTransaction : RavenTest
	{
		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		[InlineData("voron")]
		public void Can_use_transactions_to_isolate_saves(string storage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage:storage))
			{
				var company = new Company { Name = "Company Name" };
				var session = documentStore.OpenSession();
				using (RavenTransactionAccessor.StartTransaction())
				{
					session.Store(company);
					session.SaveChanges();

					using (RavenTransactionAccessor.StartTransaction())
					{
						using (var session2 = documentStore.OpenSession())
							Assert.Null(session2.Load<Company>(company.Id));
					}
					Assert.NotNull(session.Load<Company>(company.Id));
					documentStore.DatabaseCommands.PrepareTransaction(RavenTransactionAccessor.GetTransactionInformation().Id);
					documentStore.DatabaseCommands.Commit(RavenTransactionAccessor.GetTransactionInformation().Id);
				}
				Assert.NotNull(session.Load<Company>(company.Id));
			}
		}

		[Theory]
		[InlineData("munin")]
		[InlineData("esent")]
		[InlineData("voron")]
		public void Will_process_all_different_documents_enlisted_in_a_transaction(string storage)
		{
			using (var documentStore = NewDocumentStore(requestedStorage:storage))
			{
				using (RavenTransactionAccessor.StartTransaction())
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
					documentStore.DatabaseCommands.PrepareTransaction(RavenTransactionAccessor.GetTransactionInformation().Id);
					documentStore.DatabaseCommands.Commit(RavenTransactionAccessor.GetTransactionInformation().Id);
			
				}
				Thread.Sleep(100);
				using (var session = documentStore.OpenSession())
				{
					Assert.NotNull(session.Load<Contact>("contacts/1"));
					Assert.NotNull(session.Load<Company>("companies/1"));
					session.SaveChanges();
				}
			}
		}
	}
}
