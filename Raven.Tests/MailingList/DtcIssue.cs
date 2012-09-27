// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System.Linq;
using System.Transactions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class DtcIssue : RavenTest
	{

		#region Nested type: Cart

		public class Cart
		{
			public virtual string Email { get; set; }
		}

		#endregion

		[Fact]
		public void StandaloneTestForPostingOnStackOverflow()
		{
			var testDocument = new Cart { Email = "test@abc.com" };
			using (var documentStore = new EmbeddableDocumentStore { RunInMemory = true })
			{
				documentStore.Initialize();

				using (var session = documentStore.OpenSession())
				{
					using (var transaction = new TransactionScope())
					{
						session.Store(testDocument);
						session.SaveChanges();
						transaction.Complete();
					}
				}
				using (var session = documentStore.OpenSession())
				{
					using (var transaction = new TransactionScope())
					{
						var documentToDelete = session
							.Query<Cart>()
							.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
							.First(c => c.Email == testDocument.Email);

						session.Delete(documentToDelete);
						session.SaveChanges();
						transaction.Complete();
					}
				}

				using (var session = documentStore.OpenSession())
				{
					session.Advanced.AllowNonAuthoritativeInformation = false;
					RavenQueryStatistics statistics;

					var actualCount = session
						.Query<Cart>()
						.Statistics(out statistics)
						.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
						.Count(c => c.Email == testDocument.Email);

					Assert.False(statistics.IsStale);
					Assert.False(statistics.NonAuthoritativeInformation);
					Assert.Equal(0, actualCount);
				}
			}
		}
	}
}