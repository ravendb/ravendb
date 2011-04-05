using System;
using System.Transactions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.DTC
{
	public class UsingDTCForUpdates : LocalClientTest
	{
		[Fact]
		public void can_update_a_doc_within_transaction_scope()
		{
			using (var documentStore = NewDocumentStore())
			{
				var id1 = Guid.NewGuid();
				RavenJObject dummy = null;

				using (TransactionScope trnx = new TransactionScope())
				{
					using (var session = documentStore.OpenSession())
					{
						dummy = new RavenJObject();
						dummy.Add("Name", "This is the object content");
						dummy.Add("Id", RavenJToken.FromObject(id1));
						session.Store(dummy);
						session.SaveChanges();

					}
					using (var session = documentStore.OpenSession())
					{
						session.Store(dummy);
						session.SaveChanges();
					}
					trnx.Complete();
				}
			}
		}
	}
}