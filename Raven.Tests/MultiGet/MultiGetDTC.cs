using System.Transactions;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetDTC: RemoteClientTest
	{
		[Fact]
		public void CanUseMultiGetToBatchGetDocumentRequests()
		{
			using (GetNewServer())
			using (var docStore = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				for (int i = 0; i < 10; i++)
				{
					string id;
					using (var tx = new TransactionScope())
					using (var session = docStore.OpenSession())
					{

						Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
						                                  new ManyDocumentsViaDTC.DummyEnlistmentNotification(), EnlistmentOptions.None);

						var entity = new User { Name = "Ayende" };
						session.Store(entity);
						session.SaveChanges();
						id = entity.Id;
						tx.Complete();
					}

					using (var session = docStore.OpenSession())
					{
						session.Advanced.AllowNonAuthoritativeInformation = false;
						var user = session.Advanced.Lazily.Load<User>(id);
						Assert.NotNull(user.Value);
					}
				}

			}
		}
	}
}