using System.Transactions;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class RemoteTx : RemoteClientTest
	{
		[Fact]
		public void WillNotErrorOnDocumentNotYetCommitted()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
			{
				using(new TransactionScope())
				using(var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var s2 = store.OpenSession())
						{
							Assert.Null(s2.Load<User>("users/1"));
						}
					}
				}
			}
		}
	}
}