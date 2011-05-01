using System.Transactions;
using Raven.Client;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class tjbsb : RemoteClientTest
	{
		[Fact]
		public void WorkWithoutTransaction()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					var user = new User { Id = "users/user@anywhere.com" };
					session.Store(user);
					session.SaveChanges();
				}

				using (IDocumentSession session = store.OpenSession())
				{
					var user = session.Load<User>("users/user@anywhere.com");
					Assert.NotNull(user);
				}
			}
		}

		[Fact]
		public void WorkWithTransaction()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				using (var scope = new TransactionScope())
				using (IDocumentSession session = store.OpenSession())
				{
					var user = new User { Id = "users/user@anywhere.com" };
					session.Store(user);
					session.SaveChanges();
					scope.Complete();
				}

				using (IDocumentSession session = store.OpenSession())
				{
					session.Advanced.AllowNonAuthoritiveInformation = false;
					var user = session.Load<User>("users/user@anywhere.com");
					Assert.NotNull(user);
				}
			}
		}

		[Fact]
		public void WorkWithTransactionAndNoAllowNonAutoritiveInformation()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				using (new TransactionScope())
				{
					using (IDocumentSession session = store.OpenSession())
					{
						var user = new User {Id = "users/user@anywhere.com"};
						session.Store(user);
						session.SaveChanges();
					}

					using(new TransactionScope(TransactionScopeOption.Suppress))
					using (IDocumentSession session = store.OpenSession())
					{
						var user = session.Load<User>("users/user@anywhere.com");
						Assert.Null(user);
					}
				}

				
			}
		}
	}
}