using System.Transactions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class tjbsb : RavenTest
	{
		[Fact]
		public void WorkWithoutTransaction()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user = new User { Id = "users/user@anywhere.com" };
					session.Store(user);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/user@anywhere.com");
					Assert.NotNull(user);
				}
			}
		}

		[Fact]
		public void WorkWithTransaction()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);

				using (var scope = new TransactionScope())
				using (var session = store.OpenSession())
				{
					var user = new User { Id = "users/user@anywhere.com" };
					session.Store(user);
					session.SaveChanges();
					scope.Complete();
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.AllowNonAuthoritativeInformation = false;
					var user = session.Load<User>("users/user@anywhere.com");
					Assert.NotNull(user);
				}
			}
		}

		[Fact]
		public void WorkWithTransactionAndNoAllowNonAuthoritativeInformation()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(store);

				using (new TransactionScope())
				{
					using (var session = store.OpenSession())
					{
						var user = new User {Id = "users/user@anywhere.com"};
						session.Store(user);
						session.SaveChanges();
					}

					using(new TransactionScope(TransactionScopeOption.Suppress))
					using (var session = store.OpenSession())
					{
						var user = session.Load<User>("users/user@anywhere.com");
						Assert.Null(user);
					}
				}
			}
		}
	}
}