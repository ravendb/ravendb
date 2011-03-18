using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Indexing;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class UsingAsProjection : RemoteClientTest
	{
		[Fact]
		public void ShouldWork()
		{
			using(GetNewServer())
			{
				using (var documentStore = new DocumentStore { Url = "http://localhost:8080" })
				{
					documentStore.Initialize();

					using (IDocumentSession session = documentStore.OpenSession())
					{
						var company = new Account
						{
							Name = "Account A",
						};
						company.Users.Add(new User { Email = "a", Password = "1" });
						company.Users.Add(new User { Email = "b", Password = "2" });
						session.Store(company);

						company = new Account
						{
							Name = "Account B",
						};
						company.Users.Add(new User { Email = "c", Password = "3" });
						session.Store(company);

						session.SaveChanges();
					}
				}

				using (var documentStore = new DocumentStore { Url = "http://localhost:8080" })
				{
					documentStore.Initialize();
					IndexCreation.CreateIndexes(typeof(UsersIndexTask).Assembly, documentStore);
					using (IDocumentSession session = documentStore.OpenSession())
					{
						var user = session.Query<Account, UsersIndexTask>()
							.AsProjection<User>()
							.Where(x => x.Email == "a")
							.FirstOrDefault();
						var k = user.Password == "secret";
					}
				}
			}
		}

		public class UsersIndexTask : AbstractIndexCreationTask<Account, User>
		{
			public UsersIndexTask()
			{
				Map = accounts => from account in accounts
								  from user in account.Users
								  select new { user.Email, user.Password };

				Stores.Add(x => x.Email, FieldStorage.Yes);
				Stores.Add(x => x.Password, FieldStorage.Yes);
			}
		}

		public class Account
		{
			public Account()
			{
				Users = new List<User>();
			}
			public List<User> Users { get; private set; }
			public string Name { get; set; }
		}

		public class User
		{
			public string Password { get; set; }
			public string Email { get; set; }
		}
	}
}