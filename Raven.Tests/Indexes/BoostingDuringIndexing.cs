using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class BoostingDuringIndexing : RavenTest
	{
		public class User
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
		}

		public class Account
		{
			public string Name { get; set; }
		}

		public class UsersByName : AbstractIndexCreationTask<User>
		{
			public UsersByName()
			{
				Map = users => from user in users
				               select new
				               {
				               	FirstName = user.FirstName.Boost(3),
				               	user.LastName
				               };
			}
		}

		public class UsersAndAccounts : AbstractMultiMapIndexCreationTask<UsersAndAccounts.Result>
		{
			public class Result
			{
				public string Name { get; set; }
			}

			public UsersAndAccounts()
			{
				AddMap<User>(users =>
				             from user in users
				             select new {Name = user.FirstName}
					);
				AddMap<Account>(accounts =>
				                from account in accounts
				                select new {account.Name}.Boost(3)
					);
			}
		}

		[Fact]
		public void CanBoostFullDocument()
		{
			using (var store = NewDocumentStore())
			{
				new UsersAndAccounts().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Oren",
					});

					session.Store(new Account()
					{
						Name = "Oren",
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<UsersAndAccounts.Result, UsersAndAccounts>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "Oren")
						.As<object>()
						.ToList();

					Assert.Equal(2, results.Count);
					Assert.IsType<Account>(results[0]);
					Assert.IsType<User>(results[1]);
				}
			}
		}

		[Fact]
		public void CanGetBoostedValues()
		{
			using(var store = NewDocumentStore())
			{
				new UsersByName().Execute(store);

				using(var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Oren",
						LastName = "Eini"
					});

					session.Store(new User
					{
						FirstName = "Ayende",
						LastName = "Rahien"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Query<User,UsersByName>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x=>x.FirstName == "Ayende" || x.LastName == "Eini")
						.ToList();

					Assert.Equal("Ayende", users[0].FirstName);
					Assert.Equal("Oren", users[1].FirstName);
				}
			}
		}
	}
}