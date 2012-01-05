using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
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