using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB422 : RavenTest
	{
		[Fact]
		public void UsingStoreAllFields()
		{
			using(var store = NewDocumentStore())
			{
				new UserIndex().Execute(store);
				using(var session = store.OpenSession())
				{
					session.Store(new User
					{
						Name = "aye",
						Email = "de"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var r = session.Advanced.LuceneQuery<dynamic>("UserIndex")
						.WaitForNonStaleResults()
						.SelectFields<dynamic>("UN", "UE")
						.Single();

					Assert.Equal("aye", r.UN);
					Assert.Equal("de", r.UE);
				}
			}
		}

		public class User
		{
			public string Name { get; set; }
			public string Email { get; set; }
		}

		public class UserIndex : AbstractIndexCreationTask<User>
		{
			public UserIndex()
			{
				Map = users =>
				      from user in users
				      select new {UN = user.Name, UE = user.Email};

				StoreAllFields(FieldStorage.Yes);
			}
		}
	}
}