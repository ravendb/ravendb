using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class NameStartsWith : RavenTest
	{
		[Fact]
		public void can_search_for_mrs_shaba()
		{
			using (var documentStore = NewDocumentStore())
			{
				new User_Entity().Execute(documentStore);

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var user1 = new User { Id = @"user/111", Name = "King Shaba" };
					session.Store(user1);

					var user2 = new User { Id = @"user/222", Name = "Mrs. Shaba" };
					session.Store(user2);

					var user3 = new User { Id = @"user/333", Name = "Martin Shaba" };
					session.Store(user3);

					session.SaveChanges();
				}
				

				using (var session = documentStore.OpenSession())
				{
					var result5 = session.Query<User, User_Entity>()
										.Customize(x => x.WaitForNonStaleResults())
										.Where(x => x.Name.StartsWith("King S"))
										.ToArray();

                    Assert.Equal(1, result5.Length);

                    var result1 = session.Query<User, User_Entity>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Name.StartsWith("Mrs"))
						.ToArray();
                    Assert.Equal(1, result1.Length);

                    var result2 = session.Query<User, User_Entity>()
						.Customize(x => x.WaitForNonStaleResults())
								.Where(x => x.Name.StartsWith("Mrs."))
								.ToArray();
                    Assert.Equal(1, result2.Length);

                    var result3 = session.Query<User, User_Entity>()
						.Customize(x => x.WaitForNonStaleResults())
								.Where(x => x.Name.StartsWith("Mrs. S"))
								.ToArray();
                    Assert.Equal(1, result3.Length);

                    var result4 = session.Query<User, User_Entity>()
						.Customize(x => x.WaitForNonStaleResults())
								.Where(x => x.Name.StartsWith("Mrs. Shaba"))
								.ToArray();
                    Assert.Equal(1, result4.Length);

                }
			}
		}
	}
	public class User_Entity : AbstractIndexCreationTask<User>
	{
		public User_Entity()
		{
			Map = docs => from doc in docs
						  select new
						  {
							  Id = doc.Id,
							  Name = doc.Name,
						  };
		}
	}

}
