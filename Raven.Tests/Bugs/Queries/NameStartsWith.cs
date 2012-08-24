using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
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
				using (IDocumentSession session = documentStore.OpenSession())
				{
					var result1 = session.Query<User, User_Entity>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Name.StartsWith("Mrs"))
						.ToArray();
					Assert.True(result1.Length == 1);

					var result2 = session.Query<User, User_Entity>()
								.Where(x => x.Name.StartsWith("Mrs."))
								.ToArray();
					Assert.True(result2.Length == 1);

					var result3 = session.Query<User, User_Entity>()
								.Where(x => x.Name.StartsWith("Mrs. S"))
								.ToArray();
					Assert.True(result3.Length == 1);

					var result4 = session.Query<User, User_Entity>()
								.Where(x => x.Name.StartsWith("Mrs. Shaba"))
								.ToArray();
					Assert.True(result4.Length == 1);

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
