using System;
using System.Diagnostics;
using System.Linq;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Bugs.MultiMap
{
	public class MultiMapWithoutReduce : RavenTest
	{
		[Fact]
		public void CanQueryFromMultipleSources()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user = new User
							   {
								   Name = "Ayende Rahien"
							   };
					session.Store(user);

					for (int i = 0; i < 5; i++)
					{
						session.Store(new Post
									  {
										  AuthorId = user.Id,
										  Title = "blah"
									  });
					}

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();
					var posts = session.Query<Post>().Customize(x => x.WaitForNonStaleResults()).Count();

					Assert.Equal(1, users);
					Assert.Equal(5, posts);
				}
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Post
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public string AuthorId { get; set; }
		}
	}
}
