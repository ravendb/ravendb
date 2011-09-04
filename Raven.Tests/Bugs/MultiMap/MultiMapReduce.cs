using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class MultiMapReduce : LocalClientTest
	{
		protected override void CreateDefaultIndexes(Client.Embedded.EmbeddableDocumentStore documentStore)
		{
		}

		[Fact]
		public void CanGetDataFromMultipleDocumentSources()
		{
			using(var store = NewDocumentStore())
			{
				new PostCountsByUser_WithName().Execute(store);

				using(var session = store.OpenSession())
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

				using(var session = store.OpenSession())
				{
					var userPostingStatses = session.Query<UserPostingStats, PostCountsByUser_WithName>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();

					Assert.Equal(1, userPostingStatses.Count);

					Assert.Equal(5, userPostingStatses[0].PostCount);
					Assert.Equal("Ayende Rahien", userPostingStatses[0].UserName);
				}
			}

		}

		[Fact]
		public void CanQueryFromMultipleSources()
		{
			using (var store = NewDocumentStore())
			{
				new PostCountsByUser_WithName().Execute(store);

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
					var userPostingStatses = session.Query<UserPostingStats, PostCountsByUser_WithName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x=>x.UserName.StartsWith("aye"))
						.ToList();

					Assert.Equal(1, userPostingStatses.Count);

					Assert.Equal(5, userPostingStatses[0].PostCount);
					Assert.Equal("Ayende Rahien", userPostingStatses[0].UserName);
				}
			}

		}

		[Fact]
		public void CanQueryFromMultipleSources2()
		{
			using (var store = NewDocumentStore())
			{
				new PostCountsByUser_WithName().Execute(store);

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
					var userPostingStatses = session.Query<UserPostingStats, PostCountsByUser_WithName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.UserName.StartsWith("rah"))
						.ToList();

					Assert.Equal(1, userPostingStatses.Count);

					Assert.Equal(5, userPostingStatses[0].PostCount);
					Assert.Equal("Ayende Rahien", userPostingStatses[0].UserName);
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

		public class UserPostingStats
		{
			public string UserName { get; set; }
			public string UserId { get; set; }
			public int PostCount { get; set; }
		}

		public class PostCountsByUser_WithName : AbstractMultiMapIndexCreationTask<UserPostingStats>
		{
			public PostCountsByUser_WithName()
			{
				AddMap<User>(users => from user in users
									  select new
									  {
										  UserId = user.Id,
										  UserName = user.Name,
										  PostCount = 0
									  });

				AddMap<Post>(posts => from post in posts
									  select new
									  {
										  UserId = post.AuthorId,
										  UserName = (string)null,
										  PostCount = 1
									  });

				Reduce = results => from result in results
				                    group result by result.UserId
				                    into g
				                    select new
				                    {
				                    	UserId = g.Key,
				                    	UserName = g.Select(x => x.UserName).Where(x => x != null).First(),
				                    	PostCount = g.Sum(x => x.PostCount)
				                    };

				Index(x=>x.UserName, FieldIndexing.Analyzed);
			}
		}
	}
}