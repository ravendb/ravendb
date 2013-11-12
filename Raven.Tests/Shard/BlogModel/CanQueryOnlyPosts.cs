using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanQueryOnlyPosts : ShardingScenario
	{
		[Fact]
		public void WhenStoringPost()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post {Title = "Item 1"});
				session.Store(new Post {Title = "Item 2"});
				session.Store(new Post {Title = "Item 3"});
				Assert.Equal(2, Servers["Posts01"].Server.NumberOfRequests); // HiLo
				Servers.Where(server => server.Key != "Posts01")
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				session.SaveChanges();
				Assert.Equal(3, Servers["Posts01"].Server.NumberOfRequests);
				Assert.Equal(1, Servers["Posts02"].Server.NumberOfRequests);
				Assert.Equal(1, Servers["Posts03"].Server.NumberOfRequests);
				Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}


		[Fact]
		public void CanSortFromMultipleServers()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post { Title = "Item 1", UserId = "2"});
				session.Store(new Post { Title = "Item 2", UserId = "1" });
				session.Store(new Post { Title = "Item 3", UserId = "3" });
				session.SaveChanges();
			}

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<Post>()
					.Customize(x => x.WaitForNonStaleResults())
					.OrderBy(x => x.UserId)
					.ToList();

				Assert.Equal(3, posts.Count);
				Assert.Equal("Item 2", posts[0].Title);
				Assert.Equal("Item 1", posts[1].Title);
				Assert.Equal("Item 3", posts[2].Title);
			}
		}

		[Fact]
		public void CanPageFromMultipleServers()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post { Title = "Item 1", UserId = "2" });
				session.Store(new Post { Title = "Item 2", UserId = "1" });
				session.Store(new Post { Title = "Item 3", UserId = "3" });
				session.SaveChanges();
			}

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<Post>()
					.Customize(x => x.WaitForNonStaleResults())
					.OrderBy(x => x.UserId)
					.Take(2)
					.ToList();

				Assert.Equal(2, posts.Count);
				Assert.Equal("Item 2", posts[0].Title);
				Assert.Equal("Item 1", posts[1].Title);
			}
		}

		[Fact]
		public void WhenStoring6PostsInOneSession_Stores2InEachShard()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 0; i < 6; i++)
					session.Store(new Post
					              	{
					              		Id = "posts/" + i, // avoid generating an HiLo request.
					              		Title = "Item " + i
					              	});
				session.SaveChanges();
			}

			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server =>
				         	{
				         		Assert.Equal(1, server.Value.Server.NumberOfRequests);
				         		Assert.Equal(2, server.Value.SystemDatabase.Statistics.CountOfDocuments);
				         	});
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
		}

		[Fact]
		public void WhenStoring6PostsEachInADifferentSession_Stores2InEachShard()
		{
			for (int i = 0; i < 6; i++)
				using (var session = ShardedDocumentStore.OpenSession())
				{
					session.Store(new Post
					              	{
					              		Id = "posts/" + i, // avoid generating an HiLo request.
					              		Title = "Item " + i
					              	});
					session.SaveChanges();
				}

			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server => Assert.Equal(2, server.Value.Server.NumberOfRequests));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
		}

		[Fact]
		public void CanMergeResultFromAllPostsShards()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 0; i < 6; i++)
					session.Store(new Post
					{
						Id = "posts/" + i, // avoid generating an HiLo request.
						Title = "Item " + i
					});
				session.SaveChanges();
			}

			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server =>
				{
					Assert.Equal(1, server.Value.Server.NumberOfRequests);
					Assert.Equal(2, server.Value.SystemDatabase.Statistics.CountOfDocuments);
				});
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<Post>().ToList();
				Assert.Equal(6, posts.Count);
			}

			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server => Assert.Equal(2, server.Value.Server.NumberOfRequests));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
		}
	}
}