using System;
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
		public void ThrwoWhenThereIsAPostInMoreThanOneShard_Query()
		{
			var post = new Post {Id = "posts/1", Title = "Item 1"};
			for (int i = 0; i < 2; i++)
				using (var session = ShardedDocumentStore.OpenSession())
				{
					session.Store(post);
					session.SaveChanges();
				}

			Assert.Equal(2, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 1));
			Assert.Equal(1, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 0));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				Assert.Throws<InvalidOperationException>(() => session.Query<Post>().ToList());
			}
		}

		[Fact]
		public void ThrwoWhenThereIsAPostInMoreThanOneShard_Load()
		{
			var post = new Post { Id = "posts/1", Title = "Item 1" };
			for (int i = 0; i < 2; i++)
				using (var session = ShardedDocumentStore.OpenSession())
				{
					session.Store(post);
					session.SaveChanges();
				}

			Assert.Equal(2, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 1));
			Assert.Equal(1, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 0));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				Assert.Throws<InvalidOperationException>(() => session.Load<Post>(1));
			}
		}

		[Fact]
		public void ThrwoWhenThereIsAPostInMoreThanOneShard_LoadMany()
		{
			var post = new Post { Id = "posts/1", Title = "Item 1" };
			for (int i = 0; i < 2; i++)
				using (var session = ShardedDocumentStore.OpenSession())
				{
					session.Store(post);
					session.SaveChanges();
				}

			Assert.Equal(2, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 1));
			Assert.Equal(1, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 0));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				Assert.Throws<InvalidOperationException>(() => session.Load<Post>("posts/1", "posts/2"));
			}
		}

		[Fact]
		public void ThrwoWhenThereIsAPostInMoreThanOneShard_LoadManyWithInclude()
		{
			var post = new Post { Id = "posts/1", Title = "Item 1", UserId = "users/fitzchak"};
			for (int i = 0; i < 2; i++)
				using (var session = ShardedDocumentStore.OpenSession())
				{
					session.Store(post);
					session.SaveChanges();
				}

			Assert.Equal(2, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 1));
			Assert.Equal(1, Servers.Count(server => server.Key.StartsWith("Posts") && server.Value.Server.NumberOfRequests == 0));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			using (var session = ShardedDocumentStore.OpenSession())
			{
				Assert.Throws<InvalidOperationException>(() => session.Include("UserId").Load<Post>("posts/1", "posts/2"));
			}
		}
	}
}