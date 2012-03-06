using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Shard.BlogModel
{
	public class SupportLazyOperations : ShardingScenario
	{
		[Fact]
		public void WithLazyQuery()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				for (int i = 0; i < 3; i++)
					session.Store(new Post {Id = "posts/" + i, Title = "Item " + i});
				session.SaveChanges();
			}

			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server =>
				{
					Assert.Equal(1, server.Value.Server.NumberOfRequests);
					Assert.Equal(1, server.Value.Database.Statistics.CountOfDocuments);
				});
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			Lazy<IEnumerable<Post>> lazily;
			using (var session = ShardedDocumentStore.OpenSession())
			{
				lazily = session.Query<Post>().Lazily();
			}
			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server => Assert.Equal(1, server.Value.Server.NumberOfRequests));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

			var posts = lazily.Value.ToList();
			Servers.Where(server => server.Key.StartsWith("Posts"))
				.ForEach(server => Assert.Equal(2, server.Value.Server.NumberOfRequests));
			Servers.Where(server => server.Key.StartsWith("Posts") == false)
				.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
		}
	}
}