using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanQueryOnlyPosts : ShardingScenario
	{
		private readonly IEnumerable<string> userServers;

		public CanQueryOnlyPosts()
		{
			userServers = Enumerable.Range(0, 3).Select(i => "Posts #" + i);
		}

		[Fact]
		public void WhenStoringPost()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post {Title = "RavenDB is plain awesome"});
				Assert.Equal(2, Servers[userServers.First()].Server.NumberOfRequests); // HiLo
				Servers.Where(server => server.Key != userServers.First())
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));

				session.SaveChanges();
				Assert.Equal(3, Servers[userServers.First()].Server.NumberOfRequests);
				Servers.Where(server => userServers.Skip(1).Contains(server.Key))
					.ForEach(server => Assert.Equal(1, server.Value.Server.NumberOfRequests));
				Servers.Where(server => userServers.Contains(server.Key) == false)
					.ForEach(server => Assert.Equal(0, server.Value.Server.NumberOfRequests));
			}
		}
	}
}