using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Shard;
using Raven.Client.Shard.ShardStrategy;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class ShardedDocumentStoreTest
	{
		[Fact]
		public void WillThrowIsThereIsNoShards()
		{
			Assert.Throws<ArgumentException>(() => new ShardedDocumentStore(new ShardStrategy(), null));
			Assert.Throws<ArgumentException>(() => new ShardedDocumentStore(new ShardStrategy(), new List<IDocumentStore>()));
		}

		[Fact]
		public void AssertInitialized()
		{
			var shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(), null);
			Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.DatabaseCommands);
			Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.DisableAggressiveCaching());
			Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)));
		}
	}
}