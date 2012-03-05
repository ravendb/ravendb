using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
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
			var shardedDocumentStore = GetDocumentStore();
			Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.DisableAggressiveCaching());
			Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)));
			shardedDocumentStore.Initialize();
			Assert.DoesNotThrow(() => shardedDocumentStore.DisableAggressiveCaching());
			Assert.DoesNotThrow(() => shardedDocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)));
		}

		private static ShardedDocumentStore GetDocumentStore()
		{
			var shards = new List<IDocumentStore>
			             	{
			             		new DocumentStore {Url = "http://localhost:8079/"}
			             	};
			return new ShardedDocumentStore(new ShardStrategy(), shards);
		}

		[Fact]
		public void NotSupportedFeatures()
		{
			var shardedDocumentStore = GetDocumentStore().Initialize();
			Assert.Throws<NotSupportedException>(() => shardedDocumentStore.DatabaseCommands);
			Assert.Throws<NotSupportedException>(() => shardedDocumentStore.Url);
			Assert.Throws<NotSupportedException>(() => shardedDocumentStore.GetLastWrittenEtag());

			using (var session = (ShardedDocumentSession)shardedDocumentStore.OpenSession())
			{
				Assert.Throws<NotSupportedException>(() => session.Defer());
			}
		}
	}
}