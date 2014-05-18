using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class ShardedDocumentStoreTest : NoDisposalNeeded
	{
		[Fact]
		public void WillThrowIsThereIsNoShards()
		{
			Assert.Throws<ArgumentNullException>(() => new ShardStrategy(null));
			Assert.Throws<ArgumentException>(() => new ShardStrategy(new Dictionary<string, IDocumentStore>()));
		}

		[Fact]
		public void AssertInitialized()
		{
			using (var shardedDocumentStore = GetDocumentStore())
			{
				Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.DisableAggressiveCaching());
				Assert.Throws<InvalidOperationException>(() => shardedDocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)));
				shardedDocumentStore.Initialize();
				Assert.DoesNotThrow(() => shardedDocumentStore.DisableAggressiveCaching());
				Assert.DoesNotThrow(() => shardedDocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(1)));
			}
		}

		private static ShardedDocumentStore GetDocumentStore()
		{
			return new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			                                                     	{
			                                                     		{"default", CreateDocumentStore(8079)}
			                                                     	}));
		}

		private static IDocumentStore CreateDocumentStore(int port)
		{
			return new DocumentStore
			{
				Url = string.Format("http://localhost:{0}/", port),
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			};
		}

		[Fact]
		public void DtcIsNotSupported()
		{
			using (var shardedDocumentStore = GetDocumentStore().Initialize())
			using (var session = (ShardedDocumentSession)shardedDocumentStore.OpenSession())
			{
				var txId = Guid.NewGuid().ToString();
				Assert.Throws<NotSupportedException>(() => session.Commit(txId));
				Assert.Throws<NotSupportedException>(() => session.Rollback(txId));
			}
		}
	}
}