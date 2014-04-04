// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1235.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;
using Raven.Tests.MailingList;

namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Threading.Tasks;

	using Raven.Client;
	using Raven.Client.Document;
	using Raven.Client.Shard;
	using Raven.Server;

	using Xunit;

	public class RavenDB_1235 : RavenTest
	{
		private readonly RavenDbServer[] servers;
		private readonly ShardedDocumentStore shardedDocumentStore;

		private readonly IList<string> shardNames = new List<string>
		{
			"1",
			"2",
			"3"
		};

		public RavenDB_1235()
		{
			servers = new[]
			{
				GetNewServer(8079),
				GetNewServer(8078),
				GetNewServer(8077),
			};

			shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{shardNames[0], CreateDocumentStore(8079)},
				{shardNames[1], CreateDocumentStore(8078)},
				{shardNames[2], CreateDocumentStore(8077)}
			}));

			shardedDocumentStore.Initialize();
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
		public void TransformWithConversionListenerForShardedStore()
		{
			shardedDocumentStore.ExecuteTransformer(new TransformWithConversionListener.EtagTransformer());
			shardedDocumentStore.RegisterListener(new TransformWithConversionListener.DocumentConversionListener());

			string id;

			using (var session = shardedDocumentStore.OpenSession())
			{
				var item = new TransformWithConversionListener.Item { Name = "oren" };

				session.Store(item);
				session.SaveChanges();

				id = item.Id;
			}

			using (var session = shardedDocumentStore.OpenSession())
			{
				var transformedItem = session.Load<TransformWithConversionListener.EtagTransformer, TransformWithConversionListener.TransformedItem>(id);
				Assert.True(transformedItem.Transformed);
				Assert.True(transformedItem.Converted);
			}
		}

		[Fact]
		public async Task TransformWithConversionListenerForShardedStoreAsync()
		{
			shardedDocumentStore.ExecuteTransformer(new TransformWithConversionListener.EtagTransformer());
			shardedDocumentStore.RegisterListener(new TransformWithConversionListener.DocumentConversionListener());

			string id;

			using (var session = shardedDocumentStore.OpenSession())
			{
				var item = new TransformWithConversionListener.Item { Name = "oren" };

				session.Store(item);
				session.SaveChanges();

				id = item.Id;
			}

			using (var session = shardedDocumentStore.OpenAsyncSession())
			{
				var transformedItem = await session.LoadAsync<TransformWithConversionListener.EtagTransformer, TransformWithConversionListener.TransformedItem>(id);
				Assert.True(transformedItem.Transformed);
				Assert.True(transformedItem.Converted);
			}
		}

		public override void Dispose()
		{
			shardedDocumentStore.Dispose();
			foreach (var server in servers)
			{
				server.Dispose();
			}
			base.Dispose();
		}
	}
}