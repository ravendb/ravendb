// -----------------------------------------------------------------------
//  <copyright file="SimpleSharding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Commands;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.Shard.Async
{
	public class SimpleSharding : RavenTest, IDisposable
	{
		private RavenDbServer[] servers;
		private ShardedDocumentStore documentStore;

		public SimpleSharding()
		{
			servers = new[]
			{
				GetNewServer(8079),
				GetNewServer(8078),
				GetNewServer(8077),
			};

			documentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
				{"3", CreateDocumentStore(8077)}
			}));
			documentStore.Initialize();
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
		public void CanUseDeferred()
		{
			string userId;
			using (var session = documentStore.OpenAsyncSession())
			{
				var entity = new User();
				session.Store(entity);
				session.SaveChangesAsync().Wait();
				userId = entity.Id;
			}

			using(var session = documentStore.OpenAsyncSession())
			{
				Assert.NotNull(session.LoadAsync<User>(userId).Result);
			}

			using (var session = documentStore.OpenAsyncSession())
			{
				session.Advanced.Defer(new DeleteCommandData
				{
					Key = userId
				});

				session.SaveChangesAsync().Wait();
			}

			using (var session = documentStore.OpenAsyncSession())
			{
				Assert.Null(session.LoadAsync<User>(userId).Result);
			}
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			foreach (var server in servers)
			{
				server.Dispose();
			}
			base.Dispose();
		}
	}
}