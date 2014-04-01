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
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Shard
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
			using (var session = documentStore.OpenSession())
			{
				var entity = new User();
				session.Store(entity);
				userId = entity.Id;
				session.SaveChanges();
			}

			using(var session = documentStore.OpenSession())
			{
				Assert.NotNull(session.Load<User>(userId));
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.Defer(new DeleteCommandData
				{
					Key = userId
				});

				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Null(session.Load<User>(userId));
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