// -----------------------------------------------------------------------
//  <copyright file="ShardingFailure.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Bugs;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Tests.Shard
{
	public class ShardingFailure : RavenTest
	{
		[Fact]
		public void CanIgnore()
		{
			using(GetNewServer())
			{
				var shardingStrategy = new ShardStrategy(new Dictionary<string, IDocumentStore>
				{
					{"one", new DocumentStore {Url = "http://localhost:8079"}},
					{"two", new DocumentStore {Url = "http://localhost:8078"}},
				});
				shardingStrategy.ShardAccessStrategy.OnError += (commands, request, exception) => request.Query != null;

				using(var docStore = new ShardedDocumentStore(shardingStrategy).Initialize())
				{
					using(var session = docStore.OpenSession())
					{
						session.Query<AccurateCount.User>()
							.ToList();
					}
				}

			}
		}

		[Fact]
		public void CanIgnore_Lazy()
		{
			using (GetNewServer())
			{
				var shardingStrategy = new ShardStrategy(new Dictionary<string, IDocumentStore>
				{
					{"one", new DocumentStore {Url = "http://localhost:8079"}},
					{"two", new DocumentStore {Url = "http://localhost:8078"}},
				});
				shardingStrategy.ShardAccessStrategy.OnError += (commands, request, exception) => true;

				using (var docStore = new ShardedDocumentStore(shardingStrategy).Initialize())
				{
					using (var session = docStore.OpenSession())
					{
						var lazily = session.Query<AccurateCount.User>().Lazily();
						GC.KeepAlive(lazily.Value);
					}
				}

			}
		}
	}
}