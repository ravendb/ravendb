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
        public void CanIgnoreParallel()
        {
            using (GetNewServer())
            {
                var shardingStrategy = new ShardStrategy(new Dictionary<string, IDocumentStore>
                {
                    {"one", new DocumentStore {Url = "http://localhost:8079"}},
                    {"two", new DocumentStore {Url = "http://localhost:8078"}},
                })
                {
                    ShardAccessStrategy = new ParallelShardAccessStrategy()
                };
                shardingStrategy.ShardAccessStrategy.OnError += (commands, request, exception) => request.Query != null;

                using (var docStore = new ShardedDocumentStore(shardingStrategy).Initialize())
                {
                    using (var session = docStore.OpenSession())
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

        [Fact]
        public void CanIgnoreMultiShard()
        {
            using (var server1 = GetNewServer(8079))
            using (var server2 = GetNewServer(8078))
            using (var server3 = GetNewServer(8077))
            using (var server4 = server3)
            {
                Dictionary<string, IDocumentStore> shards = new Dictionary<string, IDocumentStore>
                {
                    {"Eastern", server1.DocumentStore},
                    {"Western", server2.DocumentStore},
                    {"Northern", server3.DocumentStore},
                    {"Southern", server4.DocumentStore},
                };

                ShardStrategy shardStrategy = new ShardStrategy(shards)
                    .ShardingOn<Region2>(r => r.Name)//, name => (name == "Northern" || name == "Southern") ? "NorthSouth" : name)
                    .ShardingOn<TerritoryOf>(x => x.RegionId);

                IDocumentStore store = new ShardedDocumentStore(shardStrategy);
                NotSupportedException notSuppotedEx = null;
                try
                {
                    store.Initialize();
                }
                catch (Exception ex)
                {

                    notSuppotedEx = ex as NotSupportedException;
                }

                Assert.NotNull(notSuppotedEx);
                Assert.Contains("Multiple keys in shard dictionary for", notSuppotedEx.Message);
            }
 
        }
        public class Region2
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class TerritoryOf
        {
            public string Id { get; set; }
            public string RegionId { get; set; }
            public string Code { get; set; }
            public string Name { get; set; }
        }
    }
}
