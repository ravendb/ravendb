﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    [Trait("Category", "Sharding")]
    public abstract class ShardedClusterTestBase : ClusterTestBase
    {
        protected ShardedClusterTestBase(ITestOutputHelper output) : base(output)
        {
        }

        public Task<(long Index, List<RavenServer> Servers)> CreateShardedDatabaseInCluster(string databaseName, int replicationFactor, (List<RavenServer> Nodes, RavenServer Leader) tuple, int shards = 3, X509Certificate2 certificate = null)
        {
            var record = new DatabaseRecord(databaseName)
            {
                Shards = GetDatabaseTopologyForShards(replicationFactor, tuple.Nodes.Select(x => x.ServerStore.NodeTag).ToList(), shards)
            };
            return CreateDatabaseInCluster(record, replicationFactor, tuple.Leader.WebUrl, certificate);
        }

        internal static DatabaseTopology[] GetDatabaseTopologyForShards(int replicationFactor, List<string> tags, int shards)
        {
            Assert.True(replicationFactor <= tags.Count);
            var topology = new DatabaseTopology[shards];
            for (int i = 0; i < shards; i++)
            {
                var currentTag = tags[i % tags.Count];
                var otherTags = tags.Where(x => x != currentTag).ToList();
                var members = new List<string>() { currentTag };
                var localReplicationFactor = replicationFactor;
                while (--localReplicationFactor != 0 && otherTags.Count > 0)
                {
                    int index = new Random().Next(otherTags.Count);
                    members.Add(otherTags[index]);
                    otherTags.Remove(otherTags[index]);
                }

                topology[i] = new DatabaseTopology { Members = members };
            }

            return topology;
        }

        public new void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null, X509Certificate2 clientCert = null)
        {
            var db = database ?? $"{documentStore.Database}$0";
            RavenTestBase.WaitForUserToContinueTheTest(documentStore, debug, db, clientCert);
        }

        internal static async Task<IDictionary<string, List<DocumentDatabase>>> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, List<RavenServer> Nodes, string database = null)
        {
            var dbs = new Dictionary<string, List<DocumentDatabase>>();
            foreach (var server in Nodes)
            {
                dbs.Add(server.ServerStore.NodeTag, new List<DocumentDatabase>());
                foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database ?? store.Database))
                {
                    var list = dbs[server.ServerStore.NodeTag];
                    list.Add(await task);
                    dbs[server.ServerStore.NodeTag] = list;
                }
            }

            return dbs;
        }

        protected bool WaitForShardedChangeVectorInCluster(List<RavenServer> nodes, string database, int replicationFactor, int timeout = 15000)
        {
            return AsyncHelpers.RunSync(() => WaitForShardedChangeVectorInClusterAsync(nodes, database, replicationFactor, timeout));
        }

        protected async Task<bool> WaitForShardedChangeVectorInClusterAsync(List<RavenServer> nodes, string database, int replicationFactor, int timeout = 15000)
        {
            return await WaitForValueAsync(async () =>
            {
                var cvs = new Dictionary<string, List<string>>();
                foreach (var server in nodes)
                {
                    foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database))
                    {
                        var storage = await task;
                        cvs.TryAdd(storage.Name, new List<string>());
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var list = cvs[storage.Name];
                            list.Add(DocumentsStorage.GetDatabaseChangeVector(context));
                            cvs[storage.Name] = list;
                        }
                    }
                }

                var result = true;
                foreach ((var _, List<string> shardCvs) in cvs)
                {
                    var first = shardCvs.FirstOrDefault();
                    var stringEqual = shardCvs.Any(x => x != first) == false;
                    if (string.IsNullOrEmpty(first))
                    {
                        result = stringEqual;
                    }
                    else
                    {
                        var sizeEqual = shardCvs.Any(x => x.ToChangeVectorList().Count != replicationFactor) == false;
                        result = stringEqual && sizeEqual;
                    }

                    if (result == false)
                        return false;
                }

                return true;
            }, true, timeout: timeout, interval: 333);
        }
    }
}
