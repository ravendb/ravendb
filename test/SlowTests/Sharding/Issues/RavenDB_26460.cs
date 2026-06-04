using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_26460 : ClusterTestBase
    {
        public RavenDB_26460(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding | RavenTestCategory.Logging)]
        public async Task ClusterDebugPackageShouldProduceInfoForEveryShard()
        {
            const string database = "RavenDB_26460";
            const int numberOfShards = 3;

            // 4 nodes, replication factor 1, 3 shards - the shards are spread across nodes that are not the orchestrator
            var (nodes, leader) = await CreateRaftCluster(4);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 1, (nodes, leader), shards: numberOfShards);

            using var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl } }.Initialize();

            await StoreSampleData(store);
            await AssertEveryShardHasDebugInfo(store, database);
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding | RavenTestCategory.Logging)]
        public async Task ClusterDebugPackageShouldProduceInfoForEveryShardWithMultipleOrchestrators()
        {
            const int numberOfShards = 3;

            // 4 nodes, 3 shards each with replication factor 1, but the orchestrator runs on 3 nodes.
            var (_, leader) = await CreateRaftCluster(4);
            var options = Sharding.GetOptionsForCluster(leader, shards: numberOfShards, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            options.Server = leader;

            using var store = GetDocumentStore(options);

            await AssertWaitForValueAsync(async () =>
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return record.Sharding.Orchestrator.Topology.Members.Count;
            }, 3);

            await StoreSampleData(store);
            await AssertEveryShardHasDebugInfo(store, store.Database);
        }

        private static async Task StoreSampleData(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();
            for (var i = 0; i < 100; i++)
                await session.StoreAsync(new User { Name = $"User-{i}" }, $"users/{i}");
            await session.SaveChangesAsync();
        }

        private async Task AssertEveryShardHasDebugInfo(IDocumentStore store, string database)
        {
            // map each shard to the node tags that actually host it (members, rehabs or promotables)
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            var shardHosts = new Dictionary<string, HashSet<string>>();
            foreach (var (shardNumber, topology) in record.Sharding.Shards)
                shardHosts[ShardHelper.ToShardName(database, shardNumber)] = topology.AllNodes.ToHashSet();

            // a node participates in this database only if it is an orchestrator or hosts at least one shard
            var participants = new HashSet<string>(record.Sharding.Orchestrator.Topology.Members);
            foreach (var hosts in shardHosts.Values)
                participants.UnionWith(hosts);

            var result = store.Maintenance.Server.Send(new GetClusterDebugInfoPackageOperation());

            using var ms = new MemoryStream();
            result.Stream.CopyTo(ms);
            result.Stream.Dispose();
            ms.Position = 0;

            // shardName -> entries gathered for it (with the node they came from) across all node packages
            var shardEntries = shardHosts.Keys.ToDictionary(shardName => shardName, _ => new List<(string NodeTag, string Entry)>());

            // nodeTag -> any entry that belongs to this database (the record folder or a shard folder)
            var databaseEntriesByNode = new Dictionary<string, List<string>>();

            using (var outer = new ZipArchive(ms, ZipArchiveMode.Read))
            {
                foreach (var nodeEntry in outer.Entries.Where(e => e.FullName.EndsWith(".zip")))
                {
                    var nodeTag = GetNodeTag(nodeEntry.FullName);
                    var databaseEntries = databaseEntriesByNode[nodeTag] = new List<string>();

                    using var nodeMs = new MemoryStream();
                    using (var nodeStream = nodeEntry.Open())
                        nodeStream.CopyTo(nodeMs);
                    nodeMs.Position = 0;

                    using var inner = new ZipArchive(nodeMs, ZipArchiveMode.Read);
                    foreach (var entry in inner.Entries)
                    {
                        if (entry.FullName.StartsWith(database + "/") || entry.FullName.StartsWith(database + "$"))
                            databaseEntries.Add(entry.FullName);

                        foreach (var shardName in shardEntries.Keys)
                        {
                            if (entry.FullName.StartsWith(shardName + "/"))
                                shardEntries[shardName].Add((nodeTag, entry.FullName));
                        }
                    }
                }
            }

            foreach (var (shardName, entries) in shardEntries)
            {
                var errors = entries.Where(e => e.Entry.EndsWith(".error")).ToList();
                var data = entries.Where(e => e.Entry.EndsWith(".json")).ToList();

                // no shard should produce error entries
                Assert.True(errors.Count == 0,
                    $"Shard '{shardName}' produced error entries in the debug package: {string.Join(", ", errors.Select(e => $"[{e.NodeTag}] {e.Entry}"))}");

                // every shard should have produced real debug info
                Assert.NotEmpty(data);

                // and that info must come from a node that actually hosts the shard
                foreach (var (nodeTag, _) in data)
                    Assert.Contains(nodeTag, shardHosts[shardName]);
            }

            // a node that neither orchestrates nor hosts a shard must not contribute any info for this database
            foreach (var (nodeTag, databaseEntries) in databaseEntriesByNode)
            {
                if (participants.Contains(nodeTag))
                    continue;

                Assert.True(databaseEntries.Count == 0,
                    $"Node '{nodeTag}' does not participate in '{database}' but produced: {string.Join(", ", databaseEntries)}");
            }
        }

        private static string GetNodeTag(string nodeEntryName)
        {
            // entry name looks like: "Node - [A].zip"
            var start = nodeEntryName.IndexOf('[') + 1;
            var end = nodeEntryName.IndexOf(']');
            return nodeEntryName.Substring(start, end - start);
        }
    }
}
