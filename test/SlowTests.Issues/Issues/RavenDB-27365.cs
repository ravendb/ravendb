using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server;
using Raven.Server.Commercial.WriteUsageMetering;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues
{
    public class RavenDB_27365 : ClusterTestBase
    {
        public RavenDB_27365(ITestOutputHelper output) : base(output)
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.SupervisorSamplePeriod)] = "50";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod)] = "25";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.OnErrorDelayTime)] = "15";
        }

        private const string SourceCollection = "Dtos";

        internal class Dto
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private static WriteUsageApplicationSnapshot SnapshotEntryByDatabase(RavenServer leader, string database)
            => leader.ServerStore.Observer?.LatestWriteUsageSnapshot?.Applications.SingleOrDefault(d => d.ApplicationName == database);

        /// <summary>
        /// Registers a real embeddings generation task against the embedded model, so no external provider is
        /// involved. Mirrors what EmbeddingsGenerationTestBase does, which we cannot inherit from here because
        /// this fixture needs the cluster base for a leader that runs the observer.
        /// </summary>
        private static void AddEmbeddingsGenerationTask(IDocumentStore store, string collection)
        {
            var connectionString = new AiConnectionString
            {
                Name = "Local AI connection",
                ModelType = AiModelType.TextEmbeddings,
                EmbeddedSettings = new EmbeddedSettings()
            };
            connectionString.Identifier = connectionString.GenerateIdentifier();

            var putResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var configuration = new EmbeddingsGenerationConfiguration
            {
                Name = "localAiTask",
                ConnectionStringName = connectionString.Name,
                Collection = collection,
                EmbeddingsPathConfigurations =
                [
                    new EmbeddingPathConfiguration
                    {
                        Path = nameof(Dto.Name),
                        ChunkingOptions = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
                    }
                ],
                Quantization = VectorEmbeddingType.Single,
                ChunkingOptionsForQuerying = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 }
            };
            configuration.Identifier = configuration.GenerateIdentifier();

            store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task LastEtag_IsReportedPerDatabaseId()
        {
            var (nodes, leader) = await CreateRaftCluster(1, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 1, Server = leader }))
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                        session.Store(new Dto { Name = $"name/{i}" });

                    session.SaveChanges();
                }

                // The values the single member should be reporting, read straight from its storage.
                var database = await nodes.Single().ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                string expectedDatabaseId;
                long expectedLastEtag;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    expectedDatabaseId = database.DbBase64Id;
                    expectedLastEtag = database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                }

                Assert.False(string.IsNullOrEmpty(expectedDatabaseId), "Expected the node to expose its database id.");
                Assert.True(expectedLastEtag > 0, $"Expected the writes to advance the last etag, got {expectedLastEtag}.");

                // The observer republishes the snapshot on every tick; wait for one carrying the write position.
                await WaitForValueAsync(() => SnapshotEntryByDatabase(leader, store.Database)?.Nodes?.Count ?? 0, 1,
                    timeout: 30_000, interval: 100);

                // Freeze ticks for a deterministic read of the published snapshot.
                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                // One tuple per member, unmerged: the single member reports its own (database id, last etag).
                Assert.Equal(1, entry.Nodes.Count);

                var node = entry.Nodes.Single();
                Assert.Equal(expectedDatabaseId, node.DatabaseId);
                Assert.Equal(expectedLastEtag, node.LastEtag);

                // The merged change vector is still reported alongside the per-node tuples.
                Assert.Contains(expectedDatabaseId, entry.ChangeVector);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster | RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        public async Task SystemCollections_AreReported_FromCollectionsTheProductCreates()
        {
            var (nodes, leader) = await CreateRaftCluster(1, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 1, Server = leader }))
            {
                // Storing an entity without an id makes the client ask the server for a HiLo range, and the
                // server writes the range document into '@hilo' - the one '@' collection we filter out, so
                // this also gives the exclusion below something real to check against.
                using (var session = store.OpenSession())
                {
                    session.Store(new Dto { Name = "a name to embed" });
                    session.SaveChanges();
                }

                // Running the real embeddings generation task creates '@embeddings-cache' and
                // '@embeddings/Dtos'. Nothing here writes a '@collection' by hand.
                AddEmbeddingsGenerationTask(store, SourceCollection);

                // '@empty' has no feature that owns it - it is where a write that omits '@collection' lands,
                // and a session always stamps one from the entity type, so a raw command is how it arises.
                // It carries the '@' prefix, so it is reported like any other.
                using (var commands = store.Commands())
                    commands.Put("no-collection/1", null, new { });

                var embeddingsCollection = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(SourceCollection);
                var expected = new[]
                {
                    Constants.Documents.Collections.EmbeddingsCacheCollection,
                    embeddingsCollection,
                    Constants.Documents.Collections.EmptyCollection
                };

                // Wait for a tick that carries all three.
                await WaitForValueAsync(() =>
                {
                    var reported = SnapshotEntryByDatabase(leader, store.Database)?.Nodes?.SingleOrDefault()?.SystemCollections;
                    return reported != null && expected.All(reported.ContainsKey);
                }, true, timeout: 30_000, interval: 100);

                // Freeze ticks for a deterministic read of the published snapshot.
                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                // One entry per member, each carrying its own database id and its own counts.
                var reportedByMember = entry.Nodes.Single();
                Assert.NotNull(reportedByMember.SystemCollections);

                var actual = string.Join(", ", reportedByMember.SystemCollections.Keys);

                foreach (var collection in expected)
                {
                    Assert.True(reportedByMember.SystemCollections.TryGetValue(collection, out var count),
                        $"Expected '{collection}' in the report, got: {actual}.");
                    Assert.True(count > 0, $"Expected a document count for '{collection}', got {count}.");
                }

                // Not reported: the user's own collection, which carries no '@' prefix. And '@all_docs',
                // which does carry the prefix but is a query-only pseudo-collection with no storage of
                // its own, so it never reaches the report even though nothing filters it out.
                Assert.DoesNotContain(SourceCollection, reportedByMember.SystemCollections.Keys);

                // '@hilo' is the one '@' collection that is filtered out - the HiLo range documents are an
                // internal bookkeeping detail of id generation, not something the backend should meter.
                Assert.False(reportedByMember.SystemCollections.ContainsKey(CollectionName.HiLoCollection),
                    $"Did not expect '{CollectionName.HiLoCollection}' in the report, got: {actual}.");
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster, LicenseRequired = true)]
        public async Task SystemCollections_AreReportedPerDatabaseId_Unmerged()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 2, Server = leader }))
            {
                // A write that omits '@collection' lands in '@empty', which then replicates. A session always
                // stamps a collection from the entity type, so a raw command is how it arises.
                using (var commands = store.Commands())
                    await commands.PutAsync("no-collection/1", null, new { });

                await WaitForDocumentInClusterAsync<object>(nodes, store.Database, "no-collection/1", x => x != null,
                    TimeSpan.FromSeconds(30));

                // The values each member should be reporting, read straight from its own storage.
                // Etags are node-local, so every member is checked against its own numbers - not the group's.
                var expectedByDatabaseId = new Dictionary<string, (long LastEtag, long EmptyCount)>();
                foreach (var node in nodes)
                {
                    var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        expectedByDatabaseId[database.DbBase64Id] = (
                            database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction),
                            database.DocumentsStorage.GetNumberOfDocumentsFor(Constants.Documents.Collections.EmptyCollection, context));
                    }
                }

                Assert.Equal(2, expectedByDatabaseId.Count);

                // Wait for a tick where both members have reported their own values.
                await WaitForValueAsync(() =>
                {
                    var reported = SnapshotEntryByDatabase(leader, store.Database)?.Nodes;
                    if (reported == null || reported.Count != expectedByDatabaseId.Count)
                        return false;

                    return reported.All(n => n.SystemCollections != null &&
                                             expectedByDatabaseId.TryGetValue(n.DatabaseId, out var expected) &&
                                             n.LastEtag == expected.LastEtag &&
                                             n.SystemCollections.TryGetValue(Constants.Documents.Collections.EmptyCollection, out var count) &&
                                             count == expected.EmptyCount);
                }, true, timeout: 30_000, interval: 100);

                // Freeze ticks for a deterministic read of the published snapshot.
                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                // One entry per member, keyed by its database id and carrying that member's own values - the
                // group is not collapsed into a single summary, so the backend aggregates it itself.
                Assert.Equal(expectedByDatabaseId.Count, entry.Nodes.Count);
                Assert.Equal(expectedByDatabaseId.Keys.OrderBy(id => id),
                    entry.Nodes.Select(n => n.DatabaseId).OrderBy(id => id));

                foreach (var reported in entry.Nodes)
                {
                    var expected = expectedByDatabaseId[reported.DatabaseId];
                    Assert.Equal(expected.LastEtag, reported.LastEtag);

                    Assert.NotNull(reported.SystemCollections);
                    Assert.True(reported.SystemCollections.TryGetValue(Constants.Documents.Collections.EmptyCollection, out var count),
                        $"Expected '{Constants.Documents.Collections.EmptyCollection}' for database id '{reported.DatabaseId}', " +
                        $"got: {string.Join(", ", reported.SystemCollections.Keys)}.");

                    Assert.Equal(expected.EmptyCount, count);
                    Assert.True(count > 0,
                        $"Expected a document count for '{Constants.Documents.Collections.EmptyCollection}', got {count}.");
                }
            }
        }
    }
}
