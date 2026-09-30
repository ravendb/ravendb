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

                await WaitForValueAsync(() => SnapshotEntryByDatabase(leader, store.Database)?.Nodes?.Count ?? 0, 1,
                    timeout: 30_000, interval: 100);

                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                Assert.Equal(1, entry.Nodes.Count);

                var node = entry.Nodes.Single();
                Assert.Equal(expectedDatabaseId, node.DatabaseId);
                Assert.Equal(expectedLastEtag, node.LastEtag);

                Assert.Contains(expectedDatabaseId, entry.ChangeVector);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster | RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        public async Task SystemCollections_AreReported_FromCollectionsTheProductCreates()
        {
            var (nodes, leader) = await CreateRaftCluster(1, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 1, Server = leader }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dto { Name = "a name to embed" });
                    session.SaveChanges();
                }

                AddEmbeddingsGenerationTask(store, SourceCollection);

                using (var commands = store.Commands())
                    commands.Put("no-collection/1", null, new { });

                var embeddingsCollection = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(SourceCollection);
                var expected = new[]
                {
                    Constants.Documents.Collections.EmbeddingsCacheCollection,
                    embeddingsCollection
                };

                await WaitForValueAsync(() =>
                {
                    var reported = SnapshotEntryByDatabase(leader, store.Database)?.Nodes?.SingleOrDefault()?.SystemCollections;
                    return reported != null && expected.All(reported.ContainsKey);
                }, true, timeout: 30_000, interval: 100);

                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                var reportedByMember = entry.Nodes.Single();
                Assert.NotNull(reportedByMember.SystemCollections);

                var actual = string.Join(", ", reportedByMember.SystemCollections.Keys);

                foreach (var collection in expected)
                {
                    Assert.True(reportedByMember.SystemCollections.TryGetValue(collection, out var count),
                        $"Expected '{collection}' in the report, got: {actual}.");
                    Assert.True(count > 0, $"Expected a document count for '{collection}', got {count}.");
                }

                Assert.DoesNotContain(SourceCollection, reportedByMember.SystemCollections.Keys);

                Assert.False(reportedByMember.SystemCollections.ContainsKey(CollectionName.HiLoCollection),
                    $"Did not expect '{CollectionName.HiLoCollection}' in the report, got: {actual}.");
                Assert.False(reportedByMember.SystemCollections.ContainsKey(Constants.Documents.Collections.EmptyCollection),
                    $"Did not expect '{Constants.Documents.Collections.EmptyCollection}' in the report, got: {actual}.");
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster, LicenseRequired = true)]
        public async Task SystemCollections_AreReportedPerDatabaseId_Unmerged()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 2, Server = leader }))
            {
                const string reportedCollection = "@custom-collection";
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("custom/1", null, new { },
                        new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = reportedCollection });
                }

                await WaitForDocumentInClusterAsync<object>(nodes, store.Database, "custom/1", x => x != null,
                    TimeSpan.FromSeconds(30));

                var expectedByDatabaseId = new Dictionary<string, (long LastEtag, long Count)>();
                foreach (var node in nodes)
                {
                    var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        expectedByDatabaseId[database.DbBase64Id] = (
                            database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction),
                            database.DocumentsStorage.GetNumberOfDocumentsFor(reportedCollection, context));
                    }
                }

                Assert.Equal(2, expectedByDatabaseId.Count);

                await WaitForValueAsync(() =>
                {
                    var reported = SnapshotEntryByDatabase(leader, store.Database)?.Nodes;
                    if (reported == null || reported.Count != expectedByDatabaseId.Count)
                        return false;

                    return reported.All(n => n.SystemCollections != null &&
                                             expectedByDatabaseId.TryGetValue(n.DatabaseId, out var expected) &&
                                             n.LastEtag == expected.LastEtag &&
                                             n.SystemCollections.TryGetValue(reportedCollection, out var count) &&
                                             count == expected.Count);
                }, true, timeout: 30_000, interval: 100);

                leader.ServerStore.Observer.Suspended = true;

                var entry = SnapshotEntryByDatabase(leader, store.Database);
                Assert.NotNull(entry);

                Assert.Equal(expectedByDatabaseId.Count, entry.Nodes.Count);
                Assert.Equal(expectedByDatabaseId.Keys.OrderBy(id => id),
                    entry.Nodes.Select(n => n.DatabaseId).OrderBy(id => id));

                foreach (var reported in entry.Nodes)
                {
                    var expected = expectedByDatabaseId[reported.DatabaseId];
                    Assert.Equal(expected.LastEtag, reported.LastEtag);

                    Assert.NotNull(reported.SystemCollections);
                    Assert.True(reported.SystemCollections.TryGetValue(reportedCollection, out var count),
                        $"Expected '{reportedCollection}' for database id '{reported.DatabaseId}', " +
                        $"got: {string.Join(", ", reported.SystemCollections.Keys)}.");

                    Assert.Equal(expected.Count, count);
                    Assert.True(count > 0,
                        $"Expected a document count for '{reportedCollection}', got {count}.");
                }
            }
        }
    }
}
