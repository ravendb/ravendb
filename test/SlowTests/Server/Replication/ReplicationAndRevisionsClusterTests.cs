using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationAndRevisionsClusterTests : ClusterTestBase
    {
        private const string DocId = "Docs/1";

        private const string CollectionName = "Users";

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public ReplicationAndRevisionsClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task AdoptOrphanedRevisions_MustNotRecreateTheDeletedDocument()
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            });

            await RevisionsHelper.SetupRevisionsAsync(store, leader.ServerStore, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            });

            // 1. create two revisions for the document
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, DocId);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, DocId);
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForDocumentInClusterAsync<User>(nodes, store.Database, DocId, user => user.Name == "New", TimeSpan.FromSeconds(15)));

            // 2. delete the document - this adds a delete revision, so the document is shown in the revisions bin
            using (var session = store.OpenAsyncSession())
            {
                session.Delete(DocId);
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForDocumentDeletionInClusterAsync(nodes, store.Database, DocId, TimeSpan.FromSeconds(15)));

            // 3. turn the revisions into *orphaned* revisions
            foreach (var node in nodes)
            {
                var nodeDatabase = await Databases.GetDocumentDatabaseInstanceFor(node, store);

                using (nodeDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    nodeDatabase.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(context, DocId, CollectionName);
                    tx.Commit();
                }
            }

            // Ensure no node has a delete revision for the document, so the revisions are orphaned and the document is not shown in the revisions bin.
            foreach (var node in nodes)
            {
                using var nodeStore = GetDocumentStoreForNode(store, node);
                using var session = nodeStore.OpenAsyncSession();

                Assert.Null(await session.LoadAsync<User>(DocId));

                var revisions = await session.Advanced.Revisions.GetMetadataForAsync(DocId);
                Assert.Equal(2, revisions.Count);
                Assert.DoesNotContain(revisions, revision => GetFlags(revision).Contain(DocumentFlags.DeleteRevision));
            }

            // 4. adopt the orphaned revisions
            var adoptingNode = leader;
            var adoptingNodeTag = adoptingNode.ServerStore.NodeTag;

            using var adoptingNodeStore = GetDocumentStoreForNode(store, adoptingNode);

            var operation = await adoptingNodeStore.Operations.SendAsync(new AdoptOrphanedRevisionsOperation(new AdoptOrphanedRevisionsOperation.Parameters()));
            var adoptResult = (AdoptOrphanedRevisionsResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(1, adoptResult.AdoptedCount);

            // 5. the adopted revision, on the node that ran the operation.
            using (var session = adoptingNodeStore.OpenAsyncSession())
            {
                var revisions = await session.Advanced.Revisions.GetMetadataForAsync(DocId);
                Assert.Equal(3, revisions.Count);

                var adoptedRevisionFlags = GetFlags(revisions[0]);
                var documentExists = await session.Advanced.ExistsAsync(DocId);

                Output.WriteLine($"Node {adoptingNodeTag} ran the adoption: adopted revision flags '{adoptedRevisionFlags}', " +
                                 $"'{DocId}' {(documentExists ? "EXISTS" : "doesn't exist")} on it right after the adoption");

                Assert.True(adoptedRevisionFlags.Contain(DocumentFlags.DeleteRevision),
                    $"The revision that node {adoptingNodeTag} adopted must be a delete revision, but its flags are '{adoptedRevisionFlags}'.");
            }

            // the revisions were really adopted - the document is shown in the revisions bin again
            using (adoptingNode.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new GetRevisionsBinEntryCommand(0, int.MaxValue);
                await adoptingNodeStore.GetRequestExecutor().ExecuteAsync(command, context);

                Assert.Equal(1, command.Result.Results.Length);
            }

            // 6. wait for the adopted revision to replicate, then check the whole cluster
            var documentRecreatedOn = new List<string>();
            var deleteRevisionsWithRevisionFlagOn = new List<string>();

            foreach (var node in nodes)
            {
                var nodeTag = node.ServerStore.NodeTag;

                using var nodeStore = GetDocumentStoreForNode(store, node);

                Assert.True(await WaitForValueAsync(async () =>
                {
                    using var session = nodeStore.OpenAsyncSession();
                    var revisions = await session.Advanced.Revisions.GetMetadataForAsync(DocId);
                    return revisions.Any(revision => GetFlags(revision).Contain(DocumentFlags.DeleteRevision));
                }, true), $"The adopted delete revision never reached node {nodeTag}.");

                using var nodeSession = nodeStore.OpenAsyncSession();

                var deleteRevisionsFlags = (await nodeSession.Advanced.Revisions.GetMetadataForAsync(DocId))
                    .Select(revision => GetFlags(revision))
                    .Where(flags => flags.Contain(DocumentFlags.DeleteRevision))
                    .ToList();

                var document = await nodeSession.LoadAsync<User>(DocId);
                var documentFlags = document == null ? null : GetFlags(nodeSession.Advanced.GetMetadataFor(document)).ToString();
                var deleteRevisionsDescription = string.Join(", ", deleteRevisionsFlags.Select(flags => $"'{flags}'"));

                Output.WriteLine($"Node {nodeTag}{(node == adoptingNode ? " (ran the adoption)" : "")}: " +
                                 (document == null
                                     ? $"'{DocId}' doesn't exist"
                                     : $"'{DocId}' EXISTS with flags '{documentFlags}' and no properties (Name = {document.Name ?? "null"})") +
                                 $", delete revisions: [{deleteRevisionsDescription}]");

                if (document != null)
                    documentRecreatedOn.Add($"{nodeTag} (document flags: '{documentFlags}')");

                if (deleteRevisionsFlags.Any(flags => flags.Contain(DocumentFlags.Revision)))
                    deleteRevisionsWithRevisionFlagOn.Add($"{nodeTag} (delete revisions: [{deleteRevisionsDescription}])");
            }

            Assert.True(documentRecreatedOn.Count == 0,
                $"'{DocId}' was deleted, but adopting its orphaned revisions re-created it as an empty (metadata only) document on: " +
                $"{string.Join("; ", documentRecreatedOn)}. A node that receives the adopted revision through replication puts its metadata-only " +
                $"payload back as a live document, and from there it replicates on as a regular document.");

            Assert.True(deleteRevisionsWithRevisionFlagOn.Count == 0,
                $"A delete revision must not carry the '{nameof(DocumentFlags.Revision)}' flag, but the adopted one does on: " +
                $"{string.Join("; ", deleteRevisionsWithRevisionFlagOn)}. That flag is what makes incoming replication handle it as a normal " +
                $"revision instead of a deletion.");
        }

        private static DocumentFlags GetFlags(IMetadataDictionary metadata) =>
            Enum.Parse<DocumentFlags>(metadata.GetString(Constants.Documents.Metadata.Flags));

        private static IDocumentStore GetDocumentStoreForNode(DocumentStore store, RavenServer server)
        {
            return new DocumentStore
            {
                Database = store.Database,
                Urls = new[] { server.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();
        }
    }
}
