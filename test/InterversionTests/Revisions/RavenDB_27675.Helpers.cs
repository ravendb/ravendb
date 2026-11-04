using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Commands.Cluster;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace InterversionTests.Revisions
{
    public partial class RavenDB_27675
    {
        private static readonly Dictionary<string, string> PersistentSettings = new()
        {
            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
            [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
        };

        private static DatabaseRecord PersistentRecord(string database) =>
            new(database) { Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" } };

        // A hub and a sink on 6.2.9, secured by one self-signed server certificate; Pull is the certificate the sink pulls with.
        private sealed class SecuredPair
        {
            public ProcessNode Hub, Sink;
            public Dictionary<string, string> Settings;
            public X509Certificate2 Admin, Pull;
        }

        private async Task<SecuredPair> GetSecuredPairOn62Async()
        {
            var certificates = Certificates.GenerateAndSaveSelfSignedCertificate();
            var settings = new Dictionary<string, string>(PersistentSettings)
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath
            };
            return new SecuredPair
            {
                Hub = await GetServerAsync(Server62Version, customSettings: settings, serverUrl: $"https://{Environment.MachineName}:0"),
                Sink = await GetServerAsync(Server62Version, customSettings: settings, serverUrl: $"https://{Environment.MachineName}:0"),
                Settings = settings,
                Admin = certificates.ServerCertificateForCommunication.Value,
                Pull = certificates.ClientCertificate1.Value
            };
        }

        private static async Task SetupTwoWayPullAsync(IDocumentStore hubStore, IDocumentStore sinkStore, string hubUrl,
            X509Certificate2 pullCertificate)
        {
            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(HubDefName)
            {
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
            }));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(HubDefName, new ReplicationHubAccess
            {
                Name = "sink-access",
                CertificateBase64 = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Cert))
            }));
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = "cs-hub", Database = hubStore.Database, TopologyDiscoveryUrls = new[] { hubUrl }
            }));
            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "cs-hub",
                HubName = HubDefName,
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCertificate.Export(X509ContentType.Pfx))
            }));
        }

        // The source database is retired: deleted, and its id declared unused on the hub (markUnused). A local write then persists
        // the hub's database change vector (unused ids dropped). Returns the retired database id.
        private static async Task<string> RetireSourceDatabaseAsync(IDocumentStore srcStore, IDocumentStore hubStore, bool markUnused = true)
        {
            var srcDbId = (await srcStore.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseId;
            await srcStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(srcStore.Database, hardDelete: true));
            if (markUnused)
                await hubStore.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(hubStore.Database, new HashSet<string> { srcDbId }, validate: false));
            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Name = "x" }, "sync/hub-local");
                await s.SaveChangesAsync();
            }
            return srcDbId;
        }

        // Adds the attachment a.txt to the document; with createAs, creates the document (named createAs) in the same write.
        private static async Task AddAttachmentAsync(IDocumentStore store, string id, string createAs = null)
        {
            using (var s = store.OpenAsyncSession())
            {
                if (createAs != null)
                    await s.StoreAsync(new User { Name = createAs }, id);
                s.Advanced.Attachments.Store(id, "a.txt", new MemoryStream(new byte[] { 1, 2, 3 }), "text/plain");
                await s.SaveChangesAsync();
            }
        }

        // Exports the given types, writes version nextVersion of the document, then imports the export back over it. Over a change
        // vector with a TRXN entry, the import appends ",TRXN:…" by hand (no space, not sorted): the import form.
        private async Task ImportOlderExportAsync(string url, string database, IDocumentStore writer, string id, int nextVersion,
            DatabaseItemType types = DatabaseItemType.Documents)
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");
            // no database-record types: the 7.2 client's defaults include types a 6.2 server does not know
            var noRecord = DatabaseRecordItemType.None;
            // smuggler operations are tagged with the node, so this store keeps topology updates on (unlike CreateStore)
            using (var smugglerStore = new DocumentStore { Urls = new[] { url }, Database = database }.Initialize())
            {
                var export = await smugglerStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = types, OperateOnDatabaseRecordTypes = noRecord }, file);
                await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                await WriteVersionsAsync(writer, id, clusterWide: false, from: nextVersion, count: 1);
                var import = await smugglerStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = types, OperateOnDatabaseRecordTypes = noRecord }, file);
                await import.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }

        // Waits for each document to reach the name; returns the ids that did not within the timeout.
        private static async Task<List<string>> NotConvergedAsync(IDocumentStore store, IEnumerable<string> ids, string name, int timeoutMs = 30_000)
        {
            var notConverged = new List<string>();
            foreach (var id in ids)
            {
                try
                {
                    await WaitForDocumentNameAsync(store, id, name, timeoutMs);
                }
                catch (Xunit.Sdk.XunitException)
                {
                    notConverged.Add(id);
                }
            }
            return notConverged;
        }

        // A 2-node hub cluster pull-replicating to a sink, created on 6.2.9; HubC is the hub node the task moves to.
        private sealed class HubClusterAndSink
        {
            public ProcessNode HubA, HubB, Sink;
            public string HubC;
            public string HubDb, SinkDb;
            public long HubTaskId;
        }

        // On 6.2.9: a 2-node hub cluster and a sink; the hub database on both nodes (or on node A only, so smuggler operations and
        // their state stay on one node); Users keep 100 revisions on both; the hub (mentor A) pull-replicates to the sink.
        private async Task<HubClusterAndSink> SetupHubClusterAndSinkOn62Async(bool hubOnNodeAOnly = false)
        {
            var chain = new HubClusterAndSink { HubDb = GetDatabaseName() + "-hub", SinkDb = GetDatabaseName() + "-sink" };
            var hubCluster = await CreateCluster(new[] { Server62Version, Server62Version }, customSettings: PersistentSettings);
            (chain.HubA, chain.HubB) = (hubCluster[0], hubCluster[1]);
            chain.Sink = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(chain.Sink.Url, chain.SinkDb))
            {
                var record = PersistentRecord(chain.HubDb);
                if (hubOnNodeAOnly)
                    record.Topology = new DatabaseTopology { Members = new List<string> { "A" } };
                await hubStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(record, replicationFactor: hubOnNodeAOnly ? 1 : 2));
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                chain.HubTaskId = await SetupHubToSinkAsync(hubStore, sinkStore, new[] { chain.HubA.Url, chain.HubB.Url }, mentor: "A");
            }

            return chain;
        }

        // The sink goes down for its in-place upgrade (to sinkVersion; 6.2.9 keeps it on 6.2); meanwhile the hub nodes move to
        // hubVersion, a new hub node joins and the hub task moves to it. When the sink comes back, the new node, unknown to the
        // sink, sends it everything again; this returns once the sink has received it.
        private async Task MoveHubTaskToNewNodeWhileSinkIsDownAsync(HubClusterAndSink chain, string hubVersion = "current", string sinkVersion = "current")
        {
            KillSlavedServerProcess(chain.Sink.Process);
            await UpgradeServerAsync(hubVersion, chain.HubA);
            await UpgradeServerAsync(hubVersion, chain.HubB);
            chain.HubC = hubVersion == "current"
                ? GetNewServer(new ServerCreationOptions { RunInMemory = false }).WebUrl
                : (await GetServerAsync(hubVersion, customSettings: PersistentSettings)).Url;
            await AddClusterNodeAsync(new[] { chain.HubA.Url, chain.HubB.Url }, chain.HubC);

            using (var hubStoreA = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var hubStoreC = CreateStore(chain.HubC, chain.HubDb))
            {
                var newTag = await AddDatabaseToNewClusterNodeAsync(hubStoreA, chain.HubA.Url, chain.HubC);
                await WriteMarkerAndWaitAsync(hubStoreA, hubStoreC, "on-the-new-node"); // the new node has everything before it takes the task
                await hubStoreA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(HubDefName)
                {
                    TaskId = chain.HubTaskId,
                    MentorNode = newTag,
                    PinToMentorNode = true
                }));
            }

            await UpgradeServerAsync(sinkVersion, chain.Sink);
            using (var hubStoreC = CreateStore(chain.HubC, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
                await WriteMarkerAndWaitAsync(hubStoreC, sinkStore, "re-sent"); // written after the re-send, so everything before it has arrived
        }

        // A released 7.2 that rebuilds change vectors on a 6.2-compatible pull replication send (RavenDB-26295, 7.2.5+).
        private const string Released72Version = "7.2.6";

        // On 6.2.9: a 2-node hub cluster pull-replicating to a sink. The hub moves to 7.2.6 while the sink stays connected and
        // writes 4 cluster-wide versions of the document, which the sink stores in the 7.2.6 rebuilt form; then the hub moves to
        // current. The sink stays on 6.2.9. The hub database is chosen so the rebuilt form does not start with "RAFT:".
        private async Task<HubClusterAndSink> SetupSink62FedBy726HubOnCurrentAsync(string id)
        {
            var chain = new HubClusterAndSink { SinkDb = GetDatabaseName() + "-sink" };
            var hubCluster = await CreateCluster(new[] { Server62Version, Server62Version }, customSettings: PersistentSettings);
            (chain.HubA, chain.HubB) = (hubCluster[0], hubCluster[1]);
            chain.Sink = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            chain.HubDb = await CreateHubDatabaseWhereRaftIsNotFirstAsync(chain.HubA.Url, GetDatabaseName() + "-hub");

            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(chain.Sink.Url, chain.SinkDb))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                chain.HubTaskId = await SetupHubToSinkAsync(hubStore, sinkStore, new[] { chain.HubA.Url, chain.HubB.Url }, mentor: "A");
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "on-6.2");
            }

            // ---- the hub is upgraded to 7.2.6; the 6.2 sink stays connected and receives rebuilt change vectors ----
            await UpgradeServerAsync(Released72Version, chain.HubA);
            await UpgradeServerAsync(Released72Version, chain.HubB);
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 4);
                await WaitForDocumentNameAsync(sinkStore, id, "v3", timeoutMs: 60_000);
            }

            // ---- the hub is upgraded to current; the sink is still on 6.2 ----
            await UpgradeServerAsync("current", chain.HubA);
            await UpgradeServerAsync("current", chain.HubB);
            return chain;
        }

        private sealed class AffectedSink
        {
            public ProcessNode HubA, HubB, HubC, Sink;
            public string HubDb, SinkDb;
            public string[] Ids;
            public List<string> Twins;
            public Dictionary<string, List<string>> PrunedOn62 = new();
            public List<string> PurgedOn62 = new();
            public List<string> ImportedOnSinkOn62 = new();
            public Dictionary<string, List<string>> ListedOn62 = new();
        }

        private const string DeletedId = "users/deleted";

        private const string ImportedId = "users/imported";

        // On 6.2.9 a 2-node hub cluster pull-replicates to a DR sink (revisions on; regular and cluster-wide writes). The sink
        // goes down; the hub nodes are upgraded in place to 7.2.6, a 7.2.6 node joins and the hub task moves to it. The sink is
        // upgraded in place and comes back: the new node, unknown to the sink, re-sends everything with rebuilt change vectors.
        // - default: the sink comes back on 7.2.6, stores the re-sent revisions as second rows (twins), then every node is
        //   upgraded in place to current: the sink is a database as affected customers have it.
        // - sinkReceivesOnCurrent: the sink comes back on current and receives the 7.2.6 re-send itself; the hub stays on 7.2.6.
        // sinkPrunesOn62: on 6.2 the sink keeps only 2 of 5 versions, leaving 6.2 revision tombstones (PrunedOn62).
        // sinkPurgesDeletedDocOn62: on 6.2 the hub writes users/deleted in cluster-wide transactions and deletes it the same way;
        // the sink then deletes that document's revisions (DeleteRevisionsOperation), leaving 6.2 revision tombstones (PurgedOn62).
        // withImportedAttachment: on 6.2 the hub writes users/imported in a cluster-wide transaction, adds an attachment and
        // imports an older export over it, so a revision with an attachment carries the import's ",TRXN:" form.
        // importedWithoutClusterWideRevision: as withImportedAttachment, then the 6.2 sink keeps only the newest 3 revisions of
        // users/imported, so none of its revisions starts with "RAFT:" (the hub database is chosen so that no standard or import
        // form of that document starts with "RAFT:" either). deleteImported: the hub then deletes users/imported.
        // withMixedDocument: on 6.2 the hub writes users/mixed twice in cluster-wide transactions, then once by a regular write
        // (its change vector is then in the standard form), so only its first two revisions get twins.
        // purgeTombstonesBeforeReSend (with sinkReceivesOnCurrent): the sink comes back on current while the task is still on a node it knows
        // (nothing to re-send); its pull task is removed until its tombstone cleaner (every minute) has run, then re-created,
        // and only then does the new node take the task.
        // importIsLastWrite (with withImportedAttachment): the import is the last write of users/imported, so its newest revision
        // is in the import form. onSink726 (without sinkReceivesOnCurrent): runs after the 7.2.6 re-send, while the sink still runs 7.2.6.
        private async Task<AffectedSink> SetupAffectedSinkAsync(bool sinkReceivesOnCurrent = false, bool sinkPrunesOn62 = false,
            bool sinkPurgesDeletedDocOn62 = false, bool withImportedAttachment = false, bool importedWithoutClusterWideRevision = false,
            bool deleteImported = false, bool withMixedDocument = false, bool purgeTombstonesBeforeReSend = false,
            bool importIsLastWrite = false, Func<AffectedSink, Task> onSink726 = null)
        {
            Assert.True(purgeTombstonesBeforeReSend == false || sinkReceivesOnCurrent, "purgeTombstonesBeforeReSend needs the sink on current for the re-send");
            withImportedAttachment |= importedWithoutClusterWideRevision;
            var ids = new List<string> { "users/regular", "users/cluster-tx" };
            if (sinkPurgesDeletedDocOn62)
                ids.Add(DeletedId);
            if (withImportedAttachment)
                ids.Add(ImportedId);
            if (withMixedDocument)
                ids.Add(MixedId);
            var result = new AffectedSink
            {
                HubDb = GetDatabaseName() + "-hub",
                SinkDb = GetDatabaseName() + "-sink",
                Ids = ids.ToArray()
            };
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };

            var hubCluster = await CreateCluster(new[] { Server62Version, Server62Version }, customSettings: PersistentSettings);
            (result.HubA, result.HubB) = (hubCluster[0], hubCluster[1]);
            result.Sink = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            long hubTaskId;
            if (importedWithoutClusterWideRevision)
                result.HubDb = await CreateHubDatabaseWhereRaftIsNotFirstAsync(result.HubA.Url, result.HubDb);
            // ---- on 6.2 ----
            using (var hubStore = CreateStore(result.HubA.Url, result.HubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(result.Sink.Url, result.SinkDb))
            {
                if (importedWithoutClusterWideRevision == false)
                {
                    await hubStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(PersistentRecord(result.HubDb), replicationFactor: 2));
                }
                await ConfigureRevisionsAsync(hubStore);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                hubTaskId = await SetupHubToSinkAsync(hubStore, sinkStore, new[] { result.HubA.Url, result.HubB.Url }, mentor: "A");

                var versions = sinkPrunesOn62 ? 5 : 3;
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: versions);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, $"v{versions - 1}");

                if (withMixedDocument)
                {
                    await WriteVersionsAsync(hubStore, MixedId, clusterWide: true, from: 0, count: 2);
                    await WriteVersionsAsync(hubStore, MixedId, clusterWide: false, from: 2, count: 1);
                    await WaitForDocumentNameAsync(sinkStore, MixedId, "v2");
                }

                if (sinkPrunesOn62)
                {
                    var before = new Dictionary<string, List<string>>();
                    foreach (var (id, _) in docs)
                        before[id] = await RevisionCvsAsync(sinkStore, id);

                    await PruneUsersRevisionsAsync(sinkStore, minToKeep: 2);

                    foreach (var (id, _) in docs)
                    {
                        var after = await RevisionCvsAsync(sinkStore, id);
                        result.PrunedOn62[id] = before[id].Except(after).ToList();
                        Assert.True(result.PrunedOn62[id].Count == 3, $"precondition: the 6.2 sink pruned 3 of 5 revisions of '{id}', pruned {result.PrunedOn62[id].Count}");
                    }
                }

                if (sinkPurgesDeletedDocOn62)
                {
                    await WriteVersionsAsync(hubStore, DeletedId, clusterWide: true, from: 0, count: 2);
                    await WaitForDocumentNameAsync(sinkStore, DeletedId, "v1"); // else "not on the sink" passes the wait below before it ever arrived
                    using (var s = hubStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        s.Delete(await s.LoadAsync<User>(DeletedId));
                        await s.SaveChangesAsync();
                    }
                    await WaitForDocumentDeletedAsync(sinkStore, DeletedId, timeoutMs: 60_000);

                    result.PurgedOn62 = await RevisionCvsAsync(sinkStore, DeletedId);
                    await sinkStore.Maintenance.SendAsync(new DeleteRevisionsOperation(DeletedId));
                    var left = await RevisionCvsAsync(sinkStore, DeletedId);
                    Assert.True(result.PurgedOn62.Count == 3 && left.Count == 0,
                        $"precondition: the 6.2 sink purged the 3 revisions of the deleted document, had {result.PurgedOn62.Count}, left {left.Count}");
                }

                if (withImportedAttachment)
                {
                    await WriteVersionsAsync(hubStore, ImportedId, clusterWide: true, from: 0, count: 1);
                    await AddAttachmentAsync(hubStore, ImportedId);
                    await WriteVersionsAsync(hubStore, ImportedId, clusterWide: false, from: 1, count: 1);
                    await ImportOlderExportAsync(result.HubA.Url, result.HubDb, hubStore, ImportedId, nextVersion: 2, DatabaseItemType.Documents | DatabaseItemType.Attachments);
                    if (importIsLastWrite)
                    {
                        await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-import");
                    }
                    else
                    {
                        await WriteVersionsAsync(hubStore, ImportedId, clusterWide: false, from: 3, count: 1);
                        await WaitForDocumentNameAsync(sinkStore, ImportedId, "v3", timeoutMs: 60_000);
                    }
                }

                if (importedWithoutClusterWideRevision)
                {
                    // the sink keeps the newest 3 revisions (v2, the imported version, v3): the cluster-wide one is pruned
                    await PruneUsersRevisionsAsync(sinkStore, minToKeep: 3);
                    await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);

                    var kept = await RevisionCvsAsync(sinkStore, ImportedId);
                    Assert.True(kept.Count == 3 && kept.All(cv => cv.StartsWith("RAFT:", StringComparison.Ordinal) == false) && kept.Any(cv => cv.Contains(",TRXN:", StringComparison.Ordinal)),
                        $"precondition: the 6.2 sink keeps 3 revisions of '{ImportedId}', none starting with RAFT:, one in the import form; got '{string.Join("', '", kept)}'");

                    if (deleteImported)
                    {
                        using (var s = hubStore.OpenAsyncSession())
                        {
                            s.Delete(ImportedId);
                            await s.SaveChangesAsync();
                        }
                        await WaitForDocumentDeletedAsync(sinkStore, ImportedId, timeoutMs: 60_000);
                    }

                    result.ImportedOnSinkOn62 = await RevisionCvsAsync(sinkStore, ImportedId);
                }

                foreach (var id in result.Ids)
                    result.ListedOn62[id] = await RevisionCvsAsync(sinkStore, id);
            }
            var lastVersion = $"v{(sinkPrunesOn62 ? 4 : 2)}";

            // ---- the sink goes down; the hub is upgraded to 7.2.6, gains a 7.2.6 node, and the task moves to it ----
            KillSlavedServerProcess(result.Sink.Process);
            await UpgradeServerAsync(Released72Version, result.HubA);
            await UpgradeServerAsync(Released72Version, result.HubB);
            result.HubC = await GetServerAsync(Released72Version, customSettings: PersistentSettings);
            await AddClusterNodeAsync(new[] { result.HubA.Url, result.HubB.Url }, result.HubC.Url);

            using (var hubStoreA = CreateStore(result.HubA.Url, result.HubDb))
            using (var hubStoreC = CreateStore(result.HubC.Url, result.HubDb))
            {
                var newTag = await AddDatabaseToNewClusterNodeAsync(hubStoreA, result.HubA.Url, result.HubC.Url);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(hubStoreC, id, lastVersion, timeoutMs: 60_000);
                if (withMixedDocument)
                    await WaitForDocumentNameAsync(hubStoreC, MixedId, "v2", timeoutMs: 60_000);

                if (purgeTombstonesBeforeReSend)
                {
                    // the sink's pull task consumes its tombstones; while the task is re-created (maintenance), nothing does
                    await UpgradeServerAsync("current", result.Sink, customSettings: new Dictionary<string, string>(PersistentSettings)
                    {
                        [RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = "1"
                    });
                    using (var sinkStore = CreateStore(result.Sink.Url, result.SinkDb))
                    {
                        await RemoveSinkPullTaskAsync(sinkStore);
                        var sw = Stopwatch.StartNew();
                        int legacy;
                        do
                        {
                            legacy = CountLegacyRevisionTombstones(result.Sink.Url, result.SinkDb, result.Ids);
                            if (legacy == 0)
                                break;
                            await Task.Delay(TimeSpan.FromSeconds(5));
                        } while (sw.Elapsed < TimeSpan.FromMinutes(3));

                        await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                        {
                            ConnectionStringName = $"cs-{result.HubDb}",
                            HubName = HubDefName,
                            Mode = PullReplicationMode.HubToSink
                        }));
                    }
                }

                await hubStoreA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(HubDefName)
                {
                    TaskId = hubTaskId,
                    MentorNode = newTag,
                    PinToMentorNode = true
                }));
            }

            // ---- the sink comes back (on 7.2.6, or on current with sinkReceivesOnCurrent); the new hub node re-sends everything ----
            if (purgeTombstonesBeforeReSend == false)
                await UpgradeServerAsync(sinkReceivesOnCurrent ? "current" : Released72Version, result.Sink);
            using (var hubStoreC = CreateStore(result.HubC.Url, result.HubDb))
            using (var sinkStore = CreateStore(result.Sink.Url, result.SinkDb))
            {
                // replication sends in etag order, so a write after the move arrives after everything re-sent
                await WriteMarkerAndWaitAsync(hubStoreC, sinkStore, "after-move", timeoutMs: 120_000);
                var listedTwice = new List<string>();
                foreach (var id in result.Ids)
                {
                    var listed = await RevisionCvsAsync(sinkStore, id);
                    listedTwice.AddRange(listed.Where(cv => RevisionsOfSameIdentity(listed, cv).Count > 1));
                }

                // on current the revisions key cleanup merges the twins as soon as the database loads, so they are checked on 7.2.6
                if (sinkReceivesOnCurrent == false)
                    Assert.True(listedTwice.Count > 0, "precondition: the 7.2.6 re-send left twins on the sink");
            }

            if (sinkReceivesOnCurrent)
            {
                await WaitForRevisionsKeyCleanupAsync(result.Sink.Url, result.SinkDb);
                result.Twins = TwinRevisionRows(result.Sink.Url, result.SinkDb, result.Ids);
                return result;
            }

            if (onSink726 != null)
                await onSink726(result);

            // ---- everything is upgraded to current ----
            await UpgradeServerAsync("current", result.Sink);
            await UpgradeServerAsync("current", result.HubA);
            await UpgradeServerAsync("current", result.HubB);
            await UpgradeServerAsync("current", result.HubC);

            await WaitForRevisionsKeyCleanupAsync(result.Sink.Url, result.SinkDb);
            result.Twins = TwinRevisionRows(result.Sink.Url, result.SinkDb, result.Ids);
            return result;
        }

        // The change vector with its entries sorted by database id and joined with ", " (what SerializeVector builds since 4.2.109).
        private static string SortedChangeVector(string changeVector) =>
            string.Join(", ", changeVector.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .OrderBy(e => e.Substring(e.IndexOf('-') + 1), StringComparer.Ordinal));

        private const string MixedId = "users/mixed";

        private static long CountRevisionTombstones(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = context.Transaction.InnerTransaction.OpenTable(database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
                return table?.NumberOfEntries ?? 0;
            }
        }

        private static long CountAttachmentTombstones(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = context.Transaction.InnerTransaction.OpenTable(database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
                return table?.NumberOfEntries ?? 0;
            }
        }

        private static CleanupState ReadKeyCleanerState(DocumentDatabase database, out bool allRowsHashed, out bool stored)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                allRowsHashed = RevisionsStorage.AllRevisionRowsHashed(context.Transaction.InnerTransaction);
                stored = CleanupState.TryRead(context.Transaction.InnerTransaction, out var state, out _);
                return state;
            }
        }

        // Waits until the revisions key cleanup of the database on this (current) server has run to its end: the storage has HashedRevisionPk.
        private async Task WaitForRevisionsKeyCleanupAsync(string url, string database, int timeoutMs = 120_000)
        {
            var documentDatabase = await DatabaseAtAsync(url, database);
            var sw = Stopwatch.StartNew();
            CleanupState state;
            bool allRowsHashed;
            while (true)
            {
                state = ReadKeyCleanerState(documentDatabase, out allRowsHashed, out _);
                if (allRowsHashed || sw.ElapsedMilliseconds >= timeoutMs)
                    break;
                await Task.Delay(250);
            }

            if (allRowsHashed == false)
            {
                // the cleanup stopped (an exception ends it until the next load): one more batch shows why
                try
                {
                    await documentDatabase.TxMerger.Enqueue(new CleanupBatch());
                }
                catch (Exception e)
                {
                    Assert.Fail($"the revisions key cleanup of '{database}' stopped at {state.Step}: {e}");
                }
            }
            Assert.True(state.Step == CleanupStep.Done && allRowsHashed, $"the revisions key cleanup of '{database}' ran to its end; its state is at {state.Step}, HashedRevisionPk {allRowsHashed}");

            // the database turns hash-only once the cleanup's last batch has committed
            sw.Restart();
            while (documentDatabase.DocumentsStorage.RevisionsStorage.HashedRevisionPk == false && sw.ElapsedMilliseconds < 10_000)
                await Task.Delay(50);
            Assert.True(documentDatabase.DocumentsStorage.RevisionsStorage.HashedRevisionPk, $"'{database}' is hash-only once its revisions key cleanup is done");
        }

        private static bool KeyCleanerWalkStateLeft(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                CleanupState.TryRead(context.Transaction.InnerTransaction, out var state, out var document);
                return document.IsEmpty == false || state.Cursor != 0 || state.End != 0;
            }
        }

        // Polls the cleanup's stored state until its storage is hash-only; returns whether a batch ever stopped inside a
        // document's revision attachments: the stored key is then the last row moved, longer than [lowerId][RS].
        private async Task<bool> WatchForAttachmentResumeAsync(string url, string database, int timeoutMs = 300_000)
        {
            var documentDatabase = await DatabaseAtAsync(url, database);
            var sw = Stopwatch.StartNew();
            var stoppedOnASegment = false;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tx = context.Transaction.InnerTransaction;
                    if (RevisionsStorage.AllRevisionRowsHashed(tx))
                        return stoppedOnASegment;

                    if (CleanupState.TryRead(tx, out var state, out var key) &&
                        (state.Step == CleanupStep.RevisionAttachments || state.Step == CleanupStep.RevisionAttachmentTombstones) &&
                        key.IndexOf(SpecialChars.RecordSeparator) < key.Length - 1)
                        stoppedOnASegment = true;
                }
                await Task.Delay(1);
            }
            return stoppedOnASegment;
        }

        // A concurrent version of the document, written on a database of its own on the sink's server, reaches the sink by external
        // replication, and the sink resolves the conflict. Resolving saves the sink's current version as a revision flagged
        // Conflicted (whichever version wins: a cluster-wide one may win over the latest). Waits until the write has arrived.
        private async Task CauseConflictOnSinkAsync(string sinkUrl, string sinkDb, string id, string name)
        {
            using var sinkStore = CreateStore(sinkUrl, sinkDb);
            await sinkStore.Maintenance.Server.SendAsync(new ModifyConflictSolverOperation(sinkDb, collectionByScript: null, resolveToLatest: true));
            using var otherStore = await CreatePersistentStoreAsync(sinkUrl, $"{sinkDb}-{name}");
            using (var s = otherStore.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Name = name }, id);
                await s.SaveChangesAsync();
            }
            await SetupExternalReplicationAsync(otherStore, sinkStore);
            await WriteMarkerAndWaitAsync(otherStore, sinkStore, $"conflict-{name}");
        }

        // As CauseConflictOnSinkAsync, on the hub: the writer is a database on a server of its own, replicating into the hub
        // cluster. Reads the result on the last of the hub nodes given.
        private async Task CauseConflictOnHubAsync(string[] hubUrls, string hubDb, string id)
        {
            var otherServer = GetNewServer(new ServerCreationOptions { RunInMemory = false });
            using var hubStore = CreateStore(hubUrls[^1], hubDb);
            await hubStore.Maintenance.Server.SendAsync(new ModifyConflictSolverOperation(hubDb, collectionByScript: null, resolveToLatest: true));
            using var otherStore = await CreatePersistentStoreAsync(otherServer.WebUrl, hubDb + "-other");
            using (var s = otherStore.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Name = "other-on-hub" }, id);
                await s.SaveChangesAsync();
            }
            await ReplicateToAsync(otherStore, hubDb, hubUrls);
            await WriteMarkerAndWaitAsync(otherStore, hubStore, "conflict-on-hub");
            var hubFlags = await RevisionFlagsAsync(hubStore, id);
            Assert.True(hubFlags.Values.Any(f => f.Contains("Conflicted")), $"precondition: the hub resolved a conflict on '{id}' (a revision flagged Conflicted)");
        }

        private static async Task AssertResolvedAConflictAsync(IDocumentStore store, string id)
        {
            var flags = await RevisionFlagsAsync(store, id);
            Assert.True(flags.Values.Any(f => f.Contains("Conflicted")), $"precondition: '{id}' has a revision flagged Conflicted after the conflict");
        }

        private static async Task<Dictionary<string, string>> RevisionFlagsAsync(IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 100);
            return metadata.ToDictionary(m => m.GetString("@change-vector"), m => m.TryGetValue("@flags", out object f) ? f?.ToString() ?? "" : "");
        }

        private static async Task<string> DocumentChangeVectorAsync(IDocumentStore store, string id)
        {
            using var s = store.OpenAsyncSession();
            return s.Advanced.GetChangeVectorFor(await s.LoadAsync<User>(id));
        }

        // Polls until every given revision is listed exactly once, under the given change vector; returns what is still wrong at
        // the timeout.
        private static async Task<List<string>> WaitForEachRevisionListedOnceAsync(IDocumentStore store, Dictionary<string, List<string>> expected, int timeoutMs)
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var failures = new List<string>();
                foreach (var (id, cvs) in expected)
                {
                    var listed = await RevisionCvsAsync(store, id);
                    foreach (var cv in cvs)
                    {
                        var copies = RevisionsOfSameIdentity(listed, cv);
                        if (copies.Count != 1 || copies[0] != cv)
                            failures.Add($"'{id}': the 6.2 revision '{cv}' is listed as '{string.Join("', '", copies)}'");
                    }
                }
                if (failures.Count == 0 || sw.ElapsedMilliseconds > timeoutMs)
                    return failures;
                await Task.Delay(1000);
            }
        }

        // Polls until none of the given revisions (any change vector form) is listed; returns the ones still listed at the timeout.
        private static async Task<List<string>> WaitForNoneListedAsync(IDocumentStore store, Dictionary<string, List<string>> deleted, int timeoutMs)
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var back = new List<string>();
                foreach (var (id, cvs) in deleted)
                {
                    var listed = await RevisionCvsAsync(store, id);
                    back.AddRange(cvs.SelectMany(cv => RevisionsOfSameIdentity(listed, cv)).Distinct().Select(cv => $"'{id}': '{cv}'"));
                }
                if (back.Count == 0 || sw.ElapsedMilliseconds > timeoutMs)
                    return back;
                await Task.Delay(1000);
            }
        }

        // The hub keeps minToKeep revisions of the document; returns a failure for every revision the hub pruned that the sink
        // still has (any change vector form).
        // waitForCleanup: how long the sink may take to delete them (its background cleanup); the check repeats until then.
        private async Task<List<string>> HubPrunesAndSinkMissesAsync(IDocumentStore hubStore, IDocumentStore sinkStore, string id, int minToKeep, string label,
            TimeSpan waitForCleanup = default)
        {
            var hubBefore = await RevisionCvsAsync(hubStore, id);
            await PruneUsersRevisionsAsync(hubStore, minToKeep);
            await WriteMarkerAndWaitAsync(hubStore, sinkStore, $"after-prune-{minToKeep}");

            var onHub = await RevisionCvsAsync(hubStore, id);
            var pruned = hubBefore.Where(cv => RevisionsOfSameIdentity(onHub, cv).Count == 0).ToList();
            Assert.True(pruned.Count > 0, $"precondition [{label}]: the hub pruned revisions of '{id}'");

            var sw = Stopwatch.StartNew();
            while (true)
            {
                var onSink = await RevisionCvsAsync(sinkStore, id);
                var failures = new List<string>();
                foreach (var cv in pruned)
                {
                    var left = RevisionsOfSameIdentity(onSink, cv);
                    if (left.Count > 0)
                        failures.Add($"[{label}] the hub deleted '{cv}', the sink still has '{string.Join("', '", left)}'");
                }

                if (failures.Count == 0 || sw.Elapsed >= waitForCleanup)
                {
                    return failures;
                }
                await Task.Delay(1000);
            }
        }

        private static async Task CollectListedVsCountAsync(IDocumentStore store, string id, string label, List<string> failures)
        {
            using var s = store.OpenAsyncSession();
            var listed = (await s.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 1000)).Count;
            var count = await s.Advanced.Revisions.GetCountForAsync(id);
            if (listed != count)
                failures.Add($"'{id}' {label}: {listed} revisions listed but the revision count is {count}");
        }

        // The standard form of a cluster-wide document's change vector, and the import form ("<sorted entries>,TRXN:…"), start
        // with "RAFT:" when the database group id sorts before node A's database id (entries are sorted by database id).
        // The gap tests need the opposite, so the hub database is created again until the group id sorts after it.
        private async Task<string> CreateHubDatabaseWhereRaftIsNotFirstAsync(string url, string baseName)
        {
            for (var attempt = 0; attempt < 20; attempt++)
            {
                var name = $"{baseName}{attempt}";
                using var store = CreateStore(url, name);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(PersistentRecord(name), replicationFactor: 2));
                var groupId = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(name))).Topology?.DatabaseTopologyIdBase64;
                var databaseId = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseId;
                Assert.True(groupId != null && databaseId != null, "the database record exposes its group id and the statistics its database id");
                if (string.CompareOrdinal(groupId, databaseId) > 0)
                    return name;

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(name, hardDelete: true));
            }
            throw new InvalidOperationException("no hub database with its group id sorted after node A's database id in 20 attempts");
        }

        private sealed class EchoTwins
        {
            public string HubUrl, SinkUrl, HubDb, SinkDb;
            public string[] Ids;
            public X509Certificate2 Certificate;
        }

        // The echo-twins flow up to the upgrade: on 6.2.9, two-way pull replication (secured); cluster-wide transactions written on the
        // sink come back from the hub rebuilt and stay as second 6.2 rows (echo twins); then the sink is upgraded in place to
        // current while the hub stays on 6.2.9.
        private async Task<EchoTwins> SetupTwoWayEchoTwinsOnCurrentSinkAsync()
        {
            var result = new EchoTwins
            {
                HubDb = GetDatabaseName() + "-hub",
                SinkDb = GetDatabaseName() + "-sink",
                Ids = new[] { "users/regular", "users/cluster-tx" }
            };
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };

            var pair = await GetSecuredPairOn62Async();
            (result.HubUrl, result.SinkUrl, result.Certificate) = (pair.Hub.Url, pair.Sink.Url, pair.Admin);

            using (var hubStore = await CreatePersistentStoreAsync(pair.Hub.Url, result.HubDb, pair.Admin))
            using (var sinkStore = await CreatePersistentStoreAsync(pair.Sink.Url, result.SinkDb, pair.Admin))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupTwoWayPullAsync(hubStore, sinkStore, pair.Hub.Url, pair.Pull);

                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(sinkStore, id, clusterWide, from: 0, count: 3);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(hubStore, id, "v2", timeoutMs: 60_000);
                // the hub stored the sink's revisions with their documents (waited for above); it sends them back in etag order, so
                // a marker written on the hub now reaches the sink after the echo
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "echo-barrier");

                var onSink = await RevisionCvsAsync(sinkStore, "users/cluster-tx");
                Assert.True(onSink.Count > 3, "precondition: the 6.2 echo left twins of the cluster-wide revisions on the sink");
            }

            await UpgradeServerAsync("current", pair.Sink, customSettings: pair.Settings);
            return result;
        }

        // replication sends in etag order, so a marker written after the changes arrives after them
        private static async Task WriteMarkerAndWaitAsync(IDocumentStore source, IDocumentStore destination, string name, int timeoutMs = 60_000)
        {
            using (var s = source.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Name = name }, "sync/marker");
                await s.SaveChangesAsync();
            }
            await WaitForDocumentNameAsync(destination, "sync/marker", name, timeoutMs);
        }

        private static async Task RemoveSinkPullTaskAsync(IDocumentStore sinkStore)
        {
            var record = await sinkStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(sinkStore.Database));
            foreach (var task in record.SinkPullReplications)
                await sinkStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(task.TaskId, OngoingTaskType.PullReplicationAsSink));
        }

        // Exports the given item types from the source database and imports them into the destination store; returns the import result.
        private async Task<SmugglerResult> ExportImportAsync(string sourceUrl, string sourceDb, IDocumentStore destination, DatabaseItemType types,
            X509Certificate2 certificate = null)
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");
            var noRecord = DatabaseRecordItemType.None;
            // smuggler operations are tagged with the node, so this store keeps topology updates on (unlike CreateStore)
            using (var source = new DocumentStore { Urls = new[] { sourceUrl }, Database = sourceDb, Certificate = certificate })
            {
                source.Conventions.DisposeCertificate = false;
                source.Initialize();
                var export = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = types, OperateOnDatabaseRecordTypes = noRecord }, file);
                await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
            var import = await destination.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = types, OperateOnDatabaseRecordTypes = noRecord }, file);
            return await import.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromMinutes(1));
        }

        // The revisions the hub no longer has (compared with hubBefore, any change vector form) must be gone from the sink;
        // returns how many revisions the hub pruned (distinct revisions, not rows).
        // waitForCleanup: how long the sink may take to delete them (its background cleanup); the check repeats until then.
        private async Task<int> AssertPrunedRevisionsGoneAsync(IDocumentStore hubStore, IDocumentStore sinkStore, IEnumerable<string> ids, Dictionary<string, List<string>> hubBefore,
            TimeSpan waitForCleanup = default)
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var failures = new List<string>();
                var prunedTotal = 0;
                foreach (var id in ids)
                {
                    var onHub = await RevisionCvsAsync(hubStore, id);
                    var onSink = await RevisionCvsAsync(sinkStore, id);
                    var pruned = hubBefore[id].Where(cv => RevisionsOfSameIdentity(onHub, cv).Count == 0).ToList();
                    Assert.True(pruned.Count > 0, $"precondition: the hub pruned revisions of '{id}'");
                    var distinct = new List<string>();
                    foreach (var cv in pruned)
                    {
                        if (RevisionsOfSameIdentity(distinct, cv).Count == 0)
                            distinct.Add(cv);
                    }
                    prunedTotal += distinct.Count;
                    foreach (var cv in distinct)
                    {
                        var left = RevisionsOfSameIdentity(onSink, cv);
                        if (left.Count > 0)
                            failures.Add($"'{id}': the hub pruned '{cv}', the sink still has {left.Count} copy(ies): '{string.Join("', '", left)}'");
                    }
                }

                if (failures.Count == 0 || sw.Elapsed >= waitForCleanup)
                {
                    Assert.True(failures.Count == 0, string.Join("; ", failures));
                    return prunedTotal;
                }
                await Task.Delay(1000);
            }
        }

        private static async Task<List<string>> RevisionsWithAttachmentAsync(IDocumentStore store, string id)
        {
            var result = new List<string>();
            foreach (var cv in await RevisionCvsAsync(store, id))
            {
                if (await HasRevisionAttachmentAsync(store, id, cv))
                    result.Add(cv);
            }
            return result;
        }


        // Public-API form of AssertRowsMatchCount: the revisions listed for each document match its revision count.
        private static async Task AssertListedRevisionsMatchCountAsync(IDocumentStore store, IEnumerable<string> ids)
        {
            var failures = new List<string>();
            foreach (var id in ids)
                await CollectListedVsCountAsync(store, id, "", failures);
            Assert.True(failures.Count == 0, string.Join("; ", failures));
        }

        // Change vectors from the list that identify the same revision as `changeVector` (same DbId/etag set, any spelling).
        private static List<string> RevisionsOfSameIdentity(IEnumerable<string> changeVectors, string changeVector)
        {
            static string Key(string cv) => string.Join(",", cv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Select(e => e.Substring(e.IndexOf(':') + 1)) // drop the tag: "etag-dbid"
                .OrderBy(e => e, StringComparer.Ordinal));
            var wanted = Key(changeVector);
            return changeVectors.Where(cv => Key(cv) == wanted).ToList();
        }

        private static async Task<bool> HasRevisionAttachmentAsync(IDocumentStore store, string id, string revisionCv)
        {
            using var s = store.OpenAsyncSession();
            using var attachment = await s.Advanced.Attachments.GetRevisionAsync(id, "a.txt", revisionCv);
            return attachment != null;
        }

        private async Task WaitForRevisionCountAsync(string url, string database, IEnumerable<string> ids, int expected, int timeoutMs = 60_000)
        {
            var documentDatabase = await DatabaseAtAsync(url, database);
            var sw = Stopwatch.StartNew();
            var counts = "";
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var current = ids.Select(id => (id, documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisions(context, id, 0, 1000).Revisions.Length)).ToList();
                    counts = string.Join(", ", current.Select(c => $"'{c.id}': {c.Length}"));
                    if (current.All(c => c.Length == expected))
                        return;
                }
                await Task.Delay(250);
            }
            Assert.Fail($"{url}: expected {expected} revisions per document within {timeoutMs}ms, got {counts}");
        }

        // Revision rows of each document whose identity (the tag-blind hash) equals one of the given change vectors'.
        private List<string> RevisionsWithIdentityOf(string url, string database, Dictionary<string, List<string>> changeVectors)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var revisionsStorage = documentDatabase.DocumentsStorage.RevisionsStorage;
            var found = new List<string>();

            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = new Table(revisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                foreach (var (id, cvs) in changeVectors)
                {
                    var wanted = cvs.ToDictionary(cv => Identity(context, cv), cv => cv);
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
                    using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
                    {
                        foreach (var read in table.SeekForwardFrom(revisionsStorage.RevisionsSchema.Indexes[IdAndEtagSlice], prefix, 0, startsWith: true))
                        {
                            var row = read.Result.Reader;
                            var cv = RevisionsStorage.ReadChangeVectorFromTvr(context, ref row).ToString();
                            if (wanted.TryGetValue(Identity(context, cv), out var expected))
                                found.Add($"'{id}': '{cv}' (same revision as '{expected}')");
                        }
                    }
                }
            }

            return found;
        }

        private static string Identity(DocumentsOperationContext context, string changeVector)
        {
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, context.GetChangeVector(changeVector), out RevisionKey key, strict: false))
                return key.PrefixedHash.ToString();
        }

        // 6.2-shaped (9-field) revision tombstones of the given documents.
        private int CountLegacyRevisionTombstones(string url, string database, IEnumerable<string> ids) =>
            CountRevisionTombstones(url, database, ids, legacyOnly: true);

        // Revision tombstones of the given documents (with legacyOnly, only the 6.2-shaped 9-field ones).
        private int CountRevisionTombstones(string url, string database, IEnumerable<string> ids, bool legacyOnly = false)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var count = 0;

            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = context.Transaction.InnerTransaction.OpenTable(documentDatabase.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
                if (table == null)
                    return 0;

                foreach (var id in ids)
                {
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
                    using (documentDatabase.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
                    {
                        foreach (var (_, holder) in table.SeekByPrimaryKeyPrefix(prefix, Slices.Empty, 0))
                        {
                            if (table.IsOwned(holder.Reader.Id) && (legacyOnly == false || holder.Reader.Count <= (int)TombstoneTable.RevisionVersion))
                                count++;
                        }
                    }
                }
            }

            return count;
        }

        // Change vectors of the document's revision attachments whose revision does not exist (by identity).
        private List<string> OrphanRevisionAttachments(string url, string database, string id)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var orphans = new List<string>();
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var revisions = new HashSet<string>();
                var revisionsTable = new Table(documentDatabase.DocumentsStorage.RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                var attachments = context.Transaction.InnerTransaction.OpenTable(documentDatabase.DocumentsStorage.AttachmentsStorage.AttachmentsSchema,
                    AttachmentsMetadataSlice);
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
                using (documentDatabase.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
                {
                    foreach (var read in revisionsTable.SeekForwardFrom(documentDatabase.DocumentsStorage.RevisionsStorage.RevisionsSchema.Indexes[IdAndEtagSlice], prefix, 0, startsWith: true))
                    {
                        var row = read.Result.Reader;
                        revisions.Add(Identity(context, RevisionsStorage.ReadChangeVectorFromTvr(context, ref row).ToString()));
                    }

                    var revisionAttachmentsPrefix = prefix + "r" + (char)SpecialChars.RecordSeparator;
                    using (Slice.From(context.Allocator, revisionAttachmentsPrefix, out Slice attachmentsPrefix))
                    {
                        foreach (var (key, holder) in attachments.SeekByPrimaryKeyPrefix(attachmentsPrefix, Slices.Empty, 0))
                        {
                            var rest = key.ToString().Substring(attachmentsPrefix.Size);
                            var segment = rest.Substring(0, rest.IndexOf((char)SpecialChars.RecordSeparator));
                            var changeVector = holder.Reader.Count > (int)AttachmentsTable.RevisionVersion
                                ? DocumentsStorage.TableValueToString(context, (int)AttachmentsTable.RevisionVersion, ref holder.Reader).ToString()
                                : segment;
                            if (revisions.Contains(Identity(context, changeVector)) == false)
                                orphans.Add(changeVector);
                        }
                    }
                }
            }

            return orphans;
        }

        // Revision attachments and revision-attachment tombstones of these documents whose key still embeds a change vector (6.2 keys).
        private List<string> RevisionAttachmentRowsUnder62Keys(string url, string database, IEnumerable<string> ids)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var rows = new List<string>();
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var tx = context.Transaction.InnerTransaction;
                var tables = new[]
                {
                    ("attachment", tx.OpenTable(documentDatabase.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice)),
                    ("attachment tombstone", tx.OpenTable(documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice))
                };
                foreach (var id in ids)
                {
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
                    using (documentDatabase.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
                    using (Slice.From(context.Allocator, prefix + "r" + (char)SpecialChars.RecordSeparator, out Slice revisionAttachmentsPrefix))
                    {
                        foreach (var (label, table) in tables)
                        {
                            if (table == null)
                                continue;
                            foreach (var (key, _) in table.SeekByPrimaryKeyPrefix(revisionAttachmentsPrefix, Slices.Empty, 0))
                            {
                                var rest = key.ToString().Substring(revisionAttachmentsPrefix.Size);
                                var segment = rest.Substring(0, rest.IndexOf((char)SpecialChars.RecordSeparator));
                                if (segment.Length != RevisionsStorage.RevisionKeySize)
                                    rows.Add($"'{id}' {label} under '{segment}'");
                            }
                        }
                    }
                }
            }

            return rows;
        }

        // The per-document revisions count matches the rows actually stored (twins included).
        private void AssertRowsMatchCount(string url, string database, IEnumerable<string> ids)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var id in ids)
                {
                    var (revisions, count) = documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisions(context, id, 0, 1000);
                    Assert.True(revisions.Length == count, $"'{id}': {revisions.Length} revision rows but the count is {count}");
                }
            }
        }

        // The document's revision rows still under a 6.2 key (no FullChangeVector field), by change vector.
        private List<string> RevisionRowsUnder62Keys(string url, string database, string id)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var revisionsStorage = documentDatabase.DocumentsStorage.RevisionsStorage;
            var legacy = new List<string>();
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
            using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
            {
                var table = new Table(revisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                foreach (var read in table.SeekForwardFrom(revisionsStorage.RevisionsSchema.Indexes[IdAndEtagSlice], prefix, 0, startsWith: true))
                {
                    var row = read.Result.Reader;
                    if (row.Count <= (int)RevisionsTable.FullChangeVector)
                        legacy.Add(RevisionsStorage.ReadChangeVectorFromTvr(context, ref row).ToString());
                }
            }
            return legacy;
        }

        // The sender records the destination's failure reply (as production logged it); returns the first one, for an assertion message.
        private static async Task<string> OutgoingReplicationErrorAsync(IDocumentStore hub)
        {
            var stats = await hub.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
            var error = stats.Outgoing
                .SelectMany(o => o.Performance.SelectMany(p => p.Errors ?? new List<ReplicationError>()).Select(e => (o.Destination, e)))
                .FirstOrDefault();
            return error.e == null ? "no outgoing replication errors recorded" : $"outgoing replication error to {error.Destination}: {error.e.Error}";
        }

        private static Task ConfigureUsersRevisionsAsync(IDocumentStore store, int minToKeep)
        {
            return store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 100 },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = minToKeep }
                }
            }));
        }

        // External replication from source to a database on one of urls; returns its task id.
        private static async Task<long> ReplicateToAsync(IDocumentStore source, string database, params string[] urls)
        {
            var connectionString = "cs-" + database;
            await source.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = connectionString, Database = database, TopologyDiscoveryUrls = urls
            }));
            return (await source.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(database, connectionString)))).TaskId;
        }


        // Users keep only their newest minToKeep revisions, enforced now: the older ones are deleted.
        private static async Task PruneUsersRevisionsAsync(IDocumentStore store, int minToKeep)
        {
            await ConfigureUsersRevisionsAsync(store, minToKeep);
            var enforce = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation());
            await enforce.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        }

        // Right after a rolling restart the cluster may still be electing a leader, so retry across the nodes for a while.
        private static async Task AddClusterNodeAsync(string[] clusterUrls, string newNodeUrl)
        {
            Exception last = null;
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(60))
            {
                foreach (var url in clusterUrls)
                {
                    using var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, null, DocumentConventions.DefaultForServer);
                    using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        try
                        {
                            await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(newNodeUrl), context);
                            return;
                        }
                        catch (Exception e)
                        {
                            last = e;
                        }
                    }
                }
                await Task.Delay(1000);
            }
            throw new InvalidOperationException($"failed to add {newNodeUrl} to the cluster", last);
        }

        // Adds the store's database to a node that just joined the cluster and returns the node's tag. Joining is asynchronous
        // (AddClusterNodeAsync returns once the join is accepted), so the tag is read from the cluster topology once the node is a
        // member, and the database is added to that node explicitly instead of letting the server pick one.
        private static async Task<string> AddDatabaseToNewClusterNodeAsync(IDocumentStore store, string clusterUrl, string newNodeUrl)
        {
            string tag = null;
            var sw = Stopwatch.StartNew();
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(clusterUrl, null, DocumentConventions.DefaultForServer))
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                while (tag == null && sw.Elapsed < TimeSpan.FromSeconds(60))
                {
                    var command = new GetClusterTopologyCommand();
                    await requestExecutor.ExecuteAsync(command, context);
                    tag = command.Result.Topology.Members
                        .FirstOrDefault(x => string.Equals(x.Value.TrimEnd('/'), newNodeUrl.TrimEnd('/'), StringComparison.OrdinalIgnoreCase)).Key;
                    if (tag == null)
                        await Task.Delay(250);
                }
            }

            Assert.True(tag != null, $"{newNodeUrl} did not become a member of the cluster at {clusterUrl}");
            await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database, tag));
            return tag;
        }

        private static async Task<DocumentStore> CreatePersistentStoreAsync(string url, string database, X509Certificate2 certificate = null)
        {
            var store = CreateStore(url, database, certificate);
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(PersistentRecord(database)));
            return store;
        }

        private static DocumentStore CreateStore(string url, string database, X509Certificate2 certificate = null)
        {
            var store = new DocumentStore { Urls = new[] { url }, Database = database, Certificate = certificate };
            store.Conventions.DisableTopologyUpdates = true;
            store.Conventions.DisposeCertificate = false;
            store.Initialize();
            return store;
        }

        // Upgraded nodes are in Servers; nodes from GetNewServer are only in ServersForDisposal.
        private RavenServer ServerAt(string url) => Servers.Concat(ServersForDisposal).First(s => s.WebUrl == url);

        private Task<DocumentDatabase> DatabaseAtAsync(string url, string database) =>
            ServerAt(url).ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

        private const string HubDefName = "rev-pull";

        private static async Task<long> SetupHubToSinkAsync(IDocumentStore hub, IDocumentStore sink, string[] hubUrls = null, string mentor = null)
        {
            var result = await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(HubDefName)
            {
                MentorNode = mentor,
                PinToMentorNode = mentor != null
            }));
            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = $"cs-{hub.Database}",
                Database = hub.Database,
                TopologyDiscoveryUrls = hubUrls ?? new[] { hub.Urls[0] }
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = $"cs-{hub.Database}",
                HubName = HubDefName,
                Mode = PullReplicationMode.HubToSink
            }));
            return result.TaskId;
        }

        private static async Task WriteVersionsAsync(IDocumentStore store, string id, bool clusterWide, int from, int count)
        {
            for (var i = from; i < from + count; i++)
            {
                var options = clusterWide ? new SessionOptions { TransactionMode = TransactionMode.ClusterWide } : new SessionOptions();
                using var session = store.OpenAsyncSession(options);
                var user = await session.LoadAsync<User>(id);
                if (user == null)
                    await session.StoreAsync(user = new User(), id);
                user.Name = "v" + i;
                await session.SaveChangesAsync();
            }
        }

        private static async Task<List<string>> RevisionCvsAsync(IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 100);
            return metadata.Select(m => m.GetString("@change-vector")).ToList();
        }

        // Revision rows that share a revision identity (the tag-blind hash) with another row of the same document.
        private List<string> TwinRevisionRows(string url, string database, IEnumerable<string> ids)
        {
            var documentDatabase = DatabaseAtAsync(url, database).Result;
            var revisionsStorage = documentDatabase.DocumentsStorage.RevisionsStorage;
            var twins = new List<string>();

            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = new Table(revisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                foreach (var id in ids)
                {
                    var identities = new Dictionary<string, string>();
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice lowerId))
                    using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
                    {
                        foreach (var read in table.SeekForwardFrom(revisionsStorage.RevisionsSchema.Indexes[IdAndEtagSlice], prefix, 0, startsWith: true))
                        {
                            var row = read.Result.Reader;
                            var cv = RevisionsStorage.ReadChangeVectorFromTvr(context, ref row).ToString();
                            var identity = Identity(context, cv);
                            if (identities.TryGetValue(identity, out var other))
                                twins.Add($"'{id}': '{cv}' twins '{other}'");
                            else
                                identities[identity] = cv;
                        }
                    }
                }
            }

            return twins;
        }
    }
}
