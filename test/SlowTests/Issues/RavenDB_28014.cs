using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues
{
    public class RavenDB_28014 : ReplicationTestBase
    {
        public RavenDB_28014(ITestOutputHelper output) : base(output)
        {
        }

        // The tests hold the merger with a blocking command while two client batches queue up behind it. With the default limit
        // the merger closes the transaction right after the blocker, and the two client batches do not end up in one transaction.
        private static readonly string MergerMaxTxTimeKey = RavenConfiguration.GetKey(x => x.TransactionMergerConfiguration.MaxTimeToWaitForPreviousTx);

        private class Item
        {
            public int Version { get; set; }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task WriteCommittedByMergerRerun_ShouldBeInDatabaseChangeVector()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Settings[MergerMaxTxTimeKey] = "60000"
            }))
            {
                var database = await GetDatabase(store.Database);

                var etag = await WriteViaMergerRerunAsync(database, store, "items/1", new Item { Version = 1 });

                var ownEtagInDatabaseChangeVector = GetOwnEtagInDatabaseChangeVector(database);
                Assert.True(ownEtagInDatabaseChangeVector >= etag,
                    $"Document 'items/1' was committed with etag {etag}, but the database change vector is '{GetDatabaseChangeVector(database)}'");
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task WriteCommittedByMergerRerun_ShouldReachAllMembers()
        {
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "600",
                [MergerMaxTxTimeKey] = "60000",
            };

            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false, customSettings: settings);
            var databaseName = GetDatabaseName();
            await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);

            // writer: takes the client writes; victim: misses a version; relay: the third member
            var writer = nodes[0];
            var victim = nodes[1];
            var relay = nodes[2];

            using var writerStore = OpenStore(writer, databaseName);
            using var victimStore = OpenStore(victim, databaseName);
            using var relayStore = OpenStore(relay, databaseName);

            var writerDb = await writer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            var victimDb = await victim.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            // 0. seed and let the database group settle
            using (var session = writerStore.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Version = 1 }, "items/1");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Item>(victimStore, "items/1", t => t.Version == 1));
            Assert.True(WaitForDocument<Item>(relayStore, "items/1", t => t.Version == 1));
            Assert.True(await WaitForChangeVectorInClusterAsync(nodes, databaseName));

            // Slow apply of incoming document batches on the victim and on the writer.
            // The hook runs after a document batch is read and before it is applied; heartbeats are not affected.
            using var victimGate = new ManualResetEventSlim(false);
            using var writerGate = new ManualResetEventSlim(false);
            var victimBlocked = 0;
            victim.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DelayIncomingReplication = token =>
            {
                Interlocked.Increment(ref victimBlocked);
                victimGate.Wait(token);
            };
            writer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DelayIncomingReplication = token => writerGate.Wait(token);

            try
            {
                // 1. writer->victim and relay->victim are both busy with a batch that the victim is slow to apply.
                //    writer's batch: a first write committed by the rerun path; relay's batch: a write on the relay.
                await WriteViaMergerRerunAsync(writerDb, writerStore, "others/1", new Item { Version = 1 });
                Assert.True(WaitForDocument<Item>(relayStore, "others/1", _ => true));
                Assert.True(await WaitForValueAsync(() => Volatile.Read(ref victimBlocked) >= 1, true));
                using (var session = relayStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item { Version = 1 }, "others/2");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForValueAsync(() => Volatile.Read(ref victimBlocked) >= 2, true));

                // 2. second write committed by the rerun path: the version that the victim will miss
                var etag = await WriteViaMergerRerunAsync(writerDb, writerStore, "items/1", new Item { Version = 2 });
                Assert.True(WaitForDocument<Item>(relayStore, "items/1", t => t.Version == 2));
                Output.WriteLine($"items/1 v2 etag on writer: {etag}, writer database change vector: {GetDatabaseChangeVector(writerDb)}");

                // 3. the victim heartbeats the writer; the writer answers with its database change vector and its last etag.
                await WaitForValueAsync(() => GetEtagInDatabaseChangeVector(victimDb, writerDb.DbBase64Id) >= etag, true, timeout: 5_000);
                Output.WriteLine($"victim database change vector before it has items/1 v2: {GetDatabaseChangeVector(victimDb)}");

                using (var session = victimStore.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>("items/1");
                    Assert.Equal(1, item.Version);
                }

                // 4. the victim's slow apply finishes
                victimGate.Set();
                Assert.True(WaitForDocument<Item>(victimStore, "others/1", _ => true));
                writerGate.Set();
            }
            finally
            {
                victimGate.Set();
                writerGate.Set();
                victim.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DelayIncomingReplication = null;
                writer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().DelayIncomingReplication = null;
            }

            // 5. replication keeps running: a normal write on the writer reaches everybody
            using (var session = writerStore.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Version = 1 }, "markers/1");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Item>(victimStore, "markers/1", _ => true));
            Assert.True(WaitForDocument<Item>(relayStore, "markers/1", _ => true));
            Assert.True(await WaitForChangeVectorInClusterAsync(nodes, databaseName));
            await Task.Delay(TimeSpan.FromSeconds(5)); // several heartbeat intervals

            var versions = new Dictionary<string, int>();
            foreach (var (name, s) in new[] { ("writer", writerStore), ("victim", victimStore), ("relay", relayStore) })
            {
                using var session = s.OpenAsyncSession();
                versions[name] = (await session.LoadAsync<Item>("items/1")).Version;
            }

            Output.WriteLine($"items/1 versions: writer={versions["writer"]}, relay={versions["relay"]}, victim={versions["victim"]}");
            Assert.Equal(2, versions["writer"]);
            Assert.Equal(2, versions["relay"]);
            Assert.Equal(2, versions["victim"]);
        }

        private static IDocumentStore OpenStore(RavenServer server, string databaseName)
        {
            return new DocumentStore
            {
                Urls = new[] { server.WebUrl },
                Database = databaseName,
                Conventions = { DisableTopologyUpdates = true }
            }.Initialize();
        }

        // The client write is merged into one transaction with a client write that fails with a ConcurrencyException,
        // the merged transaction throws and the merger re-runs every command in its own transaction.
        private async Task<long> WriteViaMergerRerunAsync(DocumentDatabase database, IDocumentStore store, string id, Item entity)
        {
            using var release = new ManualResetEventSlim(false);
            using var started = new ManualResetEventSlim(false);
            var blocker = database.TxMerger.Enqueue(new BlockingCommand(started, release));
            Assert.True(started.Wait(TimeSpan.FromSeconds(15)));

            using var goodSession = store.OpenAsyncSession();
            await goodSession.StoreAsync(entity, id);
            var good = goodSession.SaveChangesAsync();

            Assert.True(await WaitForValueAsync(() => QueuedClientBatches(database) >= 1, true));

            using var badSession = store.OpenAsyncSession();
            await badSession.StoreAsync(new Item(), "A:1-AAAAAAAAAAAAAAAAAAAAAA", "conflicts/" + id);
            var bad = badSession.SaveChangesAsync();

            Assert.True(await WaitForValueAsync(() => QueuedClientBatches(database) >= 2, true));
            release.Set();

            await blocker;
            await good;
            await Assert.ThrowsAsync<ConcurrencyException>(() => bad);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return database.DocumentsStorage.Get(context, id).Etag;
            }
        }

        private static readonly FieldInfo MergerQueueField =
            typeof(AbstractTransactionOperationsMerger<DocumentsOperationContext, DocumentsTransaction>).GetField("_operations", BindingFlags.Instance | BindingFlags.NonPublic);

        private static int QueuedClientBatches(DocumentDatabase database)
        {
            var queue = (IEnumerable<object>)MergerQueueField.GetValue(database.TxMerger);
            return queue.Count(x => x is MergedBatchCommand);
        }

        private static string GetDatabaseChangeVector(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return DocumentsStorage.GetDatabaseChangeVector(context);
            }
        }

        private static long GetOwnEtagInDatabaseChangeVector(DocumentDatabase database) => GetEtagInDatabaseChangeVector(database, database.DbBase64Id);

        private static long GetEtagInDatabaseChangeVector(DocumentDatabase database, string dbId) =>
            ChangeVectorUtils.GetEtagById(GetDatabaseChangeVector(database), dbId);

        private class BlockingCommand : DocumentMergedTransactionCommand
        {
            private readonly ManualResetEventSlim _started;
            private readonly ManualResetEventSlim _release;

            public BlockingCommand(ManualResetEventSlim started, ManualResetEventSlim release)
            {
                _started = started;
                _release = release;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                _started.Set();
                _release.Wait(TimeSpan.FromSeconds(30));
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                throw new NotSupportedException();
            }
        }
    }
}
