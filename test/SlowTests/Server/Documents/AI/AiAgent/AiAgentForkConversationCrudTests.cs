using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationCrudTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationCrudTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K1_CreateSnapshot_WithoutRunAsync()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: false);

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "from-snapshot");
            Assert.Equal("from-snapshot", forkResult.ConversationId);

            var messages = await store.AI.GetConversationMessagesAsync("from-snapshot");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K2_GetConversationSnapshots_ReturnsAll()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var snapshots = await store.AI.GetConversationSnapshotsAsync("chats/1");

            Assert.True(snapshots.Count >= 2, $"Expected at least 2 snapshots, got {snapshots.Count}");

            foreach (var snapshot in snapshots)
            {
                Assert.NotNull(snapshot.Token);
                Assert.True(snapshot.CreatedAt > DateTime.MinValue);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K3_GetConversationSnapshots_ExcludesPurgedRevisions()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var snapshotsBefore = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.True(snapshotsBefore.Count > 0, "Should have snapshots before purge");

            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            var snapshotsAfter = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.Equal(0, snapshotsAfter.Count);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K4_GetConversationSnapshots_EmptyForNoSnapshots()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: false);

            var snapshots = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.Empty(snapshots);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K5_PurgeConversationSnapshots_ConversationStillWorks()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);

            var r4 = await RunTurnAsync(database, "chats/1", "turn 4 after purge", snapshotBeforeRunning: false);
            Assert.NotNull(r4.Response);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K6_PurgeConversationSnapshots_DoesNotAffectOtherConversations()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/a", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/a", "turn 2", snapshotBeforeRunning: true);

            await RunTurnAsync(database, "chats/b", "turn 1", snapshotBeforeRunning: true);
            var rB2 = await RunTurnAsync(database, "chats/b", "turn 2", snapshotBeforeRunning: true);

            await store.AI.PurgeConversationSnapshotsAsync("chats/a");

            var forkResult = await store.AI.ForkConversationAsync(rB2.SnapshotToken, "forked-b");
            Assert.Equal("forked-b", forkResult.ConversationId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K7_PurgeConversationSnapshots_NonExistentConversation()
        {
            using var store = GetDocumentStore();

            // Should not throw
            await store.AI.PurgeConversationSnapshotsAsync("nonexistent/id");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K8_PurgeConversationSnapshots_DeletesRevisionsButPreservesConversation()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var r3 = await RunTurnAsync(database, "chats/1", "turn 3 for token", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);

            var preFork = await store.AI.ForkConversationAsync(r3.SnapshotToken, "pre-purge-fork");
            Assert.Equal("pre-purge-fork", preFork.ConversationId);

            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(r3.SnapshotToken, "post-purge-fork"));
            Assert.Contains("no longer exists", ex.Message);

            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);

            var r4 = await RunTurnAsync(database, "chats/1", "turn 4 after purge", snapshotBeforeRunning: false);
            Assert.NotNull(r4.Response);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K9_RevisionsRetention_LimitsSnapshotCount()
        {
            // Test: With revisions retention set to max 3, running 5 turns with snapshots
            //        should result in only 3 snapshots being available (oldest are purged).
            // Reasoning: The standard revisions retention policy on @conversations controls
            //            how many snapshots are kept.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Configure revisions: keep at most 3 for the @conversations collection
            await store.Maintenance.SendAsync(new Raven.Client.Documents.Operations.Revisions.ConfigureRevisionsOperation(
                new Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration
                {
                    Collections = new System.Collections.Generic.Dictionary<string, Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration>
                    {
                        [Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection] = new()
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 3
                        }
                    }
                }));

            // Run 5 turns with snapshots
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 5", snapshotBeforeRunning: true);

            // Only 3 snapshots should be available (retention purged the oldest)
            var snapshots = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.Equal(3, snapshots.Count);
        }
    }
}
