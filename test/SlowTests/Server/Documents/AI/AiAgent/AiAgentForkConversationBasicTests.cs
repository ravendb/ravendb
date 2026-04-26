using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationBasicTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A1_SnapshotToken_IsNull_OnFirstTurn()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var result = await RunTurnAsync(database, "chats/1", "Hello", snapshotBeforeRunning: true);
            Assert.Null(result.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A2_SnapshotToken_IsPopulated_OnSubsequentTurns()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var r1 = await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            Assert.Null(r1.SnapshotToken);

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            Assert.NotNull(r2.SnapshotToken);

            var r3 = await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);
            Assert.NotEqual(r2.SnapshotToken, r3.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A3_ForkToNewId_CreatesIndependentConversation()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");

            Assert.Equal("forked/1", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            var originalMessages = await GetDetailedMessagesAsync(store, "chats/1");

            Assert.NotNull(forkedMessages);
            Assert.NotNull(originalMessages);

            // Snapshot was taken before turn 2, so forked has 1 turn: system + 1*2 = 3 messages
            AssertExactMessageCount(forkedMessages, 1, "forked from snapshot before turn 2");
            // Original has 3 turns: system + 3*2 = 7 messages
            AssertExactMessageCount(originalMessages, 3, "original after 3 turns");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A4_ForkToExplicitId()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked-chats/my-fork");
            Assert.Equal("forked-chats/my-fork", forkResult.ConversationId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A5_ForkToClusterIdentity()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forks|");

            Assert.NotNull(forkResult.ConversationId);
            Assert.StartsWith("forks/", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<object>(forkResult.ConversationId);
                Assert.NotNull(doc);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A6_ForkPreservesParameters()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            {
                var parameters = new Dictionary<string, object> { ["company"] = "companies/90-A" };

                await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true, parameters: parameters);
                var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true, parameters: parameters);

                var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
                Assert.Equal("forked/1", forkResult.ConversationId);

                var forkedDoc = GetDocumentAsJObject(store, "forked/1");
                Assert.NotNull(forkedDoc);
                var forkedParams = forkedDoc[nameof(ConversationDocument.Parameters)] as JObject;
                Assert.NotNull(forkedParams);

                var companyParam = forkedParams["company"] as JObject;
                Assert.NotNull(companyParam);
                Assert.Equal("companies/90-A", companyParam["Value"]?.ToString());
            }
        }
    }
}
