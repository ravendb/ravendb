using System.Collections.Generic;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationRewindTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationRewindTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B1_RewindInPlace_RestoresConversationState()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var beforeRewind = await GetDetailedMessagesAsync(store, "chats/1");
            AssertExactMessageCount(beforeRewind, 3, "before rewind: 3 turns");

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            var afterRewind = await GetDetailedMessagesAsync(store, "chats/1");
            // Snapshot was before turn 2, so rewound state has 1 turn
            AssertExactMessageCount(afterRewind, 1, "after rewind to before turn 2");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B2_RewindInPlace_DeletesOrphanedSubConversations()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub1");
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub2");

            Assert.True(DocumentExists(store, "chats/1/sub1"));
            Assert.True(DocumentExists(store, "chats/1/sub2"));

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            Assert.False(DocumentExists(store, "chats/1/sub1"));
            Assert.False(DocumentExists(store, "chats/1/sub2"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B4_RewindInPlace_PreservesHistoryDocuments()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            // Run enough turns to trigger truncation (threshold = 5 messages, after truncation = 3)
            // System prompt = 1 msg, each turn = 2 msgs
            // After 3 turns: 1 + 6 = 7 messages => exceeds 5 => truncation triggered => LinkedConversations populated
            for (int i = 1; i <= 3; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }
            var r4 = await RunTurnWithAgentAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true, agent);

            List<string> historyIds = GetLinkedConversations(store, "chats/1");
            Assert.NotEmpty(historyIds);

            var forkResult = await store.AI.ForkConversationAsync(r4.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // History documents should still exist after rewind
            foreach (string historyId in historyIds)
            {
                Assert.True(DocumentExists(store, historyId));
            }
        }
    }
}
