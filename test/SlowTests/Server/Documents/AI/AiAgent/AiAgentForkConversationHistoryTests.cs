using System.Collections.Generic;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationHistoryTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationHistoryTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D1_Fork_SharesHistoryDocuments()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            // Run enough turns to trigger truncation and populate LinkedConversations
            for (int i = 1; i <= 4; i++)
            {
                await RunTurnAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent: agent);
            }

            // Take a snapshot after history docs have been created
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            var originalLinked = GetLinkedConversations(store, "chats/1");
            var forkedLinked = GetLinkedConversations(store, "forked/1");

            Assert.NotEmpty(originalLinked);
            Assert.NotEmpty(forkedLinked);

            var originalSet = new HashSet<string>(originalLinked);
            var forkedSet = new HashSet<string>(forkedLinked);

            Assert.True(forkedSet.IsSubsetOf(originalSet),
                "Forked history IDs should be a subset of the original's history IDs");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D2_Fork_ThenDeleteOriginal_HistoryStillAccessible()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            for (int i = 1; i <= 4; i++)
            {
                await RunTurnAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent: agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            List<string> forkHistoryIds = GetLinkedConversations(store, "forked/1");
            Assert.NotEmpty(forkHistoryIds);

            // Delete the original conversation
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("chats/1");
                await session.SaveChangesAsync();
            }

            // History docs should still exist
            foreach (string historyId in forkHistoryIds)
            {
                Assert.True(DocumentExists(store, historyId));
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D3_Fork_TwiceFromSameToken_BothShareHistory()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            for (int i = 1; i <= 4; i++)
            {
                await RunTurnAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent: agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            var forkA = await store.AI.ForkConversationAsync(snapshot.Token, "fork-a");
            Assert.Equal("fork-a", forkA.ConversationId);

            var forkB = await store.AI.ForkConversationAsync(snapshot.Token, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            var setA = new HashSet<string>(GetLinkedConversations(store, "fork-a"));
            var setB = new HashSet<string>(GetLinkedConversations(store, "fork-b"));

            Assert.NotEmpty(setA);
            Assert.NotEmpty(setB);
            Assert.True(setA.SetEquals(setB), "Both forks should share the same history documents");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D4_Fork_WhenSomeHistoryDocsDeleted()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            // Run enough turns to trigger truncation and create history documents
            for (int i = 1; i <= 6; i++)
            {
                await RunTurnAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent: agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            // Verify that truncation actually produced history documents
            var historyIds = GetLinkedConversations(store, "chats/1");
            Assert.NotEmpty(historyIds);

            // Delete one history doc to simulate expiration
            DeleteDocument(store, historyIds[0]);

            // Fork should still work even with a missing history doc
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);
        }
    }
}
