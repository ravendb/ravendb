using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationSubConversationTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationSubConversationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C1_Fork_AdjustsSubConversationIds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            CreateSubConversationDoc(store, "chats/1", "chats/1/Search/abc");

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify SubConversationIds via client API (exposed on GetConversationMessagesAsync)
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            Assert.NotNull(forkedMessages.SubConversationIds);
            Assert.NotEmpty(forkedMessages.SubConversationIds);
            Assert.Contains("forked/1/Search/abc", forkedMessages.SubConversationIds);
            Assert.DoesNotContain("chats/1/Search/abc", forkedMessages.SubConversationIds);

            Assert.True(DocumentExists(store, "forked/1/Search/abc"));
            Assert.True(DocumentExists(store, "chats/1/Search/abc"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void C2_Fork_PrefixReplacementIsExact()
        {
            Assert.Equal("new/id", ForkConversationCommand.AdjustId("chats/42", "chats/42", "new/id"));
            Assert.Equal("new/id/Search/abc", ForkConversationCommand.AdjustId("chats/42/Search/abc", "chats/42", "new/id"));
            Assert.Equal("new/id/chats/42/Search/abc",
                ForkConversationCommand.AdjustId("chats/42/chats/42/Search/abc", "chats/42", "new/id"));
            Assert.Equal("other/1/sub", ForkConversationCommand.AdjustId("other/1/sub", "chats/42", "new/id"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C4_Fork_CleanupUsesSubConversationIds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Create a rogue document not tracked in SubConversationIds
            PutRogueDocument(store, "chats/1/Rogue/xyz");

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // Rogue document survives because it is not tracked in SubConversationIds
            Assert.True(DocumentExists(store, "chats/1/Rogue/xyz"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C6_Fork_NestedSubConversations()
        {
            Assert.Equal("new/A/hash", ForkConversationCommand.AdjustId("old/A/hash", "old", "new"));
            Assert.Equal("new/A/hash/B/hash2", ForkConversationCommand.AdjustId("old/A/hash/B/hash2", "old", "new"));
            Assert.Equal("forked/1/sub1/hash/sub2/hash2",
                ForkConversationCommand.AdjustId("chats/1/sub1/hash/sub2/hash2", "chats/1", "forked/1"));

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            CreateSubConversationDoc(store, "chats/1", "chats/1/A");

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify via client API that SubConversationIds is adjusted
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            Assert.NotNull(forkedMessages.SubConversationIds);
            Assert.NotEmpty(forkedMessages.SubConversationIds);
            Assert.Contains("forked/1/A", forkedMessages.SubConversationIds);

            Assert.True(DocumentExists(store, "forked/1"));
            Assert.True(DocumentExists(store, "forked/1/A"));
        }
    }
}
