using System;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationIdTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationIdTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J1_Fork_NewConversationIdIsNull_GetsGuid()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, null);

            Assert.NotNull(forkResult.ConversationId);
            Assert.NotEmpty(forkResult.ConversationId);
            Assert.NotEqual("chats/1", forkResult.ConversationId);
            Assert.True(Guid.TryParse(forkResult.ConversationId, out _),
                $"Expected GUID format but got: {forkResult.ConversationId}");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J2_Fork_ToIdThatAlreadyExists_Overwrites()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "conversation 1 turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "conversation 1 turn 2", snapshotBeforeRunning: true);

            await RunTurnAsync(database, "chats/existing", "different conversation", snapshotBeforeRunning: false);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/existing");

            Assert.Equal("chats/existing", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            // Verify the document was overwritten and has the forked content via client API
            var messages = await GetDetailedMessagesAsync(store, "chats/existing");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);
            // Forked from before turn 2 = 1 turn: system + 2 = 3 messages
            AssertExactMessageCount(messages, 1, "overwritten with fork from snapshot before turn 2");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J3_Fork_ToIdWithDifferentPrefix()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/42", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/42", "turn 2", snapshotBeforeRunning: true);

            await CreateSubConversationDocAsync(database, "chats/42", "chats/42/Search/abc");

            var snapshot = await store.AI.CreateSnapshotAsync("chats/42");
            Assert.NotNull(snapshot);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "archives/fork-1");
            Assert.Equal("archives/fork-1", forkResult.ConversationId);

            Assert.True(DocumentExists(store, "archives/fork-1"));
            Assert.True(DocumentExists(store, "archives/fork-1/Search/abc"));
        }
    }
}
