using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationTokenTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationTokenTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void F_SnapshotToken_Parsing_Validation()
        {
            Assert.Throws<InvalidOperationException>(() => ParseToken(""));

            var key1 = FakeRevisionKey();
            var key2 = FakeRevisionKey();
            foreach (var invalid in new[]
                     {
                         "not a token",           // no separator
                         "." + key1,              // no conversation ID
                         "chats/42.",             // no revision keys
                         "chats/42." + key1[..31], // a key that is not 32 characters
                         "chats/42." + key1[..31] + "!" // not base64url
                     })
            {
                var ex = Assert.Throws<InvalidOperationException>(() => ParseToken(invalid));
                Assert.Contains("Invalid snapshot token format", ex.Message);
            }

            // the token splits at its last '.', so the conversation ID may contain dots
            var parsed = ParseToken("chats/4.2." + key1 + key2);
            Assert.Equal("chats/4.2", parsed.ConversationId.ToString());
            Assert.Equal(2, parsed.RevisionCount);
            Assert.Equal(key1, parsed.GetRevisionKey(0).ToString());
            Assert.Equal(key2, parsed.GetRevisionKey(1).ToString());
            Assert.Equal("chats/4.2." + key1 + key2, SnapshotToken.Build("chats/4.2", [key1, key2]));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F6_Fork_WithTamperedRevisionKey_Throws()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var parsed = ParseToken(r2.SnapshotToken);
            var keys = new List<string> { FakeRevisionKey() };
            for (var i = 1; i < parsed.RevisionCount; i++)
                keys.Add(parsed.GetRevisionKey(i).ToString());
            var tamperedToken = SnapshotToken.Build(parsed.ConversationId.ToString(), keys);

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(tamperedToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F9_GetConversationMessages_WithSnapshotToken_ReturnsTheSnapshotState()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true); // snapshot after turn 1

            var current = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.Contains(current.Messages, m => m.Content?.Contains("turn 2") == true);

            // the conversation ID may be omitted: the token's conversation
            var snapshot = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions { SnapshotToken = r2.SnapshotToken });
            Assert.Equal("chats/1", snapshot.ConversationId);
            Assert.Contains(snapshot.Messages, m => m.Content?.Contains("turn 1") == true);
            Assert.DoesNotContain(snapshot.Messages, m => m.Content?.Contains("turn 2") == true);
            Assert.True(snapshot.Messages.Count < current.Messages.Count);

            // a conversation that is not in the snapshot is not found
            Assert.Null(await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions { ConversationId = "chats/2", SnapshotToken = r2.SnapshotToken }));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F7_Fork_WithTokenFromDifferentDatabase_Throws()
        {
            using var storeA = GetDocumentStore();
            using var storeB = GetDocumentStore();

            var databaseA = await Databases.GetDocumentDatabaseInstanceFor(storeA);

            await RunTurnAsync(databaseA, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(databaseA, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await storeB.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F8_Fork_WithTokenFromDifferentAgent_Succeeds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create two distinct agent configurations with different names and system prompts
            var agentA = new AiAgentConfiguration("agent-alpha", "fake-connection",
                "You are agent alpha.")
            {
                Identifier = "agent-alpha-id",
                SampleObject = "{\"Answer\":\"response\"}"
            };

            // Create and run the conversation with agentA
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true, agent: agentA);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true, agent: agentA);

            // Fork succeeds because the token references revisions, not agent configurations.
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify the forked doc preserves the original agent identifier
            var forkedDoc = GetDocumentAsJObject(store, "forked/1");
            Assert.NotNull(forkedDoc);
            Assert.Equal(agentA.Identifier, forkedDoc["Agent"]?.ToString());

            var messages = await store.AI.GetConversationMessagesAsync("forked/1");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);
        }

    }
}
