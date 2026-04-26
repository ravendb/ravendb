using System;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.AI.Agents;
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
            // Empty
            Assert.Throws<InvalidOperationException>(() => ParseToken(""));

            // Not JSON
            var ex1 = Assert.Throws<InvalidOperationException>(() => ParseToken("not json"));
            Assert.Contains("Invalid snapshot token format", ex1.Message);

            // Null Revisions
            var ex2 = Assert.Throws<InvalidOperationException>(() =>
                ParseToken("{\"ConversationId\":\"x\",\"Revisions\":null}"));
            Assert.Contains("missing or empty", ex2.Message);

            // Empty Revisions
            var ex3 = Assert.Throws<InvalidOperationException>(() =>
                ParseToken("{\"ConversationId\":\"x\",\"Revisions\":[]}"));
            Assert.Contains("missing or empty", ex3.Message);

            // Missing ConversationId
            var ex4 = Assert.Throws<InvalidOperationException>(() =>
                ParseToken("{\"Revisions\":[{\"Id\":\"x\",\"ChangeVector\":\"cv\"}]}"));
            Assert.Contains("missing ConversationId", ex4.Message);

            // Valid token parses correctly
            var token = "{\"ConversationId\":\"chats/42\",\"CreatedAt\":\"2026-04-23T14:30:00Z\",\"Revisions\":[{\"Id\":\"chats/42\",\"ChangeVector\":\"A:10-abc\"}]}";
            var parsed = ParseToken(token);
            Assert.Equal("chats/42", parsed.ConversationId);
            Assert.Single(parsed.Revisions);
            Assert.Equal("chats/42", parsed.Revisions[0].Id);
            Assert.Equal("A:10-abc", parsed.Revisions[0].ChangeVector);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F6_Fork_WithTamperedChangeVector_Throws()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var tokenJson = JObject.Parse(r2.SnapshotToken);
            tokenJson["Revisions"][0]["ChangeVector"] = "TAMPERED:99-fake";
            var tamperedToken = tokenJson.ToString();

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(tamperedToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
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
