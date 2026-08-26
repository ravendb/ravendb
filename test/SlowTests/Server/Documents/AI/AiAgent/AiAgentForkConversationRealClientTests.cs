using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    /// <summary>
    /// End-to-end fork/snapshot tests that exercise the feature through the real Raven client API
    /// (<see cref="AiConversation.RunAsync{TAnswer}"/>, <c>store.AI.CreateSnapshotAsync</c>,
    /// <c>store.AI.ForkConversationAsync</c>, ...) against a live LLM connection.
    ///
    /// Unlike the <c>AiAgentForkConversation*Tests</c> classes — which drive the server-side
    /// conversation handler directly with a mocked LLM (<c>RunTurnAsync</c>) — these tests go over
    /// the wire exactly as a user would. They therefore require a configured AI integration and are
    /// skipped automatically when no connection is available (see <see cref="RavenGenAiDataAttribute"/>).
    /// </summary>
    public class AiAgentForkConversationRealClientTests : RavenTestBase
    {
        public AiAgentForkConversationRealClientTests(ITestOutputHelper output) : base(output)
        {
        }

        private class AssistantAnswer
        {
            public static readonly AssistantAnswer Instance = new();

            public string Answer = "A short answer to the user's question";
        }

        private static async Task<string> CreateAssistantAgentAsync(IDocumentStore store, GenAiConfiguration config)
        {
            var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName,
                "You are a helpful assistant. Reply with a short answer to the user's question.");

            return (await store.AI.CreateAgentAsync(agent, AssistantAnswer.Instance)).Identifier;
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SnapshotToken_IsPopulatedAcrossRunAsyncTurns(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions { SnapshotBeforeRunning = true });

            // Turn 1: brand-new conversation, there is nothing prior to snapshot.
            chat.SetUserPrompt("Say hello.");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);
            Assert.NotNull(r1.Answer);
            Assert.Null(r1.SnapshotToken);

            // Turn 2: the state before this turn (i.e. after turn 1) is snapshotted.
            chat.SetUserPrompt("Say goodbye.");
            var r2 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r2.Status);
            Assert.False(string.IsNullOrEmpty(r2.SnapshotToken));

            // Turn 3: another snapshot, distinct from the previous one.
            chat.SetUserPrompt("Say thanks.");
            var r3 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r3.Status);
            Assert.False(string.IsNullOrEmpty(r3.SnapshotToken));
            Assert.NotEqual(r2.SnapshotToken, r3.SnapshotToken);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Fork_FromRunAsyncSnapshotToken_CreatesIndependentConversation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);

            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions { SnapshotBeforeRunning = true });

            chat.SetUserPrompt("What is the capital of France?");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);

            // The token returned on turn 2 captures the state after turn 1.
            chat.SetUserPrompt("And the capital of Italy?");
            var r2 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r2.Status);
            Assert.False(string.IsNullOrEmpty(r2.SnapshotToken));

            // Fork that point into a brand-new conversation id.
            var fork = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", fork.ConversationId);
            Assert.False(string.IsNullOrEmpty(fork.ChangeVector));

            // The fork holds the earlier (fewer-turn) state; the original kept going.
            var forkedMessages = await store.AI.GetConversationMessagesAsync("forked/1");
            var originalMessages = await store.AI.GetConversationMessagesAsync(chat.Id);
            Assert.NotEmpty(forkedMessages.Messages);
            Assert.True(forkedMessages.Messages.Count < originalMessages.Messages.Count,
                $"Expected fork ({forkedMessages.Messages.Count}) to have fewer messages than original ({originalMessages.Messages.Count}).");

            // The fork is a real, independent conversation that can continue on its own.
            var forkedChat = store.AI.Conversation(agentId, fork.ConversationId,
                new AiConversationCreationOptions { SnapshotBeforeRunning = true }, fork.ChangeVector);
            forkedChat.SetUserPrompt("What did I ask you first?");
            var rf = await forkedChat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, rf.Status);
            Assert.NotNull(rf.Answer);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CreateSnapshotThenFork_ThroughRealClient(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);

            // Run a turn without the automatic snapshot flag ...
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Remember my favorite color is blue.");
            var r = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.Null(r.SnapshotToken);

            // ... and snapshot the current state on demand instead.
            var snapshot = await store.AI.CreateSnapshotAsync(chat.Id);
            Assert.NotNull(snapshot);
            Assert.False(string.IsNullOrEmpty(snapshot.Token));

            var fork = await store.AI.ForkConversationAsync(snapshot.Token, "forked/2");
            Assert.Equal("forked/2", fork.ConversationId);
            Assert.False(string.IsNullOrEmpty(fork.ChangeVector));

            var forkedMessages = await store.AI.GetConversationMessagesAsync("forked/2");
            Assert.NotEmpty(forkedMessages.Messages);

            // The forked conversation can continue from the snapshotted state.
            var forkedChat = store.AI.Conversation(agentId, fork.ConversationId,
                new AiConversationCreationOptions(), fork.ChangeVector);
            forkedChat.SetUserPrompt("What is my favorite color?");
            var rf = await forkedChat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, rf.Status);
            Assert.NotNull(rf.Answer);
        }
    }
}
