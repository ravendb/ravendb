using System;
using System.Threading.Tasks;
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
    /// <c>chat.CreateSnapshotAsync</c>, <c>store.AI.ForkConversationAsync</c>, ...) against a live LLM connection.
    ///
    /// Unlike the <c>AiAgentForkConversation*Tests</c> classes — which drive the server-side conversation
    /// handler directly with a mocked LLM (<c>RunTurnAsync</c>) — these tests go over the wire exactly as a
    /// user would. Tests that call <c>RunAsync</c> require a configured AI integration and are skipped
    /// automatically when no connection is available (see <see cref="RavenGenAiDataAttribute"/>).
    /// </summary>
    public class AiAgentForkConversationRealClientTests : AiAgentForkConversationTestBase
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

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CreateSnapshotAsync_ReBaselinesChangeVector_AndContinuesOnSameInstance(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

            // Turn 1 creates the conversation document on the server.
            chat.SetUserPrompt("Say hello.");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);

            var cvBeforeSnapshot = chat.ChangeVector;

            var snapshot = await chat.CreateSnapshotAsync();
            Assert.NotNull(snapshot);
            Assert.False(string.IsNullOrEmpty(snapshot.Token));
            Assert.False(string.IsNullOrEmpty(snapshot.ChangeVector));
            Assert.True(snapshot.CreatedAt > DateTime.MinValue);

            // The (first) snapshot force-created a revision and advanced the server-side change vector,
            // and the chat re-baselined its cached change vector to the returned value.
            Assert.NotEqual(cvBeforeSnapshot, snapshot.ChangeVector);
            Assert.Equal(snapshot.ChangeVector, chat.ChangeVector);

            // Continuing on the SAME chat instance must succeed (no ConcurrencyException).
            chat.SetUserPrompt("Say goodbye.");
            var r2 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r2.Status);
            Assert.NotNull(r2.Answer);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CreateSnapshotAsync_ReturnsForkableToken(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

            chat.SetUserPrompt("What is the capital of France?");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);

            var snapshot = await chat.CreateSnapshotAsync();

            // The token is well-formed and references this conversation.
            var parsed = ParseToken(snapshot.Token);
            Assert.Equal(chat.Id, parsed.ConversationId);
            Assert.NotEmpty(parsed.Revisions);

            // The token forks into an independent, non-empty conversation.
            var fork = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", fork.ConversationId);
            Assert.False(string.IsNullOrEmpty(fork.ChangeVector));

            var forkedMessages = await store.AI.GetConversationMessagesAsync("forked/1");
            Assert.NotEmpty(forkedMessages.Messages);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CreateSnapshotAsync_RepeatedSnapshotsAndTurns_AllSucceed(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

            string previousToken = null;
            for (int i = 0; i < 3; i++)
            {
                chat.SetUserPrompt($"Message number {i}.");
                var r = await chat.RunAsync<AssistantAnswer>();
                Assert.Equal(AiConversationResult.Done, r.Status);

                var snapshot = await chat.CreateSnapshotAsync();
                Assert.False(string.IsNullOrEmpty(snapshot.Token));

                // Each snapshot keeps the chat's cached change vector in sync with the server.
                Assert.Equal(snapshot.ChangeVector, chat.ChangeVector);

                // Each snapshot (taken after a distinct turn) is a distinct fork point.
                if (previousToken != null)
                    Assert.NotEqual(previousToken, snapshot.Token);
                previousToken = snapshot.Token;
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CreateSnapshotAsync_BeforeFirstRun_Throws()
        {
            using var store = GetDocumentStore();

            // A brand-new conversation has no server-side id until the first RunAsync, so there is
            // nothing to snapshot yet — CreateSnapshotAsync must fail fast rather than hit the server.
            var chat = store.AI.Conversation("some-agent", "chats/", new AiConversationCreationOptions());

            await Assert.ThrowsAsync<InvalidOperationException>(() => chat.CreateSnapshotAsync());
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CreateSnapshot_RewindInPlace_ThenContinue_RealClient(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);
            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());

            // Turn 1: establish a checkpoint we can rewind back to.
            chat.SetUserPrompt("Remember the secret word is 'apple'.");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);

            // Snapshot the checkpoint via the conversation object.
            var snapshot = await chat.CreateSnapshotAsync();
            Assert.False(string.IsNullOrEmpty(snapshot.Token));

            var messagesAtCheckpoint = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;

            // Turn 2: advance the conversation past the checkpoint on the SAME instance.
            chat.SetUserPrompt("Actually, change the secret word to 'banana'.");
            var r2 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r2.Status);

            var messagesAfterTurn2 = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.True(messagesAfterTurn2 > messagesAtCheckpoint,
                $"Expected turn 2 ({messagesAfterTurn2}) to have more messages than the checkpoint ({messagesAtCheckpoint}).");

            // Rewind IN PLACE: fork the snapshot back onto the SAME conversation id.
            var rewind = await store.AI.ForkConversationAsync(snapshot.Token, chat.Id);
            Assert.Equal(chat.Id, rewind.ConversationId);
            Assert.False(string.IsNullOrEmpty(rewind.ChangeVector));

            // The conversation is restored to the checkpoint — turn 2 was dropped.
            var messagesAfterRewind = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.Equal(messagesAtCheckpoint, messagesAfterRewind);

            // Continue AFTER the rewind, through the real client. The rewind wrote the document
            // out-of-band, so bind a conversation handle to the rewound change vector and keep going.
            var rewound = store.AI.Conversation(agentId, chat.Id, new AiConversationCreationOptions(), rewind.ChangeVector);
            rewound.SetUserPrompt("What is the secret word?");
            var r3 = await rewound.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r3.Status);
            Assert.NotNull(r3.Answer);

            // The continued turn extended the rewound conversation from the checkpoint.
            var messagesAfterContinue = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.True(messagesAfterContinue > messagesAfterRewind,
                $"Expected the continued conversation ({messagesAfterContinue}) to have more messages than the rewound checkpoint ({messagesAfterRewind}).");
        }

        // The natural "pick a past message and rewind to it" flow: with SnapshotBeforeRunning on,
        // every turn's answer carries the fork point for the state BEFORE that turn. To rewind to
        // an earlier message, fork that turn's token back onto the same conversation id — dropping
        // that turn and everything after it — then keep chatting.
        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SnapshotBeforeRunning_RewindToChosenTurn_ThenContinue_RealClient(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentId = await CreateAssistantAgentAsync(store, config);

            // Snapshots on: each turn's answer yields the fork point for the state before that turn.
            var chat = store.AI.Conversation(agentId, "chats/",
                new AiConversationCreationOptions { SnapshotBeforeRunning = true });

            // Turn 1 — the point we will rewind back to. A brand-new conversation has no prior state,
            // so its answer carries no snapshot token.
            chat.SetUserPrompt("The secret word is 'apple'.");
            var r1 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r1.Status);
            Assert.Null(r1.SnapshotToken);

            var messagesAfterTurn1 = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;

            // Turn 2 — its token captures the state BEFORE turn 2 (i.e. right after turn 1): our rewind target.
            chat.SetUserPrompt("Change the secret word to 'banana'.");
            var r2 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r2.Status);
            Assert.False(string.IsNullOrEmpty(r2.SnapshotToken));
            var rewindPointToken = r2.SnapshotToken;

            // Turn 3 — advance further, so the rewind has something to discard.
            chat.SetUserPrompt("Change the secret word to 'cherry'.");
            var r3 = await chat.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r3.Status);

            var messagesAfterTurn3 = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.True(messagesAfterTurn3 > messagesAfterTurn1,
                $"Expected 3 turns ({messagesAfterTurn3}) to have more messages than 1 turn ({messagesAfterTurn1}).");

            // Rewind to the chosen message: fork turn 2's token onto the same id, dropping turns 2 and 3.
            var rewind = await store.AI.ForkConversationAsync(rewindPointToken, chat.Id);
            Assert.Equal(chat.Id, rewind.ConversationId);
            Assert.False(string.IsNullOrEmpty(rewind.ChangeVector));

            // The conversation is restored to its state right after turn 1.
            var messagesAfterRewind = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.Equal(messagesAfterTurn1, messagesAfterRewind);

            // Continue from the rewound point through the real client.
            var rewound = store.AI.Conversation(agentId, chat.Id,
                new AiConversationCreationOptions { SnapshotBeforeRunning = true }, rewind.ChangeVector);
            rewound.SetUserPrompt("What is the secret word?");
            var r4 = await rewound.RunAsync<AssistantAnswer>();
            Assert.Equal(AiConversationResult.Done, r4.Status);
            Assert.NotNull(r4.Answer);

            var messagesAfterContinue = (await store.AI.GetConversationMessagesAsync(chat.Id)).Messages.Count;
            Assert.True(messagesAfterContinue > messagesAfterRewind,
                $"Expected the continued conversation ({messagesAfterContinue}) to have more messages than the rewound point ({messagesAfterRewind}).");
        }
    }
}
