using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationConcurrencyTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationConcurrencyTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G1_Fork_TwoClientsForkSameTokenToDifferentIds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var task1 = store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-a");
            var task2 = store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");

            await Task.WhenAll(task1, task2);

            var msgsA = await store.AI.GetConversationMessagesAsync("fork-a");
            var msgsB = await store.AI.GetConversationMessagesAsync("fork-b");
            Assert.NotNull(msgsA);
            Assert.NotEmpty(msgsA.Messages);
            Assert.NotNull(msgsB);
            Assert.NotEmpty(msgsB.Messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G2_Fork_TwoClientsForkToSameNewId()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var task1 = store.AI.ForkConversationAsync(r2.SnapshotToken, "same-target");
            var task2 = store.AI.ForkConversationAsync(r2.SnapshotToken, "same-target");

            await Task.WhenAll(task1, task2);

            var msgs = await store.AI.GetConversationMessagesAsync("same-target");
            Assert.NotNull(msgs);
            Assert.NotEmpty(msgs.Messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G3_Fork_WhileAnotherClientIsRunningATurn()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var beforeMessages = await GetDetailedMessagesAsync(store, "chats/1");
            int messagesBefore = beforeMessages.Messages.Count;

            var entered = new SemaphoreSlim(0, 1);
            var release = new SemaphoreSlim(0, 1);
            var turnTask = Task.Run(async () =>
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    var creation = new AiConversationCreationOptions { SnapshotBeforeRunning = true };
                    var blittable = ctx.ReadObject(creation.ToJson(), "params");
                    blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                    var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                        onRequest: _ =>
                        {
                            entered.Release();
                            if (release.Wait(TimeSpan.FromSeconds(30)) == false)
                                throw new TimeoutException("Mock LLM callback was not released within 30 seconds");
                            return new HttpResponseMessage(HttpStatusCode.OK)
                            {
                                Content = new StringContent(MockLlm.CreateAnswerResponse("\"concurrent turn\""))
                            };
                        })
                    {
                        Authentication = null
                    };

                    handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                    {
                        Parameters = parameters,
                        CreationOptions = creation,
                        UserPrompt = "concurrent turn"
                    }, changeVector: null);

                    return await handler.HandleRequest(ctx, CancellationToken.None);
                }
            });

            Assert.True(await entered.WaitAsync(TimeSpan.FromSeconds(30)), "Concurrent turn did not reach the mock LLM in time");

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            release.Release();
            var turnResult = await turnTask;
            Assert.NotNull(turnResult.Response);

            // Verify fork has the pre-concurrent-turn state via client API
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            var originalMessages = await GetDetailedMessagesAsync(store, "chats/1");

            Assert.True(forkedMessages.Messages.Count < originalMessages.Messages.Count,
                $"Fork ({forkedMessages.Messages.Count}) should have fewer messages than original ({originalMessages.Messages.Count}) after concurrent turn");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G4_Fork_ThenContinueOriginal()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Continue original
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3 on original", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);

            // Continue fork
            var rf = await RunTurnAsync(database, "forked/1", "turn on fork", snapshotBeforeRunning: true);
            Assert.NotNull(rf.Response);

            var originalMessages = await GetDetailedMessagesAsync(store, "chats/1");
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");

            Assert.NotNull(originalMessages);
            Assert.NotEmpty(originalMessages.Messages);
            Assert.NotNull(forkedMessages);
            Assert.NotEmpty(forkedMessages.Messages);

            // Original has 3 turns, fork has 1 (from snapshot) + 1 new = 2 turns
            AssertExactMessageCount(originalMessages, 3, "original after 3 turns");
            AssertExactMessageCount(forkedMessages, 2, "forked with 1 restored + 1 new turn");
        }
    }
}
