using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationRevisionTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationRevisionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E1_Fork_WhenRevisionDoesNotExist_Fails()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create a fake token with a change vector that doesn't exist
            string fakeToken;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                var fakeRevisions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["chats/1"] = "FAKE:99-nonexistent"
                };
                fakeToken = SnapshotTokenDto.Build(ctx, "chats/1", DateTime.UtcNow, fakeRevisions);
            }

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(fakeToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);

            // Verify no partial state was created via client API
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<object>("forked/1");
                Assert.Null(doc);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E3_Fork_AfterEnforceRevisionsConfiguration_Fails()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            await store.Maintenance.SendAsync(new Raven.Client.Documents.Operations.Revisions.ConfigureRevisionsOperation(
                new Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration
                {
                    Default = new Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 0
                    }
                }));

            var enforceOp = await store.Operations.SendAsync(
                new Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation(
                    new Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation.Parameters
                    {
                        IncludeForceCreated = true
                    }));

            await enforceOp.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E5_Fork_FromDeletedConversation_Succeeds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Delete the conversation via client API
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("chats/1");
                await session.SaveChangesAsync();
            }

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "restored/1");
            Assert.Equal("restored/1", forkResult.ConversationId);

            var messages = await store.AI.GetConversationMessagesAsync("restored/1");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E6_Fork_FromDeletedConversation_ForkToSameId()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Delete via client API
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("chats/1");
                await session.SaveChangesAsync();
            }

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);
            Assert.NotEmpty(messages.Messages);
        }
    }
}
