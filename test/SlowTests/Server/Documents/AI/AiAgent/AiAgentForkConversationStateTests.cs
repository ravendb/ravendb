using System;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationStateTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationStateTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H1_Fork_ConversationWithOpenActionCalls()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            InjectOpenActionCall(store, "chats/1", "call_pending", "UserAction", "{\"query\":\"test\"}");

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            Assert.True(HasOpenActionCalls(store, "forked/1"), "Forked doc should have open action calls from the snapshot");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H1b_Fork_AdjustsOpenActionCallsSubConversationIds()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            InjectOpenActionCallWithSubConversation(store, "chats/1",
                "call_sub", "SubAgentTool", "{}", "chats/1/SubAgent/hash123");

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");

            var openCalls = GetOpenActionCalls(store, "forked/1");
            Assert.NotNull(openCalls);
            Assert.True(openCalls.Count > 0);

            foreach (var (_, callFields) in openCalls)
            {
                if (callFields.TryGetValue("SubConversationId", out var subConvIdObj) && subConvIdObj is string subConvId)
                {
                    Assert.StartsWith("forked/1/", subConvId);
                    Assert.DoesNotContain("chats/1/", subConvId);
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2_Fork_ConversationWithMultipleTurns()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "first question", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "follow-up question", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "third question", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Fork should work and the conversation should be usable
            var r = await RunTurnAsync(database, "forked/1", "new direction", snapshotBeforeRunning: false);
            Assert.NotNull(r.Response);

            // Verify message counts via client API
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            // Forked from before turn 2 = 1 turn, then ran 1 more turn = 2 turns total
            AssertExactMessageCount(forkedMessages, 2, "forked from turn 1 snapshot + 1 new turn");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2b_Fork_PreservesAttachments()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store("chats/1", "file.txt",
                    new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("content")), "text/plain");
                await session.SaveChangesAsync();
            }

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify attachment exists on forked doc via client API
            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<object>("forked/1");
                Assert.NotNull(entity);
                var attachmentNames = session.Advanced.Attachments.GetNames(entity);
                Assert.NotNull(attachmentNames);
                Assert.NotEmpty(attachmentNames);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2c_Rewind_RemovesAttachmentsAddedAfterSnapshot()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);

            // Add an attachment BEFORE the snapshot
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store("chats/1", "original.txt",
                    new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("original")), "text/plain");
                await session.SaveChangesAsync();
            }

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Add a SECOND attachment AFTER the snapshot
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store("chats/1", "post-snapshot.txt",
                    new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("post-snapshot")), "text/plain");
                await session.SaveChangesAsync();
            }

            // Verify both attachments exist before rewind via client API
            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<object>("chats/1");
                var names = session.Advanced.Attachments.GetNames(entity);
                Assert.Equal(2, names.Length);
            }

            // Rewind to snapshot (which had only original.txt)
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // After rewind, only original.txt should remain
            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<object>("chats/1");
                var names = session.Advanced.Attachments.GetNames(entity);
                Assert.Single(names);
                Assert.Equal("original.txt", names[0].Name);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H3_Fork_ChainForking_ForkOfAFork()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkB = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            await RunTurnAsync(database, "fork-b", "turn on B", snapshotBeforeRunning: true);
            var rB2 = await RunTurnAsync(database, "fork-b", "turn 2 on B", snapshotBeforeRunning: true);

            var forkC = await store.AI.ForkConversationAsync(rB2.SnapshotToken, "fork-c");
            Assert.Equal("fork-c", forkC.ConversationId);

            // Verify all three exist via client API
            var msgsOrig = await store.AI.GetConversationMessagesAsync("chats/1");
            var msgsB = await store.AI.GetConversationMessagesAsync("fork-b");
            var msgsC = await store.AI.GetConversationMessagesAsync("fork-c");
            Assert.NotNull(msgsOrig);
            Assert.NotEmpty(msgsOrig.Messages);
            Assert.NotNull(msgsB);
            Assert.NotEmpty(msgsB.Messages);
            Assert.NotNull(msgsC);
            Assert.NotEmpty(msgsC.Messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H4_Fork_TreeOfForks_MultipleBranchesFromSamePoint()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            var r4 = await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 5", snapshotBeforeRunning: true);

            var forkB = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            var forkC = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-c");
            Assert.Equal("fork-c", forkC.ConversationId);

            var forkD = await store.AI.ForkConversationAsync(r4.SnapshotToken, "fork-d");
            Assert.Equal("fork-d", forkD.ConversationId);

            var msgsB = await GetDetailedMessagesAsync(store, "fork-b");
            var msgsC = await GetDetailedMessagesAsync(store, "fork-c");
            var msgsD = await GetDetailedMessagesAsync(store, "fork-d");

            Assert.NotNull(msgsB);
            Assert.NotEmpty(msgsB.Messages);
            Assert.NotNull(msgsC);
            Assert.NotEmpty(msgsC.Messages);
            Assert.NotNull(msgsD);
            Assert.NotEmpty(msgsD.Messages);

            // B and C from same snapshot point should have same count
            Assert.Equal(msgsB.Messages.Count, msgsC.Messages.Count);
            // B from turn 2 snapshot = 1 turn, D from turn 4 snapshot = 3 turns
            AssertExactMessageCount(msgsB, 1, "fork from before turn 2");
            AssertExactMessageCount(msgsD, 3, "fork from before turn 4");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H5_Fork_ConversationWithTotalUsage()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            var originalResult = await GetDetailedMessagesAsync(store, "chats/1");
            var forkedResult = await GetDetailedMessagesAsync(store, "forked/1");

            Assert.True(forkedResult.TotalUsage.TotalTokens < originalResult.TotalUsage.TotalTokens,
                $"Fork usage ({forkedResult.TotalUsage.TotalTokens}) should be less than original ({originalResult.TotalUsage.TotalTokens})");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H7_Fork_ConversationWithExpiration()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            {
                // Use RunTurnAsync for the first turn, then set expiration on the doc via session
                await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);

                // Set Expires on the conversation document
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<JObject>("chats/1");
                    doc["Expires"] = "01:00:00"; // 1 hour as TimeSpan string
                    session.SaveChanges();
                }

                var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

                var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
                Assert.Equal("forked/1", forkResult.ConversationId);

                var forkedDoc = GetDocumentAsJObject(store, "forked/1");
                Assert.NotNull(forkedDoc);
                var expiresValue = forkedDoc[nameof(ConversationDocument.Expires)]?.ToString();
                Assert.NotNull(expiresValue);
                var expires = TimeSpan.Parse(expiresValue);
                Assert.Equal(TimeSpan.FromSeconds(3600), expires);
            }
        }
    }
}
