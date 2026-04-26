using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationRewindTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationRewindTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B1_RewindInPlace_RestoresConversationState()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            var beforeRewind = await GetDetailedMessagesAsync(store, "chats/1");
            AssertExactMessageCount(beforeRewind, 3, "before rewind: 3 turns");

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            var afterRewind = await GetDetailedMessagesAsync(store, "chats/1");
            // Snapshot was before turn 2, so rewound state has 1 turn
            AssertExactMessageCount(afterRewind, 1, "after rewind to before turn 2");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B2_RewindInPlace_DeletesOrphanedSubConversations()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub1");
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub2");

            // Verify sub-conversations exist (SubConversationIds not exposed for checking creation, use server-side)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "chats/1/sub1"));
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "chats/1/sub2"));
            }

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // Sub-conversations should be deleted (not tracked in snapshot, use server-side check)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                Assert.Null(database.DocumentsStorage.Get(ctx2, "chats/1/sub1"));
                Assert.Null(database.DocumentsStorage.Get(ctx2, "chats/1/sub2"));
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B4_RewindInPlace_PreservesHistoryDocuments()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            // Run enough turns to trigger truncation (threshold = 5 messages, after truncation = 3)
            // System prompt = 1 msg, each turn = 2 msgs
            // After 3 turns: 1 + 6 = 7 messages => exceeds 5 => truncation triggered => LinkedConversations populated
            for (int i = 1; i <= 3; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }
            var r4 = await RunTurnWithAgentAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true, agent);

            // Get history document IDs (LinkedConversations not exposed via client API)
            List<string> historyIds;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "chats/1");
                doc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linked);
                historyIds = new List<string>();
                if (linked != null)
                {
                    for (int i = 0; i < linked.Length; i++)
                        historyIds.Add(linked[i].ToString());
                }
            }

            Assert.NotEmpty(historyIds);

            var forkResult = await store.AI.ForkConversationAsync(r4.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // History documents should still exist after rewind
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                Assert.NotEmpty(historyIds);
                foreach (string historyId in historyIds)
                {
                    var historyDoc = database.DocumentsStorage.Get(ctx2, historyId);
                    Assert.NotNull(historyDoc);
                }
            }
        }
    }
}
