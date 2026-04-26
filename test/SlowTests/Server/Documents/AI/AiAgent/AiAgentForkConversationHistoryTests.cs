using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationHistoryTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationHistoryTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D1_Fork_SharesHistoryDocuments()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            // Run enough turns to trigger truncation and populate LinkedConversations
            for (int i = 1; i <= 4; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }

            // Take a snapshot after history docs have been created
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify LinkedConversations via server-side read (not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var originalDoc = database.DocumentsStorage.Get(ctx, "chats/1");
                originalDoc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray originalLinked);

                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                forkedDoc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray forkedLinked);

                Assert.NotNull(forkedLinked);
                Assert.NotNull(originalLinked);
                Assert.True(forkedLinked.Length > 0, "Forked LinkedConversations should not be empty");
                Assert.True(originalLinked.Length > 0, "Original LinkedConversations should not be empty");

                var originalSet = new HashSet<string>();
                for (int i = 0; i < originalLinked.Length; i++)
                    originalSet.Add(originalLinked[i].ToString());
                var forkedSet = new HashSet<string>();
                for (int i = 0; i < forkedLinked.Length; i++)
                    forkedSet.Add(forkedLinked[i].ToString());

                Assert.NotEmpty(forkedSet);
                Assert.NotEmpty(originalSet);
                Assert.True(forkedSet.IsSubsetOf(originalSet),
                    "Forked history IDs should be a subset of the original's history IDs");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D2_Fork_ThenDeleteOriginal_HistoryStillAccessible()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            for (int i = 1; i <= 4; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Get fork's history IDs (LinkedConversations not exposed via client API)
            List<string> forkHistoryIds;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                forkedDoc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linked);
                forkHistoryIds = new List<string>();
                if (linked != null)
                {
                    for (int i = 0; i < linked.Length; i++)
                        forkHistoryIds.Add(linked[i].ToString());
                }
            }

            Assert.NotEmpty(forkHistoryIds);

            // Delete the original conversation
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("chats/1");
                await session.SaveChangesAsync();
            }

            // History docs should still exist
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx3))
            using (ctx3.OpenReadTransaction())
            {
                Assert.NotEmpty(forkHistoryIds);
                foreach (string historyId in forkHistoryIds)
                {
                    Assert.NotNull(database.DocumentsStorage.Get(ctx3, historyId));
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D3_Fork_TwiceFromSameToken_BothShareHistory()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            for (int i = 1; i <= 4; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            var forkA = await store.AI.ForkConversationAsync(snapshot.Token, "fork-a");
            Assert.Equal("fork-a", forkA.ConversationId);

            var forkB = await store.AI.ForkConversationAsync(snapshot.Token, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            // Both should have the same LinkedConversations (server-side, not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var docA = database.DocumentsStorage.Get(ctx, "fork-a");
                var docB = database.DocumentsStorage.Get(ctx, "fork-b");
                docA.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linkedA);
                docB.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linkedB);

                var setA = new HashSet<string>();
                if (linkedA != null)
                    for (int i = 0; i < linkedA.Length; i++)
                        setA.Add(linkedA[i].ToString());
                var setB = new HashSet<string>();
                if (linkedB != null)
                    for (int i = 0; i < linkedB.Length; i++)
                        setB.Add(linkedB[i].ToString());

                Assert.NotEmpty(setA);
                Assert.NotEmpty(setB);
                Assert.True(setA.SetEquals(setB), "Both forks should share the same history documents");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D4_Fork_WhenSomeHistoryDocsDeleted()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var agent = CreateTestAgentWithTruncation();

            for (int i = 1; i <= 4; i++)
            {
                await RunTurnWithAgentAsync(database, "chats/1", $"turn {i}", snapshotBeforeRunning: true, agent);
            }

            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            // Delete some history docs (simulate expiration)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                string historyIdToDelete = null;
                using (ctx.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.Get(ctx, "chats/1");
                    if (doc != null && doc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linked) && linked?.Length > 0)
                    {
                        historyIdToDelete = linked[0].ToString();
                    }
                }

                if (historyIdToDelete != null)
                {
                    using var tx = ctx.OpenWriteTransaction();
                    database.DocumentsStorage.Delete(ctx, historyIdToDelete, null);
                    tx.Commit();
                }
            }

            // Fork should still work
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);
        }
    }
}
