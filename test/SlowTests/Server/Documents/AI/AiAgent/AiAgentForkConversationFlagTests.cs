using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentForkConversationFlagTests : AiAgentForkConversationTestBase
    {
        public AiAgentForkConversationFlagTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I1_SnapshotToken_IsNull_WhenFlagDisabled()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var r1 = await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            Assert.Null(r1.SnapshotToken);

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: false);
            Assert.Null(r2.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I2_Flag_EnabledMidConversation()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var r1 = await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            Assert.Null(r1.SnapshotToken);

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            Assert.NotNull(r2.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I3_Flag_DisabledAfterEnabled_TokenStillValid()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: false);

            Assert.NotNull(r2.SnapshotToken);
            Assert.Null(r3.SnapshotToken);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I4_Fork_ThenContinueWithoutFlag()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            var result = await RunTurnAsync(database, "forked/1", "new direction", snapshotBeforeRunning: false);
            Assert.NotNull(result.Response);
            Assert.Null(result.SnapshotToken);
        }
    }
}
