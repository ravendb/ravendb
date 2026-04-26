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
            // Both forks succeed because there's no change vector check — last writer wins.
            // The result is effectively idempotent since both write the same revision data.
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
        public async Task G3_Fork_FromOlderToken_ReturnsOlderState()
        {
            // Forking from an older snapshot token always returns the state at that snapshot,
            // regardless of how many turns have been added since. The fork reads from the
            // immutable revision, not the live document.
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Run more turns after the snapshot
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);

            // Fork from the old token (before turn 2)
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");

            // Fork should have turn 1 only (3 messages), original should have 4 turns (9 messages)
            var forkedMessages = await GetDetailedMessagesAsync(store, "forked/1");
            var originalMessages = await GetDetailedMessagesAsync(store, "chats/1");

            AssertExactMessageCount(forkedMessages, 1, "fork from snapshot before turn 2");
            AssertExactMessageCount(originalMessages, 4, "original after 4 turns");
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
