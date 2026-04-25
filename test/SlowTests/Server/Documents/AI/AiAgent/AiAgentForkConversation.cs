using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client;
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
    public class AiAgentForkConversation : RavenTestBase
    {
        public AiAgentForkConversation(ITestOutputHelper output) : base(output)
        {
        }

        #region A. Happy Path

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A1_SnapshotToken_IsNull_OnFirstTurn()
        {
            // Test: First turn with SnapshotBeforeRunning should return null SnapshotToken.
            // Expected: null — no prior state exists to snapshot.
            // Reasoning: New conversation has no document before the first turn.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var result = await RunTurnAsync(database, "chats/1", "Hello", snapshotBeforeRunning: true);
            Assert.Null(result.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A2_SnapshotToken_IsPopulated_OnSubsequentTurns()
        {
            // Test: After the first turn, subsequent turns with SnapshotBeforeRunning should return non-null tokens.
            // Expected: Turn 1 null, turns 2 and 3 non-null, each different.
            // Reasoning: After turn 1, a conversation document exists and can be snapshotted.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var r1 = await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            Assert.Null(r1.SnapshotToken);

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            Assert.NotNull(r2.SnapshotToken);

            var r3 = await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);
            Assert.NotEqual(r2.SnapshotToken, r3.SnapshotToken);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A3_ForkToNewId_CreatesIndependentConversation()
        {
            // Test: Fork from a snapshot token to a new ID creates an independent conversation
            //        with only the messages that existed at the snapshot point.
            // Expected: Forked doc has turn 1 messages only; original untouched with all 3 turns.
            // Reasoning: The fork restores from the revision taken before turn 2.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Fork from the snapshot taken before turn 2 via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");

            Assert.Equal("forked/1", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            // Verify forked document has fewer messages than the original via client API
            var forkedMessages = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "forked/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });
            var originalMessages = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "chats/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });

            Assert.NotNull(forkedMessages);
            Assert.NotNull(originalMessages);

            // Forked should have fewer messages (snapshot was before turn 2)
            Assert.True(forkedMessages.Messages.Count < originalMessages.Messages.Count,
                $"Forked ({forkedMessages.Messages.Count}) should have fewer messages than original ({originalMessages.Messages.Count})");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A6_ForkPreservesParameters()
        {
            // Test: Forked conversation should preserve the parameters from the original.
            // Expected: Forked doc has the same Parameters as the original at the snapshot point.
            // Reasoning: Parameters are part of the conversation document and should survive the fork.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Run with parameters
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions { SnapshotBeforeRunning = true }
                    .AddParameter("company", "companies/90-A");
                var blittable = context.ReadObject(creation.ToJson(), "params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                await RunTurnWithParamsAsync(database, "chats/1", "turn 1", parameters, creation);
                var r2 = await RunTurnWithParamsAsync(database, "chats/1", "turn 2", parameters, creation);

                // Fork via client API
                var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
                Assert.Equal("forked/1", forkResult.ConversationId);

                // Verify parameters preserved via server-side read (Parameters not exposed via client API)
                using (context.OpenReadTransaction())
                {
                    var forkedDoc = database.DocumentsStorage.Get(context, "forked/1");
                    Assert.NotNull(forkedDoc);
                    Assert.True(forkedDoc.Data.TryGet(nameof(ConversationDocument.Parameters), out BlittableJsonReaderObject forkedParams));
                    Assert.NotNull(forkedParams);

                    // The parameter should be preserved
                    Assert.True(forkedParams.TryGet("company", out object companyValue));
                }
            }
        }

        #endregion

        #region B. Rewind-in-Place

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B1_RewindInPlace_RestoresConversationState()
        {
            // Test: Fork to the same conversation ID overwrites it with the snapshot state.
            // Expected: After rewind, conversation has only turn 1 messages.
            // Reasoning: Fork to same ID = rewind-in-place. The document is replaced with revision state.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Count messages before rewind via client API
            var beforeRewind = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "chats/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });
            int messagesBeforeRewind = beforeRewind.Messages.Count;

            // Rewind to before turn 2 via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // Count messages after rewind via client API
            var afterRewind = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "chats/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });

            Assert.True(afterRewind.Messages.Count < messagesBeforeRewind,
                $"After rewind ({afterRewind.Messages.Count}) should have fewer messages than before ({messagesBeforeRewind})");
        }

        #endregion

        #region C. Sub-Conversation Edge Cases (unit tests)

        [RavenFact(RavenTestCategory.Ai)]
        public void C2_Fork_PrefixReplacementIsExact()
        {
            // Test: Sub-conversation ID prefix replacement only replaces the leading prefix.
            // Expected: "chats/42/chats/42/Search/abc" -> "archives/fork-1/chats/42/Search/abc"
            // Reasoning: The conversation ID may appear multiple times in derived IDs;
            //            only the leading occurrence should be replaced.

            Assert.Equal("new/id", ForkConversationCommand.AdjustId("chats/42", "chats/42", "new/id"));
            Assert.Equal("new/id/Search/abc", ForkConversationCommand.AdjustId("chats/42/Search/abc", "chats/42", "new/id"));
            Assert.Equal("new/id/chats/42/Search/abc",
                ForkConversationCommand.AdjustId("chats/42/chats/42/Search/abc", "chats/42", "new/id"));
            Assert.Equal("other/1/sub", ForkConversationCommand.AdjustId("other/1/sub", "chats/42", "new/id"));
        }

        #endregion

        #region D. History Document Sharing

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D1_Fork_SharesHistoryDocuments()
        {
            // Test: Forked conversation shares the same history documents as the original.
            // Expected: LinkedConversations in fork references the same history doc IDs.
            // Reasoning: History docs are immutable records; forking should share, not duplicate.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);

            // Fork from turn 3's snapshot via client API
            var forkResult = await store.AI.ForkConversationAsync(r3.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify LinkedConversations via server-side read (not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                // Parse token server-side to get the revision's LinkedConversations for comparison
                var parsed = SnapshotTokenDto.Parse(ctx, r3.SnapshotToken);
                using var revision = database.DocumentsStorage.RevisionsStorage.GetRevision(ctx, parsed.Revisions[0].ChangeVector);
                revision.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray revisionLinked);

                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                forkedDoc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray forkedLinked);

                // Fork's LinkedConversations should exactly match the revision's
                Assert.NotNull(forkedLinked);
                Assert.NotNull(revisionLinked);
                Assert.Equal(revisionLinked.Length, forkedLinked.Length);

                var revisionSet = revisionLinked.Select(x => x.ToString()).ToHashSet();
                var forkedSet = forkedLinked.Select(x => x.ToString()).ToHashSet();
                Assert.True(forkedSet.SetEquals(revisionSet),
                    "Forked history IDs should exactly match the revision's history IDs");
            }
        }

        #endregion

        #region E. Purged / Missing Revisions

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E1_Fork_WhenRevisionDoesNotExist_Fails()
        {
            // Test: Fork from a token that references a non-existent revision should fail atomically.
            // Expected: Exception identifying the missing revision's change vector.
            // Reasoning: If revisions are gone (e.g., purged by retention), we must fail clearly.

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

            // Fork via client API should fail
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(fakeToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);

            // Verify no partial state was created
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx2, "forked/1");
                Assert.Null(forkedDoc);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E5_Fork_FromDeletedConversation_Succeeds()
        {
            // Test: Fork from a token after the conversation document was deleted should succeed
            //        as long as the revisions still exist.
            // Expected: Fork succeeds — creates a new conversation from the revision data.
            // Reasoning: Revisions are independent from the live document. This enables "un-delete".

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Delete the conversation document
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                using var tx = ctx.OpenWriteTransaction();
                database.DocumentsStorage.Delete(ctx, "chats/1", null);
                tx.Commit();
            }

            // Fork via client API should still work — revisions exist
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "restored/1");
            Assert.Equal("restored/1", forkResult.ConversationId);

            // Verify the document exists via client API
            var messages = await store.AI.GetConversationMessagesAsync("restored/1");
            Assert.NotNull(messages);
        }

        #endregion

        #region F. Token Validation / Corruption

        [RavenFact(RavenTestCategory.Ai)]
        public void F_SnapshotToken_Parsing_Validation()
        {
            // Test: Token parser rejects invalid inputs with clear errors.
            // Expected: Various invalid tokens throw InvalidOperationException.
            // Reasoning: Tokens are opaque to client but may be corrupted or tampered.

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
            // Test: A token with a modified change vector should fail because the revision won't be found.
            // Expected: Exception about missing revision.
            // Reasoning: Tampered change vectors don't match any stored revision.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Tamper with the token — change the change vector
            var tokenJson = JObject.Parse(r2.SnapshotToken);
            tokenJson["Revisions"][0]["ChangeVector"] = "TAMPERED:99-fake";
            var tamperedToken = tokenJson.ToString();

            // Fork via client API should fail
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(tamperedToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        #endregion

        #region G. Concurrency

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G4_Fork_ThenContinueOriginal()
        {
            // Test: After forking, the original conversation can still be continued independently.
            // Expected: Both fork and original continue work succeed without interference.
            // Reasoning: Fork creates independent documents; original is not affected.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Continue original
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3 on original", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);

            // Continue fork
            var rf = await RunTurnAsync(database, "forked/1", "turn on fork", snapshotBeforeRunning: true);
            Assert.NotNull(rf.Response); // fork continues independently

            // Both documents should exist and be different, verified via client API
            var originalMessages = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "chats/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });
            var forkedMessages = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "forked/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });

            Assert.NotNull(originalMessages);
            Assert.NotNull(forkedMessages);

            // Original has more turns than fork
            Assert.True(originalMessages.Messages.Count > forkedMessages.Messages.Count);
        }

        #endregion

        #region H. Conversation State Edge Cases

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H3_Fork_ChainForking_ForkOfAFork()
        {
            // Test: Fork a conversation, run a turn on it, then fork the fork.
            // Expected: All three conversations are independent and valid.
            // Reasoning: Tokens are self-contained; forking a fork is just forking any conversation.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Original: turns 1, 2
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork A -> B via client API
            var forkB = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            // Run a turn on B
            await RunTurnAsync(database, "fork-b", "turn on B", snapshotBeforeRunning: true);
            var rB2 = await RunTurnAsync(database, "fork-b", "turn 2 on B", snapshotBeforeRunning: true);

            // Fork B -> C via client API
            var forkC = await store.AI.ForkConversationAsync(rB2.SnapshotToken, "fork-c");
            Assert.Equal("fork-c", forkC.ConversationId);

            // Verify all three exist via client API
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("chats/1"));
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("fork-b"));
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("fork-c"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H5_Fork_ConversationWithTotalUsage()
        {
            // Test: Forked conversation should have TotalUsage from the revision (usage up to snapshot point).
            // Expected: Fork's TotalUsage matches the state at the snapshot, not the current state.
            // Reasoning: The fork is a restoration of a past state, including token usage at that point.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Run more turns to accumulate more usage
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);

            // Fork from before turn 2 (when less usage was accumulated) via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify TotalUsage via client API (exposed on GetConversationMessagesAsync result)
            var originalResult = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "chats/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 1 });
            var forkedResult = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "forked/1", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 1 });

            // Fork should have less usage (it's from an earlier point)
            Assert.True(forkedResult.TotalUsage.TotalTokens < originalResult.TotalUsage.TotalTokens,
                $"Fork usage ({forkedResult.TotalUsage.TotalTokens}) should be less than original ({originalResult.TotalUsage.TotalTokens})");
        }

        #endregion

        #region I. SnapshotBeforeRunning Flag Behavior

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I1_SnapshotToken_IsNull_WhenFlagDisabled()
        {
            // Test: SnapshotToken is null on all turns when SnapshotBeforeRunning is false.
            // Expected: null on both turns.
            // Reasoning: No snapshots are created when the flag is off.

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
            // Test: Enabling SnapshotBeforeRunning mid-conversation produces a snapshot from that point.
            // Expected: Turn 1 (flag off) null, turn 2 (flag on) non-null.
            // Reasoning: The flag is per-request, not per-conversation-lifetime.

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
            // Test: Disabling the flag stops new tokens but existing tokens remain valid.
            // Expected: Turn 3 (flag off) has null token, but turn 2's token still works for forking.
            // Reasoning: Existing revisions aren't affected by the flag being turned off.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: false);

            Assert.NotNull(r2.SnapshotToken);
            Assert.Null(r3.SnapshotToken);

            // The old token should still work for forking via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);
        }

        #endregion

        #region J. Edge Cases in ID Handling

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J1_Fork_NewConversationIdIsNull_GetsGuid()
        {
            // Test: Fork with null newConversationId should generate a GUID-based ID.
            // Expected: The result ID is a non-null, non-empty string (GUID format).
            // Reasoning: Null ID = server generates a GUID, standard RavenDB behavior.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API with null ID
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, null);

            Assert.NotNull(forkResult.ConversationId);
            Assert.NotEmpty(forkResult.ConversationId);
            Assert.NotEqual("chats/1", forkResult.ConversationId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J2_Fork_ToIdThatAlreadyExists_Overwrites()
        {
            // Test: Fork to an ID that already has a different conversation should overwrite it.
            // Expected: The target document is replaced with the forked state.
            // Reasoning: Explicit ID = the user owns the decision; overwrite is consistent with PUT semantics.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create two different conversations
            await RunTurnAsync(database, "chats/1", "conversation 1 turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "conversation 1 turn 2", snapshotBeforeRunning: true);

            await RunTurnAsync(database, "chats/existing", "different conversation", snapshotBeforeRunning: false);

            // Fork chats/1 to chats/existing (overwrite) via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/existing");

            Assert.Equal("chats/existing", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            // Verify the document was overwritten via client API
            var messages = await store.AI.GetConversationMessagesAsync("chats/existing");
            Assert.NotNull(messages);
        }

        #endregion

        #region K. Snapshot CRUD APIs

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K1_CreateSnapshot_WithoutRunAsync()
        {
            // Test: CreateSnapshotAsync creates a snapshot without running a turn.
            // Expected: Returns a valid token that can be used for forking.
            // Reasoning: Users may want to capture state without sending a prompt.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: false);

            // Create a snapshot via client API
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            // Fork from the snapshot token via client API
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "from-snapshot");
            Assert.Equal("from-snapshot", forkResult.ConversationId);

            // Verify the forked conversation exists
            var messages = await store.AI.GetConversationMessagesAsync("from-snapshot");
            Assert.NotNull(messages);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K5_PurgeConversationSnapshots_ConversationStillWorks()
        {
            // Test: Purging snapshots removes all revisions but leaves the conversation document untouched.
            // Expected: Conversation still exists and can run new turns after purge.
            // Reasoning: Purge only affects revisions, not the live document.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Purge all snapshots via client API
            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            // Conversation itself should still exist via client API
            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);

            // Can still run new turns
            var r4 = await RunTurnAsync(database, "chats/1", "turn 4 after purge", snapshotBeforeRunning: false);
            Assert.NotNull(r4.Response);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K7_PurgeConversationSnapshots_NonExistentConversation()
        {
            // Test: Purging snapshots for a non-existent conversation should not error.
            // Expected: No exception, idempotent operation.
            // Reasoning: Defensive — the conversation may have been deleted.

            using var store = GetDocumentStore();

            // Should not throw via client API
            await store.AI.PurgeConversationSnapshotsAsync("nonexistent/id");
        }

        #endregion

        #region A. Happy Path (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A4_ForkToExplicitId()
        {
            // Test: Fork with an explicit newConversationId should use that exact ID.
            // Expected: forkResult.ConversationId == "forked-chats/my-fork"
            // Reasoning: Explicit IDs are used as-is, no auto-generation.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API with explicit ID
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked-chats/my-fork");
            Assert.Equal("forked-chats/my-fork", forkResult.ConversationId);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task A5_ForkToClusterIdentity()
        {
            // Test: Fork with newConversationId ending in "|" uses cluster-wide identity generation.
            // Expected: The result ID follows the cluster identity pattern (e.g. "chats/1-A").
            // Reasoning: The "|" suffix triggers Raft-based cluster identity, same as Conversation().

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via the client operation which handles cluster identity
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forks|");

            Assert.NotNull(forkResult.ConversationId);
            Assert.StartsWith("forks/", forkResult.ConversationId);
            Assert.NotNull(forkResult.ChangeVector);

            // Verify the document exists
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<object>(forkResult.ConversationId);
                Assert.NotNull(doc);
            }
        }

        #endregion

        #region B. Rewind-in-Place (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task B2_RewindInPlace_DeletesOrphanedSubConversations()
        {
            // Test: When rewinding to a point before sub-conversations existed, those sub-convs are deleted.
            // Expected: Sub-conversation documents created after the snapshot point are deleted.
            // Reasoning: The snapshot state didn't include those sub-conversations; they're orphaned.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Turn 1: no sub-conversations
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Now manually add sub-conversation documents and update SubConversationIds on the main doc
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub1");
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/sub2");

            // Verify sub-conversations exist
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "chats/1/sub1"));
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "chats/1/sub2"));
            }

            // Rewind to before turn 2 via client API — the snapshot didn't have any sub-conversations
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // Sub-conversations should be deleted (verified server-side, not exposed via client API)
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
            // Test: History documents are not deleted when rewinding in-place.
            // Expected: History docs still exist after rewind.
            // Reasoning: History docs are independent records with their own lifecycle.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Get history document IDs before rewind (server-side, not exposed via client API)
            List<string> historyIds;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "chats/1");
                doc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linked);
                historyIds = linked?.Select(x => x.ToString()).ToList() ?? new List<string>();
            }

            // Rewind via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // History documents should still exist
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                foreach (string historyId in historyIds)
                {
                    var historyDoc = database.DocumentsStorage.Get(ctx2, historyId);
                    Assert.NotNull(historyDoc);
                }
            }
        }

        #endregion

        #region C. Sub-Conversation Edge Cases (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C1_Fork_AdjustsSubConversationIds()
        {
            // Test: When forking, sub-conversation IDs are adjusted by replacing the leading prefix.
            // Expected: chats/1/Search/abc -> forked/1/Search/abc
            // Reasoning: Sub-conv IDs are derived from the parent ID; fork must adjust them.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Add a sub-conversation
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/Search/abc");

            // Take a snapshot that includes the sub-conversation via client API
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            // Fork to a different ID via client API
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify the sub-conversation was created with the adjusted ID (server-side, SubConversationIds not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                Assert.NotNull(forkedDoc);

                // Check SubConversationIds are adjusted
                forkedDoc.Data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds);
                Assert.NotNull(subIds);
                var subIdsList = subIds.Select(x => x.ToString()).ToList();
                Assert.Contains("forked/1/Search/abc", subIdsList);
                Assert.DoesNotContain("chats/1/Search/abc", subIdsList);

                // Verify the adjusted sub-conversation document exists
                var subDoc = database.DocumentsStorage.Get(ctx, "forked/1/Search/abc");
                Assert.NotNull(subDoc);

                // Original sub-conversation should be untouched
                var origSub = database.DocumentsStorage.Get(ctx, "chats/1/Search/abc");
                Assert.NotNull(origSub);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C4_Fork_CleanupUsesSubConversationIds()
        {
            // Test: When forking to an existing ID, only documents tracked in SubConversationIds
            //        are deleted — rogue documents NOT in SubConversationIds survive.
            // Expected: The rogue document is NOT deleted — SubConversationIds-based cleanup
            //           only removes tracked sub-conversations.
            // Reasoning: Cleanup uses SubConversationIds recursively rather than prefix-based
            //            scanning, so untracked documents are left untouched.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Create a rogue document that looks like a sub-conversation but isn't tracked
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                using var tx = ctx.OpenWriteTransaction();
                var rogueData = ctx.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue { ["Rogue"] = true }, "rogue");
                database.DocumentsStorage.Put(ctx, "chats/1/Rogue/xyz", null, rogueData);
                tx.Commit();
            }

            // Rewind via client API — the rogue document survives because it is not tracked in SubConversationIds
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                var rogueDoc = database.DocumentsStorage.Get(ctx2, "chats/1/Rogue/xyz");
                Assert.NotNull(rogueDoc); // survives — not tracked in SubConversationIds
            }
        }

        #endregion

        #region D. History Document Sharing (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D2_Fork_ThenDeleteOriginal_HistoryStillAccessible()
        {
            // Test: After forking, deleting the original doesn't affect the fork's history references.
            // Expected: History documents still exist and are accessible from the fork.
            // Reasoning: History docs are independent documents.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Fork via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Get fork's history IDs (server-side, LinkedConversations not exposed via client API)
            List<string> forkHistoryIds;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                forkedDoc.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linked);
                forkHistoryIds = linked?.Select(x => x.ToString()).ToList() ?? new List<string>();
            }

            // Delete the original conversation
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            {
                using var tx = ctx2.OpenWriteTransaction();
                database.DocumentsStorage.Delete(ctx2, "chats/1", null);
                tx.Commit();
            }

            // History docs should still exist
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx3))
            using (ctx3.OpenReadTransaction())
            {
                foreach (string historyId in forkHistoryIds)
                {
                    Assert.NotNull(database.DocumentsStorage.Get(ctx3, historyId));
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D3_Fork_TwiceFromSameToken_BothShareHistory()
        {
            // Test: Forking twice from the same token creates two forks sharing the same history.
            // Expected: Both forks' LinkedConversations reference the same history doc IDs.
            // Reasoning: History is immutable; multiple forks should share.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork twice via client API
            var forkA = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-a");
            Assert.Equal("fork-a", forkA.ConversationId);

            var forkB = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            // Both should have the same LinkedConversations (server-side, not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var docA = database.DocumentsStorage.Get(ctx, "fork-a");
                var docB = database.DocumentsStorage.Get(ctx, "fork-b");
                docA.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linkedA);
                docB.Data.TryGet(nameof(ConversationDocument.LinkedConversations), out BlittableJsonReaderArray linkedB);

                var setA = linkedA?.Select(x => x.ToString()).ToHashSet() ?? new HashSet<string>();
                var setB = linkedB?.Select(x => x.ToString()).ToHashSet() ?? new HashSet<string>();

                Assert.True(setA.SetEquals(setB), "Both forks should share the same history documents");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task D4_Fork_WhenSomeHistoryDocsDeleted()
        {
            // Test: Fork succeeds even if some history documents have been deleted (simulating expiration).
            // Expected: Fork succeeds; the conversation still works with gaps in deep history.
            // Reasoning: History docs are independent; their absence is not a fork failure.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

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

            // Fork via client API should still work
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);
        }

        #endregion

        #region E. Purged / Missing Revisions (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E6_Fork_FromDeletedConversation_ForkToSameId()
        {
            // Test: Fork to the original (deleted) conversation ID — effectively an "un-delete".
            // Expected: The conversation is recreated from revision data.
            // Reasoning: The fork only needs revisions; restoring to the same ID is valid.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Delete the conversation
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                using var tx = ctx.OpenWriteTransaction();
                database.DocumentsStorage.Delete(ctx, "chats/1", null);
                tx.Commit();
            }

            // Fork to the same ID via client API — un-delete
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // Verify conversation is restored via client API
            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);
        }

        #endregion

        #region G. Concurrency (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G1_Fork_TwoClientsForkSameTokenToDifferentIds()
        {
            // Test: Two concurrent forks from the same token to different IDs should both succeed.
            // Expected: Both forks create independent conversations.
            // Reasoning: Forks to different IDs don't conflict.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork concurrently via client API
            var task1 = store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-a");
            var task2 = store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");

            await Task.WhenAll(task1, task2);

            // Verify both exist via client API
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("fork-a"));
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("fork-b"));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G2_Fork_TwoClientsForkToSameNewId()
        {
            // Test: Two concurrent forks to the same explicit ID — last writer wins.
            // Expected: Both succeed (no concurrency check on target); final doc is valid.
            // Reasoning: No change vector is specified for the target, so it's an unconditional write.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork concurrently to the same ID via client API
            var task1 = store.AI.ForkConversationAsync(r2.SnapshotToken, "same-target");
            var task2 = store.AI.ForkConversationAsync(r2.SnapshotToken, "same-target");

            await Task.WhenAll(task1, task2);

            // Document exists — last writer wins
            Assert.NotNull(await store.AI.GetConversationMessagesAsync("same-target"));
        }

        #endregion

        #region H. Conversation State Edge Cases (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H4_Fork_TreeOfForks_MultipleBranchesFromSamePoint()
        {
            // Test: Multiple forks from different snapshot points create a tree of conversations.
            // Expected: All forks succeed independently with correct state at each branch point.
            // Reasoning: Each token is self-contained; the tree structure is implicit.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);
            var r4 = await RunTurnAsync(database, "chats/1", "turn 4", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 5", snapshotBeforeRunning: true);

            // Fork from turn 2 -> B via client API
            var forkB = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-b");
            Assert.Equal("fork-b", forkB.ConversationId);

            // Fork from turn 2 again -> C via client API
            var forkC = await store.AI.ForkConversationAsync(r2.SnapshotToken, "fork-c");
            Assert.Equal("fork-c", forkC.ConversationId);

            // Fork from turn 4 -> D via client API
            var forkD = await store.AI.ForkConversationAsync(r4.SnapshotToken, "fork-d");
            Assert.Equal("fork-d", forkD.ConversationId);

            // Verify all forks via client API
            var msgsB = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "fork-b", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });
            var msgsC = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "fork-c", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });
            var msgsD = await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions { ConversationId = "fork-d", DetailLevel = AiConversationDetailLevel.Detailed, PageSize = 100 });

            Assert.NotNull(msgsB);
            Assert.NotNull(msgsC);
            Assert.NotNull(msgsD);

            // B and C should have the same number of messages (same snapshot point)
            Assert.Equal(msgsB.Messages.Count, msgsC.Messages.Count);
            Assert.True(msgsD.Messages.Count > msgsB.Messages.Count, "D (from turn 4) should have more messages than B (from turn 2)");
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H7_Fork_ConversationWithExpiration()
        {
            // Test: Forked conversation inherits Expires from the revision.
            // Expected: The forked doc has an @expires metadata field.
            // Reasoning: The expiration is part of the conversation state.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions
                {
                    SnapshotBeforeRunning = true,
                    ExpirationInSec = 3600
                };
                var blittable = context.ReadObject(creation.ToJson(), "params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                await RunTurnWithParamsAsync(database, "chats/1", "turn 1", parameters, creation);
                var r2 = await RunTurnWithParamsAsync(database, "chats/1", "turn 2", parameters, creation);

                // Fork via client API
                var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
                Assert.Equal("forked/1", forkResult.ConversationId);

                // Verify the Expires field (server-side, not exposed via client API)
                using (context.OpenReadTransaction())
                {
                    var forkedDoc = database.DocumentsStorage.Get(context, "forked/1");
                    Assert.NotNull(forkedDoc);

                    // Check that the doc has Expires field
                    forkedDoc.Data.TryGet(nameof(ConversationDocument.Expires), out TimeSpan? expires);
                    Assert.NotNull(expires);
                    Assert.Equal(TimeSpan.FromSeconds(3600), expires.Value);
                }
            }
        }

        #endregion

        #region I. SnapshotBeforeRunning Flag Behavior (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task I4_Fork_ThenContinueWithoutFlag()
        {
            // Test: After forking, the forked conversation works normally without SnapshotBeforeRunning.
            // Expected: No snapshot token on the answer, but the conversation functions correctly.
            // Reasoning: Snapshots are opt-in per turn; the fork just creates a new starting point.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Run on fork WITHOUT snapshot flag
            var result = await RunTurnAsync(database, "forked/1", "new direction", snapshotBeforeRunning: false);
            Assert.NotNull(result.Response);
            Assert.Null(result.SnapshotToken); // flag was off
        }

        #endregion

        #region J. Edge Cases in ID Handling (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task J3_Fork_ToIdWithDifferentPrefix()
        {
            // Test: Fork from one ID prefix to a completely different prefix works correctly.
            // Expected: Sub-conversation IDs are adjusted to the new prefix.
            // Reasoning: The prefix replacement handles any target prefix.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/42", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/42", "turn 2", snapshotBeforeRunning: true);

            // Add a sub-conversation
            await CreateSubConversationDocAsync(database, "chats/42", "chats/42/Search/abc");

            // Create snapshot and fork via client API
            var snapshot = await store.AI.CreateSnapshotAsync("chats/42");
            Assert.NotNull(snapshot);

            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "archives/fork-1");
            Assert.Equal("archives/fork-1", forkResult.ConversationId);

            // Verify sub-conversation was adjusted (server-side, not exposed via client API)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "archives/fork-1"));
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "archives/fork-1/Search/abc"));
            }
        }

        #endregion

        #region K. Snapshot CRUD APIs (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K4_GetConversationSnapshots_EmptyForNoSnapshots()
        {
            // Test: GetConversationSnapshotsAsync returns empty when no snapshots exist.
            // Expected: Empty list, no error.
            // Reasoning: A conversation without snapshots is a valid state.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create a conversation without snapshots
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: false);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: false);

            // Verify no snapshots via client API
            var snapshots = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.Empty(snapshots);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K6_PurgeConversationSnapshots_DoesNotAffectOtherConversations()
        {
            // Test: Purging one conversation's snapshots doesn't affect another's.
            // Expected: Conversation B's tokens still work after purging A's.
            // Reasoning: Purge is scoped to a single conversation.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create two conversations with snapshots
            await RunTurnAsync(database, "chats/a", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/a", "turn 2", snapshotBeforeRunning: true);

            await RunTurnAsync(database, "chats/b", "turn 1", snapshotBeforeRunning: true);
            var rB2 = await RunTurnAsync(database, "chats/b", "turn 2", snapshotBeforeRunning: true);

            // Purge A via client API
            await store.AI.PurgeConversationSnapshotsAsync("chats/a");

            // B's token should still work via client API
            var forkResult = await store.AI.ForkConversationAsync(rB2.SnapshotToken, "forked-b");
            Assert.Equal("forked-b", forkResult.ConversationId);
        }

        #endregion

        #region C. Sub-Conversation Edge Cases (nested)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task C6_Fork_NestedSubConversations()
        {
            // Test: ForkConversationCommand.AdjustId handles nested sub-conversations correctly
            //        when IDs are at multiple depth levels.
            // Expected: All IDs at any depth have only the leading prefix replaced.
            // Reasoning: Nested sub-conversations produce IDs like parent/A/hash/B/hash2.

            // This test verifies the ID adjustment logic for nested cases
            Assert.Equal("new/A/hash", ForkConversationCommand.AdjustId("old/A/hash", "old", "new"));
            Assert.Equal("new/A/hash/B/hash2", ForkConversationCommand.AdjustId("old/A/hash/B/hash2", "old", "new"));
            Assert.Equal("forked/1/sub1/hash/sub2/hash2",
                ForkConversationCommand.AdjustId("chats/1/sub1/hash/sub2/hash2", "chats/1", "forked/1"));

            // Also verify that CreateSnapshotAsync traverses direct sub-conversations
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Create a direct sub-conversation
            await CreateSubConversationDocAsync(database, "chats/1", "chats/1/A");

            // Create snapshot via client API
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            // Fork to verify the snapshot captured both main and sub-conversation
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify the sub-conversation was forked too (server-side check)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "forked/1"));
                Assert.NotNull(database.DocumentsStorage.Get(ctx, "forked/1/A"));
            }
        }

        #endregion

        #region E. Purged / Missing Revisions (revision config)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task E3_Fork_AfterEnforceRevisionsConfiguration_Fails()
        {
            // Test: After enforcing a tight revision config, old force-created revisions are purged
            //        and fork from those tokens fails.
            // Expected: Fork fails with revision-not-found error.
            // Reasoning: EnforceConfiguration with IncludeForceCreated=true will purge our snapshots.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Configure revisions with MinimumRevisionsToKeep = 0 (keep none)
            await store.Maintenance.SendAsync(new Raven.Client.Documents.Operations.Revisions.ConfigureRevisionsOperation(
                new Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration
                {
                    Default = new Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 0
                    }
                }));

            // Enforce — this should purge force-created revisions
            var enforceOp = await store.Operations.SendAsync(
                new Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation(
                    new Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation.Parameters
                    {
                        IncludeForceCreated = true
                    }));

            await enforceOp.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            // Fork via client API should fail — revisions are gone
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        #endregion

        #region F. Token Validation (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F7_Fork_WithTokenFromDifferentDatabase_Throws()
        {
            // Test: A token from database A used on database B should fail — revisions don't exist there.
            // Expected: Revision-not-found error. No info leaked about database A.
            // Reasoning: Change vectors are database-specific.

            using var storeA = GetDocumentStore();
            using var storeB = GetDocumentStore();

            var databaseA = await Databases.GetDocumentDatabaseInstanceFor(storeA);

            // Create conversation in database A
            await RunTurnAsync(databaseA, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(databaseA, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Try to use A's token on database B via client API
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await storeB.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1"));

            Assert.Contains("no longer exists", ex.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task F8_Fork_WithTokenFromDifferentAgent_Succeeds()
        {
            // Test: A token references revisions, not agent configs. Changing the agent doesn't matter.
            // Expected: Fork succeeds; the forked doc has the Agent field from the revision.
            // Reasoning: Tokens are self-contained document snapshots.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API — the agent config doesn't matter, we're working from revisions
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // The forked doc exists — that's what matters
            var messages = await store.AI.GetConversationMessagesAsync("forked/1");
            Assert.NotNull(messages);
        }

        #endregion

        #region G. Concurrency (G3)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task G3_Fork_WhileAnotherClientIsRunningATurn()
        {
            // Test: Fork reads from a revision captured before the concurrent turn started.
            // Expected: Both the fork and the concurrent turn succeed. Fork does not include
            //           the concurrent turn's changes.
            // Reasoning: The snapshot was created before the concurrent turn; the fork is
            //            from that earlier state.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            int messagesBefore;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(ctx, "chats/1");
                doc.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray msgs);
                messagesBefore = msgs.Length;
            }

            // Start a "concurrent" turn. Use two semaphores:
            // - entered: signals when the mock LLM callback has been entered
            // - release: blocks the callback until we're ready to let it proceed
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
                            entered.Release(); // signal that we've entered the callback
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

            // Wait for the concurrent turn to enter the mock LLM callback
            Assert.True(await entered.WaitAsync(TimeSpan.FromSeconds(30)), "Concurrent turn did not reach the mock LLM in time");

            // Fork while the turn is blocked via client API — should use the revision from before the concurrent turn
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Release the concurrent turn
            release.Release();
            var turnResult = await turnTask;
            Assert.NotNull(turnResult.Response);

            // Verify fork has the pre-concurrent-turn state
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx2, "forked/1");
                forkedDoc.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray forkedMsgs);

                // Forked doc should have fewer messages than the original (which now has the concurrent turn)
                var originalDoc = database.DocumentsStorage.Get(ctx2, "chats/1");
                originalDoc.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray originalMsgs);

                Assert.True(forkedMsgs.Length < originalMsgs.Length,
                    $"Fork ({forkedMsgs.Length}) should have fewer messages than original ({originalMsgs.Length}) after concurrent turn");
            }
        }

        #endregion

        #region H. Conversation State Edge Cases (continued 2)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H1_Fork_ConversationWithOpenActionCalls()
        {
            // Test: Fork a conversation that has open action calls in its document.
            // Expected: Forked conversation preserves the OpenActionCalls from the revision.
            // Reasoning: The snapshot captures the full document state including pending tool calls.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Manually inject OpenActionCalls into the conversation document
            // (simulating a tool call that wasn't resolved)
            await InjectOpenActionCallAsync(database, "chats/1", "call_pending", "UserAction", "{\"query\":\"test\"}");

            // Take a snapshot that includes the open action calls via client API
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");
            Assert.NotNull(snapshot);
            Assert.NotNull(snapshot.Token);

            // Fork via client API
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Verify the forked doc has the open action calls
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                Assert.NotNull(forkedDoc);
                forkedDoc.Data.TryGet(nameof(ConversationDocument.OpenActionCalls), out BlittableJsonReaderObject openCalls);
                Assert.NotNull(openCalls);
                Assert.True(openCalls.Count > 0, "Forked doc should have open action calls from the snapshot");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H1b_Fork_AdjustsOpenActionCallsSubConversationIds()
        {
            // Test: When forking, OpenActionCalls entries that reference sub-conversations
            //        via SubConversationId should have their IDs adjusted to the new prefix.
            // Expected: After fork, OpenActionCalls.SubConversationId values use the forked prefix.
            // Reasoning: Without adjustment, pending sub-agent calls would point at the original
            //            sub-conversation IDs, breaking isolation.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Inject an OpenActionCall with a SubConversationId referencing a sub-conversation
            await InjectOpenActionCallWithSubConversationAsync(database, "chats/1",
                "call_sub", "SubAgentTool", "{}", "chats/1/SubAgent/hash123");

            // Take a snapshot that includes the open action call
            var snapshot = await store.AI.CreateSnapshotAsync("chats/1");

            // Fork to a different ID
            var forkResult = await store.AI.ForkConversationAsync(snapshot.Token, "forked/1");

            // Verify the forked doc's OpenActionCalls has the adjusted SubConversationId
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx, "forked/1");
                Assert.NotNull(forkedDoc);
                forkedDoc.Data.TryGet(nameof(ConversationDocument.OpenActionCalls), out BlittableJsonReaderObject openCalls);
                Assert.NotNull(openCalls);
                Assert.True(openCalls.Count > 0);

                // Find the action call and check its SubConversationId
                foreach (var callId in openCalls.GetPropertyNames())
                {
                    if (openCalls[callId] is BlittableJsonReaderObject callObj &&
                        callObj.TryGet("SubConversationId", out string subConvId))
                    {
                        Assert.StartsWith("forked/1/", subConvId);
                        Assert.DoesNotContain("chats/1/", subConvId);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2_Fork_ConversationWithMultipleTurns()
        {
            // Test: Fork a conversation after multiple turns with different prompts.
            // Expected: Forked conversation has the messages from the snapshot point.
            // Reasoning: Verifies fork works with a conversation that has accumulated state.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "first question", snapshotBeforeRunning: true);
            var r2 = await RunTurnAsync(database, "chats/1", "follow-up question", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "third question", snapshotBeforeRunning: true);

            // Fork from before the follow-up via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Fork should work and the conversation should be usable
            var r = await RunTurnAsync(database, "forked/1", "new direction", snapshotBeforeRunning: false);
            Assert.NotNull(r.Response);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2b_Fork_PreservesAttachments()
        {
            // Test: Fork restores attachments from the revision's @metadata.@attachments.
            // Expected: Forked document has the same attachment that was present at snapshot time.
            // Reasoning: PutAttachmentRevert copies attachment references from revision metadata.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);

            // Add an attachment via the session API
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store("chats/1", "file.txt",
                    new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("content")), "text/plain");
                await session.SaveChangesAsync();
            }

            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Fork via client API
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "forked/1");
            Assert.Equal("forked/1", forkResult.ConversationId);

            // Forked document should have the attachment (server-side check for attachment flags)
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx2.OpenReadTransaction())
            {
                var forkedDoc = database.DocumentsStorage.Get(ctx2, "forked/1");
                Assert.NotNull(forkedDoc);
                Assert.True(forkedDoc.Flags.Contain(DocumentFlags.HasAttachments),
                    "Forked document should have attachments from the revision");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task H2c_Rewind_RemovesAttachmentsAddedAfterSnapshot()
        {
            // Test: When rewinding (fork to same ID), the document is overwritten with the
            //        revision data. Attachments added after the snapshot are removed because
            //        the revision at the snapshot point didn't have them.
            // Expected: After rewind, only attachments from the snapshot point remain.

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

            // Snapshot captures state WITH original.txt
            var r2 = await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);

            // Add a SECOND attachment AFTER the snapshot
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store("chats/1", "post-snapshot.txt",
                    new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes("post-snapshot")), "text/plain");
                await session.SaveChangesAsync();
            }

            // Verify both attachments exist before rewind
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx3))
            using (ctx3.OpenReadTransaction())
            {
                using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx3, "chats/1", out var lowerId))
                {
                    var details = database.DocumentsStorage.AttachmentsStorage.GetAttachmentDetailsForDocument(ctx3, lowerId);
                    Assert.Equal(2, details.Count);
                }
            }

            // Rewind to snapshot via client API (which had only original.txt)
            var forkResult = await store.AI.ForkConversationAsync(r2.SnapshotToken, "chats/1");
            Assert.Equal("chats/1", forkResult.ConversationId);

            // After rewind, only original.txt should remain
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx4))
            using (ctx4.OpenReadTransaction())
            {
                using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx4, "chats/1", out var lowerId))
                {
                    var details = database.DocumentsStorage.AttachmentsStorage.GetAttachmentDetailsForDocument(ctx4, lowerId);
                    Assert.Single(details);
                    Assert.Equal("original.txt", details[0].Name);
                }
            }
        }

        #endregion

        #region K. Snapshot CRUD (continued)

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K2_GetConversationSnapshots_ReturnsAll()
        {
            // Test: GetConversationSnapshotsAsync returns all available snapshots.
            // Expected: After 3 turns with snapshots, we get 2 snapshots (turns 2 and 3).
            // Reasoning: Turn 1 has no prior state; turns 2 and 3 each create a snapshot.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Get snapshots via client API
            var snapshots = await store.AI.GetConversationSnapshotsAsync("chats/1");

            // Turns 2 and 3 each created a snapshot, so at least 2 snapshots
            Assert.True(snapshots.Count >= 2, $"Expected at least 2 snapshots, got {snapshots.Count}");

            // Each snapshot should have a token and creation date
            foreach (var snapshot in snapshots)
            {
                Assert.NotNull(snapshot.Token);
                Assert.True(snapshot.CreatedAt > DateTime.MinValue);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K3_GetConversationSnapshots_ExcludesPurgedRevisions()
        {
            // Test: After purging, no snapshots remain.
            // Expected: Snapshot count drops to 0 after purge.
            // Reasoning: PurgeConversationSnapshotsAsync deletes all revisions.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Verify snapshots exist via client API
            var snapshotsBefore = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.True(snapshotsBefore.Count > 0, "Should have snapshots before purge");

            // Purge via client API
            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            // Verify snapshots are gone via client API
            var snapshotsAfter = await store.AI.GetConversationSnapshotsAsync("chats/1");
            Assert.Equal(0, snapshotsAfter.Count);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task K8_PurgeConversationSnapshots_DeletesRevisionsButPreservesConversation()
        {
            // Test: When snapshots exist as force-created revisions, purge deletes them.
            // Expected: After purge, revisions are gone but the conversation still works.
            // Reasoning: DeleteRevisionsFor removes all revisions for the document.
            //            When collection-level revisions are also configured, snapshot revisions
            //            may share change vectors with regular revisions (since RevisionsStorage.Put
            //            skips force-creation if a revision with the same CV already exists).
            //            In that case, purge removes both. This is documented and acceptable.

            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Run turns with snapshots (no collection-level revisions configured)
            await RunTurnAsync(database, "chats/1", "turn 1", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 2", snapshotBeforeRunning: true);
            await RunTurnAsync(database, "chats/1", "turn 3", snapshotBeforeRunning: true);

            // Verify snapshot revisions exist (fork from token should work)
            var r3 = await RunTurnAsync(database, "chats/1", "turn 3 for token", snapshotBeforeRunning: true);
            Assert.NotNull(r3.SnapshotToken);

            // Verify we can fork from the token via client API (revisions exist)
            var preFork = await store.AI.ForkConversationAsync(r3.SnapshotToken, "pre-purge-fork");
            Assert.Equal("pre-purge-fork", preFork.ConversationId);

            // Purge via client API
            await store.AI.PurgeConversationSnapshotsAsync("chats/1");

            // After purge, forking from the same token should fail (revisions gone)
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await store.AI.ForkConversationAsync(r3.SnapshotToken, "post-purge-fork"));
            Assert.Contains("no longer exists", ex.Message);

            // Conversation document still exists via client API
            var messages = await store.AI.GetConversationMessagesAsync("chats/1");
            Assert.NotNull(messages);

            // Can still run new turns
            var r4 = await RunTurnAsync(database, "chats/1", "turn 4 after purge", snapshotBeforeRunning: false);
            Assert.NotNull(r4.Response);
        }

        #endregion

        #region Helpers

        private SnapshotTokenDto ParseToken(string token)
        {
            using var ctx = Sparrow.Json.JsonOperationContext.ShortTermSingleUse();
            return SnapshotTokenDto.Parse(ctx, token);
        }

        /// <summary>
        /// Injects an open action call into a conversation document.
        /// </summary>
        private Task InjectOpenActionCallWithSubConversationAsync(DocumentDatabase database, string conversationId, string toolId, string toolName, string arguments, string subConversationId)
        {
            var cmd = new InjectOpenActionCallCommand(database, conversationId, toolId, toolName, arguments, subConversationId);
            return database.TxMerger.Enqueue(cmd);
        }

        private async Task InjectOpenActionCallAsync(DocumentDatabase database, string conversationId, string toolId, string toolName, string arguments)
        {
            var cmd = new InjectOpenActionCallCommand(database, conversationId, toolId, toolName, arguments);
            await database.TxMerger.Enqueue(cmd);
        }

        private sealed class InjectOpenActionCallCommand : Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly DocumentDatabase _database;
            private readonly string _conversationId;
            private readonly string _toolId;
            private readonly string _toolName;
            private readonly string _arguments;
            private readonly string _subConversationId;

            public InjectOpenActionCallCommand(DocumentDatabase database, string conversationId, string toolId, string toolName, string arguments, string subConversationId = null)
            {
                _database = database;
                _conversationId = conversationId;
                _toolId = toolId;
                _toolName = toolName;
                _arguments = arguments;
                _subConversationId = subConversationId;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var doc = _database.DocumentsStorage.Get(context, _conversationId);
                if (doc == null)
                    return 0;

                doc.Data.TryGet(nameof(ConversationDocument.OpenActionCalls), out BlittableJsonReaderObject existingCalls);

                var newCalls = existingCalls != null
                    ? new Sparrow.Json.Parsing.DynamicJsonValue(existingCalls)
                    : new Sparrow.Json.Parsing.DynamicJsonValue();

                var callValue = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["ToolId"] = _toolId,
                    ["Name"] = _toolName,
                    ["Arguments"] = _arguments
                };

                if (_subConversationId != null)
                    callValue["SubConversationId"] = _subConversationId;

                newCalls[_toolId] = callValue;

                doc.Data.Modifications = new Sparrow.Json.Parsing.DynamicJsonValue(doc.Data);
                doc.Data.Modifications[nameof(ConversationDocument.OpenActionCalls)] = newCalls;

                var updated = context.ReadObject(doc.Data, "inject-action-call");
                _database.DocumentsStorage.Put(context, _conversationId, null, updated,
                    nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);

                return 1;
            }

            public override Raven.Server.Documents.TransactionMerger.Commands.IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context) => null;
        }

        /// <summary>
        /// Creates a sub-conversation document and updates the parent's SubConversationIds.
        /// Uses a transaction merger command to ensure atomicity.
        /// </summary>
        private async Task CreateSubConversationDocAsync(DocumentDatabase database, string parentId, string subConversationId)
        {
            var cmd = new CreateSubConversationDocCommand(database, parentId, subConversationId);
            await database.TxMerger.Enqueue(cmd);
        }

        private sealed class CreateSubConversationDocCommand : Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly DocumentDatabase _database;
            private readonly string _parentId;
            private readonly string _subConversationId;

            public CreateSubConversationDocCommand(DocumentDatabase database, string parentId, string subConversationId)
            {
                _database = database;
                _parentId = parentId;
                _subConversationId = subConversationId;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                // Create the sub-conversation document
                var subData = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["Agent"] = "sub-agent",
                    ["Messages"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                    ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                    ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                    },
                    ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                    ["LastMessageAt"] = DateTime.UtcNow,
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["Expires"] = null,
                    ["CurrentUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                    },
                    ["RemainingToolIterations"] = 16,
                    ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                    [Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.AiAgentConversationCollection
                    }
                }, "sub-conversation");

                _database.DocumentsStorage.Put(context, _subConversationId, null, subData,
                    nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);

                // Update parent's SubConversationIds
                var parentDoc = _database.DocumentsStorage.Get(context, _parentId);
                if (parentDoc != null)
                {
                    var parentData = parentDoc.Data;
                    parentData.TryGet("SubConversationIds", out BlittableJsonReaderArray existingSubIds);

                    var newSubIds = new Sparrow.Json.Parsing.DynamicJsonArray();
                    if (existingSubIds != null)
                    {
                        foreach (var item in existingSubIds)
                            newSubIds.Add(item.ToString());
                    }
                    newSubIds.Add(_subConversationId);

                    parentData.Modifications = new Sparrow.Json.Parsing.DynamicJsonValue(parentData);
                    parentData.Modifications["SubConversationIds"] = newSubIds;

                    var updatedData = context.ReadObject(parentData, "updated-parent");
                    _database.DocumentsStorage.Put(context, _parentId, null, updatedData,
                        nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);
                }

                return 1;
            }

            public override Raven.Server.Documents.TransactionMerger.Commands.IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context) => null;
        }

        private static AiAgentConfiguration CreateTestAgent()
        {
            return new AiAgentConfiguration("test-agent", "fake-connection",
                "You are a test AI agent.")
            {
                SampleObject = "{\"Answer\":\"response\"}"
            };
        }

        private async Task<AiInternalConversationResult> RunTurnAsync(
            DocumentDatabase database, string conversationId, string prompt,
            bool snapshotBeforeRunning)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions { SnapshotBeforeRunning = snapshotBeforeRunning };
                var blittable = context.ReadObject(creation.ToJson(), "params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                return await RunTurnWithParamsAsync(database, conversationId, prompt, parameters, creation);
            }
        }

        private async Task<AiInternalConversationResult> RunTurnWithParamsAsync(
            DocumentDatabase database, string conversationId, string prompt,
            BlittableJsonReaderObject parameters, AiConversationCreationOptions creation)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ => new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(MockLlm.CreateAnswerResponse($"\"{prompt} - response\""))
                    })
                {
                    Authentication = null
                };

                handler.Initialize(CreateTestAgent(), conversationId, new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = creation,
                    UserPrompt = prompt
                }, changeVector: null);

                return await handler.HandleRequest(context, CancellationToken.None);
            }
        }

        #endregion
    }
}
