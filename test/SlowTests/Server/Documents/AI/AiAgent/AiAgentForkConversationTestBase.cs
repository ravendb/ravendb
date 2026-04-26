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
using Raven.Client.Documents;
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
    public abstract class AiAgentForkConversationTestBase : RavenTestBase
    {
        protected AiAgentForkConversationTestBase(ITestOutputHelper output) : base(output)
        {
        }

        internal SnapshotTokenDto ParseToken(string token)
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            return SnapshotTokenDto.Parse(ctx, token);
        }

        protected Task InjectOpenActionCallWithSubConversationAsync(DocumentDatabase database, string conversationId, string toolId, string toolName, string arguments, string subConversationId)
        {
            var cmd = new InjectOpenActionCallCommand(database, conversationId, toolId, toolName, arguments, subConversationId);
            return database.TxMerger.Enqueue(cmd);
        }

        protected async Task InjectOpenActionCallAsync(DocumentDatabase database, string conversationId, string toolId, string toolName, string arguments)
        {
            var cmd = new InjectOpenActionCallCommand(database, conversationId, toolId, toolName, arguments);
            await database.TxMerger.Enqueue(cmd);
        }

        protected sealed class InjectOpenActionCallCommand : Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
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

        protected async Task CreateSubConversationDocAsync(DocumentDatabase database, string parentId, string subConversationId)
        {
            var cmd = new CreateSubConversationDocCommand(database, parentId, subConversationId);
            await database.TxMerger.Enqueue(cmd);
        }

        protected sealed class CreateSubConversationDocCommand : Raven.Server.Documents.TransactionMerger.Commands.MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
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

                var parentDoc = _database.DocumentsStorage.Get(context, _parentId);
                if (parentDoc != null)
                {
                    var parentData = parentDoc.Data;
                    parentData.TryGet("SubConversationIds", out BlittableJsonReaderArray existingSubIds);

                    var newSubIds = new Sparrow.Json.Parsing.DynamicJsonArray();
                    if (existingSubIds != null)
                    {
                        for (int i = 0; i < existingSubIds.Length; i++)
                            newSubIds.Add(existingSubIds[i].ToString());
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

        protected static AiAgentConfiguration CreateTestAgent()
        {
            return new AiAgentConfiguration("test-agent", "fake-connection",
                "You are a test AI agent.")
            {
                SampleObject = "{\"Answer\":\"response\"}"
            };
        }

        protected static AiAgentConfiguration CreateTestAgentWithTruncation()
        {
            var agent = new AiAgentConfiguration("test-agent-truncation", "fake-connection",
                "You are a test AI agent.")
            {
                SampleObject = "{\"Answer\":\"response\"}",
                ChatTrimming = new AiAgentChatTrimmingConfiguration
                {
                    Truncate = new AiAgentTruncateChat
                    {
                        MessagesLengthBeforeTruncate = 5,
                        MessagesLengthAfterTruncate = 3
                    },
                    History = new AiAgentHistoryConfiguration()
                }
            };
            return agent;
        }

        protected async Task<AiInternalConversationResult> RunTurnAsync(
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

        protected async Task<AiInternalConversationResult> RunTurnWithAgentAsync(
            DocumentDatabase database, string conversationId, string prompt,
            bool snapshotBeforeRunning, AiAgentConfiguration agent)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions { SnapshotBeforeRunning = snapshotBeforeRunning };
                var blittable = context.ReadObject(creation.ToJson(), "params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ => new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(MockLlm.CreateAnswerResponse($"\"{prompt} - response\""))
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agent, conversationId, new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = creation,
                    UserPrompt = prompt
                }, changeVector: null);

                return await handler.HandleRequest(context, CancellationToken.None);
            }
        }

        protected async Task<AiInternalConversationResult> RunTurnWithParamsAsync(
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

        protected async Task<AiConversationMessagesResult> GetDetailedMessagesAsync(
            Raven.Client.Documents.IDocumentStore store, string conversationId)
        {
            return await store.AI.GetConversationMessagesAsync(
                new GetConversationMessagesOptions
                {
                    ConversationId = conversationId,
                    DetailLevel = AiConversationDetailLevel.Detailed,
                    PageSize = 100
                });
        }

        protected static void AssertExactMessageCount(AiConversationMessagesResult result, int expectedTurnCount, string description)
        {
            // System prompt = 1 message, each turn = 2 messages (user + assistant)
            int expected = 1 + 2 * expectedTurnCount;
            Assert.Equal(expected, result.Messages.Count);
        }

        protected List<string> GetLinkedConversations(IDocumentStore store, string conversationId)
        {
            using var session = store.OpenSession();
            var doc = session.Load<JObject>(conversationId);
            if (doc == null)
                return new List<string>();
            var linked = doc[nameof(ConversationDocument.LinkedConversations)] as JArray;
            return linked?.Select(x => x.ToString()).ToList() ?? new List<string>();
        }

        protected HashSet<string> GetSubConversationIds(IDocumentStore store, string conversationId)
        {
            using var session = store.OpenSession();
            var doc = session.Load<JObject>(conversationId);
            if (doc == null)
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var subs = doc[nameof(ConversationDocument.SubConversationIds)] as JArray;
            if (subs == null)
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            return subs.Select(x => x.ToString()).ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        protected bool DocumentExists(IDocumentStore store, string documentId)
        {
            using var session = store.OpenSession();
            return session.Advanced.Exists(documentId);
        }

        protected bool HasOpenActionCalls(IDocumentStore store, string conversationId)
        {
            using var session = store.OpenSession();
            var doc = session.Load<JObject>(conversationId);
            if (doc == null)
                return false;
            var openCalls = doc[nameof(ConversationDocument.OpenActionCalls)] as JObject;
            return openCalls != null && openCalls.Count > 0;
        }

        protected Dictionary<string, Dictionary<string, object>> GetOpenActionCalls(IDocumentStore store, string conversationId)
        {
            using var session = store.OpenSession();
            var doc = session.Load<JObject>(conversationId);
            var openCalls = doc?[nameof(ConversationDocument.OpenActionCalls)] as JObject;
            if (openCalls == null)
                return new Dictionary<string, Dictionary<string, object>>();

            var result = new Dictionary<string, Dictionary<string, object>>();
            foreach (var prop in openCalls.Properties())
            {
                var callObj = prop.Value as JObject;
                if (callObj == null)
                    continue;
                var entry = new Dictionary<string, object>();
                foreach (var inner in callObj.Properties())
                    entry[inner.Name] = inner.Value.Type == JTokenType.String ? inner.Value.ToString() : (object)inner.Value;
                result[prop.Name] = entry;
            }
            return result;
        }

        protected JObject GetDocumentAsJObject(IDocumentStore store, string documentId)
        {
            using var session = store.OpenSession();
            return session.Load<JObject>(documentId);
        }

        protected void PutRogueDocument(DocumentDatabase database, string documentId)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                using var tx = ctx.OpenWriteTransaction();
                var rogueData = ctx.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue { ["Rogue"] = true }, "rogue");
                database.DocumentsStorage.Put(ctx, documentId, null, rogueData);
                tx.Commit();
            }
        }

        protected void DeleteDocumentServerSide(DocumentDatabase database, string documentId)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                using var tx = ctx.OpenWriteTransaction();
                database.DocumentsStorage.Delete(ctx, documentId, null);
                tx.Commit();
            }
        }

        protected string BuildFakeSnapshotToken(DocumentDatabase database, string conversationId, Dictionary<string, string> revisions)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                return SnapshotTokenDto.Build(ctx, conversationId, DateTime.UtcNow, revisions);
            }
        }
    }
}
