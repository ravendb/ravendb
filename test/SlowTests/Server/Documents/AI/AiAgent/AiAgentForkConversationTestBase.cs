using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
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

        protected void InjectOpenActionCallWithSubConversation(IDocumentStore store, string conversationId, string toolId, string toolName, string arguments, string subConversationId)
        {
            InjectOpenActionCall(store, conversationId, toolId, toolName, arguments, subConversationId);
        }

        protected void InjectOpenActionCall(IDocumentStore store, string conversationId, string toolId, string toolName, string arguments, string subConversationId = null)
        {
            using var session = store.OpenSession();
            var doc = session.Load<JObject>(conversationId);
            if (doc == null)
                return;

            var openCalls = doc[nameof(ConversationDocument.OpenActionCalls)] as JObject ?? new JObject();

            var callValue = new JObject
            {
                ["ToolId"] = toolId,
                ["Name"] = toolName,
                ["Arguments"] = arguments
            };

            if (subConversationId != null)
                callValue["SubConversationId"] = subConversationId;

            openCalls[toolId] = callValue;
            doc[nameof(ConversationDocument.OpenActionCalls)] = openCalls;

            session.SaveChanges();
        }

        protected void CreateSubConversationDoc(IDocumentStore store, string parentId, string subConversationId)
        {
            using (var session = store.OpenSession())
            {
                var subDoc = new JObject
                {
                    ["Agent"] = "sub-agent",
                    ["Messages"] = new JArray(),
                    ["LinkedConversations"] = new JArray(),
                    ["TotalUsage"] = new JObject { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                    ["OpenActionCalls"] = new JObject(),
                    ["LastMessageAt"] = DateTime.UtcNow,
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["Expires"] = null,
                    ["CurrentUsage"] = new JObject { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                    ["RemainingToolIterations"] = 16,
                    ["SubConversationIds"] = new JArray()
                };
                session.Store(subDoc, subConversationId);

                var metadata = session.Advanced.GetMetadataFor(subDoc);
                metadata[Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection;

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var parentDoc = session.Load<JObject>(parentId);
                if (parentDoc != null)
                {
                    var subIds = parentDoc["SubConversationIds"] as JArray ?? new JArray();
                    subIds.Add(subConversationId);
                    parentDoc["SubConversationIds"] = subIds;
                    session.SaveChanges();
                }
            }
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
            bool snapshotBeforeRunning = false,
            AiAgentConfiguration agent = null,
            Dictionary<string, object> parameters = null)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions { SnapshotBeforeRunning = snapshotBeforeRunning };
                if (parameters != null)
                {
                    foreach (var (key, value) in parameters)
                        creation.AddParameter(key, value);
                }

                var blittable = context.ReadObject(creation.ToJson(), "params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject blittableParams);

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ => new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(MockLlm.CreateAnswerResponse($"\"{prompt} - response\""))
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agent ?? CreateTestAgent(), conversationId, new RequestBody
                {
                    Parameters = blittableParams,
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

        protected void PutRogueDocument(IDocumentStore store, string documentId)
        {
            using var session = store.OpenSession();
            session.Store(new JObject { ["Rogue"] = true }, documentId);
            session.SaveChanges();
        }

        protected void DeleteDocument(IDocumentStore store, string documentId)
        {
            using var session = store.OpenSession();
            session.Delete(documentId);
            session.SaveChanges();
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
