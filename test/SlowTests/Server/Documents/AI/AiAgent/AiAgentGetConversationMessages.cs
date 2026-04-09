using System;
using System.Collections.Generic;
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
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentGetConversationMessages(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string AgentName = "test-agent";
    private const string SystemPrompt = "You are a helpful test assistant.";

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_BasicDetailedView()
    {
        using var store = GetDocumentStore();
        var conversationId = await RunMockConversation(store, "chats/1", "Hello, how are you?");

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        Assert.NotNull(result);
        Assert.Equal(conversationId, result.ConversationId);
        Assert.Equal(AgentName, result.Agent);
        Assert.NotNull(result.TotalUsage);
        Assert.True(result.Messages.Count > 0);

        // Should have system, user, and assistant messages
        Assert.Contains(result.Messages, m => m.Role == AiMessageRole.System);
        Assert.Contains(result.Messages, m => m.Role == AiMessageRole.User && m.Content == "Hello, how are you?");
        Assert.Contains(result.Messages, m => m.Role == AiMessageRole.Assistant);

        // All messages should have timestamps, monotonically increasing
        for (int i = 1; i < result.Messages.Count; i++)
            Assert.True(result.Messages[i].Timestamp > result.Messages[i - 1].Timestamp,
                $"Message {i} timestamp should be after message {i - 1}");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_SimpleView()
    {
        using var store = GetDocumentStore();
        var conversationId = await RunMockConversation(store, "chats/2", "What is the weather?");

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Simple,
                PageSize = 50
            });

        Assert.NotNull(result);

        // Simple view should only have User and Assistant messages with content
        foreach (AiConversationMessage msg in result.Messages)
        {
            Assert.True(msg.Role == AiMessageRole.User || msg.Role == AiMessageRole.Assistant,
                $"Simple view should not include {msg.Role} messages");
            Assert.NotNull(msg.Content);
        }

        // Should NOT include system messages
        Assert.DoesNotContain(result.Messages, m => m.Role == AiMessageRole.System);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_ToolCallGrouping()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var agent = CreateAgentWithTools();

        int callCount = 0;
        var conversationId = await RunMockConversationWithHandler(database, agent, "chats/3", "Find my recent orders",
            onRequest: _ =>
            {
                callCount++;
                if (callCount == 1)
                {
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(MockLlm.CreateToolCallResponse("RecentOrder", "{}"))
                    };
                }
                return null; // fall through to default (echoes tool result as answer)
            });

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        // Find the assistant message that has tool calls
        var toolCallMsg = result.Messages.Find(m => m.Role == AiMessageRole.Assistant && m.ToolCalls != null);
        Assert.NotNull(toolCallMsg);
        Assert.Single(toolCallMsg.ToolCalls);
        Assert.Equal("RecentOrder", toolCallMsg.ToolCalls[0].Name);
        Assert.NotNull(toolCallMsg.ToolCalls[0].Result); // tool response should be inlined
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_BeforePaging()
    {
        using var store = GetDocumentStore();

        // Create a conversation with multiple turns
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var agent = CreateSimpleAgent();

        var conversationId = await RunMultiTurnConversation(database, agent, "chats/4",
            "Question 1", "Question 2", "Question 3");

        // Get all messages first
        var all = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 100
            });

        Assert.True(all.Messages.Count >= 6, $"Should have at least 6 messages (system + params + 3 turns), got {all.Messages.Count}");

        // Now page backward: get messages before the last message
        var lastMsg = all.Messages[^1];
        var older = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                Before = lastMsg.Timestamp,
                PageSize = 100
            });

        Assert.True(older.Messages.Count < all.Messages.Count,
            "Before paging should return fewer messages");
        foreach (AiConversationMessage msg in older.Messages)
            Assert.True(msg.Timestamp < lastMsg.Timestamp,
                "All messages should be before the cursor");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_AfterPaging()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var agent = CreateSimpleAgent();

        var conversationId = await RunMultiTurnConversation(database, agent, "chats/5",
            "First question", "Second question", "Third question");

        // Get all messages
        var all = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 100
            });

        // Pick a midpoint timestamp
        int mid = all.Messages.Count / 2;
        var midTimestamp = all.Messages[mid].Timestamp;

        // Get messages after that point
        var newer = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                After = midTimestamp,
                PageSize = 100
            });

        Assert.True(newer.Messages.Count > 0);
        foreach (AiConversationMessage msg in newer.Messages)
            Assert.True(msg.Timestamp > midTimestamp,
                "All messages should be after the cursor");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_PageSizeLimit()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var agent = CreateSimpleAgent();

        var conversationId = await RunMultiTurnConversation(database, agent, "chats/6",
            "Q1", "Q2", "Q3", "Q4", "Q5");

        // Request with small page size
        var page = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 3
            });

        Assert.True(page.Messages.Count <= 3);
        Assert.True(page.HasOlderMessages, "Should indicate there are older messages");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_DefaultReturnsLatest()
    {
        using var store = GetDocumentStore();
        var conversationId = await RunMockConversation(store, "chats/7", "Hello!");

        // Simple convenience call
        var result = store.AI.GetConversationMessages(conversationId);

        Assert.NotNull(result);
        Assert.Equal(conversationId, result.ConversationId);
        Assert.True(result.Messages.Count > 0);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_ArrayContentJoinsTextParts()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        // Directly store a conversation doc with array-format content
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = new Sparrow.Json.Parsing.DynamicJsonArray
                {
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonArray
                        {
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "text", ["text"] = "Look at this image" },
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "text", ["text"] = "What do you see?" }
                        },
                        ["date"] = DateTime.UtcNow
                    }
                },
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = DateTime.UtcNow,
                ["CreatedAt"] = DateTime.UtcNow,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");

            database.DocumentsStorage.Put(context, "chats/array-content", null, doc);
            tx.Commit();
        }

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/array-content",
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        Assert.Single(result.Messages);
        var msg = result.Messages[0];
        Assert.Equal(AiMessageRole.User, msg.Role);
        Assert.Equal($"Look at this image{Environment.NewLine}What do you see?", msg.Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_AttachmentsExtracted()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = new Sparrow.Json.Parsing.DynamicJsonArray
                {
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonArray
                        {
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "text", ["text"] = "Please analyze this" },
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "image_url", ["name"] = "report.pdf" },
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "image_url", ["name"] = "chart.png" }
                        },
                        ["date"] = DateTime.UtcNow
                    }
                },
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = DateTime.UtcNow,
                ["CreatedAt"] = DateTime.UtcNow,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");

            database.DocumentsStorage.Put(context, "chats/attachments", null, doc);
            tx.Commit();
        }

        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/attachments",
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        Assert.Single(result.Messages);
        var msg = result.Messages[0];
        Assert.Equal(AiMessageRole.User, msg.Role);
        Assert.Equal("Please analyze this", msg.Content);
        Assert.NotNull(msg.Attachments);
        Assert.Equal(2, msg.Attachments.Count);
        Assert.Equal("report.pdf", msg.Attachments[0]);
        Assert.Equal("chart.png", msg.Attachments[1]);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TruncationDoesNotSplitToolCallGroup()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        // Build a conversation with a tool call group in the middle:
        //   [0] system
        //   [1] user "q1"
        //   [2] assistant (tool_calls: [RecentOrder])
        //   [3] tool (response to RecentOrder)
        //   [4] assistant "answer1"
        //   [5] user "q2"
        //   [6] assistant "answer2"
        //
        // Truncation config: keep 4 messages after truncate (from 7).
        // Without the fix: truncateCount = 7 - 4 = 3, removes [1],[2],[3] — splits the tool group
        //   because [2] (assistant with tool_calls) goes to history but [3] (tool response) stays.
        // With the fix: the cut point at index 4 sees [4] is not a tool message, so no adjustment.
        //   Actually truncateCount=3 removes indices 1,2,3 — keeping [0],[4],[5],[6]. That's fine.
        //
        // The problematic case is truncateCount = 2, removing [1],[2] — keeping [0],[3],[4],[5],[6].
        //   Now [3] is a tool response without its parent assistant message.
        // So set: keep 5 messages → truncateCount = 7 - 5 = 2, cut lands at index 3 which is a tool msg.

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var now = DateTime.UtcNow;
            var messages = new Sparrow.Json.Parsing.DynamicJsonArray
            {
                MakeMsg(context, "system", "You are helpful", now.AddMinutes(-6)),
                MakeMsg(context, "user", "q1", now.AddMinutes(-5)),
                MakeAssistantWithToolCall(context, "call_1", "RecentOrder", "{}", now.AddMinutes(-4)),
                MakeToolResponse(context, "call_1", "order data here", now.AddMinutes(-3)),
                MakeMsg(context, "assistant", "answer1", now.AddMinutes(-2)),
                MakeMsg(context, "user", "q2", now.AddMinutes(-1)),
                MakeMsg(context, "assistant", "answer2", now),
            };

            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = messages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = now,
                ["CreatedAt"] = now.AddMinutes(-6),
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");

            database.DocumentsStorage.Put(context, "chats/truncate-test", null, doc);
            tx.Commit();
        }

        // Now load the document as a ConversationDocument, apply truncation, and verify
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var doc = database.DocumentsStorage.Get(context, "chats/truncate-test");
            var conversation = ConversationDocument.ToDocument("chats/truncate-test", doc.Data, 16);

            Assert.Equal(7, conversation.Messages.Count);

            // Simulate truncation: keep 5 → truncateCount = 2
            // Without fix: removes indices [1],[2], leaving orphan tool response at [3]
            // With fix: should advance past the tool response, removing [1],[2],[3] instead
            var truncateCount = conversation.Messages.Count - 5; // = 2
            int cutIndex = 1 + truncateCount; // = 3
            while (cutIndex < conversation.Messages.Count)
            {
                var msg = conversation.Messages[cutIndex];
                if (msg.TryGet("role", out string role) && role == "tool")
                {
                    cutIndex++;
                    truncateCount++;
                }
                else
                    break;
            }
            truncateCount = int.Min(truncateCount, conversation.Messages.Count - 1);

            // truncateCount should have been bumped from 2 to 3
            Assert.Equal(3, truncateCount);

            conversation.Messages.RemoveRange(1, truncateCount);

            // Remaining: [system, answer1, q2, answer2] — no orphan tool response
            Assert.Equal(4, conversation.Messages.Count);
            foreach (var msg in conversation.Messages)
            {
                msg.TryGet("role", out string role);
                Assert.NotEqual("tool", role);
            }
        }
    }

    #region Test Message Helpers

    private static BlittableJsonReaderObject MakeMsg(DocumentsOperationContext context, string role, string content, DateTime date)
    {
        return context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
        {
            ["role"] = role,
            ["content"] = content,
            ["date"] = date
        }, "msg");
    }

    private static BlittableJsonReaderObject MakeAssistantWithToolCall(DocumentsOperationContext context,
        string callId, string toolName, string arguments, DateTime date)
    {
        return context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
        {
            ["role"] = "assistant",
            ["content"] = null,
            ["tool_calls"] = new Sparrow.Json.Parsing.DynamicJsonArray
            {
                new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["id"] = callId,
                    ["type"] = "function",
                    ["function"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["name"] = toolName,
                        ["arguments"] = arguments
                    }
                }
            },
            ["date"] = date
        }, "msg");
    }

    private static BlittableJsonReaderObject MakeToolResponse(DocumentsOperationContext context,
        string callId, string content, DateTime date)
    {
        return context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
        {
            ["role"] = "tool",
            ["tool_call_id"] = callId,
            ["content"] = content,
            ["date"] = date
        }, "msg");
    }

    #endregion

    #region Helpers

    private AiAgentConfiguration CreateSimpleAgent()
    {
        return new AiAgentConfiguration(AgentName, "fake-connection", SystemPrompt)
        {
            Identifier = AgentName,
            SampleObject = "{\"Answer\":\"The answer\"}"
        };
    }

    private AiAgentConfiguration CreateAgentWithTools()
    {
        var agent = CreateSimpleAgent();
        agent.Queries =
        [
            new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders limit 5")
            {
                ParametersSampleObject = "{}"
            }
        ];
        return agent;
    }

    private async Task<string> RunMockConversation(IDocumentStore store, string conversationId, string userPrompt)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var agent = CreateSimpleAgent();
        return await RunMockConversationWithHandler(database, agent, conversationId, userPrompt);
    }

    private async Task<string> RunMockConversationWithHandler(
        DocumentDatabase database,
        AiAgentConfiguration agent,
        string conversationId,
        string userPrompt,
        Func<JObject, HttpResponseMessage> onRequest = null)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = new MockLlmConversationHandler(Server.ServerStore, database, onRequest: onRequest)
            {
                Authentication = null
            };

            handler.Initialize(agent, conversationId, new RequestBody
            {
                CreationOptions = new AiConversationCreationOptions(),
                UserPrompt = userPrompt
            }, changeVector: null);

            await handler.HandleRequest(context, CancellationToken.None);
            return conversationId;
        }
    }

    private async Task<string> RunMultiTurnConversation(
        DocumentDatabase database,
        AiAgentConfiguration agent,
        string conversationId,
        params string[] prompts)
    {
        string changeVector = null;

        foreach (string prompt in prompts)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database)
                {
                    Authentication = null
                };

                handler.Initialize(agent, conversationId, new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = prompt
                }, changeVector: changeVector);

                await handler.HandleRequest(context, CancellationToken.None);

                // Read back change vector for next turn's concurrency check
                using (context.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.Get(context, conversationId);
                    changeVector = doc?.ChangeVector;
                }
            }
        }

        return conversationId;
    }

    #endregion
}
