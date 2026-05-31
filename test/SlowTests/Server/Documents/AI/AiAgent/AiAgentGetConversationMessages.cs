using System;
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
        var toolCallMsg = result.Messages.Find(m => m.Role == AiMessageRole.Assistant && m.ToolCalls is { Count: > 0 });
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
        Assert.True(page.HasMoreMessages, "Should indicate there are older messages");
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
    public async Task CanGetConversationMessages_ReturnsNullForMissingConversation()
    {
        using var store = GetDocumentStore();

        var result = await store.AI.GetConversationMessagesAsync("chats/does-not-exist");

        Assert.Null(result);
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
        Assert.Equal("Look at this image" + Environment.NewLine + "What do you see?", msg.Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_AttachmentsExtracted()
    {
        // The server emits a synthetic user message of the shape "[Attachments: name1, name2, ...]"
        // when attachments are added to a turn (see ConversationHandler.AddMessageWithAttachmentsName).
        // The reader should parse those names into AiConversationMessage.Attachments.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = DateTime.UtcNow;
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = new Sparrow.Json.Parsing.DynamicJsonArray
                {
                    // Real user prompt
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "Please analyze these",
                        ["date"] = baseTime
                    },
                    // Synthetic attachments marker that the server emits when files are attached
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "[Attachments: report.pdf, chart.png, photo.jpg]",
                        ["date"] = baseTime.AddTicks(1)
                    }
                },
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddTicks(1),
                ["CreatedAt"] = baseTime,
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

        Assert.Equal(2, result.Messages.Count);

        // First message: plain user text, no attachments. The client deserializer returns an
        // empty list (not null) when the JSON key is missing/null — see JsonDeserializationBase.ToCollectionOfString.
        Assert.Equal(AiMessageRole.User, result.Messages[0].Role);
        Assert.Equal("Please analyze these", result.Messages[0].Content);
        Assert.Empty(result.Messages[0].Attachments);

        // Second message: the "[Attachments: ...]" marker — names extracted into Attachments,
        // Content nulled (the marker text itself is implementation detail).
        var marker = result.Messages[1];
        Assert.Equal(AiMessageRole.User, marker.Role);
        Assert.Null(marker.Content);
        Assert.NotNull(marker.Attachments);
        Assert.Equal(new[] { "report.pdf", "chart.png", "photo.jpg" }, marker.Attachments);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_RealWorldImageAnalyzerConversation()
    {
        // Replays a realistic persisted "image analyzer" conversation that exercises every
        // content shape the reader has to support:
        //   - string content (system, plain user, tool response)
        //   - structured content array of "text" parts (multi-part user prompt)
        //   - "[Attachments: ...]" synthetic marker (Content nulled, Attachments populated)
        //   - object content (assistant structured output — surfaced as JSON)
        //   - assistant with null content + tool_calls (tool response merged into ToolCalls[].Result)
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        const string conversationId = "chats/image-analyzer";

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            // UTC timestamps from the persisted document supplied by the user. Kept verbatim so the
            // monotonic ordering and dedup-key behaviour can be observed against real data.
            DateTime D(string iso) => DateTime.Parse(iso, null, System.Globalization.DateTimeStyles.RoundtripKind);

            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = "image-analyzer",
                ["Parameters"] = null,
                ["Messages"] = new Sparrow.Json.Parsing.DynamicJsonArray
                {
                    // 1. system
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "system",
                        ["content"] = "You are my friend have a chat with me",
                        ["date"] = D("2026-05-26T14:27:59.9348759Z")
                    },
                    // 2. user — multi-part text
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonArray
                        {
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "text", ["text"] = "what are inside the images I sent you? what are their colors?" },
                            new Sparrow.Json.Parsing.DynamicJsonValue { ["type"] = "text", ["text"] = "what do you see on those images?" }
                        },
                        ["date"] = D("2026-05-26T14:27:59.9553911Z")
                    },
                    // 3. user — synthetic attachments marker
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "[Attachments: banana.png, star.png, heart.png]",
                        ["date"] = D("2026-05-26T14:28:02.9262631Z")
                    },
                    // 4. assistant — structured output (object content)
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonValue
                        {
                            ["Answer"] = "I see three images: 1) a yellow banana with a slight shadow underneath. 2) a yellow five-pointed star on a black background. 3) a red heart (solid) on a black background."
                        },
                        ["date"] = D("2026-05-26T14:28:02.9262740Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 144, ["CompletionTokens"] = 100, ["TotalTokens"] = 244, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    },
                    // 5. user — string
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "look again at banana.png and give me more info",
                        ["date"] = D("2026-05-26T14:29:22.3172944Z")
                    },
                    // 6. assistant — null content + tool_calls
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["content"] = null,
                        ["tool_calls"] = new Sparrow.Json.Parsing.DynamicJsonArray
                        {
                            new Sparrow.Json.Parsing.DynamicJsonValue
                            {
                                ["id"] = "call_dkp18EaCZOxzr8LcOBWm47dS",
                                ["type"] = "function",
                                ["function"] = new Sparrow.Json.Parsing.DynamicJsonValue
                                {
                                    ["name"] = "__RetrieveAttachment",
                                    ["arguments"] = "{\"names\":[\"banana.png\"]}"
                                }
                            }
                        },
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["date"] = D("2026-05-26T14:29:24.9020805Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 158, ["CompletionTokens"] = 26, ["TotalTokens"] = 184, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    },
                    // 7. tool response (merged into msg 6's ToolCalls)
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["tool_call_id"] = "call_dkp18EaCZOxzr8LcOBWm47dS",
                        ["role"] = "tool",
                        ["content"] = "Attachment: banana.png",
                        ["date"] = D("2026-05-26T14:29:24.9139616Z")
                    },
                    // 8. assistant — structured output
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonValue
                        {
                            ["Answer"] = "The banana image shows a single, whole banana lying horizontally with its stem to the right."
                        },
                        ["date"] = D("2026-05-26T14:29:28.5144884Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 0, ["CompletionTokens"] = 228, ["TotalTokens"] = 222, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    },
                    // 9. user
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "who are you",
                        ["date"] = D("2026-05-26T14:29:35.2933234Z")
                    },
                    // 10. assistant — structured output
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonValue
                        {
                            ["Answer"] = "I'm your friend (an AI assistant). I can chat, help with questions, describe images you send, and help with tasks. How can I help next?"
                        },
                        ["date"] = D("2026-05-26T14:29:37.3628655Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 20, ["CompletionTokens"] = 88, ["TotalTokens"] = 108, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    },
                    // 11. user
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "user",
                        ["content"] = "look again at star.png and give me more info",
                        ["date"] = D("2026-05-26T14:29:47.2857319Z")
                    },
                    // 12. assistant — null content + tool_calls
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["content"] = null,
                        ["tool_calls"] = new Sparrow.Json.Parsing.DynamicJsonArray
                        {
                            new Sparrow.Json.Parsing.DynamicJsonValue
                            {
                                ["id"] = "call_5jFEXrZu1A4p4lomIFqabT3V",
                                ["type"] = "function",
                                ["function"] = new Sparrow.Json.Parsing.DynamicJsonValue
                                {
                                    ["name"] = "__RetrieveAttachment",
                                    ["arguments"] = "{\"names\":[\"star.png\"]}"
                                }
                            }
                        },
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["date"] = D("2026-05-26T14:29:48.5266184Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 11, ["CompletionTokens"] = 26, ["TotalTokens"] = 37, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    },
                    // 13. tool response (merged into msg 12's ToolCalls)
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["tool_call_id"] = "call_5jFEXrZu1A4p4lomIFqabT3V",
                        ["role"] = "tool",
                        ["content"] = "Attachment: star.png",
                        ["date"] = D("2026-05-26T14:29:48.5277669Z")
                    },
                    // 14. assistant — structured output
                    new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["role"] = "assistant",
                        ["refusal"] = null,
                        ["annotations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                        ["content"] = new Sparrow.Json.Parsing.DynamicJsonValue
                        {
                            ["Answer"] = "The star image is a simple five-pointed star centered on a solid black background."
                        },
                        ["date"] = D("2026-05-26T14:29:52.4408905Z"),
                        ["usage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                            { ["PromptTokens"] = 0, ["CompletionTokens"] = 229, ["TotalTokens"] = 124, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 }
                    }
                },
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 3097, ["CompletionTokens"] = 697, ["TotalTokens"] = 3794,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = D("2026-05-26T14:29:52.4408905Z"),
                ["CreatedAt"] = D("2026-05-26T14:27:59.9307710Z"),
                ["Expires"] = null,
                ["CurrentUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["PromptTokens"] = 690, ["CompletionTokens"] = 229, ["TotalTokens"] = 919,
                    ["CachedTokens"] = 0, ["ReasoningTokens"] = 0
                },
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");

            database.DocumentsStorage.Put(context, conversationId, null, doc);
            tx.Commit();
        }

        // === Detailed view ===
        // Tool messages get merged into their parent assistant's ToolCalls, so the 2 stand-alone
        // "tool" rows disappear from the result. That leaves: 1 system + 5 user + 6 assistant = 12.
        var detailed = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 100
            });

        Assert.Equal("image-analyzer", detailed.Agent);
        Assert.Equal(12, detailed.Messages.Count);

        // Chronological order (oldest first).
        for (int i = 1; i < detailed.Messages.Count; i++)
            Assert.True(detailed.Messages[i].Timestamp >= detailed.Messages[i - 1].Timestamp);

        // [0] system, plain string content. Client deserializer returns empty list (not null)
        // for missing/null Attachments — see JsonDeserializationBase.ToCollectionOfString.
        Assert.Equal(AiMessageRole.System, detailed.Messages[0].Role);
        Assert.Equal("You are my friend have a chat with me", detailed.Messages[0].Content);
        Assert.Empty(detailed.Messages[0].Attachments);

        // [1] user, multi-part text — joined with line breaks.
        Assert.Equal(AiMessageRole.User, detailed.Messages[1].Role);
        Assert.Equal(
            "what are inside the images I sent you? what are their colors?" + Environment.NewLine + "what do you see on those images?",
            detailed.Messages[1].Content);
        Assert.Empty(detailed.Messages[1].Attachments);

        // [2] user, "[Attachments: ...]" marker → Content null, Attachments populated.
        Assert.Equal(AiMessageRole.User, detailed.Messages[2].Role);
        Assert.Null(detailed.Messages[2].Content);
        Assert.NotNull(detailed.Messages[2].Attachments);
        Assert.Equal(new[] { "banana.png", "star.png", "heart.png" }, detailed.Messages[2].Attachments);

        // [3] assistant, object content → surfaced as JSON containing the Answer.
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[3].Role);
        Assert.NotNull(detailed.Messages[3].Content);
        Assert.Contains("yellow banana", detailed.Messages[3].Content);
        Assert.Contains("Answer", detailed.Messages[3].Content);    // JSON shape preserved
        Assert.NotNull(detailed.Messages[3].Usage);
        Assert.Equal(244, detailed.Messages[3].Usage.TotalTokens);
        // Client deserializer returns empty list (not null) for missing/null ToolCalls.
        Assert.Empty(detailed.Messages[3].ToolCalls);

        // [4] user, plain string.
        Assert.Equal(AiMessageRole.User, detailed.Messages[4].Role);
        Assert.Equal("look again at banana.png and give me more info", detailed.Messages[4].Content);

        // [5] assistant, null content + tool_calls; tool response from msg 7 merged into ToolCalls[0].Result.
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[5].Role);
        Assert.Null(detailed.Messages[5].Content);
        Assert.NotNull(detailed.Messages[5].ToolCalls);
        Assert.Single(detailed.Messages[5].ToolCalls);
        Assert.Equal("call_dkp18EaCZOxzr8LcOBWm47dS", detailed.Messages[5].ToolCalls[0].Id);
        Assert.Equal("__RetrieveAttachment", detailed.Messages[5].ToolCalls[0].Name);
        Assert.Equal("{\"names\":[\"banana.png\"]}", detailed.Messages[5].ToolCalls[0].Arguments);
        Assert.Equal("Attachment: banana.png", detailed.Messages[5].ToolCalls[0].Result);

        // [6] assistant, structured output (after the tool call returned).
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[6].Role);
        Assert.Contains("whole banana lying horizontally", detailed.Messages[6].Content);

        // [7] user
        Assert.Equal(AiMessageRole.User, detailed.Messages[7].Role);
        Assert.Equal("who are you", detailed.Messages[7].Content);

        // [8] assistant
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[8].Role);
        Assert.Contains("AI assistant", detailed.Messages[8].Content);

        // [9] user
        Assert.Equal(AiMessageRole.User, detailed.Messages[9].Role);
        Assert.Equal("look again at star.png and give me more info", detailed.Messages[9].Content);

        // [10] assistant, null content + tool_calls; tool response from msg 13 merged into ToolCalls[0].Result.
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[10].Role);
        Assert.Null(detailed.Messages[10].Content);
        Assert.NotNull(detailed.Messages[10].ToolCalls);
        Assert.Single(detailed.Messages[10].ToolCalls);
        Assert.Equal("call_5jFEXrZu1A4p4lomIFqabT3V", detailed.Messages[10].ToolCalls[0].Id);
        Assert.Equal("Attachment: star.png", detailed.Messages[10].ToolCalls[0].Result);

        // [11] assistant, structured output (star description).
        Assert.Equal(AiMessageRole.Assistant, detailed.Messages[11].Role);
        Assert.Contains("five-pointed star", detailed.Messages[11].Content);

        // Conversation-level metadata
        Assert.NotNull(detailed.TotalUsage);
        Assert.Equal(3794, detailed.TotalUsage.TotalTokens);
        Assert.False(detailed.HasMoreMessages);

        // === Simple view ===
        // User messages all pass; assistants are filtered to those WITH content. The two
        // tool-call-only assistant messages (Content null) get dropped: 5 user + 4 assistant = 9.
        var simple = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Simple,
                PageSize = 100
            });

        Assert.Equal(9, simple.Messages.Count);
        Assert.DoesNotContain(simple.Messages, m => m.Role == AiMessageRole.System);
        Assert.DoesNotContain(simple.Messages, m => m.Role == AiMessageRole.Assistant && m.Content == null);

        // The attachments-marker user message still appears in Simple view (User passes regardless
        // of content), with the parsed Attachments still populated.
        var attachmentsMsg = Assert.Single(simple.Messages, m => m.Attachments is { Count: 3 });
        Assert.Equal(new[] { "banana.png", "star.png", "heart.png" }, attachmentsMsg.Attachments);
        Assert.Null(attachmentsMsg.Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TruncationDoesNotSplitToolCallGroup()
    {
        // Run a conversation with tool calls through the real truncation path.
        // Configure truncation to trigger after a small number of messages, then verify
        // the persisted document has no orphan tool response messages (every tool message
        // is preceded by its parent assistant message with tool_calls).
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var agent = CreateAgentWithTools();
        agent.ChatTrimming = new AiAgentChatTrimmingConfiguration
        {
            Truncate = new AiAgentTruncateChat
            {
                // Trigger truncation early: trim when > 8 messages, keep 4
                MessagesLengthBeforeTruncate = 8,
                MessagesLengthAfterTruncate = 4
            }
        };

        // Run 3 turns with tool calls to build up messages past the truncation threshold.
        // Each turn: user msg + assistant tool_call + tool response + assistant answer = ~4 messages
        // Plus system + params = ~2 initial. After 3 turns we'll have ~14 messages → triggers truncation.
        int callCount = 0;
        var conversationId = "chats/truncate-test";
        string changeVector = null;

        for (int turn = 0; turn < 3; turn++)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                int localCallCount = callCount;
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ =>
                    {
                        localCallCount++;
                        // Every odd call triggers a tool call, every even call returns an answer
                        if (localCallCount % 2 == 1)
                        {
                            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                            {
                                Content = new StringContent(MockLlm.CreateToolCallResponse("RecentOrder", "{}"))
                            };
                        }
                        return null; // fall through to default answer
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agent, conversationId, new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = $"question {turn}"
                }, changeVector: changeVector);

                await handler.HandleRequest(context, CancellationToken.None);
                callCount = localCallCount;

                using (context.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.Get(context, conversationId);
                    changeVector = doc?.ChangeVector;
                }
            }
        }

        // Verify the persisted conversation: no tool message should appear without
        // its parent assistant message immediately before it.
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var doc = database.DocumentsStorage.Get(context, conversationId);
            var conversation = ConversationDocument.ToDocument(conversationId, doc.Data, 16, cloneMessages: false);

            for (int i = 0; i < conversation.Messages.Count; i++)
            {
                var msg = conversation.Messages[i];
                msg.TryGet("role", out string role);

                if (role != "tool")
                    continue;

                Assert.True(i > 0, "A tool message cannot be the first message in the conversation.");

                var previous = conversation.Messages[i - 1];
                previous.TryGet("role", out string previousRole);
                Assert.True(previousRole == "assistant",
                    $"Tool message at index {i} is not preceded by an assistant message (found '{previousRole}').");

                Assert.True(previous.TryGet("tool_calls", out BlittableJsonReaderArray toolCallsArr) && toolCallsArr is { Length: > 0 },
                    $"Tool message at index {i} is preceded by an assistant message without tool_calls.");

                msg.TryGet("tool_call_id", out string toolCallId);
                Assert.False(string.IsNullOrEmpty(toolCallId),
                    $"Tool message at index {i} is missing tool_call_id.");

                bool foundMatch = false;
                foreach (BlittableJsonReaderObject tc in toolCallsArr)
                {
                    if (tc.TryGet("id", out string tcId) && tcId == toolCallId)
                    {
                        foundMatch = true;
                        break;
                    }
                }
                Assert.True(foundMatch,
                    $"Tool message at index {i} has tool_call_id '{toolCallId}' that doesn't match any tool call in the preceding assistant message.");
            }
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_BackwardPagingAcrossHistoryBoundary()
    {
        // Simulate trimming: history doc has msg1-msg10, current doc has msg8-msg15 (overlap on 8-10).
        // Backward paging with pageSize=12 should stitch them into a continuous timeline.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            // History doc: messages 1-10
            var historyMessages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 1; i <= 10; i++)
                historyMessages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var historyDoc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = historyMessages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(10),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationHistoryCollection
                }
            }, "history-doc");
            database.DocumentsStorage.Put(context, "ConversationHistory/1$chats/cross-doc", null, historyDoc);

            // Current doc: messages 8-15 (overlap on 8-10), linked to history
            var currentMessages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 8; i <= 15; i++)
                currentMessages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var currentDoc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = currentMessages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray { "ConversationHistory/1$chats/cross-doc" },
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(15),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "current-doc");
            database.DocumentsStorage.Put(context, "chats/cross-doc", null, currentDoc);
            tx.Commit();
        }

        // Get latest 12 messages — should span across both docs, deduplicated
        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/cross-doc",
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 12
            });

        Assert.Equal(12, result.Messages.Count);
        Assert.True(result.HasMoreMessages);

        // Verify continuous timeline: msg4 through msg15
        for (int i = 0; i < result.Messages.Count; i++)
        {
            int expectedMsgNum = 4 + i;
            Assert.Equal($"msg{expectedMsgNum}", result.Messages[i].Content);
        }

        // Now page backward from msg4 to get the remaining messages
        var older = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/cross-doc",
                DetailLevel = AiConversationDetailLevel.Detailed,
                Before = result.Messages[0].Timestamp,
                PageSize = 20
            });

        Assert.Equal(3, older.Messages.Count); // msg1, msg2, msg3
        Assert.Equal("msg1", older.Messages[0].Content);
        Assert.Equal("msg2", older.Messages[1].Content);
        Assert.Equal("msg3", older.Messages[2].Content);
        Assert.False(older.HasMoreMessages);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_ForwardPagingFromTrimmedHistory()
    {
        // Simulate: client had timestamp at msg5, then trimming moved msg5-msg7 to history.
        // Current doc only has msg8-msg10. `after=msg5's timestamp` should return msg6-msg10
        // by reading history first, then current doc.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            // History doc: messages 1-8
            var historyMessages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 1; i <= 8; i++)
                historyMessages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var historyDoc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = historyMessages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(8),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationHistoryCollection
                }
            }, "history-doc");
            database.DocumentsStorage.Put(context, "ConversationHistory/1$chats/forward-test", null, historyDoc);

            // Current doc: messages 7-10 (overlap on 7-8), linked to history
            var currentMessages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 7; i <= 10; i++)
                currentMessages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var currentDoc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = currentMessages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray { "ConversationHistory/1$chats/forward-test" },
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(10),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "current-doc");
            database.DocumentsStorage.Put(context, "chats/forward-test", null, currentDoc);
            tx.Commit();
        }

        // Client last saw msg5 (timestamp at minute 5). Ask for everything after.
        // msg6, msg7, msg8 are in history. msg7, msg8, msg9, msg10 are in current doc.
        // Should get msg6-msg10 (5 messages, deduplicated).
        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/forward-test",
                DetailLevel = AiConversationDetailLevel.Detailed,
                After = baseTime.AddMinutes(5),
                PageSize = 20
            });

        Assert.Equal(5, result.Messages.Count);
        Assert.Equal("msg6", result.Messages[0].Content);
        Assert.Equal("msg7", result.Messages[1].Content);
        Assert.Equal("msg8", result.Messages[2].Content);
        Assert.Equal("msg9", result.Messages[3].Content);
        Assert.Equal("msg10", result.Messages[4].Content);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_ForwardPagingHasMoreMessages()
    {
        // 10 messages in a single doc. Forward page with after=msg2, pageSize=3.
        // Should return msg3,msg4,msg5 and HasMoreMessages=true (msg6-msg10 remain).
        // Then page again with after=msg5 — should return msg6,msg7,msg8, HasMoreMessages=true.
        // Then after=msg8 — msg9,msg10, HasMoreMessages=false.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var messages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 1; i <= 10; i++)
                messages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = messages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(10),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");
            database.DocumentsStorage.Put(context, "chats/forward-more", null, doc);
            tx.Commit();
        }

        // Page 1: after msg2, get 3
        var page1 = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/forward-more",
                DetailLevel = AiConversationDetailLevel.Detailed,
                After = baseTime.AddMinutes(2),
                PageSize = 3
            });

        Assert.Equal(3, page1.Messages.Count);
        Assert.Equal("msg3", page1.Messages[0].Content);
        Assert.Equal("msg5", page1.Messages[2].Content);
        Assert.True(page1.HasMoreMessages, "Should have more messages after msg5");

        // Page 2: after msg5, get 3
        var page2 = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/forward-more",
                DetailLevel = AiConversationDetailLevel.Detailed,
                After = page1.Messages[^1].Timestamp,
                PageSize = 3
            });

        Assert.Equal(3, page2.Messages.Count);
        Assert.Equal("msg6", page2.Messages[0].Content);
        Assert.Equal("msg8", page2.Messages[2].Content);
        Assert.True(page2.HasMoreMessages, "Should have more messages after msg8");

        // Page 3: after msg8, get remaining
        var page3 = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/forward-more",
                DetailLevel = AiConversationDetailLevel.Detailed,
                After = page2.Messages[^1].Timestamp,
                PageSize = 3
            });

        Assert.Equal(2, page3.Messages.Count);
        Assert.Equal("msg9", page3.Messages[0].Content);
        Assert.Equal("msg10", page3.Messages[1].Content);
        Assert.False(page3.HasMoreMessages, "No more messages after msg10");
    }

    // Sentinel-pagination regression (RavenDB-24609): the collector internally peeks one extra
    // result (pageSize + 1) to decide HasMoreMessages, then trims it before returning.
    //
    // - Exact-fill   (matching results == pageSize):     no sentinel materializes → HasMore=false,
    //                                                    every matching message is returned.
    // - One-extra    (matching results == pageSize + 1): sentinel materializes → HasMore=true,
    //                                                    but the caller must never see it — the
    //                                                    returned count must equal pageSize.
    //
    // The four [InlineData] rows below cover both axes (direction × exact-fill / one-extra)
    // against the same 6-message conversation:
    //   * backward, pageSize=5 → one-extra (sentinel = msg1, the oldest)
    //   * backward, pageSize=6 → exact-fill (all 6 returned, no sentinel)
    //   * forward (after=msg1), pageSize=4 → one-extra (sentinel = msg6, the newest)
    //   * forward (after=msg1), pageSize=5 → exact-fill (5 remaining, all returned)
    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(/* forward */ false, /* pageSize */ 5, /* expectedCount */ 5, /* expectedHasMore */ true,  /* expectedFirst */ "msg2", /* expectedLast */ "msg6")]
    [InlineData(/* forward */ false, /* pageSize */ 6, /* expectedCount */ 6, /* expectedHasMore */ false, /* expectedFirst */ "msg1", /* expectedLast */ "msg6")]
    [InlineData(/* forward */ true,  /* pageSize */ 4, /* expectedCount */ 4, /* expectedHasMore */ true,  /* expectedFirst */ "msg2", /* expectedLast */ "msg5")]
    [InlineData(/* forward */ true,  /* pageSize */ 5, /* expectedCount */ 5, /* expectedHasMore */ false, /* expectedFirst */ "msg2", /* expectedLast */ "msg6")]
    public async Task CanGetConversationMessages_SentinelPagination_RespectsPageBoundaries(
        bool forward, int pageSize, int expectedCount, bool expectedHasMore, string expectedFirst, string expectedLast)
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        const int total = 6;

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var messages = new Sparrow.Json.Parsing.DynamicJsonArray();
            for (int i = 1; i <= total; i++)
                messages.Add(MakeMsg(context, i % 2 == 1 ? "user" : "assistant", $"msg{i}", baseTime.AddMinutes(i)));

            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = messages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(total),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");
            database.DocumentsStorage.Put(context, "chats/sentinel", null, doc);
            tx.Commit();
        }

        var options = new GetConversationMessagesOptions
        {
            ConversationId = "chats/sentinel",
            DetailLevel = AiConversationDetailLevel.Detailed,
            PageSize = pageSize
        };
        if (forward)
            options.After = baseTime.AddMinutes(1); // msg1's timestamp — paging starts at msg2

        var result = await store.AI.GetConversationMessagesAsync(options);

        // Count must match expectations — and crucially, must never exceed pageSize.
        // The internal sentinel (pageSize + 1) must NEVER be exposed to the caller.
        Assert.Equal(expectedCount, result.Messages.Count);
        Assert.True(result.Messages.Count <= pageSize,
            $"Returned count {result.Messages.Count} exceeded PageSize {pageSize} — the sentinel result must never reach the caller.");

        Assert.Equal(expectedHasMore, result.HasMoreMessages);

        // Chronological order (oldest first per the DTO contract) must hold in BOTH directions —
        // backward paging collects newest→oldest and reverses on the way out, so a regression
        // in the reverse step would surface here.
        Assert.Equal(expectedFirst, result.Messages[0].Content);
        Assert.Equal(expectedLast, result.Messages[^1].Content);
        for (int i = 1; i < result.Messages.Count; i++)
            Assert.True(result.Messages[i].Timestamp > result.Messages[i - 1].Timestamp,
                $"Message {i} timestamp ({result.Messages[i].Timestamp:O}) must come after message {i - 1} ({result.Messages[i - 1].Timestamp:O}).");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetConversationMessages_SubConversationIds()
    {
        // Create a conversation with sub-agent tool calls and internal messages referencing sub-conversations.
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var messages = new Sparrow.Json.Parsing.DynamicJsonArray
            {
                MakeMsg(context, "system", "You are helpful", baseTime.AddMinutes(1)),
                MakeMsg(context, "user", "Call the sub-agent", baseTime.AddMinutes(2)),
                // Assistant calls a sub-agent tool
                MakeAssistantWithToolCall(context, "call_sub1", "user-info-agent", "{}", baseTime.AddMinutes(3)),
                // Tool response from the sub-agent, with subConversationId
                context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["role"] = "tool",
                    ["tool_call_id"] = "call_sub1",
                    ["content"] = "Sub-agent result",
                    ["subConversationId"] = "SubChats/1-A",
                    ["date"] = baseTime.AddMinutes(4)
                }, "msg"),
                // Internal message marking the sub-agent action
                context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    ["role"] = "internal",
                    ["type"] = "sub-agent-action-call",
                    ["content"] = "[sub-agent called action-tool 'GetUserName']",
                    ["toolName"] = "GetUserName",
                    ["subConversationId"] = "SubChats/1-A",
                    ["date"] = baseTime.AddMinutes(5)
                }, "msg"),
                MakeMsg(context, "assistant", "The user name is John", baseTime.AddMinutes(6)),
            };

            var doc = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
            {
                ["Agent"] = AgentName,
                ["Parameters"] = null,
                ["Messages"] = messages,
                ["LinkedConversations"] = new Sparrow.Json.Parsing.DynamicJsonArray(),
                ["TotalUsage"] = new Sparrow.Json.Parsing.DynamicJsonValue
                    { ["PromptTokens"] = 0, ["CompletionTokens"] = 0, ["TotalTokens"] = 0, ["CachedTokens"] = 0, ["ReasoningTokens"] = 0 },
                ["OpenActionCalls"] = new Sparrow.Json.Parsing.DynamicJsonValue(),
                ["LastMessageAt"] = baseTime.AddMinutes(6),
                ["CreatedAt"] = baseTime,
                ["Expires"] = null,
                ["RemainingToolIterations"] = 16,
                ["SubConversationIds"] = new Sparrow.Json.Parsing.DynamicJsonArray { "SubChats/1-A" },
                [Raven.Client.Constants.Documents.Metadata.Key] = new Sparrow.Json.Parsing.DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Raven.Client.Constants.Documents.Collections.AiAgentConversationCollection
                }
            }, "test-doc");
            database.DocumentsStorage.Put(context, "chats/sub-agent-test", null, doc);
            tx.Commit();
        }

        // Detailed view: should see the tool call with SubConversationId
        var result = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/sub-agent-test",
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });

        // SubConversationIds should be in the result metadata
        Assert.NotNull(result.SubConversationIds);
        Assert.Contains("SubChats/1-A", result.SubConversationIds);

        // The assistant message should have a tool call with SubConversationId
        var assistantWithTools = result.Messages.Find(m => m.Role == AiMessageRole.Assistant && m.ToolCalls is { Count: > 0 });
        Assert.NotNull(assistantWithTools);
        Assert.Equal("SubChats/1-A", assistantWithTools.ToolCalls[0].SubConversationId);

        // Full view: should also see the internal message with SubConversationId
        var fullResult = await store.AI.GetConversationMessagesAsync(
            new GetConversationMessagesOptions
            {
                ConversationId = "chats/sub-agent-test",
                DetailLevel = AiConversationDetailLevel.Full,
                PageSize = 50
            });

        var internalMsg = fullResult.Messages.Find(m => m.Role == AiMessageRole.Internal);
        Assert.NotNull(internalMsg);
        Assert.Equal("SubChats/1-A", internalMsg.SubConversationId);
        Assert.Contains("sub-agent called action-tool", internalMsg.Content);
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
