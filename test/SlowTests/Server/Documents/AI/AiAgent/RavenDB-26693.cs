using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_26693 : RavenTestBase
{
    public RavenDB_26693(ITestOutputHelper output) : base(output)
    {
    }

    public class OutputSchema
    {
        public static OutputSchema Instance = new()
        {
            Answer = "the answer to the user's question"
        };

        public string Answer { get; set; }
    }

    private class OrderLine
    {
        public string Product { get; set; }
        public int Quantity { get; set; }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRunConversationWithStreaming(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName, "Be helpful");

        var identifier = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions());

        chat.SetUserPrompt("Tell me a detailed story about a cat that jumped over the table, must be at least 15 paragraphs, for a 6 years old. End the message with MESSAGE_END");

        // aggregate the answer from the streamed chunks via the 'onChunk' callback
        using var cts = new CancellationTokenSource();

        var chunksCount = 0;
        var streamedAnswer = new StringBuilder();
        string stoppedAt = null;
        AiAnswer<OutputSchema> result = null;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            result = await chat.StreamAsync<OutputSchema>(a => a.Answer, chunk =>
            {
                chunksCount++;
                streamedAnswer.Append(chunk);
                if (chunksCount == 3)
                {
                    stoppedAt = streamedAnswer.ToString();
                    cts.Cancel();
                }

                return Task.CompletedTask;
            }, cts.Token);
        });
        Assert.Null(result);

        var stats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
        Assert.True(stats.Collections.TryGetValue("@conversations", out long count) == false || count == 0);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRunThenCancelStreamWithActionTool(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
            "You are a helpful shopping assistant. " +
            "When the user asks about their recent orders, you MUST call the 'RecentOrder' tool to fetch them and then summarize the result. " +
            "For any other question, answer directly without calling any tool.");

        agent.Actions =
        [
            new AiAgentToolAction("RecentOrder", "Get the recent orders of the current user")
            {
                ParametersSampleObject = "{}"
            }
        ];

        var identifier = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/", new AiConversationCreationOptions());

        // 1) casual turn via RunAsync - no action tool involved
        chat.SetUserPrompt("Who are you?");
        var intro = await chat.RunAsync<OutputSchema>();

        Assert.Equal(AiConversationResult.Done, intro.Status);
        Assert.NotNull(intro.Answer);
        Assert.False(string.IsNullOrWhiteSpace(intro.Answer.Answer));

        // 2) streaming turn that triggers the 'RecentOrder' action tool, then cancel while
        //    the action tool call is still "open" (i.e. while the handler is executing).
        var recentOrderCalled = false;
        using var cts = new CancellationTokenSource();
        chat.Handle("RecentOrder", async (object _) =>
        {
            recentOrderCalled = true;

            // cancel while the action tool call is still open, then simulate a long-running
            // operation that observes the token and aborts instead of running to completion.
            await cts.CancelAsync();
            await Task.Delay(TimeSpan.FromSeconds(5));

            return new List<OrderLine>
            {
                new() { Product = "Cheese", Quantity = 2 },
                new() { Product = "Bread", Quantity = 1 }
            };
        }, AiHandleErrorStrategy.RaiseImmediately);

        var streamedAnswer = new StringBuilder();
        var sw = Stopwatch.StartNew();
        chat.SetUserPrompt("Fetch my recent orders and tell me what I bought.");

        // cancellation raised from inside the open action tool call must propagate out of StreamAsync
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await chat.StreamAsync<OutputSchema>(a => a.Answer, chunk =>
            {
                streamedAnswer.Append(chunk);
                return Task.CompletedTask;
            }, cts.Token);
        }); 
        Assert.True(recentOrderCalled, "the 'RecentOrder' action tool should have been invoked before cancellation");

        var conversationMessages = (await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions()
        {
            ConversationId = chat.Id,
            DetailLevel = AiConversationDetailLevel.Simple
        })).Messages;
        Assert.Equal(AiMessageRole.User, conversationMessages[^1].Role);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanContinueConversationAfterCancelWithPendingActionTool(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("my assistant", config.ConnectionStringName,
            "You are a helpful shopping assistant. " +
            "When the user asks about their recent orders, you MUST call the 'RecentOrder' tool to fetch them and then summarize the result. " +
            "For any other question, answer directly without calling any tool.");

        agent.Actions =
        [
            new AiAgentToolAction("RecentOrder", "Get the recent orders of the current user")
            {
                ParametersSampleObject = "{}"
            }
        ];

        var identifier = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

        // 1) start a streaming turn that triggers the 'RecentOrder' action tool, then cancel while the tool
        //    call is still open - this leaves a pending (unanswered) action call on the persisted conversation.
        var chat = store.AI.Conversation(identifier, "chats/", new AiConversationCreationOptions());

        var recentOrderCalled = false;
        using var cts = new CancellationTokenSource();
        chat.Handle("RecentOrder", (object _) =>
        {
            recentOrderCalled = true;
            cts.Cancel(); // cancel before the action's answer is sent back to the server
            return new List<OrderLine>
            {
                new() { Product = "Cheese", Quantity = 2 },
                new() { Product = "Bread", Quantity = 1 }
            };
        });

        chat.SetUserPrompt("Fetch my recent orders and tell me what I bought.");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await chat.StreamAsync<OutputSchema>(a => a.Answer, _ => Task.CompletedTask, cts.Token);
        });

        Assert.True(recentOrderCalled, "the 'RecentOrder' action tool should have been invoked before cancellation");

        var conversationId = chat.Id;

        // the cancelled turn left the conversation with the user's prompt as its last message (the assistant
        // turn never completed) and an open 'RecentOrder' action call awaiting an answer.
        var afterCancel = (await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = conversationId,
            DetailLevel = AiConversationDetailLevel.Simple
        })).Messages;
        Assert.Equal(AiMessageRole.User, afterCancel[^1].Role);

        // 2) continue the SAME conversation with cancelPendingActionTools: true and NO handler registered.
        //    The server must auto-answer the still-open 'RecentOrder' call ("This was cancelled by the user")
        //    instead of requiring it, so the conversation proceeds with the new prompt and completes.
        //    (Without the feature the open call would surface as ActionRequired and - with no handler - throw.)
        var continued = store.AI.Conversation(identifier, conversationId, new AiConversationCreationOptions(),
            debug: null, cancelPendingActionTools: true);

        continued.SetUserPrompt("Never mind the orders. In one sentence, who are you?");

        var result = await continued.RunAsync<OutputSchema>();

        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.NotNull(result.Answer);
        Assert.False(string.IsNullOrWhiteSpace(result.Answer.Answer));
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRunThenCancelStreamWithSubAgentActionTool(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        // a sub-agent that exposes an action tool ('RecentOrder')
        var orderAgent = new AiAgentConfiguration("order-agent", config.ConnectionStringName,
            "You fetch the user's recent orders. " +
            "When asked for the recent orders, you MUST call the 'RecentOrder' tool to fetch them and then summarize the result.")
        {
            Actions =
            [
                new AiAgentToolAction("RecentOrder", "Get the recent orders of the current user")
                {
                    ParametersSampleObject = "{}"
                }
            ]
        };
        var orderAgentId = (await store.AI.CreateAgentAsync(orderAgent, OutputSchema.Instance)).Identifier;

        // a root agent that delegates order questions to the 'order-agent' sub-agent
        var rootAgent = new AiAgentConfiguration("assistant-agent", config.ConnectionStringName,
            "You are a helpful assistant. " +
            "When the user asks about their recent orders, you MUST use the 'order-agent' sub-agent to fetch them. " +
            "For any other question, answer directly without using any sub-agent.")
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = orderAgentId,
                    Description = "Use to fetch the user's recent orders."
                }
            ]
        };
        var identifier = (await store.AI.CreateAgentAsync(rootAgent, OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/", new AiConversationCreationOptions());
        chat.SetUserPrompt("Who are you?");
        var result = await chat.RunAsync<OutputSchema>();
        Assert.Equal(AiConversationResult.Done, result.Status);
        Assert.NotNull(result.Answer);
        Assert.False(string.IsNullOrWhiteSpace(result.Answer.Answer));

        int recentOrderCalled = 0;
        using var cts = new CancellationTokenSource();
        chat.Handle($"{orderAgentId}/RecentOrder", (object _) =>
        {
            recentOrderCalled++;
            cts.Cancel(); // cancel while the sub-agent's action tool call is open on the root conversation
            return new List<OrderLine>
            {
                new() { Product = "Cheese", Quantity = 2 },
                new() { Product = "Bread", Quantity = 1 }
            };
        });
        chat.SetUserPrompt("Use the order agent to fetch my recent orders and tell me what I bought.");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await chat.StreamAsync<OutputSchema>(a => a.Answer, _ => Task.CompletedTask, cts.Token);
        });
        Assert.True(recentOrderCalled == 1, "the sub-agent's 'RecentOrder' action tool should have been invoked before cancellation");

        // after cancellation, the sub-agent invocation is still an OPEN (unanswered) user tool call on the root
        var afterCancel = (await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = chat.Id,
            DetailLevel = AiConversationDetailLevel.Detailed
        })).Messages;
        var openSubAgentCall = afterCancel.SelectMany(m => m.ToolCalls ?? []).Single(tc => tc.Name == orderAgentId);
        Assert.Null(openSubAgentCall.Result); // open: no response yet (Result/SubConversationId are filled only once answered)

        var chat2 = store.AI.Conversation(identifier, chat.Id, new AiConversationCreationOptions(),
            debug: null, cancelPendingActionTools: true);

        chat2.SetUserPrompt("Never mind the orders. In one sentence, who are you?");

        var result2 = await chat2.RunAsync<OutputSchema>();
        
        Assert.Equal(AiConversationResult.Done, result2.Status);
        Assert.NotNull(result2.Answer);
        Assert.False(string.IsNullOrWhiteSpace(result2.Answer.Answer));

        // after continuing with cancelPendingActionTools, the sub-agent invocation on the root is now a CLOSED
        // user tool call, and the sub-agent's own 'RecentOrder' action was answered with the cancellation message.
        var afterContinue = (await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = chat.Id,
            DetailLevel = AiConversationDetailLevel.Detailed
        })).Messages;
        var closedSubAgentCall = afterContinue.SelectMany(m => m.ToolCalls ?? []).Single(tc => tc.Name == orderAgentId);
        Assert.NotNull(closedSubAgentCall.Result); // closed: the sub-agent call was answered

        var subMessages = (await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = closedSubAgentCall.SubConversationId,
            DetailLevel = AiConversationDetailLevel.Detailed
        })).Messages;
        var closedRecentOrder = subMessages.SelectMany(m => m.ToolCalls ?? []).Single(tc => tc.Name == "RecentOrder");
        Assert.Equal("This action was canceled by the user", closedRecentOrder.Result);
        Assert.True(recentOrderCalled == 1, "the sub-agent's 'RecentOrder' action tool should have been invoked only once");
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task MultiGetReflectsCancellationTokenAsSubRequestRequestAborted()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);

        var multiGetHandler = new MultiGetHandler();
        multiGetHandler.Init(new RequestHandlerContext
        {
            Database = database,
            RavenServer = Server,
            HttpContext = new DefaultHttpContext()
        });

        // capture the RequestAborted the executed sub-request sees
        CancellationToken capturedRequestAborted = default;
        using var processor = new CapturingMultiGetProcessor(multiGetHandler, requestAborted => capturedRequestAborted = requestAborted);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var responseStream = new MemoryStream())
        {
            using var input = context.ReadObject(new DynamicJsonValue
            {
                ["Requests"] = new DynamicJsonArray(new object[]
                {
                    new DynamicJsonValue
                    {
                        ["Url"] = $"/databases/{store.Database}/stats",
                        ["Query"] = "",
                        ["Method"] = "GET"
                    }
                })
            }, "multi-get/test");

            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            // the token passed to ExecuteMultiGetAsync must surface as the executed sub-request's HttpContext.RequestAborted
            await processor.ExecuteMultiGetAsync(context, input, responseStream, cts.Token);

            Assert.Equal(cts.Token, capturedRequestAborted);
            Assert.True(capturedRequestAborted.IsCancellationRequested);
        }
    }

    // Intercepts each sub-request and records the RequestAborted it runs with, bypassing the real endpoint handler.
    private sealed class CapturingMultiGetProcessor(Raven.Server.Documents.DatabaseRequestHandler requestHandler, Action<CancellationToken> onRequest)
        : AbstractMultiGetHandlerProcessorForPost<Raven.Server.Documents.DatabaseRequestHandler, DocumentsOperationContext>(requestHandler)
    {
        protected override HandleRequest GetRequestHandler(RouteInformation routeInformation)
        {
            return requestHandlerContext =>
            {
                onRequest(requestHandlerContext.HttpContext.RequestAborted);
                return Task.CompletedTask;
            };
        }

        protected override void FillRequestHandlerContext(RequestHandlerContext context)
        {
            context.Database = RequestHandler.Database;
        }
    }
}
