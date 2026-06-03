using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Sparrow.Server;
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
}
