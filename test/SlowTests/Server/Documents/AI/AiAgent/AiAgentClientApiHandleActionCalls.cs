using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentClientApiHandleActionCalls : RavenTestBase
{
    public AiAgentClientApiHandleActionCalls(ITestOutputHelper output) : base(output)
    {
    }

    internal const string ProductSearch = nameof(ProductSearch);
    internal const string RecentOrder = nameof(RecentOrder);
    private class Sample
    {
        public string Answer;
    }

    private class ProductSearchArgs
    {
        public string[] Query { get; set; }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanHandleToolCall(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildAgent(config.ConnectionStringName);
        var r  = await store.AI.CreateAgentAsync(agent, new Sample
        {
            Answer = "the answer"
        });
        var chat = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        var recentOrderCalled = false;
        chat.Handle(RecentOrder, (object query) =>
        {
            recentOrderCalled = true;
            return "done";
        });

        chat.SetUserPrompt("fetch my recent orders");
        var run = await chat.RunAsync<Sample>();
        Assert.Equal(run.Status, AiConversationResult.Done);
        Assert.NotNull(run.Answer.Answer);
        Assert.True(recentOrderCalled);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, 
        Data = [AiHandleErrorStrategy.SendErrorsToModel])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, 
        Data = [AiHandleErrorStrategy.RaiseImmediately])]
    public async Task CanHandleToolCallWithException(Options options, GenAiConfiguration config, AiHandleErrorStrategy strategy)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildAgent(config.ConnectionStringName);
        var r  = await store.AI.CreateAgentAsync(agent, new Sample
        {
            Answer = "the answer"
        });
        var chat = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        chat.Handle(RecentOrder, (object _) => throw new InvalidOperationException("Error in tool call"), strategy);

        chat.SetUserPrompt("fetch my recent orders");

        var runTask = chat.RunAsync<Sample>();

        switch (strategy)
        {
            case AiHandleErrorStrategy.SendErrorsToModel:
                var run = await runTask;
                Assert.Equal(run.Status, AiConversationResult.Done);
                Assert.NotNull(run.Answer.Answer);
                break;
            case AiHandleErrorStrategy.RaiseImmediately:
                await Assert.ThrowsAsync<InvalidOperationException>(() => runTask);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanHandleToolCallWithArgs(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            @"You are an AI agent of an online shop.

You MUST follow these rules strictly:

1. When the user explicitly instructs to use the tool with a specific query (e.g. 'look for X'), you MUST call the tool with EXACTLY that query.
2. DO NOT expand, rephrase, or generate alternatives to the query.
3. DO NOT add synonyms, related terms, or multiple queries.
4. The 'query' parameter MUST contain ONLY the exact text provided by the user.
5. Only use the tool when explicitly requested or when needed to search products.

If the user says: look for 'salt'
You MUST call the tool with:
{ \""query\"": [\""salt\""] }

Deviation from these rules is not allowed.");

        agent.Actions =
        [
            new AiAgentToolAction(ProductSearch,"semantic search the store product catalog")
            {
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            }
        ];

        var r  = await store.AI.CreateAgentAsync(agent, new
        {
            Answer = "the answer"
        });

        var chat = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        string[] query = null;
        chat.Handle(ProductSearch, (ProductSearchArgs args) =>
        {
            query = args.Query;
            return "not found";
        });

        chat.SetUserPrompt("look for 'sugar' with the tool I have provided to you");
        var run = await chat.RunAsync<Sample>();
        Assert.Equal(run.Status, AiConversationResult.Done);
        Assert.NotNull(run.Answer.Answer);
        Assert.Contains("sugar", query);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CantCreateNewConversationWithSameId(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = BuildAgent(config.ConnectionStringName);

        var r  = await store.AI.CreateAgentAsync(agent, new
        {
            Answer = "the answer"
        });

        var chat = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"),
            string.Empty);

        chat.Handle(RecentOrder, (object query) =>
        {
            return "done";
        });
        chat.Handle(ProductSearch, (object query) =>
        {
            return "done";
        });

        chat.SetUserPrompt("hi");
        var run = await chat.RunAsync<Sample>();
        Assert.Equal(run.Status, AiConversationResult.Done);

        var chat2 = store.AI.Conversation(
            r.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"),
            string.Empty);
        chat2.SetUserPrompt("hi");
        await Assert.ThrowsAsync<ConcurrencyException>(() => chat2.RunAsync<Sample>());
    }

    internal static AiAgentConfiguration BuildAgent(string connection)
    {
        var agent = new AiAgentConfiguration("shopping assistant", connection,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
        
        agent.Parameters.Add(new AiAgentParameter("company"));
        agent.Actions =
        [
            new AiAgentToolAction(ProductSearch,"semantic search the store product catalog")
            {
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            }
            ,
            new AiAgentToolAction(RecentOrder, "Get the recent orders of the current user")
            {
                ParametersSampleObject = "{}"
            }
        ];
        return agent;
    }
}
