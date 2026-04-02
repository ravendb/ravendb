using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;


namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentClientApiBasics : RavenTestBase
{
    public AiAgentClientApiBasics(ITestOutputHelper output) : base(output)
    {
    }

    public class AnswerSchema
    {
        public static AnswerSchema Instance = new();

        public string Answer = "Answer to the user question";

        public bool Relevant = true;

        public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

        public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
    }

    public class AddToCartArgs
    {
        public string ProductId;
        public int Quantity;
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false })]
    public async Task AiAgentClientApiBasicTest(Options options, GenAiConfiguration config, bool sendSchema)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
        
        agent.Parameters.Add(new AiAgentParameter("company"));
        agent.Queries =
        [
            new AiAgentToolQuery
            {
                Name = "ProductSearch", 
                Description =  "semantic search the store product catalog",
                Query = "from Products where vector.search(embedding.text(Name), $query)",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            }
            ,
            new AiAgentToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSampleObject = "{}"
            }
        ];

        if (sendSchema)
        {
            agent.Queries[0].ParametersSchema = ChatCompletionClient.GetSchemaForTool(null, agent.Queries[0].ParametersSampleObject);
            agent.Queries[0].ParametersSampleObject = null;

            agent.Queries[1].ParametersSchema = ChatCompletionClient.GetSchemaForTool(null, agent.Queries[1].ParametersSampleObject);
            agent.Queries[1].ParametersSampleObject = null;
        }

        var identifier = (await store.AI.CreateAgentAsync(agent, AnswerSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions()
                .AddParameter("company", "companies/90-A"));

        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        
        Assert.NotNull(r.Answer);
        Assert.NotNull(chat.Id);

        chat.SetUserPrompt("what goes well with my cheese?");
        r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        Assert.NotNull(r.Answer);

        chat.SetUserPrompt("what cheese goes well with italian food?");
        r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);
    }

    public class ProductSearchArgs
    {
        public string[] Query;
    }

    public class RecentOrderArgs
    {
        public string User;
    }

    public class ConversationState
    {
        public bool Refresh;
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false })]
    public async Task AiAgentClientApiAnswerActionTool(Options options, GenAiConfiguration config, bool sendSchema)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        var tool1 = new AiAgentToolAction
        {
            Name = "ProductSearch",
            Description = "semantic search the store product catalog",
        };
        var tool1sampleObj = "{\"query\": [\"term or phrase to search in the catalog\"]}";
        var tool2 = new AiAgentToolAction
        {
            Name = "RecentOrder", 
            Description = "Get the recent orders of the current user"
        };
        var tool2sampleObj = "{\"user\":\"the user id for which to get the order, default is users/1\"}";
        if (sendSchema)
        {
            tool1.ParametersSchema = ChatCompletionClient.GetSchemaForTool(null, tool1sampleObj);
            tool2.ParametersSchema = ChatCompletionClient.GetSchemaForTool(null, tool2sampleObj);
        }
        else
        {
            tool1.ParametersSampleObject = tool1sampleObj;
            tool2.ParametersSampleObject = tool2sampleObj;
        }

        agent.Actions = [ tool1, tool2 ];
        var agentResult = await store.AI.CreateAgentAsync(agent, AnswerSchema.Instance);

        var chat = store.AI.Conversation(
            agentResult.Identifier,
            "chats/123",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        chat.OnUnhandledAction += args => Task.CompletedTask;

        chat.SetUserPrompt("what goes well with my cheese for recent orders?");

        var r = await chat.RunAsync<AnswerSchema>();
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task AiAgentClientApi(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant",config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Parameters.Add(new AiAgentParameter("company"));
        agent.Queries =
        [
            new AiAgentToolQuery
            {
                Name = "ProductSearch", 
                Description =  "semantic search the store product catalog",
                Query = "from Products where vector.search(embedding.text(Name), $query)",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            }
            ,
            new AiAgentToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSampleObject = "{}"
            }
        ];

        var identifier = (await store.AI.CreateAgentAsync(agent, AnswerSchema.Instance)).Identifier;


        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        Assert.Throws<InvalidOperationException>(chat.RequiredActions);

        // Allowed, as we can add tool responses to an existing chat running it
        // Assert.Throws<InvalidOperationException>(() => chat.AddToolResponse("foo", "bar"));

        var e = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<AnswerSchema>());
        Assert.Contains("Cannot start a new conversation", e.Message);

        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        
        Assert.NotNull(r.Answer);
        Assert.NotNull(chat.Id);

        r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.Equal(0, chat.RequiredActions().ToList().Count);

        chat.AddActionResponse("foo","bar");
        e = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<AnswerSchema>(CancellationToken.None));
        Assert.Contains("foo is an unknown action ID", e.Message);

        chat.SetUserPrompt("what goes well with my cheese?");
        chat.AddActionResponse("foo","bar");
        e = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<AnswerSchema>(CancellationToken.None));
        Assert.Contains($"Cannot have a conversation '{chat.Id}' with open action calls and user prompt", e.Message);

        chat.SetUserPrompt("what cheese goes well with italian food?");
        r = await chat.RunAsync<AnswerSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ThrowConcurrencyException(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant",config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Parameters.Add(new AiAgentParameter("company"));

        agent.Queries =
        [
            new AiAgentToolQuery
            {
                Name = "ProductSearch", 
                Description =  "semantic search the store product catalog",
                Query = "from Products where vector.search(embedding.text(Name), $query)",
                ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
            }
            ,
            new AiAgentToolQuery
            {
                Name = "RecentOrder",
                Description = "Get the recent orders of the current user",
                Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                ParametersSampleObject = "{}"
            }
        ];

        var identifier = (await store.AI.CreateAgentAsync(agent, AnswerSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));
        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync<AnswerSchema>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        chat = store.AI.Conversation(identifier, chat.Id, creationOptions: null, "foo");
        chat.SetUserPrompt("Can you give me some alternatives?");
        await Assert.ThrowsAsync<ConcurrencyException>(() => chat.RunAsync<AnswerSchema>(CancellationToken.None));
        Assert.NotNull(chat.ChangeVector);

        chat.SetUserPrompt("Can you give me some alternatives?");
        r = await chat.RunAsync<AnswerSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(chat.ChangeVector);

        chat.SetUserPrompt("even better choice?");
        r = await chat.RunAsync<AnswerSchema>(CancellationToken.None);
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(chat.ChangeVector);
    }
}
