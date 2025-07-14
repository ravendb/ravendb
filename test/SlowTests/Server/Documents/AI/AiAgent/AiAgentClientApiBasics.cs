using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.AI.AiAgent;

public class AiAgentClientApiBasics : RavenTestBase
{
    public AiAgentClientApiBasics(ITestOutputHelper output) : base(output)
    {
    }

    public class OutputSchema
    {
        public string Answer = "Answer to the user question";

        public bool Relevant = true;

        public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

        public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false })]
    public async Task AiAgentClientApiBasicTest(Options options, GenAiConfiguration config, bool sendSchema)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };
        
        agent.Parameters.Add("company");
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

        var identifier = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

        var chat = store.AI.StartConversation<OutputSchema>(identifier,
            p => p.AddParameter("company", "companies/90-A"));

        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
        
        Assert.NotNull(chat.Answer);
        Assert.NotNull(chat.Id);

        chat.SetUserPrompt("what goes well with my cheese?");
        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);

        Assert.NotNull(chat.Answer);

        chat.SetUserPrompt("what cheese goes well with italian food?");
        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false })]
    public async Task AiAgentClientApiAnswerActionTool(Options options, GenAiConfiguration config, bool sendSchema)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };

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
        var tool2sampleObj = "{}";
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
        var agentResult = await store.AI.CreateAgentAsync<OutputSchema>(agent);

        var chat = store.AI.StartConversation<OutputSchema>(agentResult.Identifier, builder: null);

        chat.SetUserPrompt("what goes well with my cheese for recent orders?");

        var r = await chat.RunAsync(CancellationToken.None);

        Assert.True(r);
        Assert.NotNull(chat.Id);

        foreach (var request in chat.RequiredActions())
        {
            chat.AddActionResponse(request.ToolId, "{}");
        }
       
        r = await chat.RunAsync(CancellationToken.None);

        if (r)
        {
            // agent could ask for action tools again
            Assert.True(chat.RequiredActions().Any());
            Assert.NotNull(chat.Id);
        }
        else
        {
            Assert.True(chat.Answer != null);
            Assert.NotNull(chat.Id);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task AiAgentClientApi(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant",config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };
        
        agent.Parameters.Add("company");
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

        var identifier = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;


        var chat = store.AI.StartConversation<OutputSchema>(identifier,
            p => p.AddParameter("company", "companies/90-A"));

        Assert.Throws<InvalidOperationException>(() => chat.Id);
        Assert.Throws<InvalidOperationException>(() => chat.Answer);
        Assert.Throws<InvalidOperationException>(chat.RequiredActions);

        // Allowed, as we can add tool responses to an existing chat running it
        // Assert.Throws<InvalidOperationException>(() => chat.AddToolResponse("foo", "bar"));

        await Assert.ThrowsAsync<ArgumentNullException>(() => chat.RunAsync(CancellationToken.None));

        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
        
        Assert.NotNull(chat.Answer);
        Assert.NotNull(chat.Id);

        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
        Assert.Equal(0, chat.RequiredActions().ToList().Count);

        chat.AddActionResponse("foo","bar");
        var e = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync(CancellationToken.None));
        Assert.Contains("foo is an unknown action ID", e.Message);

        chat.SetUserPrompt("what goes well with my cheese?");
        chat.AddActionResponse("foo","bar");
        e = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync(CancellationToken.None));
        Assert.Contains($"Cannot have a conversation '{chat.Id}' with open action calls and user prompt", e.Message);

        chat.SetUserPrompt("what cheese goes well with italian food?");
        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task ThrowConcurrencyException(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        using var session = store.OpenAsyncSession();

        var agent = new AiAgentConfiguration("shopping assistant",config.ConnectionStringName,
            "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };
        
        agent.Parameters.Add("company");
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

        var identifier = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

        var chat = store.AI.StartConversation<OutputSchema>(identifier,
            p => p.AddParameter("company", "companies/90-A"));
        chat.SetUserPrompt("what goes well with my cheese?");
        var r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);

        chat = store.AI.ResumeConversation<OutputSchema>(chat.Id, "foo");
        chat.SetUserPrompt("Can you give me some alternatives?");
        await Assert.ThrowsAsync<ConcurrencyException>(() => chat.RunAsync(CancellationToken.None));
        Assert.NotNull(chat.ChangeVector);

        chat.SetUserPrompt("Can you give me some alternatives?");
        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
        Assert.NotNull(chat.ChangeVector);

        chat.SetUserPrompt("even better choice?");
        r = await chat.RunAsync(CancellationToken.None);
        Assert.False(r);
        Assert.NotNull(chat.ChangeVector);
    }
}
