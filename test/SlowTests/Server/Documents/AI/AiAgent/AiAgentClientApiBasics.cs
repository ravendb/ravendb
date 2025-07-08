using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
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

        var r = await store.AI.StartChatAsync<OutputSchema>(identifier, "what goes well with my cheese?",
            p => p.AddParameter("company", "companies/90-A"));

        Assert.NotNull(r.Response.Answer);
        Assert.NotNull(r.Usage);
        Assert.NotNull(r.ChatId);

        var chat = await session.LoadAsync<dynamic>(r.ChatId);
        Assert.NotNull(chat);

        var r1 = await store.AI.ContinueChatAsync<OutputSchema>(r.ChatId, "what goes well with my cheese?");

        Assert.False(string.IsNullOrEmpty(r1.Response.Answer));

        var r2 = await store.AI.ContinueChatAsync<OutputSchema>(r.ChatId, "what cheese goes well with italian food?");

        Assert.False(string.IsNullOrEmpty(r2.Response.Answer));
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

        var identifier = (await store.AI.CreateAgentAsync<OutputSchema>(agent)).Identifier;

        var r = await store.AI.StartChatAsync<OutputSchema>(identifier, "what goes well with my cheese for recent orders?");

        Assert.True(r.ToolRequests.Count > 0);
        Assert.NotNull(r.Usage);
        Assert.NotNull(r.ChatId);

        var toolResponses = new List<ToolResponse>();
        for (int i = 0; i < r.ToolRequests.Count; i++)
        {
            var request = r.ToolRequests[i];
            toolResponses.Add(new ToolResponse
            {
                ToolId = request.ToolId,
                Content = "{}"
            });
        }

        var r2 = await store.AI.ContinueChatAsync<OutputSchema>(r.ChatId, toolResponses);

        Assert.True(r2.Response?.Answer != null || r2.ToolRequests != null);
        Assert.NotNull(r2.Usage);
        Assert.NotNull(r2.ChatId);
    }
}
