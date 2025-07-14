using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24407 : RavenTestBase
{
    public RavenDB_24407(ITestOutputHelper output) : base(output)
    {
    }
    private class OutputSampleObject
    {
        public string Answer = "Answer to the user question";

        public bool Relevant = true;

        public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

        public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
    }

    private class Chat
    {
        public List<Message> Messages { get; set; }
        public List<string> HistoryDocuments { get; set; }
    }

    private class Message
    {
        [JsonProperty("role")]
        public string Role { get; set; }

        [JsonProperty("content")]
        public string Content { get; set; }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true, false })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false, false })]
    public async Task CanResumeConversationWithSummarization(Options options, GenAiConfiguration config, bool summarization, bool withHistory)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new CreateSampleDataOperation());

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        const string systemPrompt = "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.";
        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName, systemPrompt);
        agent.Identifier = "shopping-assistant";
        agent.Parameters.Add("company");
        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };
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

        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSampleObject>(agent));
        // start chat
        var r = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>("shopping-assistant", "what goes well with my cheese for recent orders?",
            new Dictionary<string, object> { ["company"] = "companies/90-A" }));

        Assert.NotNull(r.Response.Answer);
        Assert.NotNull(r.Usage);
        Assert.NotNull(r.ConversationId);

        // resume
        var r2 = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>(r.ConversationId,
            userPrompt: "can you give me a cheaper alternative?"));

        Assert.NotNull(r2.Response.Answer);
        Assert.NotNull(r2.Usage);
        Assert.NotNull(r2.ConversationId);

        var chat = await GetChat(store, r.ConversationId);
        Assert.True(chat.Messages.Count > 2, "messages count: " + chat.Messages.Count);
        Assert.Equal(systemPrompt, chat.Messages[0].Content);
        Assert.Equal(0, chat.HistoryDocuments.Count);

        // resume - with summarization
        if (summarization)
        {
            agent.ChatReduction = new AiAgentChatReductionConfiguration()
            {
                Tokens = new AiAgentSummarizationByTokens()
                {
                    MaxTokensBeforeSummarization = 0
                }
            };
        }
        else
        {
            agent.ChatReduction = new AiAgentChatReductionConfiguration()
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesLengthBeforeTruncate = 2,
                    MessagesLengthAfterTruncate = 2
                }
            };
        }
        if (withHistory)
            agent.ChatReduction.History = new();
        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSampleObject>(agent));

        var r3 = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>(r.ConversationId,
            userPrompt: "can you give me a cheaper alternative?"));

        Assert.NotNull(r3.Response.Answer);
        Assert.NotNull(r3.Usage);
        Assert.NotNull(r3.ConversationId);

        chat = await GetChat(store, r.ConversationId);
        Assert.Equal(2, chat.Messages.Count);
        Assert.Equal(systemPrompt, chat.Messages[0].Content);
        Assert.Equal(withHistory ? 1 : 0, chat.HistoryDocuments.Count);

        // resume - still with summarization

        var r33 = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>(r.ConversationId,
            userPrompt: "can you give me a cheaper alternative?"));

        Assert.NotNull(r33.Response.Answer);
        Assert.NotNull(r33.Usage);
        Assert.NotNull(r33.ConversationId);

        chat = await GetChat(store, r.ConversationId);
        Assert.Equal(2, chat.Messages.Count);
        Assert.Equal(systemPrompt, chat.Messages[0].Content);
        Assert.Equal(withHistory ? 2 : 0, chat.HistoryDocuments.Count);

        // resume
        agent.ChatReduction = null;
        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSampleObject>(agent));

        var r4 = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>(r.ConversationId,
            userPrompt: "can you give me another alternative?"));

        Assert.NotNull(r4.Response.Answer);
        Assert.NotNull(r4.Usage);
        Assert.NotNull(r4.ConversationId);

        chat = await GetChat(store, r.ConversationId);
        Assert.True(chat.Messages.Count > 2, "messages count: " + chat.Messages.Count);
        Assert.Equal(withHistory ? 2 : 0, chat.HistoryDocuments.Count);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { true, false })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Data = new object[] { false, false })]
    public async Task AnswerActionToolRequest(Options options, GenAiConfiguration config, bool summarization, bool withHistory)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new CreateSampleDataOperation());

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        const string systemPrompt = "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.";

        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName, systemPrompt);
        agent.Identifier = "shopping-assistant";

        if (summarization)
        {
            agent.ChatReduction = new AiAgentChatReductionConfiguration()
            {
                Tokens = new AiAgentSummarizationByTokens()
                {
                    MaxTokensBeforeSummarization = 0
                }
            };
        }
        else
        {
            agent.ChatReduction = new AiAgentChatReductionConfiguration()
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesLengthBeforeTruncate = 2,
                    MessagesLengthAfterTruncate = 2
                }
            };
        }
        if(withHistory)
            agent.ChatReduction.History = new();

        agent.Persistence = new AiAgentPersistenceConfiguration
        {
            Collection = "Chats",
            Expires = TimeSpan.FromDays(30)
        };

        agent.Actions =
        [
            new AiAgentToolAction
                {
                    Name = "ProductSearch",
                    Description =  "semantic search the store product catalog",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolAction
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    ParametersSampleObject = "{}"
                }
        ];

        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSampleObject>(agent));
        var r = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>("shopping-assistant", "what goes well with my cheese for recent orders?", parameters: null));

        Assert.True(r.ActionRequests.Count > 0);
        Assert.NotNull(r.Usage);
        Assert.NotNull(r.ConversationId);

        var toolResponse = new List<AiAgentActionResponse>();
        for (int i = 0; i < r.ActionRequests.Count; i++)
        {
            var request = r.ActionRequests[i];
            toolResponse.Add(new AiAgentActionResponse
            {
                ToolId = request.ToolId,
                Content = "{}"
            });
        }
        // shouldn't summarize in the middle of tool request
        var chat = await GetChat(store, r.ConversationId);
        Assert.True(chat.Messages.Count > 2, "messages count: " + chat.Messages.Count);
        Assert.Equal(systemPrompt, chat.Messages[0].Content);
        Assert.Equal(0, chat.HistoryDocuments.Count);

        var r2 = await store.Maintenance.SendAsync(new RunConversationOperation<OutputSampleObject>(r.ConversationId, actionResponses: toolResponse));

        Assert.True(r2.Response?.Answer != null || r2.ActionRequests != null);
        Assert.NotNull(r2.Usage);
        Assert.NotNull(r2.ConversationId);

        // can be answer *OR* another tool call
        if (r2.Response?.Answer != null)
        {
            // if it is 'Answer' is should be summarized
            chat = await GetChat(store, r.ConversationId);
            Assert.Equal(2, chat.Messages.Count);
            Assert.Equal(systemPrompt, chat.Messages[0].Content);
            Assert.Equal(withHistory ? 1 : 0, chat.HistoryDocuments.Count);
        }
    }

    private async Task<Chat> GetChat(DocumentStore store, string chatId)
    {
        using (var session = store.OpenAsyncSession())
        {
            return await session.LoadAsync<Chat>(chatId);
        }
    }
}
