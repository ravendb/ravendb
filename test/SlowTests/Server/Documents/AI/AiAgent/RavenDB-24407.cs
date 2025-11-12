using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
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
        public static OutputSampleObject Instance = new();

        public string Answer = "Answer to the user question";

        public bool Relevant = true;

        public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

        public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
    }

    private class Chat
    {
        public List<Message> Messages { get; set; }
        public List<string> LinkedConversations { get; set; }
    }

    private class Message
    {
        [JsonProperty("role")]
        public string Role { get; set; }

        [JsonProperty("content")]
        public JToken Content { get; set; }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true], Skip = "RavenDB-24806")]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, false], Skip = "RavenDB-24806")]
    public async Task CanResumeConversationWithSummarization(Options options, GenAiConfiguration config, bool summarization, bool withHistory)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new CreateSampleDataOperation());

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        const string systemPrompt = "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.";
        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName, systemPrompt);
        agent.Identifier = "shopping-assistant";
        agent.Parameters.Add(new AiAgentParameter("company"));

        agent.Queries =
        [
            new AiAgentToolQuery
                {
                    Name = "ProductSearch",
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query) limit 5",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 3",
                    ParametersSampleObject = "{}"
                }
        ];
        agent.MaxModelIterationsPerCall = 5;

        var identifier = (await store.AI.CreateAgentAsync<OutputSampleObject>(agent, OutputSampleObject.Instance)).Identifier;
        // start chat
        var chat = store.AI.Conversation(
            agent.Identifier,
            "chats/",
            new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

        chat.SetUserPrompt("what goes well with my cheese for recent orders?");
        var r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        // resume
        chat.SetUserPrompt("can you give me a cheaper alternative?");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        var chatDoc = await GetChat(store, chat.Id);
        Assert.True(chatDoc.Messages.Count > 2, "messages count: " + chatDoc.Messages.Count);
        Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);
        Assert.Equal(0, chatDoc.LinkedConversations.Count);

        // resume - with summarization
        if (summarization)
        {
            agent.ChatTrimming = new AiAgentChatTrimmingConfiguration()
            {
                Tokens = new AiAgentSummarizationByTokens()
                {
                    MaxTokensBeforeSummarization = 0
                }
            };
        }
        else
        {
            agent.ChatTrimming = new AiAgentChatTrimmingConfiguration()
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesLengthBeforeTruncate = 2,
                    MessagesLengthAfterTruncate = 2
                }
            };
        }
        if (withHistory)
            agent.ChatTrimming.History = new();
        
        await store.AI.CreateAgentAsync(agent, OutputSampleObject.Instance);

        chat.SetUserPrompt("use the tool I gave you and try to give me a cheaper alternative");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        chatDoc = await GetChat(store, chat.Id);
        Assert.Equal(summarization ? 3 : 2, chatDoc.Messages.Count);
        Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);

        if (withHistory)
        {
            Assert.True(chatDoc.LinkedConversations.Count > 0);

            var historyChat = await GetChat(store, chatDoc.LinkedConversations.First());
            var lastMsg = historyChat.Messages.Last();

            await AssertWithDumpAsync(lastMsg.Role, async () => await DumpAllAsync(store, chatDoc, chatDoc.LinkedConversations));
        }
        else
        {
            Assert.Equal(0, chatDoc.LinkedConversations.Count);
        }

        // resume - still with summarization

        chat.SetUserPrompt("can you give me a cheaper alternative?");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        chatDoc = await GetChat(store, chat.Id);
        Assert.Equal(summarization ? 3 : 2, chatDoc.Messages.Count);
        Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);

        // resume
        agent.ChatTrimming = null;
        await store.AI.CreateAgentAsync<OutputSampleObject>(agent, OutputSampleObject.Instance);

        chat.SetUserPrompt("can you give me another alternative?");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        chatDoc = await GetChat(store, chat.Id);
        Assert.True(chatDoc.Messages.Count > 2, "messages count: " + chatDoc.Messages.Count);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { true, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false, true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { true, false })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false, false })]
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
            agent.ChatTrimming = new AiAgentChatTrimmingConfiguration()
            {
                Tokens = new AiAgentSummarizationByTokens()
                {
                    MaxTokensBeforeSummarization = 0
                }
            };
        }
        else
        {
            agent.ChatTrimming = new AiAgentChatTrimmingConfiguration()
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesLengthBeforeTruncate = 2,
                    MessagesLengthAfterTruncate = 2
                }
            };
        }
        if (withHistory)
            agent.ChatTrimming.History = new();

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

        await store.AI.CreateAgentAsync(agent, OutputSampleObject.Instance);
        var chat = store.AI.Conversation(
            agent.Identifier,
            "chats/",
            creationOptions: null);
        
        chat.OnUnhandledAction += args => Task.CompletedTask;
        
        chat.SetUserPrompt("what goes well with my cheese for recent orders?");
        var r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);
        Assert.Equal(AiConversationResult.ActionRequired, r.Status);

        foreach (var req in chat.RequiredActions())
        {
            chat.AddActionResponse(req.ToolId, "{}");
        }

        // shouldn't summarize in the middle of tool request
        var chatDoc = await GetChat(store, chat.Id);
        Assert.True(chatDoc.Messages.Count > 2, "messages count: " + chatDoc.Messages.Count);
        Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);
        Assert.Equal(0, chatDoc.LinkedConversations.Count);

        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        // can be answer *OR* another tool call
        chatDoc = await GetChat(store, chat.Id);
        if (r.Status == AiConversationResult.ActionRequired)
        {
            // if it is 'Tool Requests' is shouldn't be summarized
            Assert.True(2 < chatDoc.Messages.Count);
            Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);
            Assert.Equal(0, chatDoc.LinkedConversations.Count);
        }
        else
        {
            // if it is 'Answer' is should be summarized
            Assert.Equal(2, chatDoc.Messages.Count);
            Assert.Equal(systemPrompt, chatDoc.Messages[0].Content);
            Assert.Equal(withHistory ? 1 : 0, chatDoc.LinkedConversations.Count);
        }
    }

    private async Task<Chat> GetChat(DocumentStore store, string chatId)
    {
        using (var session = store.OpenAsyncSession())
        {
            return await session.LoadAsync<Chat>(chatId);
        }
    }

    private static async Task AssertWithDumpAsync(
        string lastMsgRole,
        Func<Task<string>> dumpFactory)
    {
        if (lastMsgRole != "tool")
        {
            var msg = await dumpFactory(); // build dump only on failure
            Assert.Fail($"Expected last history message role 'tool' but was '{lastMsgRole}'.\n{msg}");
        }
    }

    private static string Dump(Chat c)
    {
        if (c?.Messages == null)
            return "(no messages)";
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < c.Messages.Count; i++)
        {
            var m = c.Messages[i];
            var content = m.Content?.ToString(Formatting.None) ?? "";
            if (content.Length > 300)
                content = content.Substring(0, 300); //the start is usually the most relevant
            sb.AppendLine($"[{i}] role={m.Role} content={content}");
        }
        return sb.ToString();
    }

    private static async Task<string> DumpAllAsync(DocumentStore store, Chat current, IEnumerable<string> links)
     {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("=== Linked Conversations ===");
        if (links != null)
            foreach (var id in links)
            {
                var c = await store.OpenAsyncSession().LoadAsync<Chat>(id);
                sb.AppendLine($"- {id}");
                sb.AppendLine(Dump(c));
            }

        sb.AppendLine("=== Current Conversation ===");
        sb.Append(Dump(current));
        return sb.ToString();
    }
}
