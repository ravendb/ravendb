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

    private class Drink
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Price { get; set; }
        public string[] Tags { get; set; }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true], Skip = "RavenDB-24806")]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, false], Skip = "RavenDB-24806")]
    public async Task CanResumeConversationWithSummarization(Options options, GenAiConfiguration config, bool summarization, bool withHistory)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Drink
            {
                Id = "drinks/1",
                Name = "coca cola",
                Price = 10,
                Tags = ["sweet", "cold", "carbonated"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/2",
                Name = "sprite",
                Price = 10,
                Tags = ["sweet", "sour", "cold", "carbonated"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/3",
                Name = "fanta orange",
                Price = 11,
                Tags = ["sweet", "fruity", "cold", "carbonated"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/4",
                Name = "sparkling water",
                Price = 8,
                Tags = ["bitter", "cold", "carbonated"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/5",
                Name = "beer",
                Price = 18,
                Tags = ["bitter", "alcoholic", "cold"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/6",
                Name = "lager beer",
                Price = 20,
                Tags = ["bitter", "alcoholic", "cold", "light"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/7",
                Name = "red wine",
                Price = 95,
                Tags = ["bitter", "sour", "very alcoholic"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/8",
                Name = "whiskey",
                Price = 140,
                Tags = ["bitter", "very alcoholic", "strong"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/9",
                Name = "vodka",
                Price = 120,
                Tags = ["very alcoholic", "strong"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/10",
                Name = "mojito",
                Price = 55,
                Tags = ["sweet", "sour", "alcoholic", "mint"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/11",
                Name = "lemonade",
                Price = 14,
                Tags = ["sweet", "sour", "cold"]
            });

            await session.StoreAsync(new Drink
            {
                Id = "drinks/12",
                Name = "iced tea",
                Price = 13,
                Tags = ["sweet", "cold", "tea"]
            });

            await session.SaveChangesAsync();
        }

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        const string systemPrompt = """
                                    You are a shopping assistant.

                                    Rules:
                                    - Use DrinkSearch for recommendations.
                                    - Never invent drink pairings, only use the tool results.
                                    """;
        var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName, systemPrompt);
        agent.Identifier = "shopping-assistant";

        agent.Queries =
        [
            new AiAgentToolQuery
                {
                    Name = "DrinkSearch",
                    Description =  "semantic search the store drinks catalog",
                    Query = "from Drinks where vector.search(embedding.text(Name), $query) limit 5",
                    ParametersSampleObject = "{\"query\": [\"Term or phrase to search in the catalog, for example: sweet and low alcoholic drink\"]}"
                }
        ];
        agent.MaxModelIterationsPerCall = 5;

        var identifier = (await store.AI.CreateAgentAsync<OutputSampleObject>(agent, OutputSampleObject.Instance)).Identifier;
        // start chat
        var chat = store.AI.Conversation(
            identifier,
            "chats/",
            new AiConversationCreationOptions());

        chat.SetUserPrompt("What sweet alcoholic drink do you recommend? recommend 1 drink please");
        var r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);
        Assert.NotNull(r.Answer);

        // resume
        chat.SetUserPrompt("is it sweet?");
        var r2 = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r2.Answer);
        Assert.NotEqual(r.Answer, r2.Answer);

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

        chat.SetUserPrompt("is it bitter?");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        chatDoc = await GetChat(store, chat.Id);
        Assert.Equal(2, chatDoc.Messages.Count);
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

        chat.SetUserPrompt("is it supper alcoholic?");
        r = await chat.RunAsync<OutputSampleObject>(CancellationToken.None);

        Assert.NotNull(r.Answer);

        chatDoc = await GetChat(store, chat.Id);
        Assert.Equal(2, chatDoc.Messages.Count);
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

        const string systemPrompt = "You are an AI agent of an online shop. " +
                                    "You must use the available tools to answer user questions whenever relevant. " +
                                    "For any question about products, recommendations, or catalog items, you MUST call ProductSearch. " +
                                    "For any question related to the user's orders, you MUST call RecentOrder. " +
                                    "Do not answer from your own knowledge. " +
                                    "Always base your response on tool results. " +
                                    "When talking about orders or products, include the ids as well.";

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
                    Description =  "semantic search the store product catalog" +
                                   ", MUST be used for any product-related query. Do not answer without calling this tool.",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentToolAction
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user" +
                                  ", MUST be used for any question about user orders or history. Do not answer without calling this tool.",
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
