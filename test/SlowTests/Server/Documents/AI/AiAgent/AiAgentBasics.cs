using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentBasics : RavenTestBase
    {

        public class OutputSchema
        {
            public string Answer = "Answer to the user question";

            public bool Relevant = true;

            public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

            public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
        }

        public AiAgentBasics(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanCreateAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration(config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };
            agent.Parameters.Add("company");
            agent.Queries =
            [
                new AiAgentConfiguration.ToolQuery
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentConfiguration.ToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSchema>("shopping assistant", agent));
            var r = await store.Maintenance.SendAsync(new StartChatOperation<OutputSchema>("shopping assistant", "what goes well with my cheese?",
                new Dictionary<string, object> { ["company"] = "companies/90-A" }));

            Assert.NotNull(r.Response.Answer);
            Assert.NotNull(r.Usage);
            Assert.NotNull(r.ChatId);

            var chat = await session.LoadAsync<dynamic>(r.ChatId);
            Assert.NotNull(chat);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanResumeConversation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration(config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };
            agent.Parameters.Add("company");
            agent.Queries =
            [
                new AiAgentConfiguration.ToolQuery
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentConfiguration.ToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSchema>("shopping assistant", agent));
            var r = await store.Maintenance.SendAsync(new StartChatOperation<OutputSchema>("shopping assistant", "what goes well with my cheese for recent orders?",
                new Dictionary<string, object> { ["company"] = "companies/90-A" }));

            Assert.NotNull(r.Response.Answer);
            Assert.NotNull(r.Usage);
            Assert.NotNull(r.ChatId);


            var r2 = await store.Maintenance.SendAsync(new ResumeChatOperation<OutputSchema>(r.ChatId,
                userPrompt: "can you give me a cheaper alternative?"));

            Assert.NotNull(r2.Response.Answer);
            Assert.NotNull(r2.Usage);
            Assert.NotNull(r2.ChatId);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task AnswerActionToolRequest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration(config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Persistence = new AiAgentConfiguration.PersistenceConfiguration
            {
                Collection = "Chats",
                Expires = TimeSpan.FromDays(30)
            };

            agent.Actions =
            [
                new AiAgentConfiguration.ToolAction
                {
                    Name = "ProductSearch", 
                    Description =  "semantic search the store product catalog",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                }
                ,
                new AiAgentConfiguration.ToolAction
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    ParametersSampleObject = "{}"
                }
            ];

            await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation<OutputSchema>("shopping assistant", agent));
            var r = await store.Maintenance.SendAsync(new StartChatOperation<OutputSchema>("shopping assistant", "what goes well with my cheese for recent orders?"));

            Assert.True(r.ToolRequests.Count > 0);
            Assert.NotNull(r.Usage);
            Assert.NotNull(r.ChatId);

            var toolResponse = new List<ToolResponse>();
            for (int i = 0; i < r.ToolRequests.Count; i++)
            {
                var request = r.ToolRequests[i];
                toolResponse.Add(new ToolResponse
                {
                    ToolId = request.ToolId,
                    Content = "{}"
                });
            }

            var r2 = await store.Maintenance.SendAsync(new ResumeChatOperation<OutputSchema>(r.ChatId, toolResponses: toolResponse));

            Assert.True(r2.Response?.Answer != null || r2.ToolRequests != null);
            Assert.NotNull(r2.Usage);
            Assert.NotNull(r2.ChatId);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanRunTest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));


            var body = @$"{{    
""{nameof(AiAgentProcessorForTestChat.AiAgentTestRequest.Parameters)}"": {{
        ""company"": ""companies/90-A""
    }},
""{nameof(AiAgentProcessorForTestChat.AiAgentTestRequest.Prompt)}"": ""Help to find something more to my recent order"",
""{nameof(AiAgentProcessorForTestChat.AiAgentTestRequest.Configuration)}"":{{
    ""ConnectionStringName"": ""{config.ConnectionStringName}"",
    ""SystemPrompt"": ""You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well."",
    ""SampleObject"": ""{{\""Answer\"": \""Answer to the user question\"", \""Relevant\"": true, \""RelevantOrdersId\"":[\""The order ids relevant to the query or response\""], \""MatchingProductsId\"":[\""All the product ids referenced either by the user or the system\""] }}"",
    ""Persistence"": {{
        ""Collection"": ""Chats"",
        ""Expires"": ""3.00:00:00""
    }},
    ""Parameters"": [""company""],
    ""Queries"": [
        {{
            ""Name"": ""ProductSearch"",
            ""Description"": ""semantic search the store product catalog"",
            ""Query"": ""from Products where vector.search(embedding.text(Name), $query)"",
            ""ParametersSampleObject"": ""{{\""query\"": [\""term or phrase to search in the catalog\""]}}""
        }},
        {{
            ""Name"": ""RecentOrder"",
            ""Description"": ""Get the recent orders of the current user"",
            ""Query"": ""from Orders where Company = $company order by OrderedAt desc limit 10"",
            ""ParametersSampleObject"": ""{{}}""
        }}
    ]
}}}}";
            using var test = new HttpRequestMessage
            {
                RequestUri = new Uri($"{store.Urls[0]}/databases/{store.Database}/ai/agent/test", UriKind.Absolute),
                Method = HttpMethod.Post, 
                Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json")
            };
            using var r = await store.GetRequestExecutor().HttpClient.SendAsync(test);
            Assert.True(r.IsSuccessStatusCode, "status code: " + r.StatusCode);
        }
    }
}
