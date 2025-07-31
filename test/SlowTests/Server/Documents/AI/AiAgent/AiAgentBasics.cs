using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
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

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.Persistence = new AiAgentPersistenceConfiguration ("Chats", TimeSpan.FromDays(30));
            agent.Parameters.Add(new AiAgentParameter("company", "The company ID"));
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            var createResult = await store.AI.CreateAgentAsync<OutputSchema>(agent);
            var chat = store.AI.StartConversation<OutputSchema>(
                createResult.Identifier,
                builder: p => p.AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what goes well with my cheese?");
            var r = await chat.RunAsync(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r);
            Assert.NotNull(chat.Answer);
            Assert.NotNull(chat.Id);

            var chat1 = await session.LoadAsync<dynamic>(chat.Id);
            Assert.NotNull(chat1);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanGetAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.Persistence = new AiAgentPersistenceConfiguration("Chats/", TimeSpan.FromDays(30));
            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];

            await store.AI.CreateAgentAsync<OutputSchema>(agent);
            var r = await store.AI.GetAgentsAsync();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var converter = DocumentConventions.Default.Serialization.DefaultConverter;
                var original = converter.ToBlittable(r.AiAgents[0], context);
                var fromGet = converter.ToBlittable(agent, context);
                Assert.Equal(original, fromGet);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanResumeConversation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.Persistence = new AiAgentPersistenceConfiguration("Chats/", TimeSpan.FromDays(30));
            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query) limit 3",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolQuery
                {
                    Name = "RecentOrder",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 5",
                    ParametersSampleObject = "{}"
                }
            ];

            var createResult = await store.AI.CreateAgentAsync<OutputSchema>(agent);
            var chat = store.AI.StartConversation<OutputSchema>(
                createResult.Identifier,
                builder: p => p.AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what goes well with my cheese for recent orders?");
            await chat.RunAsync(CancellationToken.None);
            Assert.NotNull(chat.Answer);
            Assert.NotNull(chat.Id);

            chat.SetUserPrompt("can you give me a cheaper alternative?");
            await chat.RunAsync(CancellationToken.None);
            Assert.NotNull(chat.Answer);
            Assert.NotNull(chat.Id);

            WaitForUserToContinueTheTest(store);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
        public async Task CanRunTest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.Persistence = new AiAgentPersistenceConfiguration("Chats/", TimeSpan.FromDays(30));

            agent.Actions =
            [
                new AiAgentToolAction
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolAction { Name = "RecentOrder", Description = "Get the recent orders of the current user", ParametersSampleObject = "{}" }
            ];

            var r = await store.Maintenance.SendAsync(new RunTestConversationOperation<OutputSchema>(
                agent,
                document: null,
                "what goes well with my cheese for recent orders?",
                new Dictionary<string, object> { ["company"] = "companies/90-A" },
                actionResponses: null));

            var responses = new List<AiAgentActionResponse>();
            foreach (var request in r.ActionRequests)
            {
                responses.Add(new AiAgentActionResponse
                {
                    ToolId = request.ToolId,
                    Content = "{}" // Simulating an empty response for the action tool
                });
            }
            r = await store.Maintenance.SendAsync(new RunTestConversationOperation<OutputSchema>(
                agent,
                document: r.Document,
                userPrompt: null, // "what goes well with my cheese for recent orders?",
                parameters: null,
                actionResponses: responses));
        }


        private class RunTestConversationOperation<TSchema> : IMaintenanceOperation<TestResult<TSchema>> where TSchema : new()
        {
            private readonly AiAgentConfiguration _agent;
            private readonly string _document;
            private readonly string _userPrompt;
            private readonly Dictionary<string, object> _parameters;
            private readonly List<AiAgentActionResponse> _actionResponses;
            public RunTestConversationOperation(AiAgentConfiguration agent, string document, string userPrompt, Dictionary<string, object> parameters, List<AiAgentActionResponse> actionResponses)
            {
                _agent = agent;
                _document = document;
                _userPrompt = userPrompt;
                _parameters = parameters;
                _actionResponses = actionResponses;
            }
            public RavenCommand<TestResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new RunConversationOperationCommand(_agent, _document, _userPrompt, _parameters, _actionResponses, conventions);
            }

            private sealed class RunConversationOperationCommand : RavenCommand<TestResult<TSchema>>
            {
                private readonly AiAgentConfiguration _agent;
                private readonly string _document;
                private readonly string _prompt;
                private readonly Dictionary<string, object> _parameters;
                private readonly List<AiAgentActionResponse> _toolResponses;
                private readonly DocumentConventions _conventions;

                public RunConversationOperationCommand(AiAgentConfiguration agent, string document, string prompt, Dictionary<string, object> parameters,
                    List<AiAgentActionResponse> toolResponses, DocumentConventions conventions)
                {
                    _agent = agent;
                    _document = document;
                    _prompt = prompt;
                    _parameters = parameters;
                    _toolResponses = toolResponses;
                    _conventions = conventions;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/ai/agent/test";
                   
                    var body = new TestRequestBody
                    {
                        Configuration = _agent,
                        Parameters = _parameters ?? new Dictionary<string, object>(), 
                        ActionResponses = _toolResponses ?? [], 
                        UserPrompt = _prompt,
                    };

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream =>
                        {
                            if (_document != null)
                                body.Document = ctx.Sync.ReadForMemory(_document, "test");

                            body.Configuration.SampleObject ??= DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new TSchema(), ctx).ToString();
                            await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(), "conversation-params")).ConfigureAwait(false);
                        }, _conventions)
                    };

                    return request;
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = TestResult<TSchema>.Convert(response, _conventions);
                }
            }
            public class TestRequestBody : IDynamicJson
            {
                public string UserPrompt { get; set; }
                public Dictionary<string, object> Parameters { get; set; }
                public AiAgentConfiguration Configuration { get; set; }
                public List<AiAgentActionResponse> ActionResponses { get; set; }

                public BlittableJsonReaderObject Document;
                public DynamicJsonValue ToJson()
                {
                    var json = new DynamicJsonValue
                    {
                        [nameof(UserPrompt)] = UserPrompt,
                        [nameof(Parameters)] = DynamicJsonValue.Convert(Parameters),
                        [nameof(Configuration)] = Configuration.ToJson(),
                        [nameof(ActionResponses)] = new DynamicJsonArray(ActionResponses.Select(x => x.ToJson()))
                    };

                    if (Document != null)
                        json[nameof(Document)] = Document;

                    return json;
                }
            }
        }

        public class TestResult<TSchema> where TSchema : new()
        {
            public string Document;
            public TSchema Response;
            public List<AiAgentActionRequest> ActionRequests;
            public AiUsage Usage;

            internal static TestResult<TSchema> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
            {
                response.TryGet(nameof(Usage), out BlittableJsonReaderObject usage);
                response.TryGet(nameof(Response), out BlittableJsonReaderObject result);
                response.TryGet(nameof(Document), out BlittableJsonReaderObject document);

                List<AiAgentActionRequest> requests = null;
                if (response.TryGet(nameof(ActionRequests), out BlittableJsonReaderArray actionRequests) && actionRequests != null)
                {
                    requests = [];
                    foreach (BlittableJsonReaderObject actionRequest in actionRequests)
                    {
                        var r = JsonDeserializationClient.ActionRequest(actionRequest);
                        requests.Add(r);
                    }
                }

                return new TestResult<TSchema>
                {
                    ActionRequests = requests,
                    Usage = JsonDeserializationClient.AiUsage(usage),
                    Document = document.ToString(),
                    Response = result == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TSchema>(result, "test")
                };
            }
        }
    }
}
