using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentBasics : RavenTestBase
    {

        public class OutputSchema
        {
            public static OutputSchema Instance = new();

            public string Answer = "Answer to the user question";

            public bool Relevant = true;

            public List<string> RelevantOrdersId = ["The order ids relevant to the query or response"];

            public List<string> MatchingProductsId = ["All the product ids referenced either by the user or the system"];
        }

        public AiAgentBasics(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanCreateAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
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

            var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);
            var chat = store.AI.Conversation(
                createResult.Identifier,
                "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what goes well with my cheese?");
            var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.NotNull(r.Answer);
            Assert.NotNull(chat.Id);

            var chat1 = await session.LoadAsync<dynamic>(chat.Id);
            Assert.NotNull(chat1);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanGetAiAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
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
            agent.ChatTrimming = null;
            await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);
            var r = await store.AI.GetAgentsAsync();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var converter = DocumentConventions.Default.Serialization.DefaultConverter;
                var original = converter.ToBlittable(r.AiAgents[0], context);
                var fromGet = converter.ToBlittable(agent, context);
                Assert.Equal(original, fromGet);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanResumeConversation(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using var session = store.OpenAsyncSession();

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
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

            var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);
            var chat = store.AI.Conversation(
                createResult.Identifier,
                "chats/",
                new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("what goes well with my cheese for recent orders?");
            var r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.NotNull(r.Answer);
            Assert.NotNull(chat.Id);

            chat.SetUserPrompt("can you give me a cheaper alternative?");
            r = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.NotNull(r.Answer);
            Assert.NotNull(chat.Id);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanRunTest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.SampleObject = JsonConvert.SerializeObject(OutputSchema.Instance);
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

            using var _ = store.GetRequestExecutor().ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var r = await store.Maintenance.SendAsync(new RunTestConversationOperation<OutputSchema>(
                context,
                agent,
                "TestConversationId",
                documents: null,
                "what goes well with my cheese for recent orders?",
                new AiConversationCreationOptions(new Dictionary<string, object>{ ["company"] = "companies/90-A" }) ,
                actionResponses: null)) as TestResult<OutputSchema>;

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
                context,
                agent,
                "TestConversationId",
                documents: r.Documents,
                userPrompt: null, // "what goes well with my cheese for recent orders?",
                options: null,
                actionResponses: responses)) as TestResult<OutputSchema>;
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanStreamTest(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("shopping-assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
            agent.Identifier = "shopping-assistant";
            agent.SampleObject = JsonConvert.SerializeObject(OutputSchema.Instance);
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

            using var _ = store.GetRequestExecutor().ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var sb = new StringBuilder();
            var r = await store.Maintenance.SendAsync(new RunTestConversationOperation<OutputSchema>(
                context,
                agent,
                "TestConversationId",
                documents: null,
                userPrompt: "what goes well with my cheese for recent orders?",
                options: new AiConversationCreationOptions(new Dictionary<string, object> { ["company"] = "companies/90-A" }),
                actionResponses : null,
                streamPropertyPath: s => s.Answer,
                streamedChunksCallback: c =>
                {
                    sb.Append(c);
                    return Task.CompletedTask;
                })) as TestResult<OutputSchema>;

            Assert.NotNull(r);
            Assert.True(r.ActionRequests.Count > 0);
            Assert.Empty(sb.ToString());

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
                context,
                agent,
                "TestConversationId",
                documents: r.Documents,
                userPrompt: null, // "what goes well with my cheese for recent orders?",
                options: null,
                actionResponses: responses,
                s => s.Answer,
                c =>
                {
                    sb.Append(c);
                    return Task.CompletedTask;
                })) as TestResult<OutputSchema>;

            Assert.NotNull(r);
            if (r.Response?.Answer is not null)
                Assert.Equal(r.Response.Answer , sb.ToString());
            else
                Assert.True(r.ActionRequests?.Count > 0); // model retry with another tool-call (probably because empty response for last tool calls)
        }

        internal class RunTestConversationOperation<TSchema> : RunConversationOperation<TSchema>
        {
            private readonly AiAgentConfiguration _agent;
            private readonly Dictionary<string, BlittableJsonReaderObject> _documents;
            private readonly string _userPrompt;
            private readonly AiConversationCreationOptions _options;
            private readonly List<AiAgentActionResponse> _actionResponses;
            private JsonOperationContext _context;

#pragma warning disable CS0618 // Type or member is obsolete
            public RunTestConversationOperation(JsonOperationContext context, AiAgentConfiguration agent, string conversationId, Dictionary<string, BlittableJsonReaderObject> documents, string userPrompt, AiConversationCreationOptions options, List<AiAgentActionResponse> actionResponses) : 
                base(agent.Identifier, conversationId, userPrompt, actionResponses, options, changeVector: null, streamPropertyPath: null, streamedChunksCallback: null)
#pragma warning restore CS0618 // Type or member is obsolete
            {
                _agent = agent;
                _documents = documents;
                _userPrompt = userPrompt;
                _options = options;
                _actionResponses = actionResponses;
                _context = context;
            }

            public RunTestConversationOperation(JsonOperationContext context, AiAgentConfiguration agent, string conversationId, Dictionary<string, BlittableJsonReaderObject> documents, string userPrompt, AiConversationCreationOptions options, List<AiAgentActionResponse> actionResponses, Expression<Func<TSchema, string>> streamPropertyPath,
#pragma warning disable CS0618 // Type or member is obsolete
                Func<string, Task> streamedChunksCallback) : 
                base(agent.Identifier, conversationId, userPrompt, actionResponses: actionResponses, options, changeVector: null, streamPropertyPath.ToPropertyPath(DocumentConventions.Default), streamedChunksCallback)
#pragma warning restore CS0618 // Type or member is obsolete
            {
                _agent = agent;
                _documents = documents;
                _userPrompt = userPrompt;
                _options = options;
                _actionResponses = actionResponses;
                _context = context;
            }

            public override RavenCommand<ConversationResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new RunTestConversationOperationCommand(_context, this, _agent, _documents, _userPrompt, _options, _actionResponses, conventions);
            }

            private sealed class RunTestConversationOperationCommand : RunConversationOperationCommand
            {
                private readonly AiAgentConfiguration _agent;
                private readonly Dictionary<string, BlittableJsonReaderObject> _documents;
                private readonly string _prompt;
                private readonly AiConversationCreationOptions _options;
                private readonly List<AiAgentActionResponse> _toolResponses;
                private readonly DocumentConventions _conventions;
                private JsonOperationContext _context;

                public RunTestConversationOperationCommand(JsonOperationContext context, RunTestConversationOperation<TSchema> parent, AiAgentConfiguration agent, Dictionary<string, BlittableJsonReaderObject> documents, string prompt, AiConversationCreationOptions options,
                    List<AiAgentActionResponse> toolResponses, DocumentConventions conventions) : base(parent, conventions)
                {
                    _agent = agent;
                    _documents = documents;
                    _prompt = prompt;
                    _options = options;
                    _toolResponses = toolResponses;
                    _conventions = conventions;
                    _context = context;
                }

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    using var _ = base.CreateRequest(ctx, node, out url);
                    url = url.Replace("/ai/agent", "/ai/agent/test");
                   
                    var body = new TestRequestBody
                    {
                        Configuration = _agent,
                        CreationOptions = _options, 
                        ActionResponses = _toolResponses ?? [], 
                        UserPrompt = _prompt,
                    };

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream =>
                        {
                            body.Documents = _documents;

                            await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(), "conversation-params")).ConfigureAwait(false);
                        }, _conventions)
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    response.TryGet(nameof(TestResult<TSchema>.Documents), out BlittableJsonReaderObject documents);

                    var r = TestResult<TSchema>.Convert(response, _conventions);
                    Result = new TestResult<TSchema>
                    {
                        TotalUsage = r.TotalUsage,
                        Response = r.Response,
                        ChangeVector = r.ChangeVector,
                        ActionRequests = r.ActionRequests,
                        ConversationId = r.ConversationId,
                        Documents = GetDocumentsDictionary(documents)
                    };
                }

                private Dictionary<string, BlittableJsonReaderObject> GetDocumentsDictionary(BlittableJsonReaderObject obj)
                {
                    var result = new Dictionary<string, BlittableJsonReaderObject>();

                    foreach (var property in obj.GetPropertyNames())
                    {
                        if (obj.TryGet(property, out BlittableJsonReaderObject nested))
                        {
                            result[property] = nested.Clone(_context);
                        }
                    }

                    return result;
                }
            }

            public class TestRequestBody : IDynamicJson
            {
                public string UserPrompt { get; set; }
                public AiConversationCreationOptions CreationOptions { get; set; }
                public AiAgentConfiguration Configuration { get; set; }
                public List<AiAgentActionResponse> ActionResponses { get; set; }

                public Dictionary<string, BlittableJsonReaderObject> Documents { get; set; }
                public DynamicJsonValue ToJson()
                {
                    var json = new DynamicJsonValue
                    {
                        [nameof(UserPrompt)] = UserPrompt,
                        [nameof(CreationOptions)] = (CreationOptions ?? new AiConversationCreationOptions()).ToJson(),
                        [nameof(Configuration)] = Configuration.ToJson(),
                        [nameof(ActionResponses)] = new DynamicJsonArray(ActionResponses.Select(x => x.ToJson()))
                    };

                    if (Documents != null)
                        json[nameof(Documents)] = DynamicJsonValue.Convert(Documents);

                    return json;
                }
            }
        }

        public class TestResult<TSchema> : ConversationResult<TSchema>
        {
            public Dictionary<string, BlittableJsonReaderObject> Documents;

            public override string ToString()
            {
                var docs = Documents == null
                    ? "null"
                    : "{ " + string.Join(", ", Documents.Select(x => $"\"{x.Key}\": {x.Value?.ToString()}")) + " }";

                return $@"
ConversationId: {ConversationId}
ChangeVector: {ChangeVector}
Response: {Response}
Elapsed: {Elapsed}
Usage: {Usage}
TotalUsage: {TotalUsage}
ActionRequests: {ActionRequests?.Count ?? 0}
Documents: {docs}
";
            }
        }
    }
}
