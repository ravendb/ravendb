using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_26268(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Ai)]
        public async Task ShouldIncludePromptCacheKeyInRequest()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "Laptop",
                            Quantity = 1
                        }
                    ]
                });
                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("test assistant", "fake-connection",
                "You are a test assistant.");

            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders where Company = $company limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer\"}";

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions().AddParameter("company", "companies/1-A");
                var blittable = context.ReadObject(creation.ToJson(), "fake-params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                string capturedPromptCacheKey = null;

                var handler = new MockHandler(Server.ServerStore, database, onRequest: payload =>
                {
                    capturedPromptCacheKey ??= payload["prompt_cache_key"]?.ToString();
                })
                {
                    Authentication = null
                };

                handler.Initialize(agent, "conversations/1-A", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "show my orders"
                }, changeVector: null);

                await handler.HandleRequest(context, CancellationToken.None);

                Assert.NotNull(capturedPromptCacheKey);
                Assert.Equal("conversations/1-A", capturedPromptCacheKey);
            }
        }

        private class MockHandler : ConversationHandler
        {
            private readonly DocumentDatabase _database;
            private readonly System.Action<JObject> _onRequest;

            public MockHandler(Raven.Server.ServerWide.ServerStore server, DocumentDatabase database, System.Action<JObject> onRequest)
                : base(server, database)
            {
                _database = database;
                _onRequest = onRequest;
            }

            protected internal override ChatCompletionClient CreateClient()
            {
                var settings = new OpenAiChatCompletionClientSettings(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o"));
                return new MockLlm(_database.DocumentsStorage.ContextPool, settings, _onRequest, ChatCompletionClient.ConventionsToUse);
            }
        }

        private class MockLlm : ChatCompletionClient
        {
            private readonly System.Action<JObject> _onRequest;

            internal MockLlm(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, System.Action<JObject> onRequest, DocumentConventions conventions = null)
                : base(contextPool, settings, conventions)
            {
                _onRequest = onRequest;
            }

            protected override async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
            {
                var r = await request.Content.ReadAsStringAsync(token);
                var payload = JObject.Parse(r);

                _onRequest(payload);

                foreach (var t in payload["messages"])
                {
                    if (t["role"].ToString() == "tool")
                    {
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(AiAgentMockLlmTests.CreateMockResponse(t["content"].ToString()))
                        };
                    }
                }

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(AiAgentMockLlmTests.MockToolResponse)
                };
            }
        }
    }
}
