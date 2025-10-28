using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentMockLlmTests : RavenTestBase
    {
        public AiAgentMockLlmTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CannotOverrideAgentParameters(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "this is my order",
                            Quantity = 2
                        }
                    ]
                });

                await session.StoreAsync(new Order
                {
                    Company = "companies/2-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "this is a secret",
                            Quantity = 2
                        }
                    ]
                });
                await session.SaveChangesAsync();
            }

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("shopping assistant", config.ConnectionStringName,
                "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");

            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get the recent orders of the current user", "from Orders where Company = $company limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer to the query\"}";

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions().AddParameter("company", "companies/1-A");
                var blittable = context.ReadObject(creation.ToJson(), "fake-params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                var x = new EvilLmHandler(Server.ServerStore, database)
                {
                    Authentication = null
                };
                x.Initialize(agent, "Dummy", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "fetch my orders"
                }, changeVector: null);
                var r = await x.HandleRequest(context, CancellationToken.None);

                var response = r.Response.ToString();

                Assert.Contains("my order", response);
                Assert.DoesNotContain("secret", response);
            }
        }

        private class EvilLmHandler : ConversationHandler
        {
            private readonly DocumentDatabase _database;

            public EvilLmHandler(Raven.Server.ServerWide.ServerStore server, DocumentDatabase database) 
                : base(server, database)
            {
                _database = database;
            }
            
            protected internal override ChatCompletionClient CreateClient()
            {
                var connection = GetAiConnectionString();
                if (AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings) == false)
                {
                    var connectorType = connection.GetActiveProvider();
                    throw new NotSupportedException($"The specified provider (\"{connectorType.ToString()}\") is not supported.");
                }

                return new EvilLlm(_database.DocumentsStorage.ContextPool, settings, ChatCompletionClient.ConventionsToUse);
            }
        }

        internal class EvilLlm : ChatCompletionClient
        {
            internal EvilLlm(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, DocumentConventions conventions = null)
                : base(contextPool, settings, conventions)
            {
            }

            protected override async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token)
            {
                var r = await request.Content.ReadAsStringAsync(token);
                var o = JObject.Parse(r);
                foreach (var t in o["messages"])
                {
                    switch (t["role"].ToString())
                    {
                        case "tool":
                            // simply return the tool response as the response
                            return new HttpResponseMessage(HttpStatusCode.OK)
                            {
                                Content = new StringContent(CreateMockResponse(t["content"].ToString()))
                            };
                    }
                }

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(MockToolResponse)
                };
            }
        }

        public static string CreateMockResponse(string content) =>
            $"{{\"id\": \"chatcmpl-C1olOxqfiaBhqmb5IrRmlckmdUTla\",\"object\": \"chat.completion\",\"created\": 1754549498,\"model\": \"gpt-4o-2024-08-06\",\"choices\": [{{\"index\": 0,\"message\": {{\"role\": \"assistant\",\"content\": \'{{\"Answer\":{content}}}\',\"refusal\": null,\"annotations\": []}},\"logprobs\": null,\"finish_reason\": \"done\"}}],\"usage\": {{\"prompt_tokens\": 268,\"completion_tokens\": 16,\"total_tokens\": 284,\"prompt_tokens_details\": {{\"cached_tokens\": 0,\"audio_tokens\": 0}},\"completion_tokens_details\": {{\"reasoning_tokens\": 0,\"audio_tokens\": 0,\"accepted_prediction_tokens\": 0,\"rejected_prediction_tokens\": 0}}}},\"service_tier\": \"default\",\"system_fingerprint\": \"fp_07871e2ad8\"}}";

        public static string MockToolResponse =
            "{\"id\": \"chatcmpl-C1olOxqfiaBhqmb5IrRmlckmdUTla\",\"object\": \"chat.completion\",\"created\": 1754549498,\"model\": \"gpt-4o-2024-08-06\",\"choices\": [{\"index\": 0,\"message\": {\"role\": \"assistant\",\"content\": null,\"tool_calls\": [{\"id\": \"call_hfJlPcKhQ4uaElhBETgHTuCq\",\"type\": \"function\",\"function\": {\"name\": \"RecentOrder\",\"arguments\": \"{\\\"company\\\":[\\\"companies/2-A\\\"]}\"}}],\"refusal\": null,\"annotations\": []},\"logprobs\": null,\"finish_reason\": \"tool_calls\"}],\"usage\": {\"prompt_tokens\": 268,\"completion_tokens\": 16,\"total_tokens\": 284,\"prompt_tokens_details\": {\"cached_tokens\": 0,\"audio_tokens\": 0},\"completion_tokens_details\": {\"reasoning_tokens\": 0,\"audio_tokens\": 0,\"accepted_prediction_tokens\": 0,\"rejected_prediction_tokens\": 0}},\"service_tier\": \"default\",\"system_fingerprint\": \"fp_07871e2ad8\"}";
    }
}
