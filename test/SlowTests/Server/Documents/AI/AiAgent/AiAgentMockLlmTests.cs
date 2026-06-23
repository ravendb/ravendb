using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentMockLlmTests : RavenTestBase
    {
        public AiAgentMockLlmTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CannotOverrideAgentParameters()
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

            var agent = new AiAgentConfiguration("shopping assistant", "fake-connection",
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

                // The "evil" part: the mock LLM tries to override the company parameter to access unauthorized data
                bool toolCalled = false;
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ =>
                    {
                        if (toolCalled)
                            return null; // fall through to default tool result handling
                        toolCalled = true;
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(MockLlm.CreateToolCallResponse("RecentOrder",
                                "{\"company\":[\"companies/2-A\"]}"))
                        };
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agent, "Dummy", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "fetch my orders"
                }, changeVector: null);
                var r = await handler.HandleRequestAsync(context, CancellationToken.None);

                var response = r.Response.ToString();

                Assert.Contains("my order", response);
                Assert.DoesNotContain("secret", response);
            }
        }
    }
}
