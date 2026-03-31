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

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        capturedPromptCacheKey ??= payload["prompt_cache_key"]?.ToString();
                        return null; // fall through to default handling
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
    }
}
