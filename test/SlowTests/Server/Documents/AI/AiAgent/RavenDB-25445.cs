using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25445 : RavenTestBase
    {
        public RavenDB_25445(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldHandleBadGateway(Options options, GenAiConfiguration config)
        {
            using (var store = GetDocumentStore())
            {
                // 1. Simulate the Cloudflare 502 HTML response
                var response = new HttpResponseMessage(HttpStatusCode.BadGateway)
                {
                    Content = new StringContent("<html><body>502 Bad Gateway</body></html>", Encoding.UTF8, "text/html")
                };

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    if (AbstractChatCompletionClientSettings.TryGetParameters(config.Connection, out var settings) == false)
                        throw new InvalidOperationException("Could not get settings from connection.");

                    var client = new ChatCompletionClient(store.GetRequestExecutor().ContextPool, settings);

                    var ex = await Assert.ThrowsAsync<UnexpectedResponseException>(async () =>
                    {
                        await client.GetResponseContentAsync(context, response, CancellationToken.None);
                    });

                    RavenTestHelper.AssertContainsRespectingNewLines("Received an unrecognized response from the server.\r\nStatus Code: BadGateway\r\nResponse:\r\nStatusCode: 502, ReasonPhrase: 'Bad Gateway'", ex.Message);

                    Assert.IsNotType<InvalidDataException>(ex.InnerException);
                }
            }
        }
    }
}
