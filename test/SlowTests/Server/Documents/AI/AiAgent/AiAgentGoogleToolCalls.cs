using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AiAgentGoogleToolCalls : RavenTestBase
    {
        public AiAgentGoogleToolCalls(ITestOutputHelper output) : base(output)
        {
        }

        private class OutputSchema
        {
            public static OutputSchema Instance = new();

            public string Answer = "Answer to the user question";

            public List<string> MatchingProductsId = ["The product ids the answer is about"];
        }

        // Google puts the thought signature on the first tool call of a response and on no other, so a
        // response holding several calls is the one that used to fail. The model decides how many calls
        // it makes, so the prompt asks for four independent lookups to make that likely.
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SeveralToolCallsInOneResponseDoNotFailTheTurn(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents));

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("product-lookup", config.ConnectionStringName,
                "You are an assistant of an online shop. Answer only from the tools you are given. Always include the product ids.")
            {
                Identifier = "product-lookup",
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "ProductById",
                        Description = "Get a single product by its id",
                        Query = "from Products where id() = $id",
                        ParametersSampleObject = "{\"id\": \"products/1-A\"}"
                    }
                ]
            };

            var createResult = await store.AI.CreateAgentAsync(agent, OutputSchema.Instance);
            var chat = store.AI.Conversation(createResult.Identifier, "chats/", new AiConversationCreationOptions());

            chat.SetUserPrompt("Name products/1-A, products/2-A, products/3-A and products/4-A.");

            var streamed = new List<string>();
            var r = await chat.StreamAsync<OutputSchema>(
                nameof(OutputSchema.Answer),
                chunk =>
                {
                    streamed.Add(chunk);
                    return Task.CompletedTask;
                },
                CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.False(string.IsNullOrWhiteSpace(r.Answer?.Answer));
        }
    }
}
