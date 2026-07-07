using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24905 : RavenTestBase
    {
        public RavenDB_24905(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldStripQueryToolMetadataToEssentialFieldsWithProjection(Options options, GenAiConfiguration config)
        {
            var store = GetDocumentStore(options);
            var conversation = await SetupTestAgentAsync(store, config, "from Products as p where search(p.Name, \"Product/1\") order by score() desc select p.Name");
            using (var session = store.OpenAsyncSession())
            {

                var product = new Product { Name = "Product/1" };
                await session.StoreAsync(product, "products/1-a");

                await session.SaveChangesAsync();
            }

            await conversation.RunAsync<object>();

            var result = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
            {
                ConversationId = conversation.Id,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });
            Assert.NotNull(result);

            var toolCallMsg = result.Messages.FindLast(m => m.ToolCalls is { Count: > 0 });
            Assert.NotNull(toolCallMsg);
            var toolContent = toolCallMsg.ToolCalls[^1].Result;

            Assert.False(string.IsNullOrEmpty(toolContent), "content of tool msg should not be null");

            var documentsArray = JArray.Parse(toolContent);
            Assert.True(documentsArray.Count > 0, "The tool should have returned documents.");

            foreach (var docToken in documentsArray)
            {
                AssertMetadataIsStripped(docToken as JObject, false);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldStripQueryToolMetadataToEssentialFields(Options options, GenAiConfiguration config)
        {
            var store = GetDocumentStore(options);
            var conversation = await SetupTestAgentAsync(store, config, "from Products");
            using (var session = store.OpenAsyncSession())
            {
                var product = new Product { Name = "Product/1" };
                await session.StoreAsync(product, "products/1-a");
                await session.SaveChangesAsync();

                var metadata = session.Advanced.GetMetadataFor(product);
                metadata["My-Custom-field"] = "Some value";
                session.TimeSeriesFor(product, "StockPrices").Append(DateTime.UtcNow, 100);

                await session.SaveChangesAsync();
            }

            await conversation.RunAsync<object>();

            var result = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
            {
                ConversationId = conversation.Id,
                DetailLevel = AiConversationDetailLevel.Detailed,
                PageSize = 50
            });
            Assert.NotNull(result);

            var toolCallMsg = result.Messages.FindLast(m => m.ToolCalls is { Count: > 0 });
            Assert.NotNull(toolCallMsg);
            var toolContent = toolCallMsg.ToolCalls[^1].Result;

            Assert.False(string.IsNullOrEmpty(toolContent), "content of tool msg should not be null");

            var documentsArray = JArray.Parse(toolContent);
            Assert.True(documentsArray.Count > 0, "The tool should have returned documents.");

            foreach (var docToken in documentsArray)
            {
                AssertMetadataIsStripped(docToken as JObject, true);
            }
        }

        private async Task<IAiConversationOperations> SetupTestAgentAsync(IDocumentStore store, GenAiConfiguration config, string query)
        {
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("metadata-stripping-tester", config.ConnectionStringName,
                "You are a test agent. Your only purpose is to run the 'get_all_products' tool, no matter what the user says.")
            {
                Queries =
                [
                    new AiAgentToolQuery { Name = "get_all_products", Query = query , Description = "A tool that gets all products", ParametersSampleObject = "{}"}
                ],
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);
            conversation.SetUserPrompt("Please run the tool.");
            return conversation;
        }

        public static void AssertMetadataIsStripped(JObject doc, bool withCustomField)
        {
            var metadata = doc["@metadata"] as JObject;

            Assert.NotNull(metadata);

            Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Id));
            Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.LastModified));
            if(withCustomField)
                Assert.True(metadata.ContainsKey("My-Custom-field"));

            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.ChangeVector));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.IndexScore));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Counters));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.TimeSeries));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Flags));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Projection));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.RavenClrType));
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Collection));

            var numOfProperties = withCustomField ? 3 : 2;

            Assert.Equal(numOfProperties, metadata.Count);
        }

        private class Product
        {
            public string Name { get; set; }
        }
    }
}
