using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24789 : RavenTestBase
    {
        public RavenDB_24789(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = true, NightlyBuildRequired = false)]
        public async Task ShouldRaiseServerAlertOnExceededActionToolResponse(Options options, GenAiConfiguration config)
        {
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "50";

            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("alert-tester", config.ConnectionStringName, "You are a test agent. Your only purpose is to call the 'get_long_text' tool, no matter what the user says.")
            {
                Actions =
                [
                    new AiAgentToolAction { Name = "get_long_text", Description = "A tool that returns a long string.", ParametersSampleObject = "{}" }
                ],
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);
            conversation.SetUserPrompt("Please run the tool.");

            var longResponse = new StringBuilder(150);
            for (var i = 0; i < 15; i++)
            {
                longResponse.Append("1234567890");
            }

            var result = await conversation.RunAsync<object>();
            Assert.Equal(AiConversationResult.ActionRequired, result.Status);

            foreach (var action in conversation.RequiredActions())
            {
                conversation.AddActionResponse(action.ToolId, longResponse.ToString());
            }

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.True(ValidateDatabaseAlert(db, expectActionTool: true, expectQueryTool: false));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = true, NightlyBuildRequired = false)]
        public async Task ShouldRaiseServerAlertOnExceededQueryToolResponse(Options options, GenAiConfiguration config)
        {
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "100";

            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 3; i++)
                {
                    await session.StoreAsync(new Product { Name = "Product/" + i});
                }

                await session.SaveChangesAsync();
            }

            var agentConfig = new AiAgentConfiguration("alert-tester", config.ConnectionStringName,
                "You are a test agent. Your only purpose is to run the 'get_all_products' tool, no matter what the user says.")
            {
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "get_all_products", Query = "from Products", Description = "A tool that gets all products", ParametersSampleObject = "{}"
                    }
                ],
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);
            conversation.SetUserPrompt("Please run the tool.");

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.True(ValidateDatabaseAlert(db, expectActionTool: false, expectQueryTool: true));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = true, NightlyBuildRequired = false)]
        public async Task ShouldRaiseServerAlertWhenBothToolTypesAreCalled(Options options, GenAiConfiguration config)
        {
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "100";

            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {

                await session.StoreAsync(new Product { Name = "Product" });
                await session.StoreAsync(new Order { Company = "company" });

                await session.SaveChangesAsync();
            }

            var agentConfig = new AiAgentConfiguration("alert-tester", config.ConnectionStringName, "You are a test agent. Your only purpose is to call all four tools I gave you.")
            {
                Actions =
                [
                    new AiAgentToolAction { Name = "get_long_text_1", Description = "A tool that returns a long string.", ParametersSampleObject = "{}" },
                    new AiAgentToolAction { Name = "get_long_text_2", Description = "Another tool that returns a long string.", ParametersSampleObject = "{}" }
                ],
                Queries =
                [
                    new AiAgentToolQuery { Name = "get_all_products", Query = "from Products", Description = "A tool that gets all products.", ParametersSampleObject = "{}" },
                    new AiAgentToolQuery { Name = "get_all_orders", Query = "from Orders", Description = "A tool that gets all orders.", ParametersSampleObject = "{}" }
                ],
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);

            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);
            conversation.SetUserPrompt("Please run all the tools.");

            var result = await conversation.RunAsync<object>();
            Assert.Equal(AiConversationResult.ActionRequired, result.Status);

            var longActionResponse = "a";

            foreach (var action in conversation.RequiredActions())
            {
                conversation.AddActionResponse(action.ToolId, longActionResponse);
            }

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.True(ValidateDatabaseAlert(db, expectActionTool: true, expectQueryTool: true));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = true, NightlyBuildRequired = false)]
        public async Task ShouldNotRaiseAlertWhenUserMessageExceedsTokenLimitAfterToolCall(Options options, GenAiConfiguration config)
        {
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "100";

            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("alert-tester", config.ConnectionStringName, "You are a test agent your only purpose is to run the tool I gave you.")
            {
                Actions = [new AiAgentToolAction { Name = "simple_tool", Description = "A simple tool.", ParametersSampleObject = "{}" }],
                SampleObject = JsonConvert.SerializeObject(new { answer = "string" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);
            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);

            conversation.SetUserPrompt("Please run the simple_tool.");
            var result = await conversation.RunAsync<object>();
            Assert.Equal(AiConversationResult.ActionRequired, result.Status);

            foreach (var action in conversation.RequiredActions())
            {
                conversation.AddActionResponse(action.ToolId, "This is a small response.");
            }
            await conversation.RunAsync<object>();

            var longUserMessage = new StringBuilder();
            for (var i = 0; i < 5; i++)
            {
                longUserMessage.Append("This is a very long user message designed to exceed the token limit. ");
            }
            conversation.SetUserPrompt(longUserMessage.ToString());

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (db.NotificationCenter.GetStored(out var actions))
            {
                Assert.Empty(actions);
            }
        }

        private static bool ValidateDatabaseAlert(DocumentDatabase db, bool expectActionTool, bool expectQueryTool)
        {
            using (db.NotificationCenter.GetStored(out var actions))
            {
                var alert = actions.FirstOrDefault();
                if (alert == null)
                    return false;

                if (alert.Json.TryGet("Details", out BlittableJsonReaderObject details) == false)
                    return false;

                if (details.TryGet("TokenCount", out int tokenCount) == false || details.TryGet("TokenThreshold", out int tokenThreshold) == false || tokenCount <= tokenThreshold)
                    return false;

                if (details.TryGet("ToolCalls", out BlittableJsonReaderArray toolCalls) == false || toolCalls.Length == 0)
                    return false;

                var actualToolTypes = new HashSet<string>();
                foreach (BlittableJsonReaderObject toolCall in toolCalls)
                {
                    if (toolCall.TryGet("Type", out string toolTypeString))
                    {
                        actualToolTypes.Add(toolTypeString);
                    }
                }

                var hasActionTool = actualToolTypes.Contains(ToolType.Action.ToString());
                var hasQueryTool = actualToolTypes.Contains(ToolType.Query.ToString());

                return hasActionTool == expectActionTool && hasQueryTool == expectQueryTool;
            }
        }

        private class Order
        {
            public string Company { get; set; }
        }

        private class Product
        {
            public string Name { get; set; }
            public string Description { get; set; }
        }
    }
}
