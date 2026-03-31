using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24789 : RavenTestBase
    {
        public RavenDB_24789(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
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

            conversation.Handle<object>("get_long_text", args =>
            {
                var longResponse = new StringBuilder(250);
                for (var i = 0; i < 25; i++)
                {
                    longResponse.Append("1234567890");
                }
                return longResponse.ToString();
            });
            conversation.SetUserPrompt("Please run the tool.");

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.True(ValidateDatabaseAlert(db, expectActionTool: true, expectQueryTool: false));
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ShouldRaiseServerAlertOnExceededQueryToolResponse()
        {
            var options = new Options();
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "50";

            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 3; i++)
                {
                    await session.StoreAsync(new Product { Name = "Product/" + i});
                }

                await session.SaveChangesAsync();
            }

            var agentConfig = new AiAgentConfiguration("alert-tester", "fake-connection",
                "You are a test agent.")
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

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                // First request: call the query tool. On tool result: respond with high token count to trigger the alert.
                bool toolCalled = false;
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ =>
                    {
                        if (toolCalled)
                            return null; // fall through to default
                        toolCalled = true;
                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(MockLlm.CreateToolCallResponse("get_all_products"))
                        };
                    },
                    onToolResult: (_, _) => new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent(MockLlm.CreateAnswerResponse("\"done\"", promptTokens: 200))
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agentConfig, "chats/alert-query", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "Please run the tool."
                }, changeVector: null);

                await handler.HandleRequest(context, CancellationToken.None);

                Assert.True(ValidateDatabaseAlert(database, expectActionTool: false, expectQueryTool: true));
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
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

            foreach (var action in agentConfig.Actions)
            {
                conversation.Handle<object>(action.Name, _ => "a");
            }

            conversation.SetUserPrompt("Please run all the tools.");

            await conversation.RunAsync<object>();

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.True(ValidateDatabaseAlert(db, expectActionTool: true, expectQueryTool: true));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldNotRaiseAlertWhenUserMessageExceedsTokenLimitAfterToolCall(Options options, GenAiConfiguration config)
        {
            options.ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold)] = "100";

            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agentConfig = new AiAgentConfiguration("payment-agent", config.ConnectionStringName,
                "You are payment assistant that knows how to charge a customer (make payment).  if you got \"status: succeeded\" do not charge the customer again!")
            {
                Actions = [new AiAgentToolAction { Name = "ChargeCustomer", Description = "Charge a customer.", ParametersSampleObject = "{}" }],
                SampleObject = JsonConvert.SerializeObject(new { answer = "Your answer" })
            };
            var agent = await store.AI.CreateAgentAsync(agentConfig);
            var conversation = store.AI.Conversation(agent.Identifier, "chats/", creationOptions: null);

            var actionToolsCalled = false;
            var secondPhase = false;

            conversation.Handle<object>("ChargeCustomer", _ =>
            {
                if (secondPhase)
                    actionToolsCalled = true;
                return new
                {
                    status = "succeeded",
                    payment_id = "pay_123",
                    amount = 100,
                    currency = "ILS",
                    idempotency_key = "customer has been charged!"
                };
            });


            conversation.SetUserPrompt("Make a payment");

            await conversation.RunAsync<object>();

            var longUserMessage = new StringBuilder();

            for (var i = 0; i < 20; i++)
            {
                longUserMessage.Append("                                                                                                       ");
            }

            longUserMessage.Append("Who are you?");
            conversation.SetUserPrompt(longUserMessage.ToString());

            var notificationsCount = 0;
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (db.NotificationCenter.GetStored(out var actions))
            {
                notificationsCount = actions.ToList().Count;
            }

            secondPhase = true;
            var r = await conversation.RunAsync<object>();

            if (actionToolsCalled == false)
            {
                using (db.NotificationCenter.GetStored(out var actions))
                {
                    Assert.Equal(notificationsCount, actions.ToList().Count);
                }
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

                if (details.TryGet("ConversationId", out string conversationId) == false ||
                    string.IsNullOrWhiteSpace(conversationId))
                    return false;

                char sep = db.IdentityPartsSeparator;

                if (conversationId[^1] == sep || conversationId[^1] == '|')
                    return false;

                int lastSep = conversationId.LastIndexOf(sep);
                if (lastSep >= 0)
                {
                    var suffix = conversationId.AsSpan(lastSep + 1);
                    if (suffix.Length == 0 || !suffix.Contains('-'))
                        return false;
                }

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
        }
    }
}
