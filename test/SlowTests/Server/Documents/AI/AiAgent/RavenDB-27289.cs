using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_27289(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string OwnHandle = "abc123";
    private const string OtherHandle = "victim99";
    private const string Secret = "this is the secret ticket";
    private const string OwnSubject = "this is my own ticket";

    private class SupportTicket
    {
        public string TelegramHandle { get; set; }
        public string Subject { get; set; }
    }

    private static AiAgentConfiguration CreateAgent(bool sendToModel, string parametersSchema = null, string sampleObject = "{}")
    {
        var agent = new AiAgentConfiguration("support assistant", "fake-connection",
            "You help users with their own support tickets.");

        agent.Parameters.Add(new AiAgentParameter("TelegramUsername", "the handle of the sender", sendToModel));
        agent.Queries =
        [
            new AiAgentToolQuery("MyTickets", "Get the tickets of the current user",
                "from SupportTickets where TelegramHandle = $TelegramUsername limit 5")
            {
                ParametersSampleObject = parametersSchema == null ? sampleObject : null,
                ParametersSchema = parametersSchema
            }
        ];
        agent.SampleObject = "{\"Answer\":\"The answer to the query\"}";
        return agent;
    }

    private static async Task SeedAsync(IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new SupportTicket { TelegramHandle = OwnHandle, Subject = OwnSubject });
        await session.StoreAsync(new SupportTicket { TelegramHandle = OtherHandle, Subject = Secret });
        await session.SaveChangesAsync();
    }

    private static MockLlmConversationHandler HandlerCallingToolOnce(
        Raven.Server.ServerWide.ServerStore serverStore,
        DocumentDatabase database,
        string toolArguments,
        Action<JObject> inspectPayload = null)
    {
        var toolCalled = false;
        return new MockLlmConversationHandler(serverStore, database,
            onRequest: payload =>
            {
                inspectPayload?.Invoke(payload);
                if (toolCalled)
                    return null;
                toolCalled = true;
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(MockLlm.CreateToolCallResponse("MyTickets", toolArguments))
                };
            })
        {
            Authentication = null
        };
    }

    private static MockLlmConversationHandler HandlerThatOnlyAnswers(
        Raven.Server.ServerWide.ServerStore serverStore,
        DocumentDatabase database)
    {
        return new MockLlmConversationHandler(serverStore, database,
            onRequest: _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(MockLlm.CreateAnswerResponse("\"nothing to do\""))
            })
        {
            Authentication = null
        };
    }

    private static BlittableJsonReaderObject SuppliedParameters(JsonOperationContext context, string handle)
    {
        var creation = new AiConversationCreationOptions().AddParameter("TelegramUsername", handle);
        var blittable = context.ReadObject(creation.ToJson(), "conversation-params");
        blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);
        return parameters;
    }

    private static RequestBody Request(BlittableJsonReaderObject parameters) => new()
    {
        Parameters = parameters,
        CreationOptions = new AiConversationCreationOptions(),
        UserPrompt = "show my tickets"
    };

    // ---- the defect -------------------------------------------------------

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SendToModelFalseParameterIsEnforcedAsRequired()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        var agent = CreateAgent(sendToModel: false);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = HandlerThatOnlyAnswers(Server.ServerStore, database);
            handler.Initialize(agent, "Dummy", Request(parameters: null), changeVector: null);

            await Assert.ThrowsAsync<MissingAiAgentParameterException>(
                () => handler.HandleRequestAsync(context, CancellationToken.None));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ConversationIsRefusedBeforeTheModelCanSupplyTheParameter()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        var agent = CreateAgent(sendToModel: false);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            // the mock stands ready to hand the tool another user's handle, but never gets the chance
            var handler = HandlerCallingToolOnce(Server.ServerStore, database, $"{{\"TelegramUsername\":\"{OtherHandle}\"}}");
            handler.Initialize(agent, "Dummy", Request(parameters: null), changeVector: null);

            var e = await Assert.ThrowsAsync<MissingAiAgentParameterException>(
                () => handler.HandleRequestAsync(context, CancellationToken.None));

            Assert.DoesNotContain(Secret, e.ToString());
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolSchemaDeclaringTheScopingParameterAsOptionalIsAcceptedAndAdvertisedToTheModel()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        const string optionalScopingParameter =
            "{\"type\":\"object\",\"properties\":{\"TelegramUsername\":{\"type\":\"string\"}},\"required\":[],\"additionalProperties\":false}";

        var agent = CreateAgent(sendToModel: false, parametersSchema: optionalScopingParameter);

        // the server accepts this configuration
        await store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agent, AiAgentBasics.OutputSchema.Instance));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var advertised = false;
            var handler = HandlerCallingToolOnce(Server.ServerStore, database,
                $"{{\"TelegramUsername\":\"{OtherHandle}\"}}",
                inspectPayload: payload =>
                {
                    if (payload["tools"]?.ToString().Contains("TelegramUsername") == true)
                        advertised = true;
                });

            // the value is supplied here, so the conversation runs and the advertised name can be observed
            handler.Initialize(agent, "Dummy", Request(SuppliedParameters(context, OwnHandle)), changeVector: null);

            var r = await handler.HandleRequestAsync(context, CancellationToken.None);

            Assert.True(advertised, "the server should have advertised the scoping parameter name to the model");
            Assert.DoesNotContain(Secret, r.Response.ToString());
        }
    }

    // ---- controls and bounds ---------------------------------------------

    [RavenFact(RavenTestCategory.Ai)]
    public async Task RequiredParameterCheckAppliesWhenSendToModelIsTrue()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        var agent = CreateAgent(sendToModel: true);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = HandlerThatOnlyAnswers(Server.ServerStore, database);
            handler.Initialize(agent, "Dummy", Request(parameters: null), changeVector: null);

            await Assert.ThrowsAsync<MissingAiAgentParameterException>(
                () => handler.HandleRequestAsync(context, CancellationToken.None));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SuppliedSendToModelFalseParameterOverridesTheModelValue()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        var agent = CreateAgent(sendToModel: false);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = HandlerCallingToolOnce(Server.ServerStore, database, $"{{\"TelegramUsername\":\"{OtherHandle}\"}}");
            handler.Initialize(agent, "Dummy", Request(SuppliedParameters(context, OwnHandle)), changeVector: null);

            var r = await handler.HandleRequestAsync(context, CancellationToken.None);
            var response = r.Response.ToString();

            Assert.Contains(OwnSubject, response);
            Assert.DoesNotContain(Secret, response);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolSampleObjectDeclaringTheScopingParameterIsRejected()
    {
        using var store = GetDocumentStore();

        var agent = CreateAgent(sendToModel: false, sampleObject: "{\"TelegramUsername\":\"abc\"}");

        await Assert.ThrowsAsync<RavenException>(
            () => store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agent, AiAgentBasics.OutputSchema.Instance)));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ParameterAddedToTheAgentIsEnforcedOnAnAlreadyRunningConversation()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        const string conversationId = "chats/running";
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            // the conversation is opened while the agent declares one parameter, and supplies it
            var handler = HandlerThatOnlyAnswers(Server.ServerStore, database);
            handler.Initialize(CreateAgent(sendToModel: false), conversationId,
                Request(SuppliedParameters(context, OwnHandle)), changeVector: null);

            await handler.HandleRequestAsync(context, CancellationToken.None);

            // the agent then gains a second parameter, which the running conversation never supplied
            var extendedAgent = CreateAgent(sendToModel: false);
            extendedAgent.Parameters.Add(new AiAgentParameter("Department", "the department of the sender", sendToModel: false));

            var next = HandlerThatOnlyAnswers(Server.ServerStore, database);
            next.Initialize(extendedAgent, conversationId, Request(parameters: null), changeVector: null);

            var e = await Assert.ThrowsAsync<MissingAiAgentParameterException>(
                () => next.HandleRequestAsync(context, CancellationToken.None));

            Assert.Contains("Department", e.Message);
        }
    }
}
