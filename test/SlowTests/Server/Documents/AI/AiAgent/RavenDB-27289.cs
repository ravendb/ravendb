using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

    private static BlittableJsonReaderObject SuppliedObjectParameter(JsonOperationContext context)
    {
        var creation = new AiConversationCreationOptions()
            .AddParameter("TelegramUsername", new DynamicJsonValue { ["Handle"] = OwnHandle });

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
    public async Task ConversationDocumentStillRefusesToBuildWithoutADeclaredValue()
    {
        // InitializeDocumentAsync rejects this earlier, but the summarization rebuild reaches Initialize directly,
        // and a Debug.Assert here would compile away, leaving a release server to carry on with the parameter
        // missing. Note the bound of this throw: the loop skips SendToModel == false before reaching it, so a
        // hidden parameter - the kind this ticket is about - never trips it, which is why the agent here declares
        // a visible one. The test-conversation endpoint does not reach Initialize at all; it builds the document
        // with ToDocument, and is covered by
        // TestEndpoint_ParameterAddedToTheAgentIsEnforcedOnASuppliedConversationDocument.
        var agent = CreateAgent(sendToModel: true);

        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var conversation = new ConversationDocument(agent.Identifier, parameters: null);

            Assert.Throws<MissingAiAgentParameterException>(
                () => conversation.Initialize(context, agent, resetRemainingToolIterations: true, maxModelIterationsPerCall: 4));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TheRefusalReachesTheClientAsItsOwnType()
    {
        // Over the wire the type survives only if the client can resolve it: ExceptionDispatcher calls
        // Type.GetType on the bare FullName the server sent, and anything that is not a RavenException comes
        // back wrapped as an inner exception instead. The other tests here catch the throw in-process.
        using var store = GetDocumentStore();
        await SeedAsync(store);

        // the agent needs a connection string to be created; the refusal happens in InitializeDocumentAsync,
        // before the provider is ever contacted, so nothing has to answer at that address
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "fake-connection",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("fake-key", "http://localhost:1/", "fake-model")
        }));

        var agentId = (await store.AI.CreateAgentAsync(CreateAgent(sendToModel: false), AiAgentBasics.OutputSchema.Instance)).Identifier;

        var chat = store.AI.Conversation(agentId, "chats/",
            new AiConversationCreationOptions().AddParameter("TelegramUsername", new DynamicJsonValue { ["Handle"] = OwnHandle }));

        chat.SetUserPrompt("show my tickets");

        var e = await Assert.ThrowsAsync<UnsupportedAiQueryParameterException>(
            () => chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None));

        Assert.Contains("$TelegramUsername", e.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolSchemaDeclaringTheScopingParameterAsOptionalIsRejected()
    {
        using var store = GetDocumentStore();

        // the parameter sits in 'properties' with an empty 'required', which the guard used to let through -
        // the server would then advertise the scoping parameter's name to the model itself
        const string optionalScopingParameter =
            "{\"type\":\"object\",\"properties\":{\"TelegramUsername\":{\"type\":\"string\"}},\"required\":[],\"additionalProperties\":false}";

        var agent = CreateAgent(sendToModel: false, parametersSchema: optionalScopingParameter);

        var e = await Assert.ThrowsAsync<AiAgentParameterCollisionException>(
            () => store.Maintenance.SendAsync(AddOrUpdateAiAgentOperation.Create(agent, AiAgentBasics.OutputSchema.Instance)));

        Assert.Contains("TelegramUsername", e.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ToolSchemaDeclaringTheScopingParameterAsOptionalStillApplies()
    {
        // The rule above must not reach ValidateConfiguration, which AddOrUpdateAiAgentCommand.Initialize runs
        // while applying an already committed Raft entry. Were it enforced there, a node on this build would fail
        // to apply an entry that a node on an older build applied, and the two database records would diverge.
        const string optionalScopingParameter =
            "{\"type\":\"object\",\"properties\":{\"TelegramUsername\":{\"type\":\"string\"}},\"required\":[],\"additionalProperties\":false}";

        var agent = CreateAgent(sendToModel: false, parametersSchema: optionalScopingParameter);

        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            AiAgentHelpers.AddDefaultValues(agent, database.Configuration.Ai);

            AddOrUpdateAiAgentCommand.ValidateConfiguration(context, agent);

            Assert.Throws<AiAgentParameterCollisionException>(
                () => AiAgentHelpers.AssertAgentParametersAreNotToolArguments(context, agent));
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ObjectSuppliedForAParameterAQueryBindsIsRejected()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        // the parameter carries no explicit Type, so the type checks below are skipped and an object used to reach
        // the query binder, which takes it without complaint - the query then matched nothing and the caller was
        // told there were no tickets. A scope that cannot be bound is an error, not an empty result
        var agent = CreateAgent(sendToModel: false);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = HandlerCallingToolOnce(Server.ServerStore, database, "{}");
            handler.Initialize(agent, "Dummy", Request(SuppliedObjectParameter(context)), changeVector: null);

            var e = await Assert.ThrowsAsync<UnsupportedAiQueryParameterException>(
                () => handler.HandleRequestAsync(context, CancellationToken.None));

            Assert.Contains("$TelegramUsername", e.Message);
            Assert.Contains("only takes a scalar value", e.Message);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ObjectSuppliedForAParameterNoQueryBindsIsLeftAlone()
    {
        using var store = GetDocumentStore();
        await SeedAsync(store);

        // a parameter can exist only to be read by the model, and then its value never has to bind to anything
        var agent = CreateAgent(sendToModel: true);
        agent.Queries = [];

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var handler = HandlerThatOnlyAnswers(Server.ServerStore, database);
            handler.Initialize(agent, "Dummy", Request(SuppliedObjectParameter(context)), changeVector: null);

            var r = await handler.HandleRequestAsync(context, CancellationToken.None);

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

        // a schema generated from a sample object lists every property under 'required', so this collision is
        // caught by the older rule in ValidateConfiguration, which still throws a plain InvalidOperationException
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

    [RavenFact(RavenTestCategory.Ai)]
    public async Task TestEndpoint_ParameterAddedToTheAgentIsEnforcedOnASuppliedConversationDocument()
    {
        // The same scenario as ParameterAddedToTheAgentIsEnforcedOnAnAlreadyRunningConversation, but on the
        // /ai/agent/test endpoint, where the caller posts the conversation document back on every turn
        // (AiAgentTestRequest.Documents) and TestConversationHandler builds it with ConversationDocument.ToDocument
        // instead of calling base.InitializeDocumentAsync.
        using var store = GetDocumentStore();
        await SeedAsync(store);

        const string conversationId = "chats/test-endpoint";
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            // turn 1: the agent has no parameters and no tools yet
            var firstAgent = new AiAgentConfiguration("support assistant", "fake-connection",
                "You help users with their own support tickets.")
            {
                SampleObject = "{\"Answer\":\"The answer to the query\"}"
            };

            var first = TestEndpointHandler(database, context, firstAgent, conversationId, documents: null, toolArguments: null);
            var firstResult = await first.HandleRequestAsync(context, CancellationToken.None);

            var firstResponse = context.ReadObject(first.GetConversationResponse(context, firstResult.Response, firstResult.ToolsIterations), "turn-1");
            Assert.True(firstResponse.TryGet(nameof(AiAgentProcessorForTestConversation.AiAgentTestResult.Documents), out BlittableJsonReaderObject returnedDocuments));

            var documents = new Dictionary<string, BlittableJsonReaderObject>();
            foreach (var id in returnedDocuments.GetPropertyNames())
                documents[id] = (BlittableJsonReaderObject)returnedDocuments[id];

            Assert.Single(documents);

            // turn 2: the agent now declares TelegramUsername (SendToModel = false) and a tool scoped by it; the
            // conversation document posted back has no value for it. The model asks for another user's tickets.
            var scopedAgent = CreateAgent(sendToModel: false);
            var next = TestEndpointHandler(database, context, scopedAgent, conversationId, documents,
                toolArguments: $"{{\"TelegramUsername\":\"{OtherHandle}\"}}");

            string response;
            try
            {
                var r = await next.HandleRequestAsync(context, CancellationToken.None);
                response = r.Response.ToString();
            }
            catch (MissingAiAgentParameterException)
            {
                return; // the declared parameter is enforced
            }

            Assert.Fail("The conversation continued without a value for the declared parameter 'TelegramUsername'. " +
                        $"Secret leaked: {response.Contains(Secret)}. Response: {response}");
        }
    }

    private AiAgentProcessorForTestConversation.TestConversationHandler TestEndpointHandler(
        DocumentDatabase database,
        DocumentsOperationContext context,
        AiAgentConfiguration agent,
        string conversationId,
        Dictionary<string, BlittableJsonReaderObject> documents,
        string toolArguments)
    {
        // what AiAgentProcessorForTestConversation.ExecuteAsync builds from the HTTP request
        var request = new AiAgentProcessorForTestConversation.AiAgentTestRequest
        {
            UserPrompt = "show my tickets",
            CreationOptions = context.ReadObject(new DynamicJsonValue(), "creation-options"),
            Configuration = agent,
            Documents = documents
        };

        var handler = new AiAgentProcessorForTestConversation.TestConversationHandler(Server.ServerStore, database, request)
        {
            Authentication = null
        };

        var toolCalled = false;
        handler.SetClient(new MockLlm(database.DocumentsStorage.ContextPool,
            new OpenAiChatCompletionProvider(new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o")),
            onRequest: _ =>
            {
                if (toolArguments == null)
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(MockLlm.CreateAnswerResponse("\"nothing to do\"")) };

                if (toolCalled)
                    return null; // MockLlm echoes the tool result back as the answer
                toolCalled = true;
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(MockLlm.CreateToolCallResponse("MyTickets", toolArguments))
                };
            },
            conventions: Raven.Server.Documents.AI.ChatCompletionClient.ConventionsToUse));

        handler.Initialize(agent, conversationId, request.RequestBody, changeVector: null);
        return handler;
    }
}
