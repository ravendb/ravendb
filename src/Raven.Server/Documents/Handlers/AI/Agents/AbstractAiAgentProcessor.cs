using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal abstract class AbstractAiAgentProcessor : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        protected AbstractAiAgentProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            var conversationId = RequestHandler.GetStringQueryString("conversationId");
            var agentId = RequestHandler.GetStringQueryString("agentId");
            var streaming = RequestHandler.GetBoolValueQueryString("streaming", required: false) ?? false;
            var changeVector = RequestHandler.GetChangeVectorStringQueryString("changeVector", required: false);
            var maxModelIterationsPerCall = RequestHandler.GetIntValueQueryString("maxModelIterationsPerCall", required: false);

            AiAgentConfiguration configuration = GetAiAgentConfiguration(agentId);

            if (configuration.Disabled)
                throw new InvalidOperationException($"The AI Agent '{configuration.Identifier}' is currently disabled. Please enable the agent before starting\\continuing a conversation.");

            using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var body = await ReadRequestBodyAsync(context, token.Token);
            var handler = new ConversationHandler(RequestHandler.ServerStore, RequestHandler.Database)
            {
                Authentication = RequestHandler.HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection
            };

            await ExecuteInternalAsync(handler, context, configuration, conversationId, body, changeVector, streaming, maxModelIterationsPerCall, token);
        }

        protected async Task ExecuteInternalAsync(ConversationHandler handler, DocumentsOperationContext context, AiAgentConfiguration configuration, string conversationId, RequestBody body, string changeVector,
            bool streaming, int? maxModelIterationsPerCall, OperationCancelToken token)
        {
            handler.Initialize(configuration, conversationId, body, changeVector, RequestHandler.GetRaftRequestIdFromQuery(), maxModelIterationsPerCall);
            (BlittableJsonReaderObject Response, AiUsage Usage, int ToolsIterations) r;

            if (streaming)
            {
                var streamPropertyPath = RequestHandler.GetStringQueryString("streamPropertyPath");
                HttpContext.Response.Headers.ContentType = "text/event-stream";
                RequestHandler.DisableResponseBuffering();

                r = await handler.HandleStreamingRequest(context, RequestHandler.ResponseBodyStream(), streamPropertyPath, token.Token);
            }
            else
            {
                try
                {
                    r = await handler.HandleRequest(context, token.Token);
                }
                catch (ConcurrencyException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new AiException($"Failed to 'talk' with the agent '{configuration.Identifier}', conversation: '{conversationId}'.", e)
                    {
                        RequestId = RequestHandler.HttpContext.Response.Headers.RequestId
                    };
                }
            }

            await using var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream());
            var finalPayload = handler.GetConversationResponse(context, r.Response, r.ToolsIterations);
            context.Write(writer, finalPayload);
        }


        public AiAgentConfiguration GetAiAgentConfiguration(string identifier)
        {
            using (RequestHandler.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var record = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
            {
                if (record.TryGetAiAgent(identifier, out var configuration) == false)
                    throw new ArgumentException($"AI Agent '{identifier}' doesn't exists");

                return configuration;
            }
        }

        public async Task<RequestBody> ReadRequestBodyAsync(JsonOperationContext context, CancellationToken token)
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            body.TryGet(nameof(ConversionRequestBody.ActionResponses), out BlittableJsonReaderArray actionResponses);
            body.TryGet(nameof(ConversionRequestBody.ArtificialActions), out BlittableJsonReaderArray artificialActions);
            body.TryGet(nameof(ConversionRequestBody.UserPrompt), out object userPrompt);
            body.TryGet(nameof(ConversionRequestBody.CreationOptions), out BlittableJsonReaderObject optionsBlittable);

            optionsBlittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);
            optionsBlittable.TryGet(nameof(AiConversationCreationOptions.ExpirationInSec), out int? conversationExpirationInSec);

            var options = new AiConversationCreationOptions
            {
                ExpirationInSec = conversationExpirationInSec
            };

            return new RequestBody
            {
                ActionResponses = actionResponses,
                ArtificialActions = artificialActions,
                UserPrompt = userPrompt,
                Parameters = parameters,
                CreationOptions = options
            };
        }
    }
}
