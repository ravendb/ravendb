using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
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
            var debugOverride = RequestHandler.GetBoolValueQueryString("debug", required: false);

            AiAgentConfiguration configuration = GetAiAgentConfiguration(agentId);

            if (configuration.Disabled)
                throw new InvalidOperationException($"The AI Agent '{configuration.Identifier}' is currently disabled. Please enable the agent before starting\\continuing a conversation.");

            using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var body = await ReadRequestBodyAsync(context, conversationId, token.Token);
            var handler = new ConversationHandler(RequestHandler.ServerStore, RequestHandler.Database)
            {
                Authentication = RequestHandler.HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection
            };

            await ExecuteInternalAsync(handler, context, configuration, conversationId, body, changeVector, streaming, debugOverride, token);
        }

        protected async Task ExecuteInternalAsync(ConversationHandler handler, DocumentsOperationContext context, AiAgentConfiguration configuration, string conversationId, RequestBody body, string changeVector,
            bool streaming, bool? debugOverride, OperationCancelToken token)
        {
            handler.Initialize(configuration, conversationId, body, changeVector, RequestHandler.GetRaftRequestIdFromQuery(), debugOverride);
            AiInternalConversationResult r;

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
                catch (MissingAiAgentParameterException)
                {
                    throw;
                }
                catch (AttachmentDoesNotExistException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new AiException($"Failed to communicate with the agent '{configuration.Identifier}', conversation: '{conversationId}'.", e)
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

        public RequestBody ReadJsonBody(BlittableJsonReaderObject body)
        {
            body.TryGet(nameof(ConversionRequestBody.ActionResponses), out BlittableJsonReaderArray actionResponses);
            body.TryGet(nameof(ConversionRequestBody.ArtificialActions), out BlittableJsonReaderArray artificialActions);
            body.TryGet(nameof(ConversionRequestBody.UserPrompt), out object userPrompt);
            body.TryGet(nameof(ConversionRequestBody.CreationOptions), out BlittableJsonReaderObject optionsBlittable);

            optionsBlittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);
            optionsBlittable.TryGet(nameof(AiConversationCreationOptions.ExpirationInSec), out int? conversationExpirationInSec);
            optionsBlittable.TryGet(nameof(AiConversationCreationOptions.MaxModelIterationsPerCall), out int? maxModelIterationsPerCall);

            var options = new AiConversationCreationOptions
            {
                ExpirationInSec = conversationExpirationInSec,
                MaxModelIterationsPerCall = maxModelIterationsPerCall
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

        public async Task<RequestBody> ReadRequestBodyAsync(DocumentsOperationContext context, string destinationDocumentId, CancellationToken token)
        {

            var contentType = HttpContext.Request.ContentType;
            if (contentType != null &&
                (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase)))
            {
                using (var commandsReader = new DatabaseBatchCommandsReader(RequestHandler, RequestHandler.Database))
                {
                    return await ReadMultipartBodyAsync(context, contentType, commandsReader, token);
                }
            }

            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            return ReadJsonBody(body);
        }

        private async Task<RequestBody> ReadMultipartBodyAsync(
            DocumentsOperationContext context,
            string contentType,
            DatabaseBatchCommandsReader commandsReader,
            CancellationToken token)
        {
            var requestBody = await ParseMultipartAsync(commandsReader, context, RequestHandler.RequestBodyStream(), contentType, RequestHandler.IdentityPartsSeparator, token);

            requestBody.AttachmentCommands = await commandsReader.GetCommandAsync(null);

            return requestBody;
        }

        public async Task<RequestBody> ParseMultipartAsync(DatabaseBatchCommandsReader commandsReader, JsonOperationContext context, Stream stream, string contentType, char separator, CancellationToken token)
        {
            var boundary = MultipartRequestHelper.GetBoundary(
                MediaTypeHeaderValue.Parse(contentType),
                MultipartRequestHelper.MultipartBoundaryLengthLimit);
            var reader = new MultipartReader(boundary, stream);
            RequestBody result = null;
            for (var i = 0;; i++)
            {
                var section = await reader.ReadNextSectionAsync(token).ConfigureAwait(false);
                if (section == null)
                    break;

                var bodyStream = RequestHandler.GetBodyStream(section);
                if (i == 0)
                {
                    var body = await context.ReadForMemoryAsync(bodyStream, "ai-agent", token);
                    result = ReadJsonBody(body);
                    continue;
                }

                if (i == 1)
                {
                    await commandsReader.BuildCommandsAsync(context, bodyStream, separator, token);
                    continue;
                }

                await commandsReader.SaveStreamAsync(context, bodyStream, token);
            }

            return result;
        }
    }
}
