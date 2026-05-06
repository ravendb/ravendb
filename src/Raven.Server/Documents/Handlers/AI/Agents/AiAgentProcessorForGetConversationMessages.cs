using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal sealed partial class AiAgentProcessorForGetConversationMessages : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiAgentProcessorForGetConversationMessages([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();

        var conversationId = RequestHandler.GetStringQueryString("conversationId");
        var pageSize = RequestHandler.GetPageSize(25);
        var detailLevel = RequestHandler.GetEnumQueryString<AiConversationDetailLevel>("detailLevel", required: false);
        var before = RequestHandler.GetDateTimeQueryString("before", required: false);
        var after = RequestHandler.GetDateTimeQueryString("after", required: false);

        if (before.HasValue && after.HasValue)
            throw new Raven.Client.Exceptions.BadRequestException("Cannot specify both 'before' and 'after' parameters.");

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.Get(context, conversationId);
            if (document == null)
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)System.Net.HttpStatusCode.NotFound;
                return;
            }

            // Intentionally not catching exceptions here — if the document is not a valid conversation
            // (wrong format, missing fields, corrupted), we want to surface a clear error to the caller.
            var conversation = ConversationDocument.ToDocument(conversationId, document.Data, maxModelIterationsPerCall: 0);

            var collector = new Collector(context, RequestHandler.Database.DocumentsStorage, conversation, pageSize, detailLevel, before, after);
            collector.Collect();

            var result = new AiConversationMessagesResult
            {
                ConversationId = conversationId,
                Agent = conversation.Agent,
                Parameters = GetParameters(conversation),
                TotalUsage = conversation.TotalUsage,
                LastMessageAt = conversation.LastMessageAt,
                HasMoreMessages = collector.HasMoreMessages,
                AttachmentNames = collector.AttachmentNames.ToArray(),
                SubConversationIds = conversation.SubConversationIds.ToList(),
                Messages = collector.GetResults()
            };

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    private static Dictionary<string, object> GetParameters(ConversationDocument conversation)
    {
        var parameters = new Dictionary<string, object>();
        if (conversation.Parameters == null)
            return parameters;

        var keys = conversation.Parameters.GetPropertyNames();
        foreach (var paramName in keys)
        {
            conversation.Parameters.TryGetMember(paramName, out object value);
            parameters[paramName] = ConversationHandler.GetAiConversationParameter(paramName, value).Value;
        }

        return parameters;
    }
}
