using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
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

        var snapshotToken = RequestHandler.GetStringQueryString("snapshotToken", required: false);
        var conversationId = RequestHandler.GetStringQueryString("conversationId", required: snapshotToken == null);
        var pageSize = RequestHandler.GetPageSize();
        var detailLevel = RequestHandler.GetEnumQueryString<AiConversationDetailLevel>("detailLevel", required: false);
        var before = RequestHandler.GetDateTimeQueryString("before", required: false);
        var after = RequestHandler.GetDateTimeQueryString("after", required: false);

        if (before.HasValue && after.HasValue)
            throw new Raven.Client.Exceptions.BadRequestException("Cannot specify both 'before' and 'after' parameters.");

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            Document document;
            if (snapshotToken == null)
            {
                document = RequestHandler.Database.DocumentsStorage.Get(context, conversationId);
            }
            else
            {
                var snapshot = SnapshotToken.Parse(snapshotToken);
                conversationId ??= snapshot.ConversationId.ToString();
                document = GetSnapshotRevision(context, snapshot, conversationId);
            }

            if (document == null)
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)System.Net.HttpStatusCode.NotFound;
                return;
            }

            // Intentionally not catching exceptions here — if the document is not a valid conversation
            // (wrong format, missing fields, corrupted), we want to surface a clear error to the caller.
            // cloneMessages: false — read-only flow, the read transaction stays open through the response write,
            // so we borrow the source array reference instead of paying for a full-array clone.
            var conversation = ConversationDocument.ToDocument(conversationId, document.Data, maxModelIterationsPerCall: 0, cloneMessages: false);

            var collector = new Collector(context, RequestHandler.Database.DocumentsStorage, conversation, pageSize, detailLevel, before, after);
            collector.Collect();
            var messages = collector.GetResults();

            var result = new AiConversationMessagesResult
            {
                ConversationId = conversationId,
                Agent = conversation.Agent,
                Parameters = GetParameters(conversation),
                TotalUsage = conversation.TotalUsage,
                LastMessageAt = conversation.LastMessageAt,
                HasMoreMessages = collector.HasMoreMessages,
                SubConversationIds = conversation.SubConversationIds.ToList(),
                Messages = messages,
                Attachments = snapshotToken == null
                    ? ConversationHandlerAttachments.GetConversationPersistedAttachmentsNames(RequestHandler.Database, context, conversationId)
                    : GetAttachmentNames(document)
            };

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }

    /// <summary>The revision of <paramref name="conversationId"/> (the root or a sub-conversation) in the snapshot, or null</summary>
    private Document GetSnapshotRevision(DocumentsOperationContext context, SnapshotToken snapshot, string conversationId)
    {
        for (var i = 0; i < snapshot.RevisionCount; i++)
        {
            var revision = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionByTokenKey(context, snapshot.GetRevisionKey(i));
            if (revision != null && string.Equals(revision.Id, conversationId, StringComparison.OrdinalIgnoreCase))
                return revision;

            revision?.Dispose();
        }

        return null;
    }

    // a revision keeps the attachment names it had in its metadata
    private static List<string> GetAttachmentNames(Document revision)
    {
        var names = new List<string>();
        if (revision.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
            metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
        {
            foreach (BlittableJsonReaderObject attachment in attachments)
            {
                if (attachment.TryGet(nameof(AttachmentName.Name), out string name))
                    names.Add(name);
            }
        }

        return names;
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
