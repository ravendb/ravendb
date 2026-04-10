using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
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
        var pageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? 25;
        var detailLevel = RequestHandler.GetEnumQueryString<AiConversationDetailLevel>("detailLevel", required: false);
        var before = RequestHandler.GetDateTimeQueryString("before", required: false);
        var after = RequestHandler.GetDateTimeQueryString("after", required: false);

        if (pageSize <= 0)
            pageSize = 25;

        if (before.HasValue && after.HasValue)
            throw new ArgumentException("Cannot specify both 'before' and 'after' parameters.");

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.Get(context, conversationId);
            if (document == null)
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)System.Net.HttpStatusCode.NotFound;
                return;
            }

            var conversation = ConversationDocument.ToDocument(conversationId, document.Data, maxModelIterationsPerCall: 0);

            var collector = new Collector(context, RequestHandler.Database.DocumentsStorage, conversation.LinkedConversations, pageSize, detailLevel, before, after);
            collector.Collect(conversation.Messages);

            bool hasMoreMessages = collector.HasMoreMessages;
            var filtered = collector.GetResults();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.ConversationId));
                writer.WriteString(conversationId);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.Agent));
                writer.WriteString(conversation.Agent);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.TotalUsage));
                conversation.TotalUsage.Write(writer);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.LastMessageAt));
                writer.WriteDateTime(conversation.LastMessageAt, isUtc: true);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.HasMoreMessages));
                writer.WriteBool(hasMoreMessages);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AiConversationMessagesResult.Messages));
                WriteMessages(writer, filtered);

                writer.WriteEndObject();
            }
        }
    }

    private static void WriteMessages(AsyncBlittableJsonTextWriter writer, List<AiConversationMessage> messages)
    {
        writer.WriteStartArray();
        bool first = true;
        foreach (AiConversationMessage msg in messages)
        {
            if (first == false)
                writer.WriteComma();
            first = false;

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(AiConversationMessage.Role));
            writer.WriteString(msg.Role.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(AiConversationMessage.Content));
            writer.WriteString(msg.Content);
            writer.WriteComma();

            writer.WritePropertyName(nameof(AiConversationMessage.Attachments));
            writer.WriteStartArray();
            bool firstAtt = true;
            foreach (string att in msg.Attachments ?? [])
            {
                if (firstAtt == false)
                    writer.WriteComma();
                firstAtt = false;
                writer.WriteString(att);
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(AiConversationMessage.Timestamp));
            writer.WriteDateTime(msg.Timestamp, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(AiConversationMessage.ToolCalls));
             writer.WriteStartArray();
            bool firstTc = true;
            foreach (AiToolCallResult tc in msg.ToolCalls ?? [])
            {
                if (firstTc == false)
                    writer.WriteComma();
                firstTc = false;

                writer.WriteStartObject();
                writer.WritePropertyName(nameof(AiToolCallResult.Id));
                writer.WriteString(tc.Id);
                writer.WriteComma();
                writer.WritePropertyName(nameof(AiToolCallResult.Name));
                writer.WriteString(tc.Name);
                writer.WriteComma();
                writer.WritePropertyName(nameof(AiToolCallResult.Arguments));
                writer.WriteString(tc.Arguments);
                writer.WriteComma();
                writer.WritePropertyName(nameof(AiToolCallResult.Result));
                writer.WriteString(tc.Result);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(AiConversationMessage.Usage));
            if (msg.Usage != null)
                msg.Usage.Write(writer);
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
        writer.WriteEndArray();
    }

}
