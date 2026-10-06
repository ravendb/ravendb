using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal static class ConversationHandlerAttachments
{
    public sealed class ConversationAttachmentsNames
    {
        public List<string> All { get; } = new();
        public List<string> HiddenFromModel { get; } = new();
        public List<string> VisibleToModel { get; } = new();
    }

    public static ConversationAttachmentsNames GetConversationAttachmentsNames(DocumentDatabase database, DocumentsOperationContext context, string documentId, HashSet<string> hiddenFromModel)
    {
        var names = new ConversationAttachmentsNames();

        using (Slice.From(context.Allocator, documentId.ToLowerInvariant(), out Slice lowerId))
        {
            var storedAttachmentsDetails = database.DocumentsStorage.AttachmentsStorage.GetAttachmentDetailsForDocument(context, lowerId);
            foreach (var attachment in storedAttachmentsDetails)
            {
                names.All.Add(attachment.Name);

                if (hiddenFromModel.Contains(attachment.Name))
                    names.HiddenFromModel.Add(attachment.Name);
                else
                    names.VisibleToModel.Add(attachment.Name);
            }
        }

        return names;
    }

    public static string RetrieveAndAddAttachment(DocumentDatabase database, DocumentsOperationContext context, RequestBody request, string conversationId, string fileName, string sourceDoc) =>
        AddAttachment(request, RetrieveAttachment(database, context, fileName, sourceDoc));

    public static AiAttachment RetrieveAttachment(DocumentDatabase database, DocumentsOperationContext context, string fileName, string sourceDoc)
    {
        var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
            context,
            sourceDoc,
            fileName,
            AttachmentType.Document,
            null);

        if (attachment == null)
            return CreateNotFoundAttachment(fileName);

        return new AiAttachment
        {
            Name = attachment.Name,
            Type = attachment.ContentType.ToString(),
            Data = GetAttachmentDataAsBase64(attachment),
            Source = AiAttachmentSource.FromAttachment
        };
    }

    private static AiAttachment CreateNotFoundAttachment(string fileName) =>
        new()
        {
            Name = fileName,
            Type = ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeApplicationPdf,
            Data = string.Empty,
            Source = AiAttachmentSource.NotFound
        };

    private static string AddAttachment(RequestBody request, AiAttachment attachment)
    {
        request.Attachments ??= new List<AiAttachment>();
        request.Attachments.Add(attachment);

        return $"Attachment: {attachment.Name}";
    }

    public static AiAttachment CreateAttachmentFromStream(Stream stream, string name, string contentType)
    {
        stream.Position = 0;

        return new AiAttachment
        {
            Name = name,
            Type = contentType,
            Source = AiAttachmentSource.FromAttachment,
            Data = GenAiScriptTransformer.GetAttachmentDataAsBase64(stream, contentType)
        };
    }

    public static string GetAttachmentDataAsBase64(Attachment attachment)
    {
        using var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream();
        using var transform = new ToBase64Transform();
        using var cryptoStream = new CryptoStream(attachment.Stream, transform, CryptoStreamMode.Read);

        cryptoStream.CopyTo(memoryStream);

        Span<byte> readOnlySpan = memoryStream.GetBuffer();
        return System.Text.Encoding.UTF8.GetString(readOnlySpan[..(int)memoryStream.Length]);
    }

    public static bool NeedsReadTransactionForInternalActions(List<AiAgentActionRequest> toolCalls)
    {
        if (toolCalls is null || toolCalls.Count == 0)
            return false;

        foreach (var call in toolCalls)
        {
            if (call.IsInternalToolCall())
                return true;
        }

        return false;
    }

    public static void HandleInternalSystemActions(
        DocumentDatabase database,
        JsonOperationContext context,
        DocumentsOperationContext docContext,
        ConversationDocument document,
        RequestBody request,
        string conversationId,
        List<AiAgentActionRequest> toolCalls)
    {
        foreach (var call in toolCalls)
        {
            if (call.IsInternalToolCall() == false)
                continue;

            var result = new List<string>();

            using (var args = Sparrow.Server.Json.Sync.JsonOperationContextSyncExtensions.ReadForMemory(docContext.Sync, call.Arguments, "tool-args"))
            {
                if (args.TryGet(ChatCompletionClient.Constants.ResponseFields.Names, out BlittableJsonReaderArray namesArray))
                {
                    foreach (var attachmentName in namesArray)
                    {
                        var name = attachmentName.ToString();
                        if (document.AttachmentsHiddenFromModel.Contains(name))
                            result.Add(AddAttachment(request, CreateNotFoundAttachment(name)));
                        else if (request.OnDemandAttachments?.TryGetValue(name, out var onDemand) == true)
                            result.Add(AddAttachment(request, onDemand));
                        else
                            result.Add(RetrieveAndAddAttachment(database, docContext, request, conversationId, name, document.Id));
                    }
                }
            }

            var content = result.Count > 0
                ? string.Join(Environment.NewLine, result)
                : "No attachments requested.";

            document.OpenActionCalls.Remove(call.ToolId);
            document.AddToolResponse(context, call.ToolId, content);
        }
    }
}
