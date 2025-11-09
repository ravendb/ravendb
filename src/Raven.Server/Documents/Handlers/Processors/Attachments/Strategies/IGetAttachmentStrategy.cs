using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IGetAttachmentStrategy
{
    public void DisposeReadTransactionIfNeeded(DocumentsTransaction tx);
    public void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken tcs);

    public static void CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(DocumentsOperationContext context, DocumentDatabase database, Attachment attachment, string documentId, string name, string operation)
    {
        using var document = database.DocumentsStorage.Get(context, documentId, DocumentFields.Id);
        if (document == null)
        {
            throw new InvalidOperationException($"Cannot perform '{operation}' for remote attachment '{name}' on document '{documentId}' because the document does not exist.");
        }

        var config = database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.Configuration;
        if (config == null)
        {
            throw new InvalidOperationException(
                $"Cannot perform '{operation}' for remote attachment '{name}' on document '{documentId}' because the database does not have a {nameof(RemoteAttachmentsConfiguration)} configured.");
        }

        if (config.Disabled)
        {
            throw new InvalidOperationException(
                $"Cannot perform '{operation}' for remote attachment '{name}' on document '{documentId}' because the {nameof(RemoteAttachmentsConfiguration)} is disabled.");
        }

        if (config.Destinations == null || config.Destinations.TryGetValue(attachment.RemoteParameters.Identifier, out var destination) == false)
        {
            throw new InvalidOperationException(
                $"Cannot perform '{operation}' for remote attachment '{name}' (identifier: '{attachment.RemoteParameters.Identifier}') on document '{documentId}' because the destination is not defined in {nameof(RemoteAttachmentsConfiguration)}.{nameof(RemoteAttachmentsConfiguration.Destinations)}.");
        }
        if (destination.Disabled)
        {
            throw new InvalidOperationException(
                $"Cannot perform '{operation}' for remote attachment '{name}' (identifier: '{attachment.RemoteParameters.Identifier}') on document '{documentId}' because its destination is disabled in {nameof(RemoteAttachmentsConfiguration)}.{nameof(RemoteAttachmentsConfiguration.Destinations)}.");
        }
    }
}
