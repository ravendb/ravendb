using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IGetAttachmentStrategy
{
    public void DisposeReadTransactionIfNeeded(DocumentsTransaction tx);
    public string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token);

    public static string CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(DocumentsOperationContext context, DocumentDatabase database, Attachment attachment, string documentId, string name, string method)
    {
        if (attachment.IsRetired() == false)
        {
            throw new InvalidOperationException($"Cannot {method} retired attachment '{name}' on document '{documentId}' because it is not retired. Please use dedicated API.");
        }

        using var document = database.DocumentsStorage.Get(context, documentId, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
        if (document.TryGetCollection(out string collectionStr) == false)
        {
            throw new InvalidOperationException(
                $"Cannot {method} retired attachment '{name}' on document '{documentId}' because it is doesn't have a collection. Should not happen a likely a bug !");
        }

        var config = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Configuration;

        if (config == null)
        {
            throw new InvalidOperationException(
                $"Cannot {method} retired attachment '{name}' on document '{documentId}' because it is doesn't have a {nameof(RetiredAttachmentsConfiguration)}.");
        }

        var destination = config.Destinations[attachment.RetireParameters.Identifier];
        if (destination == null)
        {
            throw new InvalidOperationException(
                $"Cannot {method} retired attachment '{name}' with identifier '{attachment.RetireParameters.Identifier}' on document '{documentId}' because it is doesn't have {nameof(RetiredAttachmentsConfiguration)}.{nameof(RetiredAttachmentsConfiguration.Destinations)}.");
        }
        if (destination.Disabled)
        {
            throw new InvalidOperationException(
                $"Cannot {method} retired attachment '{name}' with identifier '{attachment.RetireParameters.Identifier}' on document '{documentId}' because {nameof(RetiredAttachmentsConfiguration)}.{nameof(RetiredAttachmentsConfiguration.Destinations)} is disabled.");
        }

        return collectionStr;
    }

}
