using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public class AiStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;

    public AiStorage([NotNull] DocumentsStorage documentsStorage)
    {
        _documentsStorage = documentsStorage ?? throw new ArgumentNullException(nameof(documentsStorage));
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        if (document == null)
            return null;

        return document;
    }

    public List<ValueEmbeddingsDocument> GetValueEmbeddingsDocument(DocumentsOperationContext context, AiEtlConfiguration configuration, List<string> values,
        out List<string> valueEmbeddingsDocumentsIds)
    {
        var valueEmbeddingsDocuments = new List<ValueEmbeddingsDocument>();
        valueEmbeddingsDocumentsIds = new List<string>();

        foreach (var value in values)
        {
            var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, AiHelper.CalculateValueHash(value));

            valueEmbeddingsDocumentsIds.Add(valueEmbeddingsDocumentId);

            var valueEmbeddingsDocument = GetValueEmbeddingsDocument(context, valueEmbeddingsDocumentId);

            valueEmbeddingsDocuments.Add(valueEmbeddingsDocument);
        }

        return valueEmbeddingsDocuments;
    }

    private ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, string documentId)
    {
        var document = _documentsStorage.Get(context, documentId);
        if (document == null)
            return null;

        return new ValueEmbeddingsDocument(document);
    }

    public string AddOrUpdateValueEmbeddingsDocument(DocumentsOperationContext context, string documentId, AiEtlEmbeddingItemValue item)
    {
        Debug.Assert((item.EmbeddingValue.IsEmpty && item.ValueEmbeddingsAttachmentName != null) ||
                     (item.EmbeddingValue.IsEmpty == false && item.ValueEmbeddingsAttachmentName == null));

        var document = GetValueEmbeddingsDocument(context, item.ValueEmbeddingsDocumentId);
        string attachmentName = item.ValueEmbeddingsAttachmentName ?? Guid.NewGuid().ToString();

        if (item.EmbeddingValue.IsEmpty == false)
        {
            if (document == null)
            {
                var djv = CreateDocument(item.TextualValue, attachmentName);

                using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                    PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);

                return attachmentName;
            }

            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [item.TextualValue] = attachmentName };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);

            return attachmentName;
        }

        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);
        if (attachment == null)
            AttachmentDoesNotExistException.ThrowFor(item.ValueEmbeddingsDocumentId, attachmentName);

        if (document == null)
        {
            var djv = CreateDocument(item.TextualValue, attachmentName);

            using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromAttachment(json, attachment, attachmentName);

            return attachmentName;
        }

        if (document.Inner.Data.TryGet(item.TextualValue, out attachmentName) == false || attachment.Name != attachmentName)
        {
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [item.TextualValue] = attachment.Name };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);
        }

        return attachmentName;

        void PutValueEmbeddingsDocumentFromEmbeddingValue(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, string attachmentName)
        {
            using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(embeddingValue.Span).ToArray()))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(context, stream);

                _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, EmbeddingAttachmentContentType, hash, null,
                    stream);
            }
        }

        void PutValueEmbeddingsDocumentFromAttachment(BlittableJsonReaderObject json, Attachment attachment, string attachmentName)
        {
            _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
            _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, attachment.ContentType,
                attachment.Base64Hash.ToString(), null, attachment.Stream);
        }

        DynamicJsonValue CreateDocument(string textualValue, string attachmentName)
        {
            return new DynamicJsonValue
            {
                [textualValue] = attachmentName,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCollection
                }
            };
        }
    }
}
