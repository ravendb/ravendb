using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
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

    public ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, AiEtlConfiguration configuration, string value, out string valueEmbeddingsDocumentId)
    {
        valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.Name, AiHelper.CalculateValueHash(value));

        return GetValueEmbeddingsDocument(context, valueEmbeddingsDocumentId);
    }

    private ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, string documentId)
    {
        var document = _documentsStorage.Get(context, documentId);
        if (document == null)
            return null;

        return new ValueEmbeddingsDocument(document);
    }

    public string AddOrUpdateValueEmbeddingsDocument(DocumentsOperationContext context, AiEtlEmbeddingItem item)
    {
        Debug.Assert((item.EmbeddingValue.IsEmpty && item.ValueEmbeddingsAttachmentName != null) || (item.EmbeddingValue.IsEmpty == false && item.ValueEmbeddingsAttachmentName == null));

        var document = GetValueEmbeddingsDocument(context, item.ValueEmbeddingsDocumentId);
        string attachmentName = item.ValueEmbeddingsAttachmentName ?? Guid.NewGuid().ToString();

        if (item.EmbeddingValue.IsEmpty == false)
        {
            if (document == null)
            {
                var djv = CreateDocument();

                using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                    PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue);

                return attachmentName;
            }

            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data)
            {
                [item.Value] = attachmentName
            };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue);

            return attachmentName;
        }

        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);
        if (attachment == null)
            AttachmentDoesNotExistException.ThrowFor(item.ValueEmbeddingsDocumentId, attachmentName);

        if (document == null)
        {
            var djv = CreateDocument();

            using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromAttachment(json, attachment);

            return attachmentName;
        }

        if (document.Inner.Data.TryGet(item.Value, out attachmentName) == false || attachment.Name != attachmentName)
        {
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data)
            {
                [item.Value] = attachment.Name
            };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue);
        }

        return attachmentName;

        void PutValueEmbeddingsDocumentFromEmbeddingValue(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue)
        {
            using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(embeddingValue.Span).ToArray()))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(context, stream);

                _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, EmbeddingAttachmentContentType, hash, null, stream);
            }
        }

        void PutValueEmbeddingsDocumentFromAttachment(BlittableJsonReaderObject json, Attachment attachment)
        {
            _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
            _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, attachment.ContentType, attachment.Base64Hash.ToString(), null, attachment.Stream);
        }

        DynamicJsonValue CreateDocument()
        {
            return new DynamicJsonValue
            {
                [item.Value] = attachmentName,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCollection
                }
            };
        }
    }

    public void AddOrUpdateDocumentEmbeddingsDocument(DocumentsOperationContext context, string configurationName, AiEtlEmbeddingItem item)
    {
        var embeddingsDocumentId = AiHelper.GetDocumentEmbeddingsId(item.DocumentId);
        var attachmentName = item.ValueEmbeddingsAttachmentName;

        var document = _documentsStorage.Get(context, embeddingsDocumentId);

        if (document == null)
        {
            var documentDjv = new DynamicJsonValue
            {
                [configurationName] = new DynamicJsonValue()
                {
                    [item.ValuePath] = new DynamicJsonArray() { attachmentName }
                }
            };

            using (var bjro = context.ReadObject(documentDjv, embeddingsDocumentId))
            {
                _documentsStorage.Put(context, embeddingsDocumentId, null, bjro);
            }

            var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);

            // todo hash
            _documentsStorage.AttachmentsStorage.PutAttachment(context, embeddingsDocumentId, attachmentName, attachment.ContentType, attachment.Base64Hash.ToString(), attachment.ChangeVector, attachment.Stream);

            return;
        }

        if (document.Data.TryGet(configurationName, out BlittableJsonReaderObject propertiesUnderConfiguration) == false)
        {
            document.Data.Modifications = new DynamicJsonValue(document.Data);
            document.Data.Modifications[configurationName] = new DynamicJsonValue()
            {
                [item.ValuePath] = new DynamicJsonArray() { attachmentName }
            };

            using (var bjro = context.ReadObject(document.Data, embeddingsDocumentId))
            {
                _documentsStorage.Put(context, embeddingsDocumentId, document.ChangeVector, bjro);
            }

            var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);

            // todo hash
            _documentsStorage.AttachmentsStorage.PutAttachment(context, embeddingsDocumentId, attachmentName, attachment.ContentType, attachment.Base64Hash.ToString(), attachment.ChangeVector, attachment.Stream);

            return;
        }

        if (propertiesUnderConfiguration.TryGet(item.ValuePath, out BlittableJsonReaderArray valuesUnderProperty) == false)
        {
            document.Data.Modifications = new DynamicJsonValue(document.Data);

            var configurationObject = (DynamicJsonValue)document.Data.Modifications[configurationName];

            configurationObject[item.ValuePath] = new DynamicJsonArray() { attachmentName };

            using (var bjro = context.ReadObject(document.Data, embeddingsDocumentId))
            {
                _documentsStorage.Put(context, embeddingsDocumentId, document.ChangeVector, bjro);
            }

            var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);

            // todo hash
            _documentsStorage.AttachmentsStorage.PutAttachment(context, embeddingsDocumentId, attachmentName, attachment.ContentType, attachment.Base64Hash.ToString(), attachment.ChangeVector, attachment.Stream);

            return;
        }

        if (valuesUnderProperty.Contains(attachmentName))
            return;


    }
}
