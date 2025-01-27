using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using JetBrains.Annotations;
using Microsoft.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public class AiStorage
{
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
        var document = GetValueEmbeddingsDocument(context, item.ValueEmbeddingsDocumentId);
        string attachmentName = item.ValueEmbeddingsAttachmentName;
        
        // cache document doesn't exist
        if (document == null)
        {
            // no embeddings
            if (item.EmbeddingValue != null)
            {
                attachmentName = Guid.NewGuid().ToString();
                
                var documentDjv = new DynamicJsonValue
                {
                    [item.Value] = attachmentName,
                    // todo expiration
                    ["@metadata"] = new DynamicJsonValue()
                    {
                        ["@collection"] = "@embeddings"
                    }
                };

                using (var bjro = context.ReadObject(documentDjv, item.ValueEmbeddingsDocumentId)) 
                    // todo
                using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(item.EmbeddingValue).ToArray()))
                using (var stream2 = new MemoryStream())
                {
                    var hash = AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, stream2, stream, CancellationToken.None).GetAwaiter().GetResult();
                    
                    _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, bjro);
                    // todo content type
                    _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, "application/octet-stream", hash, null, stream2);
                }
                
                return attachmentName;
            }

            // todo embeddings should exist
            throw new InvalidOperationException("Attachment exists but document was already deleted");
        }

        if (item.EmbeddingValue != null)
        {
            // document exists but property doesn't exist
            if (document.Inner.Data.TryGet(item.Value, out attachmentName) == false)
                attachmentName = Guid.NewGuid().ToString();
            
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data);
            document.Inner.Data.Modifications[item.Value] = attachmentName;
            
            using (var bjro = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                // todo
            using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(item.EmbeddingValue).ToArray()))
            using (var streamCopy = new MemoryStream())
            {
                var hash = AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, streamCopy, stream, CancellationToken.None).GetAwaiter().GetResult();

                _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, bjro);
                // todo content type
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, "application/octet-stream", hash, null,
                    streamCopy);
            }
            
            return attachmentName;
        }

        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);

        // todo 
        if (attachment == null)
            throw new Exception("todo");

        if (document.Inner.Data.TryGet(item.Value, out attachmentName) == false || attachment.Name != attachmentName)
        {
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data);
            document.Inner.Data.Modifications[item.Value] = attachment.Name;

            using (var bjro = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
            {
                // change vector?
                _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, document.Inner.ChangeVector, bjro);
            }
        }
        
        return attachmentName;
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
