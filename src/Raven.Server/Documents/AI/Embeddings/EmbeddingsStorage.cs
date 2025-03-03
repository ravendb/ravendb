using System;
using System.IO;
using System.Runtime.InteropServices;
using Corax.Utils;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;

    public EmbeddingsStorage([NotNull] DocumentDatabase database)
    {
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(_documentsStorage));
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        
        return document;
    }

    public bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, string valueHash, in VectorEmbeddingType targetQuantization,
        out string embeddingCacheDocumentId, out EmbeddingCacheDocument result)
    {
        embeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, valueHash, targetQuantization);
        
        return TryGetEmbeddingCacheDocument(context, embeddingCacheDocumentId, out result);
    }

    private bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, string documentId, out EmbeddingCacheDocument result)
    {
        var document = _documentsStorage.Get(context, documentId);

        if (document == null)
        {
            result = null;
            return false;
        }

        result = new EmbeddingCacheDocument(document);
        return true;
    }

    public void AddOrUpdateEmbeddingDocument(DocumentsOperationContext context, EmbeddingGenerationItem item, DateTime currentDate)
    {
        if (item.OutputValue.IsEmpty)
        {
            using var document = context.DocumentDatabase.DocumentsStorage.Get(context, item.EmbeddingCacheDocumentId);
            var metadataExists = document.TryGetMetadata(out var metadata);
            PortableExceptions.ThrowIfNot<InvalidDataException>(metadataExists, $"The embedding cache document exists, but its metadata is missing: {item.EmbeddingCacheDocumentId}");

            var expiresExistsInMetadata = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, metadata, Constants.Documents.Metadata.Expires, out var expiresObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(expiresExistsInMetadata, $"The embedding cache document exists, has metadata, but its expiration is missing: {item.EmbeddingCacheDocumentId}");
            
            var expires = (DateTime)expiresObject;
            bool shouldUpdateExpiration = expires - currentDate > TimeSpan.FromDays(10); //TODO AREK: this is wrong, however we're still missing this in configuration, so this line always return true for testing purposes.
            
            var attachmentsExistsInMetadata = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, metadata, Constants.Documents.Metadata.Attachments, out var attachmentsArrayObject);            
            PortableExceptions.ThrowIfNot<InvalidDataException>(attachmentsExistsInMetadata, $"The embedding cache document exists, has metadata and expiration, but attachment field is missing: {item.EmbeddingCacheDocumentId}");

            if (attachmentsArrayObject is BlittableJsonReaderArray attachmentsArray)
            {
                PortableExceptions.ThrowIfNot<InvalidDataException>(attachmentsArray.Length == 1, $"The embedding document suppose to have only one attachment, but it has {attachmentsArray.Length}.");
                
                var attachment = attachmentsArray[0] as BlittableJsonReaderObject;
                PortableExceptions.ThrowIfNull(attachment, $"The embedding document has no attachment.");
                
                var hasHash = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, attachment, "Hash", out var hashObject);
                PortableExceptions.ThrowIfNot<InvalidDataException>(hasHash && hashObject is LazyStringValue, "hasHash && hashObject is LazyStringValue");
                
                LazyStringValue hash = (LazyStringValue)(hashObject);
                var existsInStorage = _documentsStorage.AttachmentsStorage.AttachmentExists(context, hash);
                PortableExceptions.ThrowIfNot<InvalidDataException>(existsInStorage, $"The embedding document has attachment, but it doesn't exist in storage: {item.EmbeddingCacheDocumentId}");
            }


            if (shouldUpdateExpiration)
            {
                // document.Data.Modifications = new DynamicJsonValue()
                // {
                //     [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                //     {
                //         [Constants.Documents.Metadata.Expires] = expires.AddMonths(3) // TODO arek
                //     }
                // };
                //
                // using (var reader = context.ReadObject(document.Data, item.EmbeddingCacheDocumentId))
                //     _documentsStorage.Put(context, item.EmbeddingCacheDocumentId, null, reader);
            }

            return;
        }

        var newDoc = CreateEmbeddingCacheDocument(currentDate);
        using (var reader = context.ReadObject(newDoc, item.EmbeddingCacheDocumentId))
        {
            PutEmbeddingCacheDocument(reader, item.OutputValue, item.UsedBytes);
        }


        void PutEmbeddingCacheDocument(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, int usedBytes)
        {
            using (var attachmentContentStream = new ReadOnlyMemoryStream<float>(embeddingValue, usedBytes))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.Cast<float, byte>(embeddingValue.Span)[..usedBytes]);

                _documentsStorage.Put(context, item.EmbeddingCacheDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.EmbeddingCacheDocumentId, item.InputValueHash, EmbeddingAttachmentContentType, hash, null,
                    attachmentContentStream);
            }
        }
    }
    
    public static DynamicJsonValue CreateEmbeddingCacheDocument(DateTime lastModified)
    {
        return new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCacheCollection,
                [Constants.Documents.Metadata.Expires] = lastModified.AddMonths(3)// TODO arek
            }
        };
    }
    public bool ExistsEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, EmbeddingGenerationItem value, in VectorEmbeddingType targetQuantization)
    {
        value.EmbeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, value.InputValueHash, targetQuantization);
        using var document = _documentsStorage.Get(context, value.EmbeddingCacheDocumentId);

        return document != null;
    }

    public VectorValue GetCachedEmbeddingValue(DocumentsOperationContext documentsContext, string embeddingCacheDocumentId, string valueHash)
    {
        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(documentsContext, embeddingCacheDocumentId, valueHash, AttachmentType.Document, null);

        var stream = attachment.Stream;

        var len = (int)stream.Length;
        var allocator = documentsContext.Transaction.InnerTransaction.Allocator;
        var memScope = allocator.Allocate((int)stream.Length, out Memory<byte> mem);
        stream.ReadExactly(mem.Span);

        return GenerateEmbeddings.FromArray(allocator, memScope, mem, new VectorOptions(), len);
    }
}
