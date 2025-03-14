using System;
using System.IO;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;

    public EmbeddingsStorage([NotNull] DocumentDatabase database)
    {
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(database.DocumentsStorage));
    }

    public Document GetEmbeddingDocument(DocumentsOperationContext context, string sourceDocumentId, out string embeddingDocumentId)
    {
        embeddingDocumentId = EmbeddingsHelper.GetEmbeddingDocumentId(sourceDocumentId);

        var document = _documentsStorage.Get(context, embeddingDocumentId);
        
        return document;
    }

    public bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, string valueHash, in VectorEmbeddingType targetQuantization,
        out string embeddingCacheDocumentId, out Document result)
    {
        embeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, valueHash, targetQuantization);
        
        return TryGetEmbeddingCacheDocument(context, embeddingCacheDocumentId, out result);
    }

    private bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, string documentId, out Document result)
    {
        result = _documentsStorage.Get(context, documentId);

        return result != null;
    }

    public void PutOrUpdateEmbeddingCacheDocument(DocumentsOperationContext context, EmbeddingGenerationItem item, DateTime expireAt,
        TimeSpan cacheExpiration)
    {
        if (item.EmbeddingValue != null)
        {
            CacheEmbedding(context, item, expireAt);
            return;
        }

        // embedding value was already cached

        using var document = context.DocumentDatabase.DocumentsStorage.Get(context, item.CacheDocumentId);
        var metadataExists = document.TryGetMetadata(out var metadata);
        PortableExceptions.ThrowIfNot<InvalidDataException>(metadataExists, $"The embedding cache document exists, but its metadata is missing: {item.CacheDocumentId}");

        var expiresExistsInMetadata = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, metadata, Constants.Documents.Metadata.Expires, out var expiresObject);
        PortableExceptions.ThrowIfNot<InvalidDataException>(expiresExistsInMetadata, $"The embedding cache document exists, has metadata, but its expiration is missing: {item.CacheDocumentId}");

        var currentExpireAt = (DateTime)expiresObject;

        bool shouldUpdateExpiration;

        if (expireAt > currentExpireAt)
        {
            shouldUpdateExpiration = expireAt - currentExpireAt > cacheExpiration / 3;
        }
        else
        {
            shouldUpdateExpiration = false;
        }

        var attachmentsExistsInMetadata = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, metadata, Constants.Documents.Metadata.Attachments, out var attachmentsArrayObject);
        PortableExceptions.ThrowIfNot<InvalidDataException>(attachmentsExistsInMetadata, $"The embedding cache document exists, has metadata and expiration, but attachment field is missing: {item.CacheDocumentId}");

        if (attachmentsArrayObject is BlittableJsonReaderArray attachmentsArray)
        {
            PortableExceptions.ThrowIfNot<InvalidDataException>(attachmentsArray.Length == 1, $"The embedding document suppose to have only one attachment, but it has {attachmentsArray.Length}.");

            var attachment = attachmentsArray[0] as BlittableJsonReaderObject;
            PortableExceptions.ThrowIfNull(attachment, $"The embedding document has no attachment.");

            var hasHash = BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, attachment, "Hash", out var hashObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(hasHash && hashObject is LazyStringValue, "hasHash && hashObject is LazyStringValue");

            LazyStringValue hash = (LazyStringValue)(hashObject);
            var existsInStorage = _documentsStorage.AttachmentsStorage.AttachmentExists(context, hash);
            PortableExceptions.ThrowIfNot<InvalidDataException>(existsInStorage, $"The embedding document has attachment, but it doesn't exist in storage: {item.CacheDocumentId}");
        }

        if (shouldUpdateExpiration)
        {
            document.Data.Modifications = CreateEmbeddingCacheDocumentJson(expireAt);

            using (var reader = context.ReadObject(document.Data, item.CacheDocumentId))
                _documentsStorage.Put(context, item.CacheDocumentId, null, reader);
        }
    }

    public bool IsEmbeddingValueCached(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, ref EmbeddingGenerationItem value, in VectorEmbeddingType targetQuantization)
    {
        value.CacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, value.ValueHash, targetQuantization);
        using var document = _documentsStorage.Get(context, value.CacheDocumentId);

        return document != null;
    }

    public IEmbeddingValue GetCachedEmbeddingValue(DocumentsOperationContext documentsContext, string embeddingCacheDocumentId, string valueHash)
    {
        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(documentsContext, embeddingCacheDocumentId, valueHash, AttachmentType.Document, null);

        var stream = attachment.Stream;

        return new StreamedEmbeddingValue(stream);
    }

    public void CacheEmbedding(DocumentsOperationContext context, EmbeddingGenerationItem item, DateTime expireAt)
    {
        using (var embeddingValueStream = item.EmbeddingValue.GetEmbeddingStream())
        {
            var embeddingHash = AttachmentsStorageHelper.CalculateHash(item.EmbeddingValue.GetEmbedding());

            var docId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(item.ConnectionStringIdentifier, item.ValueHash, item.Quantization);

            var docJson = CreateEmbeddingCacheDocumentJson(expireAt);

            using (var json = context.ReadObject(docJson, docId))
            {
                _documentsStorage.Put(context, docId, null, json);
            }

            _documentsStorage.AttachmentsStorage.PutAttachment(context, docId, item.ValueHash, EmbeddingAttachmentContentType, embeddingHash, null,
                embeddingValueStream);
        }
    }

    private DynamicJsonValue CreateEmbeddingCacheDocumentJson(DateTime expireAt)
    {
        return new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCacheCollection,
                [Constants.Documents.Metadata.Expires] = expireAt
            }
        };
    }
}
