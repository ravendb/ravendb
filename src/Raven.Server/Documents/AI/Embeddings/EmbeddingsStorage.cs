using System;
using System.IO;
using System.Runtime.InteropServices;
using Corax.Utils;
using JetBrains.Annotations;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.ServerWide;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public class EmbeddingsStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;

    
    private readonly EmbeddingsCacher _embeddingsCacher;
    
    public EmbeddingsStorage([NotNull] DocumentDatabase database)
    {
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(_documentsStorage));
        _embeddingsCacher = new EmbeddingsCacher(database, database.Loggers.GetLogger<EmbeddingsCacher>(), database.DatabaseShutdown);
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        
        return document;
    }

    public bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, string value, in VectorEmbeddingType targetQuantization,
        out string embeddingCacheDocumentId, out EmbeddingCacheDocument result)
    {
        embeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, "TODO arek", targetQuantization);
        
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
            // Cache contains embedding for our input value. Extend the expiration date.

            // todo logic
            // rozszerzenie eksipracji
            // validacja istnienia attachmentu


            return;
        }

        var newDoc = CreateEmbeddingCacheDocument(currentDate);
        using (var reader = context.ReadObject(newDoc, item.EmbeddingCacheDocumentId))
        {
            PutEmbeddingCacheDocument(reader, item.OutputValue, item.UsedBytes);
        }


        void PutEmbeddingCacheDocument(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, int usedBytes)
        {
            var embeddingSpan = MemoryMarshal.Cast<float, byte>(embeddingValue.Span)[..usedBytes];
            
            // TODO: Implement a Stream wrapper around ReadOnlyMemory<float> and usedBytes to avoid cloning memory and unnecessary allocations.
            using (var stream = new MemoryStream(embeddingSpan.ToArray()))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.Cast<float, byte>(embeddingValue.Span));

                _documentsStorage.Put(context, item.EmbeddingCacheDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.EmbeddingCacheDocumentId, item.InputValueHash, EmbeddingAttachmentContentType, hash, null,
                    stream);
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
