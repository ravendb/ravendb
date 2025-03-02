using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;
using Corax.Utils;
using Microsoft.Extensions.AI;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, EmbeddingsCacher cacher)
{
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public EmbeddingsCacher Cacher { get; private set; } = cacher;
    private readonly EmbeddingsBatchingService _batchingService = new(aiIntegrations);

    private readonly ArrayPool<byte> _embeddingPool = ArrayPool<byte>.Create();

    public async Task<object> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext, ByteStringContext allocator,
        AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value, VectorEmbeddingType destinationEmbeddingType)
    {
        if (aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
            throw new ArgumentException($"Couldn't find Embeddings Generation task for connection string '{connectionStringId.Value}' ");

        if (documentsContext.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingTaskId, out var taskConfig) == false)
            throw new Exception($"Could not find embeddings generation configuration for embedding task '{embeddingTaskId.Value}'");

        var chunkingOptions = taskConfig.ChunkingOptionsForQuerying;
        
        var chunks = TextChunker.ChunkValue(value, chunkingOptions);
        var vectorValues = new VectorValue[chunks.Count];
        var chunksForGeneration = new List<string>();
        int vectorValuesCount = 0;

        foreach (var chunk in chunks)
        {
            if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, value, destinationEmbeddingType, out var embeddingCacheDocumentId, out _)) 
            {
                var valueHash = EmbeddingsHelper.CalculateInputValueHash(value);

                var cachedVectorValue = Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, valueHash);
                
                vectorValues[vectorValuesCount++] = cachedVectorValue;
            }
            else
                chunksForGeneration.Add(chunk);
        }

        List<EmbeddingCacheItem> embeddingsToCache = null;

        // var embedding = await _batchingService.GetEmbeddingAsync(connectionStringId, value); // TODO Lev - uncomment when batching is implemented
        var embeddings = await service.GenerateEmbeddingsAsync(chunksForGeneration, cancellationToken: aiIntegrations.Database.DatabaseShutdown);

        for (int i = 0; i < embeddings.Count; i++)
        {
            var embedding = embeddings[i];

            var vectorValue = GenerateEmbeddings.FromArray(allocator, embedding, VectorEmbeddingType.Single, destinationEmbeddingType);

            vectorValues[vectorValuesCount++] = vectorValue;

            embeddingsToCache ??= new (embeddings.Count);

            byte[] bytes = _embeddingPool.Rent(vectorValue.Length);

            vectorValue.GetEmbedding().CopyTo(bytes);

            var embeddingCacheItem = new EmbeddingCacheItem(
                chunksForGeneration[i], 
                new ReadOnlyMemory<byte>(bytes, 0, vectorValue.Length), 
                destinationEmbeddingType,
                connectionStringId, 
                new ReturnEmbeddingBuffer(bytes, _embeddingPool));

            embeddingsToCache.Add(embeddingCacheItem);
        }

        Cacher.EnqueueEmbeddingToCache(embeddingsToCache);

        if (vectorValues.Length == 1) 
            return vectorValues[0];
        
        return vectorValues;
    }

    public void Dispose()
    {
        _batchingService?.Dispose();
    }

    private readonly struct ReturnEmbeddingBuffer(byte[] embeddingBuffer, ArrayPool<byte> pool) : IDisposable
    {
        public void Dispose()
        {
            pool.Return(embeddingBuffer);
        }
    }
}
