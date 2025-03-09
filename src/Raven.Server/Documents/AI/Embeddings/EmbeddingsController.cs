using Raven.Server.Documents.ETL.Providers.AI;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, QueryEmbeddingsCacher queryEmbeddingsCacher) : IDisposable
{
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public QueryEmbeddingsCacher QueryEmbeddingsCacher { get; private set; } = queryEmbeddingsCacher;
    private readonly QueryEmbeddingsBatchingService _queryBatchingService = new(aiIntegrations);

    public async Task<IEmbeddingValue[]> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext, ByteStringContext allocator,
        AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value)
    {
        if (documentsContext.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingTaskId, out var taskConfig) == false)
            throw new Exception($"Could not find embeddings generation configuration for embedding task '{embeddingTaskId.Value}'");

        var quantization = taskConfig.Quantization;

        var chunkingOptions = taskConfig.ChunkingOptionsForQuerying;
        
        var chunks = TextChunker.ChunkValue(value, chunkingOptions);
        var embeddingValues = new IEmbeddingValue[chunks.Count];
        var chunksForGeneration = new List<string>();
        int vectorValuesCount = 0;

        foreach (var chunk in chunks)
        {
            var chunkHash = EmbeddingsHelper.CalculateInputValueHash(chunk);

            if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, chunkHash, quantization, out var embeddingCacheDocumentId, out _))
            {
                var valueHash = EmbeddingsHelper.CalculateInputValueHash(value);

                var cachedEmbeddingValue = Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, valueHash);
                
                embeddingValues[vectorValuesCount++] = cachedEmbeddingValue;
            }
            else
                chunksForGeneration.Add(chunk);
        }

        if (chunksForGeneration.Count == 0)
            return embeddingValues;

        List<EmbeddingGenerationItem> embeddingsToCache = null;

        var embeddings = await _queryBatchingService.GetEmbeddingAsync(connectionStringId, chunksForGeneration);

        var expireAt = aiIntegrations.Database.Time.GetUtcNow().Add(taskConfig.EmbeddingsCacheForQueryingExpiration);

        for (int i = 0; i < embeddings.Length; i++)
        {
            var embedding = embeddings[i];

            var embeddingValue = EmbeddingsHelper.CreateEmbeddingValue(embedding, quantization);

            embeddingValues[vectorValuesCount++] = embeddingValue;

            embeddingsToCache ??= new List<EmbeddingGenerationItem>(embeddings.Length);

            string textualValue = chunksForGeneration[i];

            var embeddingCacheItem = new EmbeddingGenerationItem(
                textualValue,
                embeddingValue,
                quantization,
                connectionStringId) { ExpireAt = expireAt };

            embeddingsToCache.Add(embeddingCacheItem);
        }

        QueryEmbeddingsCacher.EnqueueEmbeddingsToCache(embeddingsToCache);

        return embeddingValues;
    }

    public void Dispose()
    {
        _queryBatchingService?.Dispose();
    }
}
