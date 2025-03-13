using Raven.Server.Documents.ETL.Providers.AI;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Exception = System.Exception;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, QueryEmbeddingsCacher queryEmbeddingsCacher) : IDisposable
{
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public QueryEmbeddingsCacher QueryEmbeddingsCacher { get; private set; } = queryEmbeddingsCacher;
    private readonly QueryEmbeddingsBatchingService _queryBatchingService = new(aiIntegrations);
    
    public Task<IEmbeddingValue[]> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext,
        AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string[] values)
    {
        return GetEmbeddingsForQueryAsync(documentsContext, connectionStringId, embeddingTaskId, values, ChunkValues);
    }
    
    public Task<IEmbeddingValue[]> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext,
        AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value)
    {
        return GetEmbeddingsForQueryAsync(documentsContext, connectionStringId, embeddingTaskId, value, TextChunker.ChunkValue);
    }
    
    private async Task<IEmbeddingValue[]> GetEmbeddingsForQueryAsync<T>(DocumentsOperationContext documentsContext,
        AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, T values, Func<T, ChunkingOptions, List<string>> chunkingMethod)
    {
        if (documentsContext.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingTaskId, out var taskConfig) == false)
            throw new Exception($"Could not find Embeddings Generation configuration for the task with '{embeddingTaskId.Value}' identifier");

        var quantization = taskConfig.Quantization;
        var chunkingOptions = taskConfig.ChunkingOptionsForQuerying;
        var expireAt = aiIntegrations.Database.Time.GetUtcNow().Add(taskConfig.EmbeddingsCacheForQueryingExpiration);
        
        var chunks = chunkingMethod(values, chunkingOptions);
        var embeddingValues = await GetEmbeddingsInternal(documentsContext, connectionStringId, quantization, chunks, expireAt);

        return embeddingValues;
    }
    
    private async Task<IEmbeddingValue[]> GetEmbeddingsInternal(DocumentsOperationContext documentsContext, AiConnectionStringIdentifier connectionStringId,
        VectorEmbeddingType quantization, List<string> chunks, DateTime expireAt)
    {
        var embeddingValues = new IEmbeddingValue[chunks.Count];
        var chunksForGeneration = new List<string>();
        int vectorValuesCount = 0;
        
        foreach (var chunk in chunks)
        {
            var chunkHash = EmbeddingsHelper.CalculateInputValueHash(chunk);

            if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, chunkHash, quantization, out var embeddingCacheDocumentId, out _))
            {
                var cachedEmbeddingValue = Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, chunkHash);
                
                embeddingValues[vectorValuesCount++] = cachedEmbeddingValue;
            }
            else
                chunksForGeneration.Add(chunk);
        }

        if (chunksForGeneration.Count == 0)
            return embeddingValues;

        List<EmbeddingGenerationItem> embeddingsToCache = null;

        var embeddings = await _queryBatchingService.GetEmbeddingAsync(connectionStringId, chunksForGeneration);
        
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

    public Task RemoveBatchingWorkerForConnectionStringIdAsync(AiConnectionStringIdentifier connectionStringId) => _queryBatchingService.RemoveWorkerAsync(connectionStringId);

    public Task UpdateBatchingWorkerForConnectionStringIdAsync(AiConnectionString newConnectionString) => _queryBatchingService.UpdateWorkerIfNecessaryAsync(newConnectionString);

    private List<string> ChunkValues(string[] values, ChunkingOptions chunkingOptions)
    {
        var chunks = new List<string>();

        foreach (var value in values)
        {
            var chunksFromSingleValue = TextChunker.ChunkValue(value, chunkingOptions);
            
            chunks.AddRange(chunksFromSingleValue);
        }
        
        return chunks;
    }

    public async Task DisposeAsync()
    {
        if (_queryBatchingService != null)
            await _queryBatchingService.DisposeAsync();

        QueryEmbeddingsCacher?.Dispose();
    }

    public void Dispose() => AsyncHelpers.RunSync(DisposeAsync);
}
