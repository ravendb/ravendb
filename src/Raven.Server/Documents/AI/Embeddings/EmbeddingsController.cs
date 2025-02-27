using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, EmbeddingsCacher cacher)
{
    private readonly AiIntegrationsController _aiIntegrations = aiIntegrations;
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public EmbeddingsCacher Cacher { get; private set; } = cacher;

    public async Task<object> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext, AiConnectionStringIdentifier connectionStringId,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value, VectorEmbeddingType destinationEmbeddingType)
    {
        if (_aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
            throw new ArgumentException($"Couldn't find Embeddings Generation task for connection string '{connectionStringId.Value}' ");

        var allocator = documentsContext.Transaction.InnerTransaction.Allocator; // TODO arek - use buildparameters.Allocator

        if (documentsContext.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingTaskId, out var taskConfig) == false)
            throw new Exception($"Could not find embeddings generation configuration for embedding task '{embeddingTaskId.Value}'");

        var chunkingOptions = taskConfig.ChunkingOptionsForQuerying;
        
        var chunks = TextChunker.ChunkValue(value, chunkingOptions);
        var vectorValues = new VectorValue[chunks.Count];
        var chunksForGeneration = new List<string>();
        int vectorValuesCount = 0;

        foreach (var chunk in chunks)
        {
            if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, value, destinationEmbeddingType, out var embeddingCacheDocumentId, out var toDoArek)) 
            {
                var valueHash = EmbeddingsHelper.CalculateInputValueHash(value);

                var cachedVectorValue = Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, valueHash);
                
                vectorValues[vectorValuesCount++] = cachedVectorValue;
            }
            else
                chunksForGeneration.Add(chunk);
        }

        for (var i = 0; i < chunksForGeneration.Count; i++)
        {
            var embedding = await service.GenerateEmbeddingAsync(chunksForGeneration[i]);
            var vectorValue = GenerateEmbeddings.FromArray(allocator, embedding, VectorEmbeddingType.Single, destinationEmbeddingType);

            vectorValues[vectorValuesCount++] = vectorValue;
        }

        // TODO arek Cacher.EnqueueEmbeddingToCache(connectionStringId, );

        if (vectorValues.Length == 1) 
            return vectorValues[0];
        
        return vectorValues;
    }
}
