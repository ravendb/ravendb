using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System;
using System.Threading.Tasks;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Client.Documents.Operations.AI;
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
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value, VectorOptions vectorOptions)
    {
        if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, value, targetQuantization, out var embeddingCacheDocumentId, out var toDoArek)) 
        {
            var valueHash = EmbeddingsHelper.CalculateInputValueHash(value);

            return Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, valueHash);
        }
        
        if (_aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
            throw new ArgumentException($"Couldn't find Embeddings Generation task for connection string '{connectionStringId.Value}' ");

        var allocator = documentsContext.Transaction.InnerTransaction.Allocator; // TODO arek - use buildparameters.Allocator

        var taskConfig = documentsContext.DocumentDatabase.AiIntegrations.GetConfigurationByTaskIdentifier(embeddingTaskId);

        var chunkingOptions = taskConfig.ChunkingOptionsForQuerying;
        
        var chunks = TextChunker.ChunkValue(value, chunkingOptions);
        
        var vectorValues = new VectorValue[chunks.Count];

        for (var i = 0; i < chunks.Count; i++)
        {
            var embedding = await service.GenerateEmbeddingAsync(chunks[i]);
            var vectorValue = GenerateEmbeddings.FromArray(allocator, embedding, vectorOptions);

            vectorValues[i] = vectorValue;
        }

        // TODO arek Cacher.EnqueueEmbeddingToCache(connectionStringId, );

        if (vectorValues.Length == 1) 
            return vectorValues[0];
        
        return vectorValues;
    }
}
