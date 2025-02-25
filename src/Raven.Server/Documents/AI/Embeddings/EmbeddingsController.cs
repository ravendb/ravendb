using Corax.Utils;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server;
using System.Runtime.InteropServices;
using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Voron.Data.Graphs;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, EmbeddingsCacher cacher)
{
    private readonly AiIntegrationsController _aiIntegrations = aiIntegrations;
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public EmbeddingsCacher Cacher { get; private set; } = cacher;

    public async Task<VectorValue> GetEmbeddingForQueryAsync(DocumentsOperationContext documentsContext, AiConnectionStringIdentifier connectionStringId,
        string value, int dimensions)
    {
        if (Storage.TryGetEmbeddingCacheDocument(documentsContext, connectionStringId, value, out var embeddingCacheDocumentId, out var toDoArek)) 
        {
            var valueHash = EmbeddingsHelper.CalculateInputValueHash(value);

            return Storage.GetCachedEmbeddingValue(documentsContext, embeddingCacheDocumentId, valueHash);
        }
        
        if (_aiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
            throw new ArgumentException($"Couldn't find Embeddings Generation task for connection string '{connectionStringId.Value}' ");

        var allocator = documentsContext.Transaction.InnerTransaction.Allocator; // TODO arek - use buildparameters.Allocator

        var embedding = await service.GenerateEmbeddingAsync(value);

        // TODO arek Cacher.EnqueueEmbeddingToCache(connectionStringId, );
        
        var memoryScope = allocator.Allocate(dimensions, out Memory<byte> memory);
        MemoryMarshal.AsBytes(embedding.Span).CopyTo(memory.Span);

        return new VectorValue(memoryScope, memory, VectorEmbeddingType.Single, dimensions);
    }
}
