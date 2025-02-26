using Corax.Utils;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Server;
using System.Runtime.InteropServices;
using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide.Context;
using Voron.Data.Graphs;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsController(AiIntegrationsController aiIntegrations, EmbeddingsStorage storage, EmbeddingsCacher cacher)
{
    private readonly AiIntegrationsController _aiIntegrations = aiIntegrations;
    public EmbeddingsStorage Storage { get; private set; } = storage;
    public EmbeddingsCacher Cacher { get; private set; } = cacher;

    public async Task<VectorValue> GetEmbeddingForQueryAsync(DocumentsOperationContext documentsContext, AiConnectionStringIdentifier connectionStringId, Client.Documents.Indexes.Vector.VectorEmbeddingType targetQuantization, string value)
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
        
        //TODO - Quantize in place
        var bytesRequired = targetQuantization switch
        {
            Client.Documents.Indexes.Vector.VectorEmbeddingType.Single => embedding.Length * sizeof(float),
            Client.Documents.Indexes.Vector.VectorEmbeddingType.Int8 => embedding.Length * sizeof(float) + sizeof(float),
            Client.Documents.Indexes.Vector.VectorEmbeddingType.Binary => embedding.Length  * sizeof(float),
            _ => throw new ArgumentException($"Unknown quantization type '{targetQuantization}'")
        };
        
        var memScope = allocator.Allocate(bytesRequired, out Memory<byte> mem);
        var rawEmbeddings = MemoryMarshal.Cast<float, byte>(embedding.Span);
        rawEmbeddings.CopyTo(mem.Span);
        return GenerateEmbeddings.Quantize(allocator, targetQuantization, memScope, mem, rawEmbeddings.Length);

        // TODO arek Cacher.EnqueueEmbeddingToCache(connectionStringId, );


    }
}
