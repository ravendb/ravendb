using System;
using System.Diagnostics;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.ETL.Providers.AI;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingGenerationItem
{
    public IEmbeddingValue EmbeddingValue { get; private set; }

    public string TextualValue { get; }

    public AiConnectionStringIdentifier ConnectionStringIdentifier { get; private set; }

    public VectorEmbeddingType Quantization { get; private set; }

    public string CacheDocumentId { get; set; }

    public DateTime? ExpireAt { get; set; }

    private string _inputValueHash;

    public EmbeddingGenerationItem(string textualValue,
        IEmbeddingValue embeddingValue,
        VectorEmbeddingType quantization,
        AiConnectionStringIdentifier connectionStringIdentifier)
    {
        EmbeddingValue = embeddingValue;
        TextualValue = textualValue;
        ConnectionStringIdentifier = connectionStringIdentifier;
        Quantization = quantization;
    }

    public EmbeddingGenerationItem(string textualValue)
    {
        TextualValue = textualValue;
    }

    public void SetEmbedding(IEmbeddingValue embeddingValue,
        VectorEmbeddingType quantization,
        AiConnectionStringIdentifier connectionStringIdentifier)
    {
        EmbeddingValue = embeddingValue;
        ConnectionStringIdentifier = connectionStringIdentifier;
        Quantization = quantization;
    }

    public string ValueHash
    {
        get
        {
            return _inputValueHash ??= EmbeddingsHelper.CalculateInputValueHash(TextualValue);
        }
    }

    public void GenerateDestinationAttachmentName(string prefix, in VectorEmbeddingType quantization)
    {
        Debug.Assert(TextualValue is not null);
        DestinationAttachmentName = EmbeddingsHelper.GenerateDestinationAttachmentName(prefix, ValueHash, quantization);
    }

    public string DestinationAttachmentName { get; private set; }
}
