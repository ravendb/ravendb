using System;
using System.Diagnostics;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingGenerationItem
{
    private string _inputValueHash;
    public string InputValue { get; set; }

    public string InputValueHash
    {
        get
        {
            return _inputValueHash ??= EmbeddingsHelper.CalculateInputValueHash(InputValue);
        }
    }

    public string EmbeddingCacheDocumentId { get; set; }

    public ReadOnlyMemory<float> OutputValue { get; set; }

    public void GenerateDestinationAttachmentName(string prefix, in VectorEmbeddingType quantization)
    {
        Debug.Assert(InputValue is not null, "ValueEmbeddingsSourceAttachmentName is not null");
        DestinationAttachmentName = EmbeddingsHelper.GenerateDestinationAttachmentName(prefix, InputValueHash, quantization);
    }

    public string DestinationAttachmentName { get; private set; }

    public int UsedBytes { get; set; }

}
