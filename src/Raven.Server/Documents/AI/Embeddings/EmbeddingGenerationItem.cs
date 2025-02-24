using System;
using System.Diagnostics;

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

    public void SetPrefixForDestinationAttachmentName(string prefix)
    {
        Debug.Assert(InputValue is not null, "ValueEmbeddingsSourceAttachmentName is not null");
        DestinationAttachmentName = $"{prefix}{InputValueHash}";
    }

    public string DestinationAttachmentName { get; private set; }

    public int UsedBytes { get; set; }

}
