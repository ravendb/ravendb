using System;

namespace Raven.Client.Documents.Indexes.Vector;

public sealed class AutoVectorOptions : VectorOptions
{
    /// <summary>
    /// Data source of embeddings
    /// </summary>
    public string SourceFieldName { get; set; }

    public AutoVectorOptions()
    {
        
    }
    public AutoVectorOptions(AutoVectorOptions options)
    {
        Dimensions = options.Dimensions;
        SourceEmbeddingType = options.SourceEmbeddingType;
        DestinationEmbeddingType = options.DestinationEmbeddingType;
        SourceFieldName = options.SourceFieldName;
        IndexingStrategy = options.IndexingStrategy;
    }

    public override bool Equals(object obj)
    {
        if (obj is not AutoVectorOptions otherOptions)
            return false;
        
        return Equals(otherOptions);
    }

    public bool Equals(AutoVectorOptions other)
    {
        return base.Equals(other) && SourceFieldName == other.SourceFieldName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(base.GetHashCode(), SourceFieldName.GetHashCode());
    }
}
