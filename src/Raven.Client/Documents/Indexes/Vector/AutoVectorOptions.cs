using System;

namespace Raven.Client.Documents.Indexes.Vector;

public sealed class AutoVectorOptions : VectorOptions
{
    /// <summary>
    /// Data source of embeddings
    /// </summary>
    public string SourceFieldName { get; set; }
    
    public string EmbeddingsGenerationTaskIdentifier { get; set; }

    public AutoVectorOptions()
    {
        
    }
    public AutoVectorOptions(AutoVectorOptions options)
    {
        Dimensions = options.Dimensions;
        SourceEmbeddingType = options.SourceEmbeddingType;
        DestinationEmbeddingType = options.DestinationEmbeddingType;
        SourceFieldName = options.SourceFieldName;
        NumberOfCandidatesForIndexing = options.NumberOfCandidatesForIndexing;
        NumberOfEdges = options.NumberOfEdges;
        EmbeddingsGenerationTaskIdentifier = options.EmbeddingsGenerationTaskIdentifier;
    }

    public override bool Equals(object obj)
    {
        if (obj is not AutoVectorOptions otherOptions)
            return false;
        
        return Equals(otherOptions);
    }

    public bool Equals(AutoVectorOptions other)
    {
        return base.Equals(other) 
               && SourceFieldName == other.SourceFieldName
               && EmbeddingsGenerationTaskIdentifier == other.EmbeddingsGenerationTaskIdentifier;
    }
    
    // todo add validate

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = base.GetHashCode();
            hashCode = (hashCode * 397) ^ (SourceFieldName != null ? SourceFieldName.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (EmbeddingsGenerationTaskIdentifier != null ? EmbeddingsGenerationTaskIdentifier.GetHashCode() : 0);

            return hashCode;
        }
    }
}
