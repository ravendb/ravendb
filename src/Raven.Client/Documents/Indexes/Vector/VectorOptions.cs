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

    protected bool Equals(AutoVectorOptions other)
    {
        return base.Equals(other) && SourceFieldName == other.SourceFieldName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(base.GetHashCode(), SourceFieldName.GetHashCode());
    }
}

/// <summary>
/// Configure vector field in index
/// </summary>
public class VectorOptions
{
    public VectorOptions()
    {
    }
    
    public VectorOptions(VectorOptions options)
    {
        Dimensions = options.Dimensions;
        IndexingStrategy = options.IndexingStrategy;
        SourceEmbeddingType = options.SourceEmbeddingType;
        DestinationEmbeddingType = options.DestinationEmbeddingType;
    }
    
    public static readonly VectorOptions Default = new() {Dimensions = null, IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32, DestinationEmbeddingType = EmbeddingType.Float32};
    
    /// <summary>
    /// Defines dimensions size of embedding. When null we're locking the space to size we got from first item indexed.
    /// </summary>
    public short? Dimensions { get; set; }
    
    /// <summary>
    /// Defines indexing strategy for vector field inside index.
    /// </summary>
    public VectorIndexingStrategy IndexingStrategy { get; set; }
    
    /// <summary>
    /// Defines embedding generator.
    /// </summary>
    public EmbeddingType SourceEmbeddingType { get; set; }
    
    /// <summary>
    /// Defines in what point are embeddings. Default: single
    /// </summary>
    public EmbeddingType DestinationEmbeddingType { get; set; }
    
    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != GetType())
            return false;

        if (obj is not VectorOptions options)
            return false;
        
        return options.Dimensions == Dimensions 
               && options.IndexingStrategy == IndexingStrategy
               && options.SourceEmbeddingType == SourceEmbeddingType
               && options.DestinationEmbeddingType == DestinationEmbeddingType;
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = SourceEmbeddingType.GetHashCode();
            hashCode = (hashCode * 397) ^ DestinationEmbeddingType.GetHashCode();
            hashCode = (hashCode * 397) ^ IndexingStrategy.GetHashCode();
            hashCode = (hashCode * 397) ^ Dimensions.GetHashCode();
            
            return hashCode;
        }
    }
}
