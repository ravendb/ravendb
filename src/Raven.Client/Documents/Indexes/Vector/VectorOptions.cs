using System;
using System.Diagnostics;
using System.IO;
using Sparrow;

namespace Raven.Client.Documents.Indexes.Vector;

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
    
    public static readonly VectorOptions Default = new()
    {
        Dimensions = null, 
        IndexingStrategy = VectorIndexingStrategy.Exact, 
        SourceEmbeddingType = EmbeddingType.Single, 
        DestinationEmbeddingType = EmbeddingType.Single
    };
    
    public static readonly VectorOptions DefaultText = new()
    {
        Dimensions = null, 
        IndexingStrategy = VectorIndexingStrategy.Exact, 
        SourceEmbeddingType = EmbeddingType.Text, 
        DestinationEmbeddingType = EmbeddingType.Single
    };
    
    /// <summary>
    /// Defines dimensions size of embedding. When null we're locking the space to size we got from first item indexed.
    /// </summary>
    public int? Dimensions { get; set; }
    
    /// <summary>
    /// Defines indexing strategy for vector field inside index.
    /// </summary>
    public VectorIndexingStrategy IndexingStrategy { get; set; }
    
    /// <summary>
    /// Defines embedding source.
    /// </summary>
    public EmbeddingType SourceEmbeddingType { get; set; }
    
    /// <summary>
    /// Defines quantization of embedding.
    /// </summary>
    public EmbeddingType DestinationEmbeddingType { get; set; }

    [Conditional("DEBUG")]
    internal void ValidateDebug() => Validate();
    
    internal void Validate()
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(DestinationEmbeddingType is EmbeddingType.Text, "Destination embedding type cannot be Text.");
        PortableExceptions.ThrowIf<InvalidOperationException>(Dimensions is <= 0, "Number of vector dimensions has to be positive.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is EmbeddingType.Text && Dimensions is not null, "Dimensions are set internally by the embedder.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is EmbeddingType.Int8 && DestinationEmbeddingType is not EmbeddingType.Int8, "Quantization cannot be performed on already quantized vector.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is EmbeddingType.Binary && DestinationEmbeddingType is not EmbeddingType.Binary, "Quantization cannot be performed on already quantized vector.");
        PortableExceptions.ThrowIf<InvalidOperationException>(IndexingStrategy is not (VectorIndexingStrategy.Exact or VectorIndexingStrategy.HNSW), $"Unknown indexing strategy. Expected {VectorIndexingStrategy.Exact} or {VectorIndexingStrategy.HNSW} but was {IndexingStrategy}.");
    }
    
    public static bool Equals(VectorOptions left, VectorOptions right)
    {
        if (left is null && right is null)
            return true;

        return left?.Equals(right) ?? false;
    }
    
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
