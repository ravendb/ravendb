using System;
using System.Diagnostics;
using Sparrow;

namespace Raven.Client.Documents.Indexes.Vector;

/// <summary>
/// Configure vector field in index
/// </summary>
public class VectorOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="VectorOptions"/> class.
    /// </summary>
    public VectorOptions()
    {
    }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="VectorOptions"/> class by copying values from another instance.
    /// </summary>
    /// <param name="options">The VectorOptions instance to copy from.</param>
    public VectorOptions(VectorOptions options)
    {
        Dimensions = options.Dimensions;
        SourceEmbeddingType = options.SourceEmbeddingType;
        DestinationEmbeddingType = options.DestinationEmbeddingType;
        NumberOfCandidatesForIndexing = options.NumberOfCandidatesForIndexing;
        NumberOfEdges = options.NumberOfEdges;
    }
    
    internal static readonly VectorOptions Default = new()
    {
        Dimensions = null, 
        SourceEmbeddingType = VectorEmbeddingType.Single, 
        DestinationEmbeddingType = VectorEmbeddingType.Single
    };
    
    internal static readonly VectorOptions DefaultText = new()
    {
        Dimensions = null, 
        SourceEmbeddingType = VectorEmbeddingType.Text, 
        DestinationEmbeddingType = VectorEmbeddingType.Single
    };
    
    /// <summary>
    /// Defines dimensions size of embedding. When null we're locking the space to size we got from first item indexed.
    /// </summary>
    public int? Dimensions { get; set; }
    
    /// <summary>
    /// Defines embedding source.
    /// </summary>
    public VectorEmbeddingType SourceEmbeddingType { get; set; }
    
    /// <summary>
    /// Defines quantization of embedding.
    /// </summary>
    public VectorEmbeddingType DestinationEmbeddingType { get; set; }

    /// <summary>
    /// Gets or sets the number of candidate vectors to consider during indexing for graph construction.
    /// </summary>
    public int? NumberOfCandidatesForIndexing { get; set; }
    
    /// <summary>
    /// Gets or sets the number of edges (connections) each node in the vector graph should have.
    /// </summary>
    public int? NumberOfEdges { get; set; }
    
    [Conditional("DEBUG")]
    internal void ValidateDebug() => Validate();
    
    internal virtual void Validate()
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(DestinationEmbeddingType is VectorEmbeddingType.Text, "Destination embedding type cannot be Text.");
        PortableExceptions.ThrowIf<InvalidOperationException>(Dimensions is <= 0, "Number of vector dimensions has to be positive.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is VectorEmbeddingType.Text && Dimensions is not null, "Dimensions are set internally by the embedder.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is VectorEmbeddingType.Int8 && DestinationEmbeddingType is not VectorEmbeddingType.Int8, "Quantization cannot be performed on already quantized vector.");
        PortableExceptions.ThrowIf<InvalidOperationException>(SourceEmbeddingType is VectorEmbeddingType.Binary && DestinationEmbeddingType is not VectorEmbeddingType.Binary, "Quantization cannot be performed on already quantized vector.");
        PortableExceptions.ThrowIf<InvalidOperationException>(NumberOfEdges <= 0, "Number of edges has to be positive.");
        PortableExceptions.ThrowIf<InvalidOperationException>(NumberOfCandidatesForIndexing <= 0, "Number of candidate nodes has to be positive.");
    }
    
    /// <summary>
    /// Compares two VectorOptions instances for equality.
    /// </summary>
    /// <param name="left">The first VectorOptions instance to compare.</param>
    /// <param name="right">The second VectorOptions instance to compare.</param>
    /// <returns>True if the instances are equal; otherwise, false.</returns>
    public static bool Equals(VectorOptions left, VectorOptions right)
    {
        if (left is null && right is null)
            return true;

        return left?.Equals(right) ?? false;
    }
    
    /// <summary>
    /// Determines whether the specified object is equal to the current VectorOptions instance.
    /// </summary>
    /// <param name="obj">The object to compare with the current instance.</param>
    /// <returns>True if the specified object is equal to the current instance; otherwise, false.</returns>
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
               && options.SourceEmbeddingType == SourceEmbeddingType
               && options.DestinationEmbeddingType == DestinationEmbeddingType
               && options.NumberOfEdges == NumberOfEdges
               && options.NumberOfCandidatesForIndexing == NumberOfCandidatesForIndexing;
    }

    /// <summary>
    /// Returns the hash code for this VectorOptions instance.
    /// </summary>
    /// <returns>A 32-bit signed integer hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = SourceEmbeddingType.GetHashCode();
            hashCode = (hashCode * 397) ^ DestinationEmbeddingType.GetHashCode();
            hashCode = (hashCode * 397) ^ (Dimensions ?? 0).GetHashCode();
            hashCode = (hashCode * 397) ^ (NumberOfEdges ?? 0).GetHashCode();
            hashCode = (hashCode * 397) ^ (NumberOfCandidatesForIndexing ?? 0).GetHashCode();
            
            return hashCode;
        }
    }
}
