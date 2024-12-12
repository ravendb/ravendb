using System;
using Sparrow;

namespace Raven.Client.Documents.Indexes.Vector;

public sealed class VectorOptionsFactory
{
    internal VectorOptions _vectorOptions;
    internal VectorOptionsFactory Default => new VectorOptionsFactory(VectorOptions.Default);

    private VectorOptionsFactory(VectorOptions vectorOptions)
    {
        _vectorOptions = vectorOptions;
    }
    
    internal VectorOptionsFactory() : this(new VectorOptions())
    {
    }

    public VectorOptionsFactory SourceEmbedding(VectorEmbeddingType sourceType)
    {
        _vectorOptions.SourceEmbeddingType = sourceType;
        _vectorOptions.DestinationEmbeddingType = sourceType switch
        {
            VectorEmbeddingType.Int8 => VectorEmbeddingType.Int8,
            VectorEmbeddingType.Binary => VectorEmbeddingType.Binary,
            _ => default(VectorEmbeddingType) // default.
        };
        
        return this;
    }
    
    public VectorOptionsFactory DestinationEmbedding(VectorEmbeddingType destinationType)
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(_vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Int8 or VectorEmbeddingType.Binary && _vectorOptions.SourceEmbeddingType != destinationType,
            $"Cannot change the quantization of the already quantizied vector.");

        _vectorOptions.DestinationEmbeddingType = destinationType;
        return this;
    }

    public VectorOptionsFactory Dimensions(int? dimensions)
    {
        _vectorOptions.Dimensions = dimensions;
        return this;
    }
    
    public VectorOptionsFactory NumberOfCandidates(int numberOfCandidates)
    {
        _vectorOptions.NumberOfCandidatesForIndexing = numberOfCandidates;
        return this;
    }
    
    public VectorOptionsFactory NumberOfEdges(int numberOfEdges)
    {
        _vectorOptions.NumberOfEdges = numberOfEdges;
        return this;
    }
}


