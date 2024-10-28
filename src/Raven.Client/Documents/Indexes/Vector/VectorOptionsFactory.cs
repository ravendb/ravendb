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
        _vectorOptions.DestinationEmbeddingType = destinationType;
        return this;
    }

    public VectorOptionsFactory Dimensions(int? dimensions)
    {
        _vectorOptions.Dimensions = dimensions;
        return this;
    }
    
    public VectorOptionsFactory IndexingStrategy(VectorIndexingStrategy indexingStrategy)
    {
        _vectorOptions.IndexingStrategy = indexingStrategy;
        return this;
    }
}


