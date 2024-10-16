namespace Raven.Client.Documents.Indexes.Vector;

public sealed class VectorOptionsFactory(VectorOptions options)
{
    internal VectorOptions VectorOptions = options;

    public VectorOptionsFactory Default => new VectorOptionsFactory(VectorOptions.Default);
    
    public VectorOptionsFactory() : this(new VectorOptions())
    {
    }

    public VectorOptionsFactory SourceEmbedding(EmbeddingType sourceType)
    {
        var newOptions = new VectorOptions(VectorOptions)
        {
            SourceEmbeddingType = sourceType,
            DestinationEmbeddingType = sourceType switch
            {
                EmbeddingType.Int8 => EmbeddingType.Int8,
                EmbeddingType.Binary => EmbeddingType.Binary,
                _ => default(EmbeddingType) // default.
            }
        };
        
        return new VectorOptionsFactory(newOptions);
    }
    
    public VectorOptionsFactory DestinationEmbedding(EmbeddingType destinationType)
    {
        var newOptions = new VectorOptions(VectorOptions)
        {
            DestinationEmbeddingType = destinationType
        };
        
        return new VectorOptionsFactory(newOptions);
    }

    public VectorOptionsFactory Dimensions(int? dimensions)
    {
        var newOptions = new VectorOptions(VectorOptions)
        {
            Dimensions = dimensions
        };
        
        return new VectorOptionsFactory(newOptions);
    }
    
    public VectorOptionsFactory IndexingStrategy(VectorIndexingStrategy indexingStrategy)
    {
        var newOptions = new VectorOptions(VectorOptions)
        {
            IndexingStrategy = indexingStrategy
        };
        
        return new VectorOptionsFactory(newOptions);
    }
}


