namespace Raven.Client.Documents.Indexes.Vector;

/// <summary>
/// Specifies the data type used for vector embeddings in RavenDB indexes.
/// </summary>
public enum VectorEmbeddingType
{
    /// <summary>
    /// Single-precision floating-point numbers (32-bit).
    /// </summary>
    Single,
    
    /// <summary>
    /// 8-bit signed integer quantized values.
    /// </summary>
    Int8,
    
    /// <summary>
    /// Binary representation (1-bit per dimension).
    /// </summary>
    Binary,
    
    /// <summary>
    /// Text data that will be converted to embeddings using a text embedding model.
    /// </summary>
    Text
}
    
