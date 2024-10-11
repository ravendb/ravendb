namespace Raven.Client.Documents.Indexes.Vector;

/// <summary>
/// Defines indexing method for Vector field
/// </summary>
public enum VectorIndexingStrategy
{
    /// <summary>
    /// Exact stores vector inside index entry and during querying time compare queried vector to each vector.
    /// </summary>
    Exact = 1,
    
    /// <summary>
    /// Uses Hierarchical navigable small world to index
    /// </summary>
    HNSW = 2,
}
