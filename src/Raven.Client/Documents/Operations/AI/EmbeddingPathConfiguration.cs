using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Configuration describing how to extract and chunk content from a document path for embeddings generation.
/// </summary>
public class EmbeddingPathConfiguration : IDynamicJsonValueConvertible
{
    /// <summary>
    /// The document path (field or expression) from which text will be extracted.
    /// </summary>
    public string Path { get; set; }
    
    /// <summary>
    /// Chunking behavior to apply when splitting text extracted from <see cref="Path"/>.
    /// </summary>
    public ChunkingOptions ChunkingOptions { get; set; }
    
    /// <summary>
    /// Serializes this path configuration to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        var jsv = new DynamicJsonValue();
        jsv[nameof(Path)] = Path;
        jsv[nameof(ChunkingOptions)] = ChunkingOptions?.ToJson();
        return jsv;
    }
}
