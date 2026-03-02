using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Configuration describing how to extract and chunk content from a document path for embeddings generation.
/// </summary>
public class EmbeddingPathConfiguration : IDynamicJson
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

    internal static bool AreEqual(EmbeddingPathConfiguration left, EmbeddingPathConfiguration right)
    {
        if (left == null && right == null)
            return true;
        
        if (left == null || right == null)
            return false;

        return left.Equals(right);
    }

    private bool Equals(EmbeddingPathConfiguration other)
    {
        return Path == other.Path && 
               ChunkingOptions.AreEqual(ChunkingOptions, other.ChunkingOptions);
    }

    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((EmbeddingPathConfiguration)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Path, ChunkingOptions);
    }
}
