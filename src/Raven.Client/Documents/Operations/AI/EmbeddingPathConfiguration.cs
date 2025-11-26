using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class EmbeddingPathConfiguration : IDynamicJson
{
    public string Path { get; set; }
    
    public ChunkingOptions ChunkingOptions { get; set; }
    
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
